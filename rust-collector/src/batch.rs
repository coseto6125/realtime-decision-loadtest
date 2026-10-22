//! Batch partitioning: split a JSON array body into valid events (msgpack-encoded) and quarantined raw elements.
//!
//! Validation mirrors the Python `lt.logic.Event` msgspec Struct. Lengths count Unicode code points
//! (Python `len`), not UTF-8 bytes.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt;

use serde::de::{Deserializer, Visitor};
use serde::{Deserialize, Serialize};
use sonic_rs::LazyValue;

pub const EVENT_TYPES: [&str; 13] = [
    "login", "enter_room", "watch_progress", "chat", "send_gift", "following", "click_banner",
    "watch_ad", "leave_room", "click_recharge", "cancel_recharge", "recharge", "purchase",
];
const ID_MAX: usize = 96;

/// A props value: string, integer, float or bool. null, arrays and objects are rejected.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
pub enum PropVal {
    Str(String),
    Int(i64),
    Float(f64),
    Bool(bool),
}

impl<'de> Deserialize<'de> for PropVal {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl Visitor<'_> for V {
            type Value = PropVal;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a string, integer, float or bool")
            }
            fn visit_bool<E>(self, v: bool) -> Result<PropVal, E> {
                Ok(PropVal::Bool(v))
            }
            fn visit_i64<E>(self, v: i64) -> Result<PropVal, E> {
                Ok(PropVal::Int(v))
            }
            // i64 only, as Python's PropInt: a wider int would fit msgpack here but be rejected there.
            fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<PropVal, E> {
                i64::try_from(v).map(PropVal::Int).map_err(|_| E::custom("Expected `int` <= 9223372036854775807"))
            }
            // |v| < 2^63, as Python's PropFloat. An int literal past the i64/u64 range arrives here as
            // a float, so this bound also rejects it, as Python does.
            fn visit_f64<E: serde::de::Error>(self, v: f64) -> Result<PropVal, E> {
                if v.abs() < 9_223_372_036_854_775_808.0 {
                    Ok(PropVal::Float(v))
                } else {
                    Err(E::custom("Expected `float` < 9.223372036854776e+18"))
                }
            }
            fn visit_str<E>(self, v: &str) -> Result<PropVal, E> {
                Ok(PropVal::Str(v.to_owned()))
            }
            fn visit_string<E>(self, v: String) -> Result<PropVal, E> {
                Ok(PropVal::Str(v))
            }
        }
        d.deserialize_any(V)
    }
}

/// props map. BTreeMap sorts keys; Python keeps insertion order. Same map, different byte order for props.
pub type Props = BTreeMap<String, PropVal>;

#[derive(Deserialize)]
struct Event<'a> {
    #[serde(borrow)]
    event_id: Cow<'a, str>,
    #[serde(borrow)]
    user_id: Cow<'a, str>,
    #[serde(borrow)]
    session_id: Cow<'a, str>,
    #[serde(borrow, rename = "type")]
    kind: Cow<'a, str>,
    client_ts: i64,
    #[serde(default)]
    props: Props,
}

/// sonic-rs appends a multi-line source snippet to its messages; keep the message line only.
fn first_line(msg: &str) -> String {
    msg.lines().next().unwrap_or(msg).to_owned()
}

fn check_id(name: &str, s: &str) -> Result<(), String> {
    // A UTF-8 byte length <= 96 implies <= 96 code points, so count only when the string is longer.
    if s.is_empty() {
        return Err(format!("Expected `str` of length >= 1 - at `$.{name}`"));
    }
    if s.len() > ID_MAX && s.chars().count() > ID_MAX {
        return Err(format!("Expected `str` of length <= {ID_MAX} - at `$.{name}`"));
    }
    Ok(())
}

impl Event<'_> {
    fn validate(&self) -> Result<(), String> {
        check_id("event_id", &self.event_id)?;
        check_id("user_id", &self.user_id)?;
        check_id("session_id", &self.session_id)?;
        if !EVENT_TYPES.contains(&&*self.kind) {
            return Err(format!("Invalid enum value `{}` - at `$.type`", self.kind));
        }
        if self.client_ts < 0 {
            return Err("Expected `int` >= 0 - at `$.client_ts`".to_owned());
        }
        Ok(())
    }

    /// msgpack ARRAY of 6: [event_id, user_id, session_id, type, client_ts, props].
    /// A tuple serializes as an array under rmp-serde's default config; `props` is a map.
    fn to_msgpack(&self) -> Vec<u8> {
        let tuple = (&*self.event_id, &*self.user_id, &*self.session_id, &*self.kind, self.client_ts, &self.props);
        rmp_serde::to_vec(&tuple).expect("msgpack encode into a Vec cannot fail")
    }
}

/// An invalid element: its original JSON bytes and the validation error text.
#[derive(Debug, PartialEq)]
pub struct Rejected {
    pub raw: Vec<u8>,
    pub err: String,
}

#[derive(Debug, Default, PartialEq)]
pub struct Partition {
    /// msgpack bytes of each valid event, in input order.
    pub good: Vec<Vec<u8>>,
    pub bad: Vec<Rejected>,
}

#[derive(Debug, PartialEq)]
pub enum BatchError {
    /// The body is not a JSON array. Carries the parser message.
    NotArray(String),
    TooLarge,
}

/// Parse the body as an array of raw elements, then validate each one on its own.
///
/// The size check runs right after the array parse (before any element validation). Python validates
/// first and counts after; the response is identical, and an oversize batch costs less here.
pub fn partition(body: &[u8], max_batch: usize) -> Result<Partition, BatchError> {
    let items: Vec<LazyValue> = sonic_rs::from_slice(body).map_err(|e| BatchError::NotArray(e.to_string()))?;
    if items.len() > max_batch {
        return Err(BatchError::TooLarge);
    }
    let mut out = Partition { good: Vec::with_capacity(items.len()), bad: Vec::new() };
    for item in &items {
        let raw = item.as_raw_str();
        match sonic_rs::from_str::<Event>(raw).map_err(|e| first_line(&e.to_string())).and_then(|ev| ev.validate().map(|()| ev)) {
            Ok(ev) => out.good.push(ev.to_msgpack()),
            Err(err) => out.bad.push(Rejected { raw: raw.as_bytes().to_vec(), err }),
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    type Wire = (String, String, String, String, i64, Props);

    fn ev(extra: &str) -> String {
        format!(r#"{{"event_id":"e1","user_id":"u1","session_id":"s1","type":"chat","client_ts":1700000000000{extra}}}"#)
    }

    fn part(body: &str) -> Partition {
        partition(body.as_bytes(), 500).expect("array body")
    }

    #[test]
    fn partition_all_valid_accepts_all() {
        let p = part(&format!("[{},{}]", ev(""), ev(r#","props":{"a":1}"#)));
        assert_eq!((p.good.len(), p.bad.len()), (2, 0));
    }

    #[test]
    fn partition_malformed_among_valid_quarantines_only_bad() {
        let bad = r#"{"event_id":"","user_id":"u","session_id":"s","type":"chat","client_ts":1}"#;
        let p = part(&format!("[{}, {bad} ,{}]", ev(""), ev("")));
        assert_eq!((p.good.len(), p.bad.len()), (2, 1));
        assert_eq!(p.bad[0].raw, bad.as_bytes(), "original bytes, no surrounding whitespace");
        assert!(p.bad[0].err.contains("event_id"), "{}", p.bad[0].err);
    }

    #[test]
    fn partition_empty_array_is_empty() {
        assert_eq!(part("[]"), Partition::default());
        assert_eq!(part(" [ ] ").good.len(), 0);
    }

    #[test]
    fn partition_non_array_body_is_not_array_error() {
        for body in ["", "   ", "{}", r#"{"a":1}"#, "[", "[1,", "nope", "null", "1", r#""x""#, "[{}] x"] {
            assert!(matches!(partition(body.as_bytes(), 500), Err(BatchError::NotArray(m)) if !m.is_empty()), "body {body:?}");
        }
    }

    #[test]
    fn partition_scalar_elements_are_quarantined() {
        let p = part(r#"[1, "x", null, true, [], {}]"#);
        assert_eq!((p.good.len(), p.bad.len()), (0, 6));
        assert_eq!(p.bad[1].raw, br#""x""#);
    }

    #[test]
    fn partition_props_bad_values_are_quarantined() {
        for props in [r#"[1]"#, r#"{"a":[1]}"#, r#"{"a":{"b":1}}"#, r#"{"a":null}"#, "null", r#""s""#] {
            let p = part(&format!("[{}]", ev(&format!(r#","props":{props}"#))));
            assert_eq!((p.good.len(), p.bad.len()), (0, 1), "props {props}");
        }
    }

    #[test]
    fn partition_props_scalar_values_are_accepted() {
        let p = part(&format!("[{}]", ev(r#","props":{"s":"x","i":-3,"f":1.5,"b":false,"max":9223372036854775807,"min":-9223372036854775808}"#)));
        assert_eq!((p.good.len(), p.bad.len()), (1, 0));
    }

    #[test]
    fn partition_props_number_outside_i64_is_quarantined() {
        // Same bounds as Python's PropInt: an int that does not fit i64 is rejected, not widened.
        for v in ["9223372036854775808", "18446744073709551615", "18446744073709551616", "-9223372036854775809", "1e19", "-1e19"] {
            let p = part(&format!("[{}]", ev(&format!(r#","props":{{"a":{v}}}"#))));
            assert_eq!((p.good.len(), p.bad.len()), (0, 1), "props int {v}");
        }
    }

    #[test]
    fn partition_client_ts_float_bool_negative_string_are_quarantined() {
        for ts in ["1.5", "1.0", "true", "-1", "\"5\"", "null", "9223372036854775808", "1e3"] {
            let body = ev("").replace("1700000000000", ts);
            let p = part(&format!("[{body}]"));
            assert_eq!((p.good.len(), p.bad.len()), (0, 1), "client_ts {ts}");
        }
        let zero = part(&format!("[{}]", ev("").replace("1700000000000", "0")));
        assert_eq!(zero.good.len(), 1, "client_ts 0 is valid");
        let max = part(&format!("[{}]", ev("").replace("1700000000000", "9223372036854775807")));
        assert_eq!(max.good.len(), 1, "i64::MAX is valid");
    }

    #[test]
    fn partition_id_length_bounds() {
        let with_id = |id: &str| part(&format!("[{}]", ev("").replace(r#""e1""#, &format!("\"{id}\""))));
        assert_eq!(with_id("").bad.len(), 1, "empty id");
        assert_eq!(with_id(&"a".repeat(96)).good.len(), 1, "96 chars");
        assert_eq!(with_id(&"a".repeat(97)).bad.len(), 1, "97 chars");
    }

    #[test]
    fn partition_id_length_counts_code_points_not_bytes() {
        let with_id = |id: &str| part(&format!("[{}]", ev("").replace(r#""e1""#, &format!("\"{id}\""))));
        assert_eq!(with_id(&"\u{4e2d}".repeat(96)).good.len(), 1, "96 CJK chars = 288 bytes");
        assert_eq!(with_id(&"\u{4e2d}".repeat(97)).bad.len(), 1);
        assert_eq!(with_id(&"\u{1f600}".repeat(96)).good.len(), 1, "96 astral chars");
        assert_eq!(with_id(&"\\u00e9".repeat(96)).good.len(), 1, "escaped input counts decoded chars");
    }

    #[test]
    fn partition_unknown_type_is_quarantined_and_all_13_types_pass() {
        assert_eq!(part(&format!("[{}]", ev("").replace("chat", "dance"))).bad.len(), 1);
        for t in EVENT_TYPES {
            assert_eq!(part(&format!("[{}]", ev("").replace("chat", t))).good.len(), 1, "{t}");
        }
    }

    #[test]
    fn partition_unknown_extra_field_is_accepted() {
        assert_eq!(part(&format!("[{}]", ev(r#","extra":{"x":[1,2]},"n":null"#))).good.len(), 1);
    }

    #[test]
    fn partition_missing_field_is_quarantined() {
        let p = part(r#"[{"event_id":"e","user_id":"u","session_id":"s","type":"chat"}]"#);
        assert_eq!((p.good.len(), p.bad.len()), (0, 1));
        assert!(p.bad[0].err.contains("client_ts"), "{}", p.bad[0].err);
    }

    #[test]
    fn partition_more_than_max_batch_is_too_large() {
        let body = format!("[{}]", vec![ev(""); 3].join(","));
        assert_eq!(partition(body.as_bytes(), 2), Err(BatchError::TooLarge));
        assert_eq!(partition(body.as_bytes(), 3).map(|p| p.good.len()), Ok(3));
    }

    #[test]
    fn partition_oversize_with_malformed_json_is_not_array() {
        assert!(matches!(partition(b"[1,2,3,", 1), Err(BatchError::NotArray(_))));
    }

    #[test]
    fn msgpack_layout_is_array_of_six_with_props_map() {
        let p = part(&format!("[{}]", ev(r#","props":{"s":"x","i":7,"f":1.5,"b":true}"#)));
        let bytes = &p.good[0];
        assert_eq!(bytes[0], 0x96, "fixarray(6)");
        let (a, b, c, t, ts, props): Wire = rmp_serde::from_slice(bytes).unwrap();
        assert_eq!((a.as_str(), b.as_str(), c.as_str(), t.as_str(), ts), ("e1", "u1", "s1", "chat", 1_700_000_000_000));
        assert_eq!(props.len(), 4);
        assert_eq!(props["s"], PropVal::Str("x".into()));
        assert_eq!(props["i"], PropVal::Int(7));
        assert_eq!(props["f"], PropVal::Float(1.5));
        assert_eq!(props["b"], PropVal::Bool(true));
    }

    #[test]
    fn msgpack_empty_props_is_empty_map_and_float_is_float64() {
        let p = part(&format!("[{}]", ev("")));
        assert_eq!(*p.good[0].last().unwrap(), 0x80, "fixmap(0)");
        let q = part(&format!("[{}]", ev(r#","props":{"f":1.5}"#)));
        assert!(q.good[0].windows(9).any(|w| w[0] == 0xcb && w[1..] == 1.5f64.to_be_bytes()), "float64 marker 0xcb");
    }

    #[test]
    fn msgpack_unicode_and_escapes_roundtrip() {
        let body = ev("").replace("\"u1\"", r#""中\"q\n""#);
        let p = part(&format!("[{body}]"));
        let (_, user, ..): Wire = rmp_serde::from_slice(&p.good[0]).unwrap();
        assert_eq!(user, "\u{4e2d}\"q\n");
    }

    /// Writes (UPDATE_FIXTURE=1) or checks tests/fixtures/sample_events.hex, one hex line per event.
    /// tests/fixtures/check_py.py decodes the file with msgspec to prove the cross-language contract.
    #[test]
    fn msgpack_sample_fixture_matches_committed_hex() {
        let body = format!(
            "[{},{},{}]",
            r#"{"event_id":"ev-0001","user_id":"u-1","session_id":"s-1","type":"login","client_ts":1700000000000}"#,
            r#"{"event_id":"ev-0002","user_id":"u-中文","session_id":"s-1","type":"send_gift","client_ts":0,"props":{"gift":"rose","qty":3,"price":9.5,"vip":true}}"#,
            r#"{"event_id":"ev-0003","user_id":"u-2","session_id":"s-9","type":"recharge","client_ts":9223372036854775807,"props":{"amount":-1,"note":""}}"#,
        );
        let hex: String = part(&body).good.iter().map(|b| b.iter().map(|x| format!("{x:02x}")).collect::<String>() + "\n").collect();
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/sample_events.hex");
        if std::env::var_os("UPDATE_FIXTURE").is_some() {
            std::fs::write(path, &hex).unwrap();
        }
        assert_eq!(std::fs::read_to_string(path).unwrap(), hex);
    }
}
