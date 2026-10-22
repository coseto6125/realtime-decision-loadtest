//! Latency histograms, counters, /stats JSON and /metrics Prometheus text. Mirror of Python `lt.common`.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::Mutex;
use std::time::Instant;

use serde::Serialize;

/// Log-bucketed latency histogram: 4% relative error, O(1) observe, fixed memory.
pub struct Hist {
    buckets: [u64; Self::N],
    count: u64,
    sum: f64,
    max: f64,
}

impl Hist {
    const LO: f64 = 0.01; // ms
    const N: usize = 420; // covers 0.01 ms .. ~140 s
    const R: f64 = 0.039_220_713_153_281_295; // ln(1.04)

    pub fn new() -> Self {
        Self { buckets: [0; Self::N], count: 0, sum: 0.0, max: 0.0 }
    }

    pub fn observe(&mut self, ms: f64) {
        let i = if ms > Self::LO { ((ms / Self::LO).ln() / Self::R) as usize + 1 } else { 0 };
        self.buckets[i.min(Self::N - 1)] += 1;
        self.count += 1;
        self.sum += ms;
        if ms > self.max {
            self.max = ms;
        }
    }

    fn upper(i: usize) -> f64 {
        Self::LO * (Self::R * i as f64).exp()
    }

    /// Upper bound of the bucket where the cumulative count reaches q*count, capped at the observed max.
    /// The overflow bucket returns max.
    pub fn quantile(&self, q: f64) -> f64 {
        if self.count == 0 {
            return 0.0;
        }
        let rank = q * self.count as f64;
        let mut acc = 0u64;
        for (i, &c) in self.buckets.iter().enumerate() {
            acc += c;
            if acc as f64 >= rank {
                return if i == Self::N - 1 { self.max } else { Self::upper(i).min(self.max) };
            }
        }
        self.max
    }

    /// Cumulative count at or below `bound` (bucket resolution).
    pub fn le(&self, bound: f64) -> u64 {
        let n = (((bound / Self::LO).ln() / Self::R) as usize + 1).min(Self::N - 1);
        self.buckets[..=n].iter().sum()
    }

    fn summary(&self) -> Summary {
        Summary {
            count: self.count,
            p50: round3(self.quantile(0.5)),
            p95: round3(self.quantile(0.95)),
            p99: round3(self.quantile(0.99)),
            max: round3(self.max),
            mean: if self.count == 0 { 0.0 } else { round3(self.sum / self.count as f64) },
        }
    }
}

fn round3(x: f64) -> f64 {
    (x * 1000.0).round() / 1000.0
}

#[derive(Serialize, Debug)]
struct Summary {
    count: u64,
    p50: f64,
    p95: f64,
    p99: f64,
    max: f64,
    mean: f64,
}

#[derive(Serialize)]
struct Stats {
    service: &'static str,
    since_s: f64,
    latency_ms: BTreeMap<&'static str, Summary>,
    counters: BTreeMap<&'static str, u64>,
    gauges: BTreeMap<&'static str, f64>,
    cpu_s: f64,
    maxrss_mb: f64,
    rss_mb: f64,
}

/// Counter names present from the start (value 0), so readers never miss a key.
const COUNTERS: [&str; 6] = ["accepted", "quarantined", "batches", "batch_rejected", "batch_too_large", "redis_unavailable"];
const PROM_LE: [u32; 13] = [1, 2, 5, 10, 25, 50, 100, 150, 250, 500, 1000, 2500, 5000];

struct Inner {
    hists: BTreeMap<&'static str, Hist>,
    counters: BTreeMap<&'static str, u64>,
    started: Instant,
}

impl Inner {
    fn fresh() -> Self {
        Self { hists: BTreeMap::new(), counters: COUNTERS.iter().map(|&k| (k, 0)).collect(), started: Instant::now() }
    }
}

/// One Mutex guards everything: a request takes it twice for a few hundred ns, contention is negligible.
pub struct Metrics {
    service: &'static str,
    inner: Mutex<Inner>,
}

impl Metrics {
    pub fn new(service: &'static str) -> Self {
        Self { service, inner: Mutex::new(Inner::fresh()) }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn observe(&self, op: &'static str, ms: f64) {
        self.lock().hists.entry(op).or_insert_with(Hist::new).observe(ms);
    }

    pub fn inc(&self, deltas: &[(&'static str, u64)]) {
        let mut g = self.lock();
        for &(k, n) in deltas {
            *g.counters.entry(k).or_insert(0) += n;
        }
    }

    pub fn reset(&self) {
        *self.lock() = Inner::fresh();
    }

    pub fn stats_json(&self) -> Vec<u8> {
        let (cpu, maxrss_kb) = rusage();
        let g = self.lock();
        let stats = Stats {
            service: self.service,
            since_s: (g.started.elapsed().as_secs_f64() * 10.0).round() / 10.0,
            latency_ms: g.hists.iter().map(|(&k, h)| (k, h.summary())).collect(),
            counters: g.counters.clone(),
            gauges: BTreeMap::new(),
            cpu_s: round3(cpu),
            maxrss_mb: (maxrss_kb as f64 / 1024.0 * 10.0).round() / 10.0,
            rss_mb: (rss_bytes() as f64 / 1_048_576.0 * 10.0).round() / 10.0,
        };
        drop(g);
        sonic_rs::to_vec(&stats).expect("stats serialize")
    }

    pub fn prometheus(&self) -> String {
        let (cpu, _) = rusage();
        let rss = rss_bytes();
        let g = self.lock();
        let svc = self.service;
        let mut out = String::with_capacity(4096);
        out.push_str("# TYPE lt_handle_ms histogram\n");
        for (name, h) in &g.hists {
            let lbl = format!(r#"service="{svc}",op="{name}""#);
            for b in PROM_LE {
                let _ = writeln!(out, r#"lt_handle_ms_bucket{{{lbl},le="{b}"}} {}"#, h.le(f64::from(b)));
            }
            let _ = writeln!(out, r#"lt_handle_ms_bucket{{{lbl},le="+Inf"}} {}"#, h.count);
            let _ = writeln!(out, "lt_handle_ms_sum{{{lbl}}} {:.3}", h.sum);
            let _ = writeln!(out, "lt_handle_ms_count{{{lbl}}} {}", h.count);
        }
        out.push_str("# TYPE lt_events_total counter\n");
        for (k, v) in &g.counters {
            let _ = writeln!(out, r#"lt_events_total{{service="{svc}",name="{k}"}} {v}"#);
        }
        drop(g);
        let _ = write!(
            out,
            "# TYPE process_cpu_seconds_total counter\nprocess_cpu_seconds_total {cpu:.3}\n\
             # TYPE process_resident_memory_bytes gauge\nprocess_resident_memory_bytes {rss}\n"
        );
        out
    }
}

/// (user + sys CPU seconds, peak RSS in KiB) from getrusage(RUSAGE_SELF).
fn rusage() -> (f64, i64) {
    // SAFETY: getrusage fills the zeroed struct we pass; RUSAGE_SELF is always valid.
    let ru = unsafe {
        let mut ru: libc::rusage = std::mem::zeroed();
        libc::getrusage(libc::RUSAGE_SELF, &mut ru);
        ru
    };
    let tv = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1e6;
    (tv(ru.ru_utime) + tv(ru.ru_stime), ru.ru_maxrss)
}

/// Resident set size from /proc/self/statm (second field, in pages).
fn rss_bytes() -> u64 {
    let pages = std::fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|s| s.split_whitespace().nth(1)?.parse::<u64>().ok())
        .unwrap_or(0);
    // SAFETY: sysconf with a valid name has no preconditions.
    let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) }.max(1) as u64;
    pages * page
}

#[cfg(test)]
mod tests {
    use super::*;
    use sonic_rs::{JsonContainerTrait, JsonValueTrait};

    #[test]
    fn hist_empty_quantile_is_zero() {
        assert_eq!(Hist::new().quantile(0.5), 0.0);
        assert_eq!(Hist::new().summary().mean, 0.0);
    }

    #[test]
    fn hist_r_constant_is_ln_1_04() {
        assert!((Hist::R - 1.04f64.ln()).abs() < 1e-15);
    }

    #[test]
    fn hist_quantiles_are_within_four_percent() {
        let mut h = Hist::new();
        for v in 1..=1000 {
            h.observe(f64::from(v));
        }
        for (q, exact) in [(0.5, 500.0), (0.95, 950.0), (0.99, 990.0)] {
            let got = h.quantile(q);
            assert!(got >= exact && got <= exact * 1.04 + 1e-9, "q={q} got={got}");
        }
        assert_eq!(h.quantile(1.0), 1000.0, "capped at observed max");
    }

    #[test]
    fn hist_single_value_quantile_is_capped_at_max() {
        let mut h = Hist::new();
        h.observe(7.3);
        assert_eq!(h.quantile(0.5), 7.3);
        assert_eq!(h.quantile(0.99), 7.3);
    }

    #[test]
    fn hist_below_lowest_bound_goes_to_bucket_zero() {
        let mut h = Hist::new();
        h.observe(0.001);
        assert_eq!(h.buckets[0], 1);
        assert_eq!(h.quantile(0.5), 0.001, "min(upper(0)=0.01, max=0.001)");
    }

    #[test]
    fn hist_overflow_bucket_returns_max() {
        let mut h = Hist::new();
        h.observe(1.0);
        h.observe(1.0e9);
        assert_eq!(h.buckets[Hist::N - 1], 1);
        assert_eq!(h.quantile(0.99), 1.0e9);
    }

    #[test]
    fn hist_le_is_cumulative_at_bucket_resolution() {
        let mut h = Hist::new();
        for v in [0.5, 3.0, 40.0, 4000.0, 9999.0] {
            h.observe(v);
        }
        assert_eq!(h.le(1.0), 1);
        assert_eq!(h.le(5.0), 2);
        assert_eq!(h.le(50.0), 3);
        assert_eq!(h.le(5000.0), 4);
        assert!(h.le(5000.0) <= h.count);
    }

    #[test]
    fn metrics_counters_start_at_zero_and_reset_clears() {
        let m = Metrics::new("lt-collector-rs");
        m.inc(&[("accepted", 5), ("batches", 1)]);
        m.observe("http POST /events 2xx", 1.0);
        let v: sonic_rs::Value = sonic_rs::from_slice(&m.stats_json()).unwrap();
        assert_eq!(v["counters"]["accepted"].as_u64(), Some(5));
        assert_eq!(v["counters"]["redis_unavailable"].as_u64(), Some(0));
        assert_eq!(v["latency_ms"]["http POST /events 2xx"]["count"].as_u64(), Some(1));
        assert_eq!(v["service"].as_str(), Some("lt-collector-rs"));
        assert!(v["gauges"].is_object() && v["cpu_s"].is_number() && v["rss_mb"].as_f64().unwrap() > 0.0);
        m.reset();
        let v: sonic_rs::Value = sonic_rs::from_slice(&m.stats_json()).unwrap();
        assert_eq!(v["counters"]["accepted"].as_u64(), Some(0));
        assert!(v["latency_ms"].as_object().unwrap().is_empty());
    }

    #[test]
    fn prometheus_text_is_valid_exposition_format() {
        let m = Metrics::new("lt-collector-rs");
        m.inc(&[("accepted", 2)]);
        for ms in [0.4, 3.0, 12.0, 7000.0] {
            m.observe("http POST /events 2xx", ms);
        }
        let text = m.prometheus();
        assert!(text.ends_with('\n'));
        let mut last_bucket: Option<u64> = None;
        let mut buckets = 0;
        for line in text.lines() {
            if line.starts_with('#') {
                assert!(line.starts_with("# TYPE ") && line.split(' ').count() == 4, "{line}");
                continue;
            }
            let (series, value) = line.rsplit_once(' ').expect(line);
            value.parse::<f64>().unwrap_or_else(|_| panic!("bad value in {line}"));
            let name_end = series.find('{').unwrap_or(series.len());
            assert!(series[..name_end].chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == ':'), "{line}");
            if series.starts_with("lt_handle_ms_bucket") {
                assert!(series.contains(r#"service="lt-collector-rs""#) && series.contains(r#"op="http POST /events 2xx""#) && series.contains("le=\""));
                let n: u64 = value.parse().unwrap();
                assert!(last_bucket.is_none_or(|p| p <= n), "buckets are cumulative: {line}");
                last_bucket = Some(n);
                buckets += 1;
            }
        }
        assert_eq!(buckets, 14, "13 finite bounds + +Inf");
        assert_eq!(last_bucket, Some(4), "+Inf bucket equals count");
        assert!(text.contains(r#"lt_handle_ms_bucket{service="lt-collector-rs",op="http POST /events 2xx",le="+Inf"} 4"#));
        assert!(text.contains(r#"lt_handle_ms_sum{service="lt-collector-rs",op="http POST /events 2xx"} 7015.400"#));
        assert!(text.contains(r#"lt_events_total{service="lt-collector-rs",name="accepted"} 2"#));
        assert!(text.contains("\nprocess_cpu_seconds_total "));
        assert!(text.contains("\nprocess_resident_memory_bytes "));
        for fam in ["lt_handle_ms histogram", "lt_events_total counter", "process_cpu_seconds_total counter", "process_resident_memory_bytes gauge"] {
            assert_eq!(text.matches(&format!("# TYPE {fam}\n")).count(), 1, "{fam}");
        }
    }
}
