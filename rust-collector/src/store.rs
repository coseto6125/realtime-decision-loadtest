//! The only module that talks to Redis/Valkey: one pipelined round trip per request, no hidden retries.

use std::time::Duration;

use bytes::Bytes;
use fred::prelude::*;
use fred::types::config::ConnectionConfig;
use fred::types::streams::XID;

use crate::batch::Partition;

pub const STREAM: &str = "events";
pub const QUARANTINE: &str = "events:quarantine";
const QUARANTINE_MAXLEN: i64 = 100_000;

pub struct Store {
    pool: Pool,
    timeout: Duration,
    stream_maxlen: i64,
}

impl Store {
    pub async fn connect(host: &str, port: u16, pool_size: usize, timeout: Duration, stream_maxlen: i64) -> Result<Self, Error> {
        let config = Config { server: ServerConfig::new_centralized(host, port), ..Config::default() };
        let pool = Builder::from_config(config)
            .with_connection_config(|c: &mut ConnectionConfig| {
                c.connection_timeout = Duration::from_secs(1);
                c.max_command_attempts = 1; // no hidden retries: a stalled Redis surfaces as a fast 503
            })
            .with_performance_config(|p| p.default_command_timeout = timeout)
            .set_policy(ReconnectPolicy::new_constant(0, 200)) // background reconnect, endless, 200 ms apart
            .build_pool(pool_size)?;
        pool.connect_pool(); // handles are detached; the tasks run until the process exits
        // Wait briefly so the first requests do not race the handshake; a down Redis is not fatal at start.
        if let Err(e) = tokio::time::timeout(Duration::from_secs(3), pool.wait_for_connect()).await {
            eprintln!("redis not connected after 3s ({e}); requests return 503 until it is up");
        }
        Ok(Self { pool, timeout, stream_maxlen })
    }

    /// XADD every valid event to `events` first, then every rejected one to `events:quarantine`,
    /// all in ONE pipelined round trip. `st` is the same server time (epoch ms) for the whole request.
    /// Err carries the error kind name (the 503 `detail`).
    pub async fn append(&self, p: Partition, st: u64) -> Result<(), String> {
        let st = st.to_string();
        let pipe = self.pool.next_connected().pipeline();
        let queue_err = |e: Error| format!("{:?}", e.kind());
        for d in p.good {
            let fields = vec![("d", Value::Bytes(Bytes::from(d))), ("st", Value::from(st.clone())), ("src", Value::from_static_str("client"))];
            pipe.xadd::<(), _, _, _, _>(STREAM, false, ("MAXLEN", "~", self.stream_maxlen), XID::Auto, fields).await.map_err(queue_err)?;
        }
        for r in p.bad {
            let fields = vec![("d", Value::Bytes(Bytes::from(r.raw))), ("err", Value::from(r.err)), ("st", Value::from(st.clone()))];
            pipe.xadd::<(), _, _, _, _>(QUARANTINE, false, ("MAXLEN", "~", QUARANTINE_MAXLEN), XID::Auto, fields).await.map_err(queue_err)?;
        }
        match tokio::time::timeout(self.timeout, pipe.all::<Value>()).await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => Err(format!("{:?}", e.kind())),
            Err(_) => Err("Timeout".to_owned()),
        }
    }
}
