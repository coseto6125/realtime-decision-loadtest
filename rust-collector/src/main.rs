//! lt-collector-rs: Rust control group for the Python lt-collector. Same wire contract, same work per event.

mod batch;
mod metrics;
mod store;

use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::serve::ListenerExt;
use axum::Router;

use batch::BatchError;
use metrics::Metrics;
use store::Store;

const SERVICE: &str = "lt-collector-rs";
const MAX_BODY: usize = 1_000_000; // Python: REQUEST_MAX_SIZE

struct App {
    metrics: Metrics,
    store: Store,
    max_batch: usize,
}

fn env_or<T: FromStr>(name: &str, default: T) -> T {
    match std::env::var(name) {
        Ok(v) => v.trim().parse().unwrap_or_else(|_| panic!("env {name}={v:?} is not valid")),
        Err(_) => default,
    }
}

fn main() {
    // The container has 0.5 CPU. One worker thread avoids two threads fighting over half a core
    // (context switches and cache churn that would inflate CPU per event); raise it via TOKIO_WORKERS.
    let workers: usize = env_or("TOKIO_WORKERS", 1);
    let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(workers.max(1)).enable_all().build().expect("tokio runtime");
    rt.block_on(run());
}

async fn run() {
    let timeout = Duration::from_millis(env_or("REDIS_TIMEOUT_MS", 1000));
    let host = std::env::var("REDIS_HOST").unwrap_or_else(|_| "lt-redis".to_owned());
    let store = Store::connect(&host, env_or("REDIS_PORT", 6379), env_or::<usize>("REDIS_POOL", 2).max(1), timeout, env_or("STREAM_MAXLEN", 500_000))
        .await
        .expect("redis pool config");
    let app = Arc::new(App { metrics: Metrics::new(SERVICE), store, max_batch: env_or("MAX_BATCH", 500) });

    let router = Router::new()
        .route("/events", post(events))
        .route("/health", get(|| async { json(StatusCode::OK, br#"{"ok":true}"#.to_vec()) }))
        .route("/stats", get(stats))
        .route("/metrics", get(prometheus))
        .route("/admin/reset", post(reset))
        .layer(DefaultBodyLimit::max(MAX_BODY))
        .with_state(app);

    let port: u16 = env_or("PORT", 8000);
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await.expect("bind").tap_io(|tcp| {
        let _ = tcp.set_nodelay(true);
    });
    eprintln!("{SERVICE} listening on 0.0.0.0:{port}");
    axum::serve(listener, router).with_graceful_shutdown(shutdown()).await.expect("serve");
}

/// PID 1 in a container ignores SIGTERM unless it handles it; docker stop would otherwise wait 10 s.
async fn shutdown() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
    tokio::select! {
        _ = term.recv() => {}
        _ = tokio::signal::ctrl_c() => {}
    }
}

fn json(status: StatusCode, body: Vec<u8>) -> Response {
    (status, [(header::CONTENT_TYPE, "application/json")], body).into_response()
}

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_millis() as u64)
}

fn op_name(status: StatusCode) -> &'static str {
    match status.as_u16() / 100 {
        2 => "http POST /events 2xx",
        4 => "http POST /events 4xx",
        _ => "http POST /events 5xx",
    }
}

/// Timed from handler entry to response built. The body is already read by the extractor by then.
async fn events(State(app): State<Arc<App>>, body: Bytes) -> Response {
    let t0 = Instant::now();
    let (status, payload) = handle_events(&app, &body).await;
    let resp = json(status, payload);
    app.metrics.observe(op_name(status), t0.elapsed().as_secs_f64() * 1000.0);
    resp
}

async fn handle_events(app: &App, body: &[u8]) -> (StatusCode, Vec<u8>) {
    let part = match batch::partition(body, app.max_batch) {
        Ok(p) => p,
        Err(BatchError::NotArray(detail)) => {
            app.metrics.inc(&[("batch_rejected", 1)]);
            let out = sonic_rs::to_vec(&ErrDetail { error: "body must be a JSON array of events", detail: Some(&detail) });
            return (StatusCode::BAD_REQUEST, out.expect("serialize"));
        }
        Err(BatchError::TooLarge) => {
            app.metrics.inc(&[("batch_too_large", 1)]);
            let msg = format!("batch exceeds {} events", app.max_batch);
            return (StatusCode::PAYLOAD_TOO_LARGE, sonic_rs::to_vec(&ErrDetail { error: &msg, detail: None }).expect("serialize"));
        }
    };
    let (accepted, quarantined) = (part.good.len(), part.bad.len());
    if accepted + quarantined > 0 {
        let st = now_ms(); // once per request
        if let Err(kind) = app.store.append(part, st).await {
            app.metrics.inc(&[("redis_unavailable", 1)]);
            let out = sonic_rs::to_vec(&ErrDetail { error: "event store unavailable, retry", detail: Some(&kind) });
            return (StatusCode::SERVICE_UNAVAILABLE, out.expect("serialize"));
        }
    }
    app.metrics.inc(&[("accepted", accepted as u64), ("quarantined", quarantined as u64), ("batches", 1)]);
    (StatusCode::OK, format!(r#"{{"accepted":{accepted},"quarantined":{quarantined}}}"#).into_bytes())
}

#[derive(serde::Serialize)]
struct ErrDetail<'a> {
    error: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    detail: Option<&'a str>,
}

async fn stats(State(app): State<Arc<App>>) -> Response {
    json(StatusCode::OK, app.metrics.stats_json())
}

async fn prometheus(State(app): State<Arc<App>>) -> Response {
    (StatusCode::OK, [(header::CONTENT_TYPE, "text/plain; version=0.0.4")], app.metrics.prometheus()).into_response()
}

async fn reset(State(app): State<Arc<App>>) -> Response {
    app.metrics.reset();
    json(StatusCode::OK, br#"{"reset":true}"#.to_vec())
}
