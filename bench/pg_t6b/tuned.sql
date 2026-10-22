-- T6b context design "tuned": a small fixed-width hot row per user, updated in place. fillfactor 70
-- leaves room on each page and no index covers an updated column, so the updates stay HOT.
-- The event history is an append-only day-partitioned table read through (user_id, ts DESC) LIMIT 50.
CREATE TABLE ctx_state (
    updated_ms    bigint   NOT NULL DEFAULT 0,
    watch_minutes integer  NOT NULL DEFAULT 0,
    rooms_visited integer  NOT NULL DEFAULT 0,
    chats         integer  NOT NULL DEFAULT 0,
    gifts         integer  NOT NULL DEFAULT 0,
    payment_state smallint NOT NULL DEFAULT 0,
    user_id       text     PRIMARY KEY
) WITH (fillfactor = 70, autovacuum_vacuum_scale_factor = 0.05);

CREATE TABLE recent_events (
    user_id text   NOT NULL,
    ts      bigint NOT NULL,
    payload bytea  NOT NULL
) PARTITION BY RANGE (ts);
CREATE INDEX recent_events_user_ts ON recent_events (user_id, ts DESC);

INSERT INTO t6b_meta (k, v) VALUES ('design', 'tuned'), ('unlogged', 'false');
