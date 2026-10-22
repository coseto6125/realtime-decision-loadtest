-- T6b context design "tuned_unlogged": tuned.sql with ctx_state and the recent_events partitions
-- UNLOGGED: no WAL, truncated after a crash, not replicated to standbys (nor to an RDS Multi-AZ
-- standby). PostgreSQL 18 rejects UNLOGGED on a partitioned parent, so the parent stays a plain
-- partitioned table and t6b_ensure_partitions() creates each partition UNLOGGED (meta unlogged=true).
CREATE UNLOGGED TABLE ctx_state (
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

INSERT INTO t6b_meta (k, v) VALUES ('design', 'tuned'), ('unlogged', 'true');
