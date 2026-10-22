-- T6b context design "naive": one row per user; context jsonb holds the counters and the last 50
-- events, and every event rewrites the whole row with an upsert (see SQL_NAIVE_APPLY in pg_bench.py).
CREATE TABLE ctx_naive (
    user_id text  PRIMARY KEY,
    context jsonb NOT NULL
);
INSERT INTO t6b_meta (k, v) VALUES ('design', 'naive'), ('unlogged', 'false');
