-- T6b stream role, shared by every context design. Runs after teardown.sql, in database lt_t6b.

CREATE TABLE t6b_meta (
    k text PRIMARY KEY,
    v text NOT NULL
);

-- The event stream. txid is the inserting transaction's id; readers order by (txid, id) and read
-- only below pg_snapshot_xmin(pg_current_snapshot()), so a row that commits late with a smaller id
-- is never skipped (see bench/pg_bench.py). Day partitions on ts (epoch ms, UTC): retention drops a
-- partition once every group's cursor is past it, never a row-by-row DELETE.
CREATE TABLE events (
    id      bigint   GENERATED ALWAYS AS IDENTITY,
    txid    xid8     NOT NULL DEFAULT pg_current_xact_id(),
    shard   smallint NOT NULL,
    user_id text     NOT NULL,
    ts      bigint   NOT NULL,
    payload bytea    NOT NULL
) PARTITION BY RANGE (ts);
CREATE INDEX events_shard_txid_id ON events (shard, txid, id);

-- One cursor per (consumer group, shard). A consumer claims a row with FOR UPDATE SKIP LOCKED and
-- holds it by lease (lease_until = visibility timeout); an expired lease is claimable by anyone.
CREATE TABLE stream_cursors (
    group_name  text        NOT NULL,
    shard       smallint    NOT NULL,
    last_txid   xid8        NOT NULL DEFAULT '0',
    last_id     bigint      NOT NULL DEFAULT 0,
    last_ts     bigint      NOT NULL DEFAULT 0,
    consumed    bigint      NOT NULL DEFAULT 0,
    lease_owner text,
    lease_until timestamptz NOT NULL DEFAULT '-infinity',
    PRIMARY KEY (group_name, shard)
) WITH (fillfactor = 50);

-- Create the day partitions of parent for today and the next days-1 days (UTC), UNLOGGED on request.
CREATE FUNCTION t6b_ensure_partitions(parent text, days int, unlogged boolean) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    d  date;
    lo bigint;
BEGIN
    FOR i IN 0 .. days - 1 LOOP
        d := (now() AT TIME ZONE 'UTC')::date + i;
        lo := (extract(epoch FROM d::timestamp) * 1000)::bigint;
        EXECUTE format('CREATE %s TABLE IF NOT EXISTS %I PARTITION OF %I FOR VALUES FROM (%s) TO (%s)',
                       CASE WHEN unlogged THEN 'UNLOGGED' ELSE '' END,
                       parent || '_' || to_char(d, 'YYYYMMDD'), parent, lo, lo + 86400000);
    END LOOP;
END $$;

-- Retention: drop day partitions that ended more than keep_days ago. An events partition is dropped
-- only when every group's cursor has passed its upper bound, so no consumer loses unread events.
CREATE FUNCTION t6b_retention(keep_days int) RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
    r       record;
    n       int := 0;
    cutoff  bigint := (extract(epoch FROM (now() AT TIME ZONE 'UTC')::date::timestamp) * 1000)::bigint
                      - keep_days::bigint * 86400000;
    read_to bigint := (SELECT coalesce(min(last_ts), 0) FROM stream_cursors);
BEGIN
    FOR r IN
        SELECT c.oid::regclass AS part, p.relname AS parent,
               (regexp_match(pg_get_expr(c.relpartbound, c.oid), 'TO \(''?(\d+)''?\)'))[1]::bigint AS hi
        FROM pg_inherits i
        JOIN pg_class c ON c.oid = i.inhrelid
        JOIN pg_class p ON p.oid = i.inhparent
        WHERE p.relname IN ('events', 'recent_events')
    LOOP
        IF r.hi <= cutoff AND (r.parent <> 'events' OR r.hi <= read_to) THEN
            EXECUTE format('DROP TABLE %s', r.part);
            n := n + 1;
        END IF;
    END LOOP;
    RETURN n;
END $$;
