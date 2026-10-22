\pset footer off
\echo '## pg_stat_statements top 15 by total time'
SELECT calls, round(total_exec_time::numeric, 1) AS total_ms, round(mean_exec_time::numeric, 3) AS mean_ms,
       round(stddev_exec_time::numeric, 3) AS stddev_ms, rows, shared_blks_hit, shared_blks_read,
       left(regexp_replace(query, '\s+', ' ', 'g'), 110) AS query
FROM pg_stat_statements WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
ORDER BY total_exec_time DESC LIMIT 15;
\echo '## pg_stat_wal'
SELECT wal_records, wal_fpi, wal_bytes, wal_buffers_full, stats_reset FROM pg_stat_wal;
\echo '## pg_stat_checkpointer'
SELECT num_timed, num_requested, write_time, sync_time, buffers_written, stats_reset FROM pg_stat_checkpointer;
\echo '## pg_stat_io (client backends and autovacuum, non-zero)'
SELECT backend_type, object, context, reads, writes, extends, fsyncs, hits, round(write_time::numeric, 1) AS write_ms
FROM pg_stat_io WHERE (coalesce(reads, 0) + coalesce(writes, 0) + coalesce(extends, 0) + coalesce(fsyncs, 0)) > 0
ORDER BY backend_type, object, context;
\echo '## pg_stat_user_tables'
SELECT relname, seq_scan, idx_scan, n_tup_ins, n_tup_upd, n_tup_hot_upd,
       CASE WHEN n_tup_upd > 0 THEN round(100.0 * n_tup_hot_upd / n_tup_upd, 1) END AS hot_pct,
       n_live_tup, n_dead_tup, autovacuum_count, autoanalyze_count,
       pg_size_pretty(pg_total_relation_size(relid)) AS total_size
FROM pg_stat_user_tables ORDER BY relname;
\echo '## pg_stat_user_indexes'
SELECT relname, indexrelname, idx_scan, idx_tup_read, pg_size_pretty(pg_relation_size(indexrelid)) AS size
FROM pg_stat_user_indexes ORDER BY relname, indexrelname;
