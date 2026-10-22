SELECT 'wal_bytes' AS k, wal_bytes::text AS v FROM pg_stat_wal
UNION ALL SELECT 'wal_records', wal_records::text FROM pg_stat_wal
UNION ALL SELECT 'wal_fpi', wal_fpi::text FROM pg_stat_wal
UNION ALL SELECT 'checkpoints_timed', num_timed::text FROM pg_stat_checkpointer
UNION ALL SELECT 'checkpoints_requested', num_requested::text FROM pg_stat_checkpointer
UNION ALL SELECT 'checkpoint_buffers_written', buffers_written::text FROM pg_stat_checkpointer
UNION ALL SELECT 'xact_commit', xact_commit::text FROM pg_stat_database WHERE datname = current_database()
UNION ALL SELECT 'autovacuum_runs', sum(autovacuum_count)::text FROM pg_stat_user_tables
UNION ALL SELECT 'seq_scan_' || relname, seq_scan::text FROM pg_stat_user_tables
UNION ALL SELECT 'hot_pct_' || relname, CASE WHEN n_tup_upd > 0 THEN round(100.0 * n_tup_hot_upd / n_tup_upd, 1)::text END
  FROM pg_stat_user_tables WHERE n_tup_upd > 0;
