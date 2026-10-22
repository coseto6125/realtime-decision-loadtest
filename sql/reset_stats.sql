SELECT pg_stat_statements_reset();
SELECT pg_stat_reset();
SELECT pg_stat_reset_shared('wal');
SELECT pg_stat_reset_shared('io');
SELECT pg_stat_reset_shared('checkpointer');
