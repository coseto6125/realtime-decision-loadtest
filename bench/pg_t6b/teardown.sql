-- T6b teardown: drops every object the T6b setup files create in database lt_t6b, and nothing else.
DROP TABLE IF EXISTS ctx_naive, ctx_state, recent_events, events, stream_cursors, t6b_meta CASCADE;
DROP FUNCTION IF EXISTS t6b_ensure_partitions(text, int, boolean);
DROP FUNCTION IF EXISTS t6b_retention(int);
