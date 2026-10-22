# Index audit

Generated 2026-10-03 04:58:37 CST against a populated stack right after a baseline-load run; pg_stat_statements and the table/index counters cover that run. Write statements ran inside a rolled-back transaction.

| statement | plan nodes (rows, buffers hit/read) | exec ms | pg_stat_statements calls / mean ms |
|---|---|---|---|
| `decision.SQL_GET` | Index Scan using decisions_event_id_key (rows 1.0, hit 4, read 0) | 0.065 | 63560 / 0.024 |
| `decision.SQL_WRITE` | Result (rows 1.0, hit 33, read 0)<br>ModifyTable on decisions (rows 1.0, hit 33, read 0)<br>Result (rows 1.0, hit 12, read 0)<br>ModifyTable on outbox (rows 1.0, hit 79, read 0)<br>CTE Scan (rows 1.0, hit 11, read 0)<br>ModifyTable on next_day_jobs (rows 1.0, hit 21, read 0)<br>CTE Scan (rows 1.0, hit 12, read 0)<br>CTE Scan (rows 1.0, hit 33, read 0) | 1.027 | not called in this run |
| `decision.SQL_POLICIES` | Unique (rows 4.0, hit 7, read 0)<br>Sort (rows 4.0, hit 7, read 0)<br>Seq Scan on policies (rows 4.0, hit 1, read 0) | 0.071 | 746 / 0.043 |
| `payments.SQL_LOCK` | Result (rows 1.0, hit 0, read 0) | 0.037 | 8751 / 0.008 |
| `payments.SQL_PAY` | Result (rows 1.0, hit 28, read 0)<br>ModifyTable on payments (rows 1.0, hit 26, read 0)<br>Result (rows 1.0, hit 11, read 0)<br>ModifyTable on entitlements (rows 0.0, hit 2, read 0)<br>Nested Loop (rows 0.0, hit 2, read 0)<br>Nested Loop (rows 0.0, hit 2, read 0)<br>CTE Scan (rows 1.0, hit 0, read 0)<br>Index Scan using entitlements_user_id_campaign_key (rows 0.0, hit 2, read 0)<br>Index Scan using payments_user (rows 0, hit 0, read 0)<br>CTE Scan (rows 1.0, hit 26, read 0)<br>CTE Scan (rows 0.0, hit 2, read 0) | 0.292 | not called in this run |
| `payments.SQL_EMITTED` | ModifyTable on payments (rows 0.0, hit 23, read 0)<br>Index Scan using payments_pkey (rows 5.0, hit 6, read 0) | 0.178 | not called in this run |
| `payments.SQL_UNEMITTED` | Limit (rows 0.0, hit 1, read 0)<br>Index Scan using payments_unemitted (rows 0.0, hit 1, read 0) | 0.022 | not called in this run |
| `scheduler.SQL_CLAIM` | Limit (rows 0.0, hit 1, read 0)<br>LockRows (rows 0.0, hit 1, read 0)<br>Index Scan using next_day_jobs_due (rows 0.0, hit 1, read 0) | 0.029 | 1831 / 0.039 |
| `scheduler.SQL_FACTS` | Result (rows 1.0, hit 10, read 0)<br>Aggregate (rows 1.0, hit 6, read 0)<br>Index Only Scan using payments_user (rows 2.0, hit 6, read 0)<br>Aggregate (rows 1.0, hit 2, read 0)<br>Index Only Scan using payments_user (rows 0.0, hit 2, read 0)<br>Index Only Scan using entitlements_user_id_campaign_key (rows 0.0, hit 2, read 0) | 0.095 | not called in this run |
| `scheduler.SQL_GRANT` | Result (rows 1.0, hit 58, read 0)<br>ModifyTable on contact_log (rows 1.0, hit 25, read 0)<br>Result (rows 1.0, hit 11, read 0)<br>ModifyTable on entitlements (rows 1.0, hit 33, read 0)<br>CTE Scan (rows 1.0, hit 0, read 0)<br>Result (rows 1.0, hit 11, read 0)<br>ModifyTable on outbox (rows 1.0, hit 12, read 0)<br>CTE Scan (rows 1.0, hit 1, read 0)<br>CTE Scan (rows 1.0, hit 25, read 0)<br>CTE Scan (rows 1.0, hit 33, read 0) | 0.670 | not called in this run |
| `scheduler.SQL_DONE` | ModifyTable on next_day_jobs (rows 0.0, hit 6, read 0)<br>Index Scan using next_day_jobs_pkey (rows 1.0, hit 3, read 0) | 0.169 | not called in this run |
| `scheduler.SQL_RELAY` | ModifyTable on outbox (rows 0.0, hit 3, read 0)<br>Limit (rows 0.0, hit 3, read 0)<br>LockRows (rows 0.0, hit 3, read 0)<br>Index Scan using outbox_pending (rows 0.0, hit 3, read 0)<br>Nested Loop (rows 0.0, hit 3, read 0)<br>CTE Scan (rows 0.0, hit 3, read 0)<br>Index Scan using outbox_pkey (rows 0, hit 0, read 0) | 0.054 | not called in this run |
| `scheduler.SQL_BACKLOG` | Result (rows 1.0, hit 2, read 0)<br>Aggregate (rows 1.0, hit 1, read 0)<br>Index Only Scan using next_day_jobs_due (rows 0.0, hit 1, read 0)<br>Aggregate (rows 1.0, hit 1, read 0)<br>Index Only Scan using outbox_pending (rows 0.0, hit 1, read 0) | 0.041 | 1831 / 0.046 |

## pg_stat_user_tables (since the run's stats reset)

| table | seq_scan | idx_scan | n_tup_upd | n_tup_hot_upd |
|---|---|---|---|---|
| contact_log | 0 | 2274 | 0 | 0 |
| decisions | 138 | 168470 | 0 | 0 |
| entitlements | 395 | 10767 | 436 | 0 |
| next_day_jobs | 126 | 13363 | 5267 | 1 |
| outbox | 17 | 45711 | 38462 | 0 |
| payments | 1 | 20635 | 5257 | 5 |
| policies | 747 | 0 | 0 | 0 |

## pg_stat_user_indexes

| table | index | idx_scan | constraint index |
|---|---|---|---|
| contact_log | contact_log_pkey | 0 | True |
| contact_log | contact_log_user_id_day_key | 2274 | True |
| decisions | decisions_created_brin | 0 | False |
| decisions | decisions_event_id_key | 127058 | True |
| decisions | decisions_pkey | 41412 | True |
| entitlements | entitlements_job | 2 | False |
| entitlements | entitlements_payment | 2 | False |
| entitlements | entitlements_pkey | 0 | True |
| entitlements | entitlements_user_id_campaign_key | 10763 | True |
| next_day_jobs | next_day_jobs_decision_id_key | 0 | True |
| next_day_jobs | next_day_jobs_due | 3598 | False |
| next_day_jobs | next_day_jobs_pkey | 9765 | True |
| outbox | outbox_decision | 3 | False |
| outbox | outbox_job | 4 | False |
| outbox | outbox_pending | 3656 | False |
| outbox | outbox_pkey | 42048 | True |
| payments | payments_pkey | 6565 | True |
| payments | payments_provider_txn_id_key | 5256 | True |
| payments | payments_unemitted | 1229 | False |
| payments | payments_user | 7585 | False |
| policies | policies_pkey | 0 | True |
