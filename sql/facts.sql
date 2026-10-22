SELECT 'decisions' AS what, trigger || '/' || arm || '/' || action AS k, count(*) AS n FROM decisions GROUP BY 1, 2
UNION ALL SELECT 'model_status', trigger || '/' || model_status, count(*) FROM decisions WHERE trigger IN ('abandonment', 'leave_room') GROUP BY 1, 2
UNION ALL SELECT 'abandonment_source', CASE WHEN event_id LIKE 'timeout:%' THEN 'state_timeout' ELSE 'cancel_recharge' END, count(*) FROM decisions WHERE trigger = 'abandonment' GROUP BY 1, 2
UNION ALL SELECT 'next_day_jobs', status || '/' || coalesce(result, '-') || '/' || coalesce(processed_by, '-'), count(*) FROM next_day_jobs GROUP BY 1, 2
UNION ALL SELECT 'entitlements', CASE WHEN redeemed_at IS NULL THEN 'granted_unredeemed' ELSE 'redeemed' END, count(*) FROM entitlements GROUP BY 1, 2
UNION ALL SELECT 'payments', provider, count(*) FROM payments GROUP BY 1, 2
UNION ALL SELECT 'outbox', action || '/' || status, count(*) FROM outbox GROUP BY 1, 2
ORDER BY 1, 2;
