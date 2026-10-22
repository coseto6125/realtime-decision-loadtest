SELECT 'bonus_granted_to_prior_payer' AS invariant, count(*) AS violations FROM entitlements e
 WHERE EXISTS (SELECT 1 FROM payments p WHERE p.user_id = e.user_id AND p.created_at < e.granted_at)
UNION ALL SELECT 'bonus_redeemed_on_non_first_payment', count(*) FROM entitlements e JOIN payments p ON p.id = e.payment_id
 WHERE EXISTS (SELECT 1 FROM payments q WHERE q.user_id = e.user_id AND q.id <> p.id AND q.created_at <= p.created_at)
UNION ALL SELECT 'holdout_action_now', count(*) FROM outbox o JOIN decisions d ON d.id = o.decision_id WHERE d.arm = 'holdout'
UNION ALL SELECT 'holdout_action_next_day', count(*) FROM outbox o JOIN next_day_jobs j ON j.id = o.job_id WHERE j.arm = 'holdout'
UNION ALL SELECT 'holdout_entitlement', count(*) FROM entitlements e JOIN next_day_jobs j ON j.id = e.job_id WHERE j.arm = 'holdout'
UNION ALL SELECT 'entitlements_per_user_campaign_gt1', count(*) FROM (SELECT 1 FROM entitlements GROUP BY user_id, campaign HAVING count(*) > 1) x
UNION ALL SELECT 'push_per_user_day_gt1', count(*) FROM (SELECT 1 FROM outbox WHERE action = 'push_bonus_offer' GROUP BY user_id, day HAVING count(*) > 1) x
UNION ALL SELECT 'redemption_without_verified_webhook', count(*) FROM entitlements e LEFT JOIN payments p ON p.id = e.payment_id
 WHERE e.redeemed_at IS NOT NULL AND (p.id IS NULL OR NOT p.verified OR p.provider = 'seed')
UNION ALL SELECT 'duplicate_decision_per_event_id', count(*) FROM (SELECT 1 FROM decisions GROUP BY event_id HAVING count(*) > 1) x
UNION ALL SELECT 'arm_changed_for_user', count(*) FROM (SELECT 1 FROM decisions GROUP BY user_id, trigger HAVING count(DISTINCT arm) > 1) x
UNION ALL SELECT 'forged_recharge_redeemed', count(*) FROM entitlements WHERE user_id LIKE 'f%' AND redeemed_at IS NOT NULL
UNION ALL SELECT 'forged_recharge_payment_row', count(*) FROM payments WHERE user_id LIKE 'f%'
UNION ALL SELECT 'bad_signature_payment_row', count(*) FROM payments WHERE txn_id LIKE 'bad-%'
UNION ALL SELECT 'job_processed_more_than_once', count(*) FROM next_day_jobs WHERE attempts > 1
UNION ALL SELECT 'push_rows_per_job_gt1', count(*) FROM (SELECT 1 FROM outbox WHERE job_id IS NOT NULL GROUP BY job_id HAVING count(*) > 1) x
UNION ALL SELECT 'payment_event_not_emitted_after_30s', count(*) FROM payments WHERE NOT emitted AND created_at < now() - interval '30 seconds';
