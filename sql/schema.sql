-- Columns are ordered by alignment (8-byte, 4-byte, bool, variable length) so rows carry no padding.
-- Tables updated in place use fillfactor 70 to leave room for HOT updates; hot tables get tighter
-- per-table autovacuum thresholds. Integrity lives in the schema: FKs, CHECKs and unique keys.
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

CREATE TABLE policies (
    created_at  timestamptz NOT NULL DEFAULT now(),
    threshold   float8      NOT NULL DEFAULT 0.4,
    version     int         NOT NULL,
    active      bool        NOT NULL DEFAULT true,
    trigger     text        NOT NULL CHECK (trigger IN ('abandonment', 'leave_room', 'watch_30min', 'login')),
    salt        text        NOT NULL,
    arms        jsonb       NOT NULL,
    options     text[]      NOT NULL DEFAULT '{}',
    question    text        NOT NULL DEFAULT '',
    approved_by text        NOT NULL DEFAULT 'product',
    PRIMARY KEY (trigger, version)
);

CREATE TABLE decisions (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    created_at     timestamptz NOT NULL DEFAULT now(),
    top_p          float8,
    policy_version int         NOT NULL,
    event_id       text        NOT NULL UNIQUE,
    user_id        text        NOT NULL,
    trigger        text        NOT NULL CHECK (trigger IN ('abandonment', 'leave_room', 'watch_30min', 'login')),
    arm            text        NOT NULL CHECK (arm IN ('holdout', 'rule', 'model')),
    action         text        NOT NULL CHECK (action IN ('none', 'small_plan_hint', 'alt_payment_hint',
                                                          'recommend_next_room', 'follow_prompt', 'home_rank')),
    reason         text        NOT NULL,
    model_status   text        NOT NULL CHECK (model_status IN ('ok', 'timeout', 'error', 'skipped', 'breaker_open')),
    top_intent     text,
    body           bytea       NOT NULL
) WITH (autovacuum_vacuum_scale_factor = 0.01, autovacuum_vacuum_insert_scale_factor = 0.05,
        autovacuum_analyze_scale_factor = 0.05);
CREATE INDEX decisions_created_brin ON decisions USING brin (created_at);

CREATE TABLE next_day_jobs (
    id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    decision_id  bigint      NOT NULL UNIQUE REFERENCES decisions (id),
    abandoned_at timestamptz NOT NULL,
    due_at       timestamptz NOT NULL,
    processed_at timestamptz,
    attempts     int         NOT NULL DEFAULT 0,
    user_id      text        NOT NULL,
    arm          text        NOT NULL CHECK (arm IN ('holdout', 'rule', 'model')),
    status       text        NOT NULL DEFAULT 'due' CHECK (status IN ('due', 'done')),
    result       text CHECK (result IN ('holdout_no_action', 'paid_after_abandonment', 'prior_payer_no_offer',
                                        'lifetime_once', 'lifetime_once_race', 'no_push_consent', 'daily_push_cap',
                                        'pushed_offer')),
    processed_by text
) WITH (fillfactor = 70, autovacuum_vacuum_scale_factor = 0.01, autovacuum_vacuum_insert_scale_factor = 0.05,
        autovacuum_analyze_scale_factor = 0.05);
CREATE INDEX next_day_jobs_due ON next_day_jobs (due_at) WHERE status = 'due';

CREATE TABLE payments (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    paid_at    timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    amount     int         NOT NULL CHECK (amount > 0),
    verified   bool        NOT NULL,
    emitted    bool        NOT NULL DEFAULT false,
    provider   text        NOT NULL,
    txn_id     text        NOT NULL,
    user_id    text        NOT NULL,
    currency   text        NOT NULL CHECK (currency IN ('TWD', 'USD')),
    UNIQUE (provider, txn_id)
) WITH (fillfactor = 70);
CREATE INDEX payments_user ON payments (user_id, created_at);
CREATE INDEX payments_unemitted ON payments (id) WHERE NOT emitted;

CREATE TABLE entitlements (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id          bigint      REFERENCES next_day_jobs (id),
    payment_id      bigint      REFERENCES payments (id),
    granted_at      timestamptz NOT NULL DEFAULT now(),
    expires_at      timestamptz NOT NULL,
    redeemed_at     timestamptz,
    bonus_pct       int         NOT NULL CHECK (bonus_pct BETWEEN 1 AND 100),
    redeemed_points int,
    user_id         text        NOT NULL,
    campaign        text        NOT NULL,
    UNIQUE (user_id, campaign),
    CHECK ((redeemed_at IS NULL) = (payment_id IS NULL))
) WITH (fillfactor = 70);
CREATE INDEX entitlements_job ON entitlements (job_id);
CREATE INDEX entitlements_payment ON entitlements (payment_id);

CREATE TABLE outbox (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    decision_id bigint REFERENCES decisions (id),
    job_id      bigint REFERENCES next_day_jobs (id),
    created_at  timestamptz NOT NULL DEFAULT now(),
    sent_at     timestamptz,
    day         int,
    user_id     text        NOT NULL,
    action      text        NOT NULL CHECK (action IN ('small_plan_hint', 'alt_payment_hint', 'recommend_next_room',
                                                       'follow_prompt', 'home_rank', 'push_bonus_offer')),
    status      text        NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'sent')),
    payload     bytea       NOT NULL,
    CHECK (num_nonnulls(decision_id, job_id) = 1)
) WITH (fillfactor = 70, autovacuum_vacuum_scale_factor = 0.01, autovacuum_vacuum_insert_scale_factor = 0.05,
        autovacuum_analyze_scale_factor = 0.05);
CREATE INDEX outbox_pending ON outbox (id) WHERE status = 'pending';
CREATE INDEX outbox_decision ON outbox (decision_id);
CREATE INDEX outbox_job ON outbox (job_id);

CREATE TABLE contact_log (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT now(),
    day        int         NOT NULL,
    user_id    text        NOT NULL,
    channel    text        NOT NULL CHECK (channel IN ('push')),
    UNIQUE (user_id, day)
);

INSERT INTO policies (version, trigger, salt, arms, options, question) VALUES
(1, 'abandonment', 'exp-abandon-2026-10', '[["holdout",34],["rule",33],["model",33]]',
 '{price_sensitive,payment_friction,just_browsing,about_to_leave}', 'Most likely reason the user cancelled the recharge?'),
(1, 'leave_room', 'leave-2026-10', '[["holdout",10],["model",90]]',
 '{switch_room,about_to_leave}', 'Is the user switching rooms or about to leave?'),
(1, 'watch_30min', 'follow-2026-10', '[["holdout",10],["rule",90]]', '{}', ''),
(1, 'login', 'home-2026-10', '[["holdout",10],["rule",90]]', '{}', '');

INSERT INTO payments (paid_at, created_at, amount, verified, emitted, provider, txn_id, user_id, currency)
SELECT now() - interval '30 days', now() - interval '30 days', 300, true, true, 'seed', 'seed-' || g, 'u' || g, 'TWD'
FROM generate_series(1, 10000) AS g;

ANALYZE;
