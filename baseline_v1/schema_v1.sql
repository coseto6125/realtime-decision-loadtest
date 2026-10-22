CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

CREATE TABLE policies (
    trigger     text        NOT NULL,
    version     int         NOT NULL,
    salt        text        NOT NULL,
    arms        jsonb       NOT NULL,
    threshold   float8      NOT NULL DEFAULT 0.4,
    options     text[]      NOT NULL DEFAULT '{}',
    question    text        NOT NULL DEFAULT '',
    active      bool        NOT NULL DEFAULT true,
    approved_by text        NOT NULL DEFAULT 'product',
    created_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (trigger, version)
);

CREATE TABLE decisions (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id       text        NOT NULL UNIQUE,
    user_id        text        NOT NULL,
    trigger        text        NOT NULL,
    arm            text        NOT NULL,
    action         text        NOT NULL,
    reason         text        NOT NULL,
    policy_version int         NOT NULL,
    model_status   text        NOT NULL,
    top_intent     text,
    top_p          float8,
    body           text        NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now()
) WITH (autovacuum_vacuum_scale_factor = 0.05, autovacuum_analyze_scale_factor = 0.05,
        autovacuum_vacuum_insert_scale_factor = 0.05);
CREATE INDEX decisions_created_brin ON decisions USING brin (created_at);

CREATE TABLE next_day_jobs (
    id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    decision_id  bigint      NOT NULL UNIQUE,
    user_id      text        NOT NULL,
    arm          text        NOT NULL,
    abandoned_at timestamptz NOT NULL,
    due_at       timestamptz NOT NULL,
    status       text        NOT NULL DEFAULT 'due',
    result       text,
    processed_by text,
    processed_at timestamptz,
    attempts     int         NOT NULL DEFAULT 0
) WITH (fillfactor = 80, autovacuum_vacuum_scale_factor = 0.05, autovacuum_analyze_scale_factor = 0.05);
CREATE INDEX next_day_jobs_due ON next_day_jobs (due_at) WHERE status = 'due';

CREATE TABLE outbox (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    decision_id bigint,
    job_id      bigint,
    user_id     text        NOT NULL,
    action      text        NOT NULL,
    day         int,
    status      text        NOT NULL DEFAULT 'pending',
    created_at  timestamptz NOT NULL DEFAULT now(),
    sent_at     timestamptz
) WITH (fillfactor = 80, autovacuum_vacuum_scale_factor = 0.05, autovacuum_analyze_scale_factor = 0.05);
CREATE INDEX outbox_pending ON outbox (id) WHERE status = 'pending';

CREATE TABLE payments (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    provider   text        NOT NULL,
    txn_id     text        NOT NULL,
    user_id    text        NOT NULL,
    amount     int         NOT NULL,
    currency   text        NOT NULL,
    paid_at    timestamptz NOT NULL,
    verified   bool        NOT NULL,
    emitted    bool        NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (provider, txn_id)
) WITH (fillfactor = 90);
CREATE INDEX payments_user ON payments (user_id, created_at);
CREATE INDEX payments_unemitted ON payments (id) WHERE NOT emitted;

CREATE TABLE entitlements (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id         text        NOT NULL,
    campaign        text        NOT NULL,
    bonus_pct       int         NOT NULL,
    job_id          bigint,
    granted_at      timestamptz NOT NULL DEFAULT now(),
    expires_at      timestamptz NOT NULL,
    redeemed_at     timestamptz,
    payment_id      bigint,
    redeemed_points int,
    UNIQUE (user_id, campaign)
) WITH (fillfactor = 80);

CREATE TABLE contact_log (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id    text        NOT NULL,
    day        int         NOT NULL,
    channel    text        NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (user_id, day)
);

INSERT INTO policies (trigger, version, salt, arms, options, question) VALUES
('abandonment', 1, 'exp-abandon-2026-10', '[["holdout",34],["rule",33],["model",33]]',
 '{price_sensitive,payment_friction,just_browsing,about_to_leave}', 'Most likely reason the user cancelled the recharge?'),
('leave_room', 1, 'leave-2026-10', '[["holdout",10],["model",90]]',
 '{switch_room,about_to_leave}', 'Is the user switching rooms or about to leave?'),
('watch_30min', 1, 'follow-2026-10', '[["holdout",10],["rule",90]]', '{}', ''),
('login', 1, 'home-2026-10', '[["holdout",10],["rule",90]]', '{}', '');

INSERT INTO payments (provider, txn_id, user_id, amount, currency, paid_at, verified, emitted, created_at)
SELECT 'seed', 'seed-' || g, 'u' || g, 300, 'TWD', now() - interval '30 days', true, true, now() - interval '30 days'
FROM generate_series(1, 10000) AS g;

ANALYZE;
