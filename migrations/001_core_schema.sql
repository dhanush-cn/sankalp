-- 001_core_schema.sql -- Phase 1: the durable engine.
--
-- Forward-only. Once this file is committed and applied it is never edited;
-- schema changes go in a new NNN_*.sql. See docs/spec.md, Phase 1.

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Exactly six statuses. FAILED_DIRTY means compensation itself failed and a
-- human must resolve it. Do not add a seventh.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'workflow_status') THEN
        CREATE TYPE workflow_status AS ENUM (
            'PENDING', 'RUNNING', 'SUCCESS',
            'COMPENSATING', 'COMPENSATED', 'FAILED_DIRTY'
        );
    END IF;
END
$$;

CREATE TABLE workflows (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_type     TEXT NOT NULL,
    idempotency_key   TEXT NOT NULL,
    status            workflow_status NOT NULL DEFAULT 'PENDING',

    input             JSONB NOT NULL,
    output            JSONB,
    error             TEXT,

    -- execution position
    current_step      TEXT,
    attempt           INT NOT NULL DEFAULT 0,
    max_attempts      INT NOT NULL DEFAULT 5,

    -- lease / ownership: this is what makes crash recovery safe
    owner_id          TEXT,
    lease_expires_at  TIMESTAMPTZ,
    fencing_token     BIGINT NOT NULL DEFAULT 0,

    -- scheduling; retry backoff writes into this
    run_after         TIMESTAMPTZ NOT NULL DEFAULT now(),

    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_idempotency UNIQUE (workflow_type, idempotency_key)
);

-- Partial, so it only covers rows that are actually claimable. The dequeue
-- query's ORDER BY run_after rides this index.
CREATE INDEX idx_workflows_claimable
    ON workflows (run_after, id)
    WHERE status IN ('PENDING', 'RUNNING', 'COMPENSATING');

-- The primary key is the checkpoint AND the idempotency guard in one: a step is
-- done if and only if a row exists here. Never add a boolean completed flag.
CREATE TABLE step_outputs (
    workflow_id   UUID NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    step_name     TEXT NOT NULL,
    seq           INT  NOT NULL,
    kind          TEXT NOT NULL DEFAULT 'FORWARD'
                       CHECK (kind IN ('FORWARD', 'COMPENSATION')),
    output        JSONB,
    started_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (workflow_id, step_name, kind)
);

-- workflows is a high-churn UPDATE table (every claim, every lease renewal) and
-- will bloat. Vacuum it far more eagerly than the 20% default.
ALTER TABLE workflows SET (
    autovacuum_vacuum_scale_factor  = 0.02,
    autovacuum_analyze_scale_factor = 0.02
);
