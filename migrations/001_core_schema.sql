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

-- ---------------------------------------------------------------------------
-- workflows -- one row per workflow instance: state, lease, and schedule.
--
-- This row is the queue entry AND the execution state. There is deliberately no
-- separate queue table and no recovery daemon: a worker claims a row by
-- stamping owner_id and lease_expires_at on it, and a dead worker's rows become
-- claimable again for exactly one reason -- lease_expires_at drifts into the
-- past and the dequeue query's `status = 'RUNNING' AND lease_expires_at < now()`
-- branch picks them up. Crash recovery IS that predicate. A reaper process
-- would only duplicate it, and add a second thing that can be wrong.
--
-- fencing_token is the column most likely to look unused and get "cleaned up".
-- Leave it. It increments on every claim, monotonically, per workflow row:
--
--   A worker stalls mid-step (GC pause, VM freeze, network partition) holding
--   token 7. Its lease expires. A second worker legitimately claims the row,
--   gets token 8, and completes the step. The first worker then wakes up, still
--   believing it owns the workflow, and issues its write. That write MUST lose.
--
-- No lock can settle this on its own -- both workers hold what each sees as a
-- valid claim. What settles it is that every ownership-scoped statement carries
-- `AND owner_id = $x AND fencing_token = $y`, so the zombie's UPDATE matches
-- zero rows and it learns it was preempted (docs/spec.md, "Lease renewal").
-- Phase 3 extends the same idea to resources.last_fencing_token, which accepts
-- a write only from a strictly higher token. This is what makes the distributed
-- lock safe; Redlock alone is not, and Kleppmann's critique of it is precisely
-- that it "lacks a facility for generating fencing tokens".
--
-- The token lives here, on the workflow row, rather than in Redis, because the
-- store that accepts the write has to be the same store that rejects the stale
-- writer. Check in Redis and write in Postgres and the race is simply re-created
-- one statement lower down. Never reset it: monotonicity is the whole mechanism.
-- ---------------------------------------------------------------------------
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

    -- lease / ownership: this is what makes crash recovery safe. fencing_token
    -- increments on every claim and is checked on every ownership-scoped write
    -- -- read the note above before changing or removing it.
    owner_id          TEXT,
    lease_expires_at  TIMESTAMPTZ,
    fencing_token     BIGINT NOT NULL DEFAULT 0,

    -- scheduling; retry backoff writes into this
    run_after         TIMESTAMPTZ NOT NULL DEFAULT now(),

    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_idempotency UNIQUE (workflow_type, idempotency_key)
);

-- Partial ON PURPOSE -- the WHERE clause is the point, not decoration.
--
-- Terminal rows (SUCCESS, COMPENSATED, FAILED_DIRTY) accumulate forever and are
-- never claimable. Indexing them would grow the dequeue index without bound
-- while the set of rows it actually serves stays roughly the size of the live
-- backlog -- every claim would walk a structure made mostly of history. With
-- the predicate, the index stays proportional to work in flight (on an idle
-- system, nearly empty) and rows drop out of it for free as they go terminal.
--
-- Two properties must hold or this silently stops being used:
--   * the predicate must remain a superset of the dequeue query's status
--     filter, or the planner cannot prove the index is applicable and falls
--     back to a sequential scan of the whole table;
--   * run_after must stay the leading column, matching that query's
--     ORDER BY run_after, so a claim is an ordered walk that stops at LIMIT
--     instead of sorting the entire backlog to find the oldest few rows.
--
-- id is in the key only so rows sharing a run_after have a deterministic claim
-- order; FOR UPDATE has to visit the heap regardless, so this was never about
-- index-only scans. This is an access path, not a constraint -- do not make it
-- UNIQUE and do not "simplify" it by dropping the WHERE.
CREATE INDEX idx_workflows_claimable
    ON workflows (run_after, id)
    WHERE status IN ('PENDING', 'RUNNING', 'COMPENSATING');

-- ---------------------------------------------------------------------------
-- step_outputs -- the checkpoint log. One row per completed step (or per undo).
--
-- PRIMARY KEY (workflow_id, step_name, kind) is the load-bearing choice in this
-- schema, and the obvious "improvement" -- a completed BOOLEAN, or a status
-- column on some steps table -- is exactly what it exists to rule out.
--
-- A step is done IF AND ONLY IF a row exists here. Checkpoint and idempotency
-- guard are therefore the same fact, established by a single INSERT, committed
-- in the same transaction as the workflow state change. Replay after a crash is
-- a lookup ("which rows exist?"), never a flag some code path has to remember
-- to set.
--
-- A boolean splits that one fact into two writes -- perform the effect, then
-- mark it done -- and the gap between them is precisely the crash window that
-- double-charges. No ordering closes it: mark first and a crash silently skips
-- real work; mark second and a crash repeats a side effect. The row-as-
-- checkpoint has no such gap, because there is only one write, and a second
-- attempt is a primary key violation rather than a second effect.
--
-- kind is in the key so compensations reuse this table instead of needing one
-- of their own: the forward run and the undo of the same step are two distinct
-- rows, each idempotent under the identical rule. seq records forward order so
-- the COMPENSATING pass can walk the completed steps in reverse.
--
-- To preserve: never UPDATE a row here, never DELETE one, never add a status /
-- completed / attempt column, and keep ON DELETE CASCADE -- a checkpoint has no
-- meaning once its workflow is gone.
-- ---------------------------------------------------------------------------
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
