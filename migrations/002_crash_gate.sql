-- 002_crash_gate.sql -- the observable substrate for the Phase 1 crash gate.
--
-- Forward-only. Once this file is committed and applied it is never edited;
-- schema changes go in a new NNN_*.sql. See docs/spec.md, Phase 1 Gate.
--
-- Why these tables exist
-- ---------------------------------------------------------------------------
-- The Phase 1 gate SIGKILLs a worker mid-step and asserts that the workflow
-- resumes without any step's side effect running twice. A SIGKILLed process
-- takes its in-memory call counters with it, so the "your mock's call counter
-- still reads 1" half of the gate (docs/spec.md, Phase 1 Gate) cannot be a
-- closure in the test -- the counting has to survive the process that did the
-- work. These three tables are that counter, plus the two pieces of
-- coordination the test needs to kill at a chosen instant instead of guessing.
--
-- They are demo/test scaffolding for the gate, deliberately kept in the real
-- schema so the killed worker is the ordinary `python -m sankalp.engine.worker`
-- against the ordinary migrated database -- not a special build that proves
-- something about a test harness rather than about the engine.

-- ---------------------------------------------------------------------------
-- side_effects -- one row per side effect that ACTUALLY COMMITTED.
--
-- This is the measurement the whole gate turns on, and there are two ways to
-- ruin it, both of which look like improvements:
--
-- 1. Do NOT add a unique constraint on (workflow_id, step_name), and do NOT
--    let the demo steps write with ON CONFLICT DO NOTHING. Idempotency by
--    construction is Phase 2 (see the note in engine/executor.py above
--    _commit_finished_step). If a duplicate insert were silently swallowed
--    here, "exactly one row" would hold whether or not crash recovery worked,
--    and the gate would pass while asserting nothing. The count must be a
--    measurement of what the engine did, not a constraint the database
--    enforces on its behalf.
--
-- 2. Do NOT make the demo step commit this row before it does its slow work.
--    The gate's step 2 opens a transaction, INSERTs here, and only commits
--    after its work finishes. That is what makes a SIGKILL mid-step leave
--    zero rows behind -- the transaction never committed -- and it is why the
--    replay can be asserted to produce exactly one.
--
-- The honest scope, stated plainly: a kill in the microsecond between this row
-- committing and step_outputs being written WOULD produce two rows on replay.
-- That is at-least-once execution working as designed, and it is exactly why
-- CLAUDE.md says exactly-once *effects* via at-least-once execution plus
-- idempotency, never exactly-once delivery.
-- ---------------------------------------------------------------------------
CREATE TABLE side_effects (
    id           BIGSERIAL PRIMARY KEY,
    workflow_id  UUID NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    step_name    TEXT NOT NULL,
    executed_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_side_effects_workflow ON side_effects (workflow_id, step_name);

-- ---------------------------------------------------------------------------
-- step_attempts -- one row per time a step STARTED, whoever it was.
--
-- Written on its own connection and committed immediately, before the step
-- does anything slow -- the opposite of side_effects on purpose. Together the
-- two tables say something neither says alone:
--
--     step_attempts = 2, side_effects = 1   ->  attempted twice, took effect
--                                               once. This is the guarantee.
--
-- Without this table the gate could pass by killing a worker that had not yet
-- reached the step at all: the workflow would still finish and every count
-- would still read 1, having proven nothing about resuming mid-step. Requiring
-- exactly two attempts for the killed step is what makes the test unable to
-- cheat.
--
-- pid and owner_id are recorded by the process itself, so the test kills the
-- pid that is actually running the step rather than one it inferred, and can
-- afterwards prove the recovering attempt came from a DIFFERENT process.
-- ---------------------------------------------------------------------------
CREATE TABLE step_attempts (
    id            BIGSERIAL PRIMARY KEY,
    workflow_id   UUID NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    step_name     TEXT NOT NULL,
    owner_id      TEXT NOT NULL,
    pid           INT  NOT NULL,
    attempted_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_step_attempts_workflow ON step_attempts (workflow_id, step_name);

-- ---------------------------------------------------------------------------
-- crash_gates -- lets the TEST decide when a step is allowed to finish.
--
-- The alternative is a step that sleeps for N seconds while the test tries to
-- kill it somewhere in the middle. That makes every repetition cost N seconds
-- and makes the kill a timing guess; at --count=20 both add up. Here the step
-- announces itself in step_attempts and then blocks until a row appears here,
-- so the test kills the instant the step is confirmed running, and releases the
-- gate so the *recovering* attempt returns immediately.
--
-- A row's existence is the release -- there is no boolean to be half-set, and
-- the insert is a single committed statement.
-- ---------------------------------------------------------------------------
CREATE TABLE crash_gates (
    workflow_id  UUID NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    step_name    TEXT NOT NULL,
    released_at  TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (workflow_id, step_name)
);
