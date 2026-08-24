-- 003_saga.sql -- Phase 2: the outbox and the ledger.
--
-- Forward-only. Once this file is committed and applied it is never edited;
-- schema changes go in a new NNN_*.sql. See docs/spec.md, Phase 2.
--
-- Two tables, each answering one problem Phase 1 left open:
--
--   outbox         -- an event and the state change that caused it must become
--                     true together, or neither. Written in the SAME transaction
--                     as the step output; that single transaction is the entire
--                     answer to the dual-write problem.
--   ledger_entries -- where money actually is. Append-only, double-entry, and
--                     guarded against replay by a unique constraint rather than
--                     by anything the application has to remember to check.

-- ---------------------------------------------------------------------------
-- outbox -- events, durable and atomic with the state change that produced them.
--
-- The alternative everyone writes first is: commit the step, then publish to the
-- broker. That is a dual write, and it fails in both directions -- crash after
-- the commit and the event is lost forever; publish first and crash before the
-- commit and you have announced something that never happened. No ordering of
-- the two saves it, because the failure is that they are two.
--
-- Here the INSERT rides along in the step's transaction (docs/spec.md, "The
-- Outbox"), and a separate drain loop moves rows to Redis afterwards:
--
--     SELECT ... WHERE published_at IS NULL ORDER BY id
--     FOR UPDATE SKIP LOCKED LIMIT 100;   -- publish (XADD), then:
--     UPDATE outbox SET published_at = now() WHERE id = ANY($1);
--
-- That drain is AT-LEAST-ONCE and should be described that way out loud: a crash
-- between the XADD and the UPDATE republishes the row. Consumers dedupe on
-- outbox.id. What the system provides is exactly-once *effects* -- never write
-- exactly-once delivery, here or anywhere (CLAUDE.md).
--
-- published_at is the entire state machine: NULL means owed, non-NULL means
-- done. Resist adding a status column or a `published BOOLEAN` -- the timestamp
-- already carries strictly more information, and a boolean beside it would be a
-- second copy of the same fact, free to disagree.
--
-- attempts exists so a row that keeps failing to publish is visible (and can be
-- backed off or parked) rather than silently spinning at the head of the queue.
-- ---------------------------------------------------------------------------
CREATE TABLE outbox (
    id             BIGSERIAL PRIMARY KEY,
    workflow_id    UUID NOT NULL REFERENCES workflows(id),
    event_type     TEXT NOT NULL,
    payload        JSONB NOT NULL,

    -- W3C traceparent, captured at write time. This looks unused until Phase 4
    -- and is exactly the sort of column that gets "cleaned up" before then.
    -- Leave it: automatic context propagation works within a request but does
    -- NOT cross the outbox, because the handoff is durable and asynchronous --
    -- the publisher runs in a different process, minutes later. The context has
    -- to travel as data or the trace simply ends at the commit
    -- (docs/spec.md, "Trace context across the outbox").
    trace_context  JSONB,

    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at   TIMESTAMPTZ,
    attempts       INT NOT NULL DEFAULT 0
);

-- Partial ON PURPOSE, for the same reason as idx_workflows_claimable
-- (001_core_schema.sql). Published rows accumulate forever and the drain never
-- looks at one again. Without the predicate this index grows without bound while
-- the set of rows it actually serves stays the size of the undrained backlog --
-- every claim would walk a structure made almost entirely of history. With it,
-- the index stays proportional to work in flight, shrinks to near-nothing once
-- the drain keeps up, and rows leave it for free when published_at is stamped.
--
-- The predicate must stay a superset of the drain query's WHERE clause or the
-- planner cannot prove the index applicable and silently falls back to a
-- sequential scan. id leads the key to match that query's ORDER BY id, so a
-- claim is an ordered walk that stops at LIMIT instead of sorting the backlog.
CREATE INDEX idx_outbox_unpublished
    ON outbox (id) WHERE published_at IS NULL;

-- ---------------------------------------------------------------------------
-- ledger_entries -- the append-only, double-entry record of where money is.
--
-- Two properties carry this table, and both are enforced by the database rather
-- than by convention, because "everyone knows not to do that" is not a control.
--
-- 1. THE IDEMPOTENCY GUARD -- uq_ledger_entry below.
--
--    A step that posts money can run more than once: that is the premise of the
--    whole engine (at-least-once execution). It may crash after the INSERT but
--    before its step_outputs checkpoint commits, and the replay will post again.
--    UNIQUE (workflow_id, step_name, account_id, direction) is what makes the
--    second posting a constraint violation instead of a second debit. The step
--    writes ON CONFLICT DO NOTHING and the arithmetic comes out right whether it
--    ran once or five times.
--
--    This is the same idea as step_outputs' primary key, one level down: the row
--    IS the guard. Do not add a `posted BOOLEAN` and do not move this check into
--    Python -- a SELECT-then-INSERT is two statements with a crash window and a
--    race between them, which is precisely what this constraint has no version of.
--
-- 2. APPEND-ONLY -- see the trigger below.
--
--    No UPDATE, no DELETE, ever. A correction is a NEW pair of entries in the
--    opposite direction, never a mutation of what was already recorded. This is
--    how real ledgers work, and the reason is not bookkeeping etiquette: history
--    that can be rewritten cannot be reconciled, because the reconciliation
--    query and the auditor are no longer looking at the same past.
--
-- Sign lives in `direction`, never in `amount_minor` -- hence the > 0 check. A
-- signed amount plus a direction column would be two sources of truth for one
-- fact, and they would eventually disagree. amount_minor is integer MINOR UNITS
-- (paise, not rupees): BIGINT here, int in Python, never float, never Decimal
-- (CLAUDE.md). Format for humans at the API boundary and nowhere else.
--
-- The FK deliberately has NO ON DELETE CASCADE, unlike every other table in this
-- schema. That asymmetry is the point: a workflow with money posted against it
-- cannot be deleted, and an attempt to do so fails loudly. Cascading here would
-- mean a stray DELETE on workflows silently erases the record of a transfer that
-- really happened.
-- ---------------------------------------------------------------------------
CREATE TABLE ledger_entries (
    id             BIGSERIAL PRIMARY KEY,
    transfer_id    UUID NOT NULL,              -- groups the debit/credit pair
    workflow_id    UUID NOT NULL REFERENCES workflows(id),
    step_name      TEXT NOT NULL,
    account_id     TEXT NOT NULL,
    direction      TEXT NOT NULL CHECK (direction IN ('DEBIT','CREDIT')),
    amount_minor   BIGINT NOT NULL CHECK (amount_minor > 0),
    currency       CHAR(3) NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- The idempotency guard: a given step can post a given entry exactly once.
    CONSTRAINT uq_ledger_entry
        UNIQUE (workflow_id, step_name, account_id, direction)
);

-- Balance is a SUM over an account, so the read path is (account_id, created_at):
-- "everything for this account", optionally as of a point in time. A snapshot
-- table can materialise it later if the scan ever gets long; it is not needed to
-- be correct, only to be fast, and it must never become the source of truth.
CREATE INDEX idx_ledger_account ON ledger_entries (account_id, created_at);

-- ---------------------------------------------------------------------------
-- Append-only, enforced by the database.
--
-- The function is one unconditional RAISE and nothing else. There is
-- deliberately NO bypass: no pg_trigger_depth() check, no session GUC escape
-- hatch, no current_user exemption, no WHEN clause on the trigger. A guard with
-- a condition in it is a guard with a hole in it, and the hole is where the bug
-- eventually lives -- usually added by someone with a genuinely urgent fix.
-- The way to correct a posting is a new pair of entries, which is why that is
-- what the HINT says.
--
-- FOR EACH STATEMENT, not FOR EACH ROW: this bans the OPERATION, not the row. A
-- row-level trigger only fires when something matched, so `DELETE FROM
-- ledger_entries WHERE false` would succeed and the ban would be conditional on
-- the WHERE clause finding something. At statement level there is no shape of
-- UPDATE or DELETE that succeeds against this table. It also raises once instead
-- of once per row.
--
-- ERRCODE is restrict_violation (23001) so callers can match a typed error --
-- asyncpg.RestrictViolationError -- rather than grepping the message text.
--
-- ---------------------------------------------------------------------------
-- DELIBERATELY NOT COVERED: TRUNCATE.
--
-- The append-only guard covers the row-mutation corruption paths -- UPDATE and
-- DELETE. TRUNCATE is a different kind of thing: a privilege-gated bulk
-- operation, and a separate trigger event that this trigger does not fire on. It
-- is left outside the trigger because tests/conftest.py truncates this table
-- between tests, and that truncation is where the whole suite's isolation comes
-- from.
--
-- State the resulting gap plainly rather than implying a protection that does
-- not exist: RIGHT NOW THIS TABLE IS TRUNCATE-ABLE BY ANYONE WHO CAN CONNECT.
-- There is no restricted application role. `sankalp` is POSTGRES_USER (see
-- docker-compose.yml) -- a superuser, and the owner of both databases -- and the
-- API, the workers, the migration runner and pytest all connect as it. A
-- superuser bypasses privilege checks outright, and a table's owner may TRUNCATE
-- it no matter what has been revoked. So there is nothing here to point at and
-- call a control.
--
-- TODO (Phase 3, "Restricted application role" in docs/spec.md): create a
-- dedicated `sankalp_app` role that is neither a superuser nor the owner of
-- these tables, connect the API and the workers as it, and
--
--     REVOKE TRUNCATE, DELETE, UPDATE ON ledger_entries FROM sankalp_app;
--
-- leaving it SELECT, INSERT only. Both non-superuser and non-owner are
-- load-bearing: a REVOKE against the owning superuser role is theatre. Until
-- that role exists, this trigger is the only enforcement on this table, and it
-- stops UPDATE and DELETE -- nothing else.
-- ---------------------------------------------------------------------------
CREATE FUNCTION ledger_entries_append_only() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'ledger_entries is append-only: % is not permitted', TG_OP
        USING ERRCODE = 'restrict_violation',
              HINT = 'Reverse a posting with a new pair of entries in the '
                     'opposite direction. Never mutate a committed entry.';
END;
$$;

CREATE TRIGGER trg_ledger_entries_append_only
    BEFORE UPDATE OR DELETE ON ledger_entries
    FOR EACH STATEMENT EXECUTE FUNCTION ledger_entries_append_only();

-- outbox is high-churn: every row is INSERTed, UPDATEd once by the drain, and
-- eventually aged out. Vacuum it far more eagerly than the 20% default, as 001
-- does for workflows (docs/spec.md, "Operational Notes"). If the table ever
-- outgrows this, the next step is partitioning by day with a drop-old-partitions
-- job -- not a DELETE loop, which would generate exactly the bloat being fought.
--
-- ledger_entries deliberately gets no such tuning: it is append-only, so it does
-- not accumulate dead tuples and has nothing for autovacuum to reclaim. The
-- asymmetry is intentional, not an omission.
ALTER TABLE outbox SET (
    autovacuum_vacuum_scale_factor  = 0.02,
    autovacuum_analyze_scale_factor = 0.02
);
