-- 004_restricted_role.sql -- Phase 3: the restricted application role.
--
-- Forward-only. Once this file is committed and applied it is never edited;
-- schema changes go in a new NNN_*.sql. See docs/spec.md, "Restricted Application
-- Role", and the trailing comment in 003_saga.sql this closes.
--
-- Until now every process -- API, workers, migration runner, pytest -- connected as
-- `sankalp`, POSTGRES_USER in docker-compose.yml: a superuser and the owner of both
-- databases. A superuser bypasses privilege checks outright, and an owner may TRUNCATE
-- a table no matter what has been revoked, so nothing short of a role that is BOTH
-- non-superuser AND non-owner can actually be restricted. `sankalp_app` is that role.
-- Both conditions are load-bearing: a REVOKE against a superuser or against the table's
-- owner is theatre, not a control.
--
-- Migrations and tests/conftest.py's truncate fixture keep connecting as `sankalp` --
-- DDL and the fixture's per-test TRUNCATE are exactly the things this role must not
-- have. Only the worker and drain processes' main pools (src/sankalp/engine/worker.py,
-- drain.py) move to sankalp_app. The crash-gate instrumentation tables (side_effects,
-- step_attempts, crash_gates) deliberately get no grants here -- their pool
-- (workflows/_instrumentation.py) stays on the owning role, untouched by this file.

-- CREATE ROLE is cluster-wide, but this migration runs once per database (sankalp and
-- sankalp_test are the same Postgres cluster) via storage/migrate.py -- so a bare
-- CREATE ROLE would succeed on the first run and fail with "role already exists" on
-- the second. Dev-only plaintext password, matching the sankalp/sankalp convention
-- already in docker-compose.yml.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'sankalp_app') THEN
        CREATE ROLE sankalp_app LOGIN PASSWORD 'sankalp_app';
    END IF;
END
$$;

-- CONNECT is a per-database grant and this file is applied to both sankalp and
-- sankalp_test under one literal migration -- current_database() picks up whichever
-- one this run is actually connected to.
DO $$
BEGIN
    EXECUTE format('GRANT CONNECT ON DATABASE %I TO sankalp_app', current_database());
END
$$;

GRANT USAGE ON SCHEMA public TO sankalp_app;

-- What the running engine actually does: claim/update workflow state, write step
-- checkpoints, write outbox events and mark them published.
GRANT SELECT, INSERT, UPDATE ON workflows, step_outputs, outbox TO sankalp_app;

-- INSERT into a BIGSERIAL column needs USAGE on its backing sequence. workflows.id is
-- gen_random_uuid() and step_outputs' PK is composite (workflow_id, step_name, kind) --
-- neither has a sequence. outbox and ledger_entries are the only BIGSERIAL PKs here.
GRANT USAGE, SELECT ON outbox_id_seq, ledger_entries_id_seq TO sankalp_app;

-- The ledger is the exception: append and read, nothing else. REVOKE first as a
-- defensive no-op -- nothing above granted UPDATE/DELETE/TRUNCATE on this table -- so
-- the "SELECT/INSERT only" intent reads as a stated pair rather than an absence someone
-- could widen later without anything here saying otherwise.
REVOKE ALL ON ledger_entries FROM sankalp_app;
GRANT SELECT, INSERT ON ledger_entries TO sankalp_app;
