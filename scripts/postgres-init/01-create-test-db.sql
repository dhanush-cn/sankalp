-- Runs once, on a fresh data volume only (docker-entrypoint-initdb.d contract).
-- One Postgres instance, two databases: sankalp (dev) and sankalp_test (pytest).
-- If the volume already exists, this file is skipped -- the migration runner
-- (sankalp.storage.migrate) creates the database instead.
CREATE DATABASE sankalp_test OWNER sankalp;
