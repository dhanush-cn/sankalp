"""Migration runner: raw numbered .sql, applied in numeric order, no ORM.

Rules this enforces, from CLAUDE.md:

* Migrations are ``NNN_description.sql`` and **forward-only**. Once a file has
  been applied its checksum is recorded; editing it afterwards is an error, not
  a silent re-apply.
* Schema changes are applied to **both** databases -- ``sankalp`` and
  ``sankalp_test`` -- so pytest never runs against a stale schema.
* Re-running is a no-op. Each migration and its ``schema_migrations`` row commit
  in the same transaction, so a crash mid-run leaves no half-applied version.

Usage::

    python -m sankalp.storage.migrate                 # both databases
    python -m sankalp.storage.migrate --target test   # sankalp_test only
    python -m sankalp.storage.migrate --dry-run       # show the plan, apply nothing
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import logging
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import asyncpg

from sankalp.config import Settings, get_settings

log = logging.getLogger("sankalp.migrate")

Target = Literal["dev", "test", "both"]

MIGRATION_FILENAME = re.compile(r"^(?P<version>\d+)_(?P<name>[A-Za-z0-9_\-]+)\.sql$")

#: Migrations that cannot run inside a transaction (CREATE INDEX CONCURRENTLY,
#: ALTER TYPE ... ADD VALUE on older servers) opt out with this marker.
NO_TRANSACTION_MARKER = "sankalp:no-transaction"

#: Session-level advisory lock, so two runners (CI and a developer, two workers
#: booting at once) cannot apply the same migration twice.
ADVISORY_LOCK_KEY = 4_071_115_539

_SCHEMA_MIGRATIONS_DDL = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version     INT PRIMARY KEY,
    filename    TEXT NOT NULL,
    checksum    TEXT NOT NULL,
    duration_ms INT  NOT NULL,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


class MigrationError(RuntimeError):
    """Raised when the migration set on disk disagrees with what was applied."""


@dataclass(frozen=True, slots=True)
class Migration:
    version: int
    filename: str
    path: Path
    sql: str
    checksum: str

    @property
    def in_transaction(self) -> bool:
        return NO_TRANSACTION_MARKER not in self.sql

    @classmethod
    def load(cls, path: Path) -> Migration:
        match = MIGRATION_FILENAME.match(path.name)
        if match is None:
            raise MigrationError(
                f"{path.name!r} is not a valid migration name. Expected NNN_description.sql"
            )
        sql = path.read_text(encoding="utf-8")
        return cls(
            version=int(match.group("version")),
            filename=path.name,
            path=path,
            sql=sql,
            # Normalise line endings so a checkout on Windows and one on Linux
            # produce the same checksum for the same committed file.
            checksum=hashlib.sha256(sql.replace("\r\n", "\n").encode("utf-8")).hexdigest(),
        )


def default_migrations_dir() -> Path:
    """``<repo root>/migrations``, resolved from this file's location."""
    return Path(__file__).resolve().parents[3] / "migrations"


def discover(directory: Path) -> list[Migration]:
    """Load every ``.sql`` in ``directory``, sorted by numeric version."""
    if not directory.is_dir():
        raise MigrationError(f"migrations directory not found: {directory}")

    migrations = [Migration.load(p) for p in sorted(directory.glob("*.sql"))]
    migrations.sort(key=lambda m: m.version)

    seen: dict[int, str] = {}
    for m in migrations:
        if m.version in seen:
            raise MigrationError(
                f"duplicate migration version {m.version}: {seen[m.version]} and {m.filename}"
            )
        seen[m.version] = m.filename
    return migrations


async def ensure_database(maintenance_dsn: str, database: str) -> bool:
    """Create ``database`` if it does not exist. Returns True if it was created.

    CREATE DATABASE cannot run inside a transaction, so this connects to the
    ``postgres`` maintenance database and issues it standalone. Needed because
    the compose init script only fires on a fresh volume.
    """
    conn = await asyncpg.connect(maintenance_dsn)
    try:
        exists = await conn.fetchval("SELECT 1 FROM pg_database WHERE datname = $1", database)
        if exists:
            return False
        # Identifier, not a value -- cannot be parameterised. The name comes from
        # settings, and this rejects anything that is not a plain identifier.
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", database):
            raise MigrationError(f"refusing to create database with unsafe name: {database!r}")
        await conn.execute(f'CREATE DATABASE "{database}"')
        log.info("created database %s", database)
        return True
    finally:
        await conn.close()


async def _applied(conn: asyncpg.Connection) -> dict[int, tuple[str, str]]:
    rows = await conn.fetch("SELECT version, filename, checksum FROM schema_migrations")
    return {r["version"]: (r["filename"], r["checksum"]) for r in rows}


def _verify(migrations: list[Migration], applied: dict[int, tuple[str, str]], label: str) -> None:
    """Fail loudly if disk and database disagree. Migrations are never edited."""
    for m in migrations:
        record = applied.get(m.version)
        if record is None:
            continue
        filename, checksum = record
        if checksum != m.checksum:
            raise MigrationError(
                f"[{label}] {m.filename} was modified after it was applied "
                f"(recorded {checksum[:12]}, on disk {m.checksum[:12]}). Migrations are "
                "forward-only: revert the edit and add a new NNN_*.sql instead."
            )
        if filename != m.filename:
            raise MigrationError(
                f"[{label}] migration {m.version} was applied as {filename!r} "
                f"but is now named {m.filename!r}"
            )

    on_disk = {m.version for m in migrations}
    missing = sorted(set(applied) - on_disk)
    if missing:
        raise MigrationError(
            f"[{label}] versions {missing} are recorded as applied but no longer exist on disk"
        )

    highest_applied = max(applied, default=0)
    out_of_order = [m.filename for m in migrations if m.version not in applied
                    and m.version < highest_applied]
    if out_of_order:
        raise MigrationError(
            f"[{label}] out-of-order migrations {out_of_order}: version {highest_applied} is "
            "already applied. Renumber them above the highest applied version."
        )


async def _apply_one(conn: asyncpg.Connection, migration: Migration, label: str) -> None:
    started = time.monotonic()
    record = (
        "INSERT INTO schema_migrations (version, filename, checksum, duration_ms) "
        "VALUES ($1, $2, $3, $4)"
    )

    if migration.in_transaction:
        # The DDL and its schema_migrations row commit together. A crash here
        # leaves the version unapplied and unrecorded -- never half of each.
        async with conn.transaction():
            await conn.execute(migration.sql)
            elapsed_ms = int((time.monotonic() - started) * 1000)
            await conn.execute(
                record, migration.version, migration.filename, migration.checksum, elapsed_ms
            )
    else:
        await conn.execute(migration.sql)
        elapsed_ms = int((time.monotonic() - started) * 1000)
        await conn.execute(
            record, migration.version, migration.filename, migration.checksum, elapsed_ms
        )

    log.info("[%s] applied %s (%dms)", label, migration.filename, elapsed_ms)


async def migrate_database(
    dsn: str,
    migrations: list[Migration],
    *,
    label: str,
    dry_run: bool = False,
) -> list[Migration]:
    """Apply every unapplied migration to ``dsn``. Returns what was applied."""
    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute("SELECT pg_advisory_lock($1)", ADVISORY_LOCK_KEY)
        try:
            await conn.execute(_SCHEMA_MIGRATIONS_DDL)
            applied = await _applied(conn)
            _verify(migrations, applied, label)

            pending = [m for m in migrations if m.version not in applied]
            if not pending:
                log.info("[%s] up to date (%d applied)", label, len(applied))
                return []

            if dry_run:
                for m in pending:
                    log.info("[%s] would apply %s", label, m.filename)
                return pending

            for m in pending:
                await _apply_one(conn, m, label)
            return pending
        finally:
            await conn.execute("SELECT pg_advisory_unlock($1)", ADVISORY_LOCK_KEY)
    finally:
        await conn.close()


async def run(
    *,
    target: Target = "both",
    directory: Path | None = None,
    dry_run: bool = False,
    settings: Settings | None = None,
) -> None:
    settings = settings or get_settings()
    migrations = discover(directory or default_migrations_dir())
    log.info("found %d migration(s)", len(migrations))

    targets: list[tuple[str, str, str]] = []
    if target in ("dev", "both"):
        targets.append(("dev", settings.database_name, str(settings.database_url)))
    if target in ("test", "both"):
        targets.append(("test", settings.test_database_name, str(settings.test_database_url)))

    for label, database, dsn in targets:
        await ensure_database(settings.maintenance_database_url, database)
        await migrate_database(dsn, migrations, label=label, dry_run=dry_run)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="sankalp-migrate", description=__doc__)
    parser.add_argument(
        "--target",
        choices=("dev", "test", "both"),
        default="both",
        help="which database(s) to migrate (default: both)",
    )
    parser.add_argument("--dir", type=Path, default=None, help="migrations directory")
    parser.add_argument("--dry-run", action="store_true", help="print the plan, apply nothing")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)-7s %(name)s: %(message)s",
    )

    try:
        asyncio.run(run(target=args.target, directory=args.dir, dry_run=args.dry_run))
    except (MigrationError, OSError, asyncpg.PostgresError) as exc:
        log.error("%s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
