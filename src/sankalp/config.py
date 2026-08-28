"""Runtime configuration, read from environment variables.

One Postgres instance holds two databases: ``sankalp`` (dev, used by the API and
workers) and ``sankalp_test`` (pytest). Isolation between tests comes from the
truncate fixture, not from a container per session -- so the fixture must be able
to prove which database it is connected to before it truncates anything. That is
why :attr:`Settings.test_database_url` is validated to end in ``_test`` here: a
soak run pointed at the dev database would wipe it.
"""

from __future__ import annotations

import os
from functools import lru_cache
from typing import Literal
from urllib.parse import urlsplit, urlunsplit

from pydantic import AliasChoices, Field, PostgresDsn, RedisDsn, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

Environment = Literal["dev", "test", "prod"]


def _database_name(dsn: str) -> str:
    """Return the database name from a Postgres DSN (the path, minus the slash)."""
    return urlsplit(dsn).path.lstrip("/")


def _with_database(dsn: str, database: str) -> str:
    """Return ``dsn`` pointed at a different database on the same server."""
    parts = urlsplit(dsn)
    return urlunsplit(parts._replace(path=f"/{database}"))


class Settings(BaseSettings):
    """Every knob the engine reads at startup. Environment wins over ``.env``."""

    model_config = SettingsConfigDict(
        env_prefix="SANKALP_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        frozen=True,
    )

    environment: Environment = "dev"
    log_level: str = "INFO"

    # The crash gates (workflows/_instrumentation.py) let a test hold a step or a compensation
    # open until it inserts a crash_gates row, so a SIGKILL lands at a chosen instant. The
    # demo workflows that use them are imported by every worker -- workflows/__init__.py is
    # what registers definitions -- so the mechanism is reachable from a production process.
    #
    # It therefore takes TWO independent facts to arm, and this flag is only one of them:
    # `Settings.crash_gate_armed` also requires environment == "test". A single leaked env var
    # must not be able to make a real compensation block on a row that will never arrive; that
    # is a stuck unwind with money in limbo, and it would look like a hung worker rather than
    # like a misconfiguration.
    crash_gate_enabled: bool = Field(
        default=False,
        description="Half of the crash-gate arming condition. Requires environment='test' too.",
    )

    # ---- Postgres -----------------------------------------------------------
    database_url: PostgresDsn = Field(
        default="postgresql://sankalp:sankalp@localhost:5432/sankalp",
        description="Dev/prod database. Used by the API and the worker.",
    )
    test_database_url: PostgresDsn = Field(
        default="postgresql://sankalp:sankalp@localhost:5432/sankalp_test",
        description="Pytest database. Must end in _test; the truncate fixture checks it.",
    )
    # sankalp_app (migrations/004_restricted_role.sql) is neither a superuser nor the
    # owner of any table -- the API and workers connect as it. Migrations and the
    # truncate fixture keep using database_url/test_database_url above, since DDL and
    # TRUNCATE are exactly what this role must not have (docs/spec.md, "Restricted
    # Application Role").
    app_database_url: PostgresDsn = Field(
        default="postgresql://sankalp_app:sankalp_app@localhost:5432/sankalp",
        description="Restricted role for the API and workers. Dev/prod database.",
    )
    test_app_database_url: PostgresDsn = Field(
        default="postgresql://sankalp_app:sankalp_app@localhost:5432/sankalp_test",
        description="Restricted role's DSN for the pytest database.",
    )
    # Size the pool to Postgres cores x 2-4, NOT to HTTP concurrency. A pool of
    # ~16 serves thousands of concurrent async requests; oversized pools make
    # Postgres slower, not faster (docs/spec.md, Operational Notes).
    db_pool_min_size: int = Field(default=4, ge=1)
    db_pool_max_size: int = Field(default=16, ge=1)
    db_command_timeout_seconds: float = Field(default=30.0, gt=0)
    db_statement_cache_size: int = Field(default=0, ge=0)

    # ---- Redis --------------------------------------------------------------
    redis_url: RedisDsn = Field(default="redis://localhost:6379/0")
    # Bounds how long a stuck XADD can hold a claim transaction's row locks (and its xmin
    # horizon) open. FOR UPDATE SKIP LOCKED locks live exactly as long as the drain's
    # transaction does, so a hung Redis without this would pin vacuum on `outbox`, a table
    # 003_saga.sql already flags as high-churn.
    outbox_redis_timeout_seconds: float = Field(default=5.0, gt=0)

    # ---- Worker / queue -----------------------------------------------------
    worker_id: str = Field(
        default_factory=lambda: f"worker-{os.getpid()}",
        description="Written to workflows.owner_id. Must be unique per process.",
    )
    worker_concurrency: int = Field(default=32, ge=1)
    poll_interval_seconds: float = Field(default=0.5, gt=0)
    dequeue_batch_size: int = Field(default=10, ge=1)

    # A dead worker's rows become claimable when its lease expires -- there is no
    # separate recovery daemon. Lease duration is therefore the crash-recovery
    # latency bound the Phase 1 gate asserts against.
    lease_duration_seconds: int = Field(default=30, ge=1)
    lease_renew_divisor: int = Field(
        default=3,
        ge=2,
        description="Renew the lease every lease_duration / this while a step runs.",
    )
    # On SIGTERM the worker stops claiming and lets in-flight workflows finish. This bounds
    # that wait: a step still running afterwards is cancelled, and its workflow is recovered
    # the ordinary way -- its lease expires and another worker resumes it from the last
    # checkpoint. Keep it comfortably above a normal step so a rolling deploy does not
    # routinely orphan work it could have finished in another second.
    worker_shutdown_grace_seconds: float = Field(default=30.0, gt=0)

    # ---- Retry / backoff ----------------------------------------------------
    max_attempts: int = Field(default=5, ge=1)
    # The unwind's own retry budget, deliberately tighter than max_attempts and
    # deliberately not the same counter. A forward retry is spent across re-claims:
    # the row goes back to the queue, costs nothing while it waits, and
    # workflows.attempt records it. These attempts are spent inside a single claim,
    # holding a lease and a concurrency slot through every backoff -- so they are
    # counted in memory, workflows.attempt is left alone (its forward history stays
    # intact for debugging), and a crash mid-unwind does not consume the budget.
    compensation_max_attempts: int = Field(
        default=3,
        ge=1,
        description="Tries for one compensation before the workflow goes FAILED_DIRTY.",
    )
    backoff_cap_seconds: int = Field(
        default=60,
        ge=1,
        description="The cap in min(2 ** attempt, cap) * (0.5 + random()). Jitter is not optional.",
    )

    # ---- Outbox -------------------------------------------------------------
    outbox_batch_size: int = Field(default=100, ge=1)
    outbox_poll_interval_seconds: float = Field(default=0.2, gt=0)
    outbox_stream: str = Field(
        default="sankalp.events",
        description="Redis Stream key the drain XADDs to.",
    )
    # No persistence on the Redis container (docker-compose.yml: --save "" --appendonly no) --
    # the stream is transport, not storage, and MAXLEN keeps it from growing without bound.
    # The ~ makes it approximate (Redis trims lazily, on whole macro-nodes) so XADD does not
    # pay for an exact trim on every call.
    outbox_stream_maxlen: int = Field(default=100_000, ge=1)
    # Whether run_worker() also starts a DrainLoop as a sibling task, sharing the worker's
    # pool. False runs the drain only via `python -m sankalp.engine.drain` / `make drain`.
    outbox_drain_in_worker: bool = Field(default=True)

    # ---- API ----------------------------------------------------------------
    api_host: str = "0.0.0.0"
    api_port: int = Field(default=8000, ge=1, le=65535)

    # ---- Observability ------------------------------------------------------
    otel_service_name: str = Field(
        default="sankalp",
        validation_alias=AliasChoices("SANKALP_OTEL_SERVICE_NAME", "OTEL_SERVICE_NAME"),
    )
    otel_exporter_otlp_endpoint: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            "SANKALP_OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_ENDPOINT"
        ),
        description="e.g. http://localhost:4317. Tracing is a no-op when unset.",
    )
    otel_traces_enabled: bool = True
    metrics_port: int = Field(default=9464, ge=1, le=65535)

    # ---- Validation ---------------------------------------------------------

    @field_validator("test_database_url")
    @classmethod
    def _test_db_must_be_named_test(cls, v: PostgresDsn) -> PostgresDsn:
        name = _database_name(str(v))
        if not name.endswith("_test"):
            raise ValueError(
                f"test_database_url points at database {name!r}, which does not end in '_test'. "
                "The truncate fixture refuses to run against anything else -- this guard is what "
                "stops a soak run from wiping the dev database."
            )
        return v

    @model_validator(mode="after")
    def _check_pool_and_distinct_databases(self) -> Settings:
        if self.db_pool_min_size > self.db_pool_max_size:
            raise ValueError(
                f"db_pool_min_size ({self.db_pool_min_size}) exceeds "
                f"db_pool_max_size ({self.db_pool_max_size})"
            )
        if _database_name(str(self.database_url)) == _database_name(str(self.test_database_url)):
            raise ValueError("database_url and test_database_url must name different databases")
        if _database_name(str(self.app_database_url)) != _database_name(str(self.database_url)):
            raise ValueError(
                "app_database_url must name the same database as database_url -- "
                "sankalp_app is a restricted role on the same database, not a different one"
            )
        if _database_name(str(self.test_app_database_url)) != _database_name(
            str(self.test_database_url)
        ):
            raise ValueError(
                "test_app_database_url must name the same database as test_database_url"
            )
        return self

    # ---- Derived ------------------------------------------------------------

    @property
    def active_database_url(self) -> str:
        """The database this process should use, chosen by ``environment``."""
        return str(self.test_database_url if self.environment == "test" else self.database_url)

    @property
    def maintenance_database_url(self) -> str:
        """Same server, ``postgres`` database -- for CREATE DATABASE, which cannot
        run inside a transaction or against the database being created."""
        return _with_database(str(self.database_url), "postgres")

    @property
    def active_app_database_url(self) -> str:
        """The restricted-role DSN this process's worker/drain pool should use."""
        if self.environment == "test":
            return str(self.test_app_database_url)
        return str(self.app_database_url)

    @property
    def database_name(self) -> str:
        return _database_name(str(self.database_url))

    @property
    def test_database_name(self) -> str:
        return _database_name(str(self.test_database_url))

    @property
    def lease_renew_interval_seconds(self) -> float:
        return self.lease_duration_seconds / self.lease_renew_divisor

    @property
    def crash_gate_armed(self) -> bool:
        """Whether a crash gate may block. Requires BOTH facts, never either alone.

        ``crash_gate_enabled`` is deliberately not sufficient. The demo workflows are imported
        by every worker process, so an env var that leaked into a production deployment would
        otherwise be able to park a compensation on a ``crash_gates`` row that no test is ever
        going to insert -- an unwind stopped mid-flight with money committed on one side of it.

        Requiring ``environment == "test"`` as well means arming the gate takes a deliberate,
        visible statement that this process is a test process. ``environment`` also decides
        which database the worker connects to (:attr:`active_database_url`), so a process that
        satisfies this is by construction pointed at ``sankalp_test``.
        """
        return self.environment == "test" and self.crash_gate_enabled


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Process-wide settings. Cached; call ``get_settings.cache_clear()`` in tests
    that mutate the environment."""
    return Settings()
