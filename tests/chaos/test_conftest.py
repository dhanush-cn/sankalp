"""Unit tests for tests/chaos/conftest.py's DSN helper.

_rewrite_port has no Toxiproxy or fault-injection dependency -- it is pure string surgery
-- so this is an ordinary unit test, not a chaos scenario: it stays unmarked and keeps
running under `make test`. It exists so a future refactor of _rewrite_port cannot silently
point a chaos test at the wrong database (a swapped credential) or the wrong Redis db index
(a swapped path) without a test failing first.
"""

from __future__ import annotations

from chaos.conftest import _rewrite_port


def test_rewrite_port_on_a_postgres_dsn_keeps_credentials_and_database() -> None:
    dsn = "postgresql://sankalp:sankalp@localhost:5432/sankalp_test"

    assert _rewrite_port(dsn, 15432) == "postgresql://sankalp:sankalp@localhost:15432/sankalp_test"


def test_rewrite_port_on_a_redis_dsn_keeps_the_db_index_path() -> None:
    dsn = "redis://localhost:6379/0"

    assert _rewrite_port(dsn, 16379) == "redis://localhost:16379/0"
