"""Toxiproxy fixture layer for the Phase 4 chaos suite.

Toxiproxy (docker-compose.yml, service ``toxiproxy``) sits between the chaos tests and the
real Postgres/Redis. Its control API lives at ``localhost:8474``; its proxied listeners are
published at ``localhost:15432`` (Postgres) and ``localhost:16379`` (Redis).

No new config fields. The proxied DSN is the same database, role and credentials as
``test_database_url`` / ``test_app_database_url`` / ``redis_url`` -- only the port differs,
because Toxiproxy's published listener sits on the same host those settings already point
at. So this module DERIVES the proxied DSN by rewriting the port on the existing setting
rather than adding a parallel one to src/sankalp/config.py. A derived URL cannot drift onto
a different database the way an independently-configured one could, and it leaves
config.py's cross-field validation (lines 258-288) untouched.

The routing asymmetry, spelled out (see also docker-compose.yml's comment on the service):
Toxiproxy's upstream for each proxy is the IN-NETWORK address -- ``postgres:5432``,
``redis:6379`` -- because Toxiproxy resolves those names inside the compose network, the
same way every other container does. Tests, however, run on the host, so they reach
Toxiproxy through the ports docker-compose PUBLISHES (15432, 16379), not through the
compose network. A test's ordinary DSN (port 5432, direct) and its chaos DSN (port 15432,
through the proxy) therefore take physically different paths to the same database.

Proxy lifecycle. Proxies are created and destroyed per test, never defined in
docker-compose.yml, so a toxic that outlived its test cannot silently corrupt the next one.
:func:`chaos_proxies` also clears any proxy left over from an unclean previous shutdown
before creating its own, so a crashed prior run cannot leave a stale proxy (and therefore a
stale toxic) behind.

Skipping cleanly. If the control API is not reachable, :func:`toxiproxy_client` skips with
a message naming the fix (``docker compose up -d toxiproxy``) rather than surfacing
whatever ``httpx.ConnectError`` looks like -- someone running ``make test`` without the
container should be told what to do, not left to guess.

The ``chaos`` marker. Scenario files (``test_chaos_*.py``) are auto-marked ``chaos`` by the
collection hook at the bottom of this file, a dedicated marker rather than the existing
``slow`` one. ``slow`` already means something specific -- the 1,000-workflow soak test
(``make test-soak``) -- and that target has nothing to do with Toxiproxy; reusing ``slow``
for chaos scenarios would make ``make test-soak`` start collecting them too, demanding a
container it never needed. ``pyproject.toml`` registers ``chaos`` alongside ``slow`` and
excludes both from the default run; ``make chaos`` passes ``-m chaos`` to select exactly the
scenario files.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from urllib.parse import urlsplit, urlunsplit

import httpx
import pytest

from sankalp.config import get_settings

#: Toxiproxy's control API -- create/list/delete proxies and toxics here.
TOXIPROXY_CONTROL_URL = "http://localhost:8474"

#: Toxiproxy's published listeners (docker-compose.yml). Chaos tests connect to these
#: instead of 5432 / 6379 directly.
TOXIPROXY_POSTGRES_PORT = 15432
TOXIPROXY_REDIS_PORT = 16379

#: Fixed proxy names -- one Postgres proxy, one Redis proxy, matching the upstreams named
#: in docker-compose.yml's toxiproxy comment.
PROXY_POSTGRES = "postgres"
PROXY_REDIS = "redis"


def _rewrite_port(dsn: str, port: int) -> str:
    """Return ``dsn`` with its port replaced, everything else -- host, user, path -- as-is.

    Mirrors ``_with_database`` in src/sankalp/config.py: same technique (parse, replace one
    component, reassemble), applied to the port instead of the path, for the same reason --
    a derived URL cannot name a different database (here: a different host) than the one it
    was derived from. Works on both DSN shapes this module derives:
    ``postgresql://user:pass@host:5432/sankalp_test`` (credentials in the netloc, database
    in the path) and ``redis://host:6379/0`` (no credentials, the db index in the path) --
    the path is untouched either way, so the db index survives. See
    tests/chaos/test_conftest.py.
    """
    parts = urlsplit(dsn)
    userinfo, _, hostinfo = parts.netloc.rpartition("@")
    host = hostinfo.split(":", 1)[0]
    netloc = f"{userinfo}@{host}:{port}" if userinfo else f"{host}:{port}"
    return urlunsplit(parts._replace(netloc=netloc))


@dataclass(frozen=True)
class ChaosProxies:
    """The two live proxies for one test, and the DSNs that route through them."""

    client: httpx.AsyncClient
    postgres_name: str
    redis_name: str
    #: Owning role (test_database_url), through the Postgres proxy.
    postgres_dsn: str
    #: Restricted role (test_app_database_url), through the Postgres proxy.
    postgres_app_dsn: str
    #: Through the Redis proxy.
    redis_url: str


@pytest.fixture
async def toxiproxy_client() -> AsyncIterator[httpx.AsyncClient]:
    """An HTTP client for the control API, or a clean skip if Toxiproxy isn't running."""
    client = httpx.AsyncClient(base_url=TOXIPROXY_CONTROL_URL, timeout=5.0)
    try:
        response = await client.get("/version")
        response.raise_for_status()
    except httpx.HTTPError:
        await client.aclose()
        pytest.skip(
            "Toxiproxy control API not reachable at "
            f"{TOXIPROXY_CONTROL_URL} -- run `docker compose up -d toxiproxy` "
            "before the chaos suite."
        )
    try:
        yield client
    finally:
        await client.aclose()


@pytest.fixture
async def chaos_proxies(toxiproxy_client: httpx.AsyncClient) -> AsyncIterator[ChaosProxies]:
    """Create the Postgres and Redis proxies for one test, and delete them afterwards.

    Deletion happens in a ``finally``, so it runs whether the test passed, failed, or raised
    -- a toxic left attached to a proxy that outlives the test is exactly the leak this
    fixture exists to prevent.
    """
    settings = get_settings()
    proxies = ChaosProxies(
        client=toxiproxy_client,
        postgres_name=PROXY_POSTGRES,
        redis_name=PROXY_REDIS,
        postgres_dsn=_rewrite_port(str(settings.test_database_url), TOXIPROXY_POSTGRES_PORT),
        postgres_app_dsn=_rewrite_port(
            str(settings.test_app_database_url), TOXIPROXY_POSTGRES_PORT
        ),
        redis_url=_rewrite_port(str(settings.redis_url), TOXIPROXY_REDIS_PORT),
    )

    for name, listen, upstream in (
        (PROXY_POSTGRES, f"0.0.0.0:{TOXIPROXY_POSTGRES_PORT}", "postgres:5432"),
        (PROXY_REDIS, f"0.0.0.0:{TOXIPROXY_REDIS_PORT}", "redis:6379"),
    ):
        # Clears a proxy an unclean previous run left behind -- 404 here just means
        # there was nothing to clear, which is the common case.
        await toxiproxy_client.delete(f"/proxies/{name}")
        response = await toxiproxy_client.post(
            "/proxies",
            json={"name": name, "listen": listen, "upstream": upstream, "enabled": True},
        )
        response.raise_for_status()

    try:
        yield proxies
    finally:
        for name in (PROXY_POSTGRES, PROXY_REDIS):
            await toxiproxy_client.delete(f"/proxies/{name}")


# ---------------------------------------------------------------------------
# Toxic helpers -- one context manager per fault type. Each adds a toxic on entry and
# removes it on exit, in a `finally`, so an exception raised inside the `with` block
# still leaves the proxy clean for whatever the test does next.
# ---------------------------------------------------------------------------


def _toxic_name(proxy_name: str, kind: str) -> str:
    """A toxic name unique enough that stacking two faults in one test can't collide."""
    return f"{proxy_name}-{kind}-{uuid.uuid4().hex[:8]}"


@asynccontextmanager
async def latency(
    client: httpx.AsyncClient,
    proxy_name: str,
    *,
    ms: int,
    jitter_ms: int = 0,
    stream: str = "downstream",
) -> AsyncIterator[None]:
    """Delay every packet by ``ms`` (+/- ``jitter_ms``) for the duration of the block.

    docs/spec.md's "DB latency +500ms" row: ``latency(client, proxies.postgres_name, ms=500)``.
    """
    name = _toxic_name(proxy_name, "latency")
    response = await client.post(
        f"/proxies/{proxy_name}/toxics",
        json={
            "name": name,
            "type": "latency",
            "stream": stream,
            "toxicity": 1.0,
            "attributes": {"latency": ms, "jitter": jitter_ms},
        },
    )
    response.raise_for_status()
    try:
        yield
    finally:
        await client.delete(f"/proxies/{proxy_name}/toxics/{name}")


@asynccontextmanager
async def cut_connection(
    client: httpx.AsyncClient, proxy_name: str, *, stream: str = "downstream"
) -> AsyncIterator[None]:
    """Freeze the connection: no data flows and it is never closed -- a hang, not a drop.

    A ``timeout`` toxic with ``timeout: 0`` means Toxiproxy never times it out either, so
    the caller sees exactly what a stuck network looks like: no error, no data, no close.
    """
    name = _toxic_name(proxy_name, "cut")
    response = await client.post(
        f"/proxies/{proxy_name}/toxics",
        json={
            "name": name,
            "type": "timeout",
            "stream": stream,
            "toxicity": 1.0,
            "attributes": {"timeout": 0},
        },
    )
    response.raise_for_status()
    try:
        yield
    finally:
        await client.delete(f"/proxies/{proxy_name}/toxics/{name}")


@asynccontextmanager
async def reset_connection(
    client: httpx.AsyncClient, proxy_name: str, *, stream: str = "downstream"
) -> AsyncIterator[None]:
    """Abruptly RST the connection -- the other half of "connection cut" from a hang.

    Where :func:`cut_connection` makes the socket go silent, this makes it error
    immediately, the way a killed peer or a firewall rule would.

    ``reset_peer``'s only attribute in Toxiproxy 2.9 is ``timeout`` (milliseconds, the delay
    before the reset is fired) -- confirmed against the toxic's attribute table, same
    attribute name as the ``timeout`` toxic above but a different toxic type and a different
    effect: that one holds the connection open and silent, this one kills it. ``timeout: 0``
    means fire the RST immediately.
    """
    name = _toxic_name(proxy_name, "reset")
    response = await client.post(
        f"/proxies/{proxy_name}/toxics",
        json={
            "name": name,
            "type": "reset_peer",
            "stream": stream,
            "toxicity": 1.0,
            "attributes": {"timeout": 0},
        },
    )
    response.raise_for_status()
    try:
        yield
    finally:
        await client.delete(f"/proxies/{proxy_name}/toxics/{name}")


@asynccontextmanager
async def proxy_disabled(client: httpx.AsyncClient, proxy_name: str) -> AsyncIterator[None]:
    """Take the whole proxy offline: refuses new connections, drops existing ones.

    docs/spec.md's "Redis down" and "Network partition worker<->DB" rows both want the
    service to disappear entirely rather than degrade -- this is a proxy-level toggle, not
    a toxic, since no single toxic type models "not there at all".
    """
    response = await client.post(f"/proxies/{proxy_name}", json={"enabled": False})
    response.raise_for_status()
    try:
        yield
    finally:
        await client.post(f"/proxies/{proxy_name}", json={"enabled": True})


# ---------------------------------------------------------------------------
# Auto-mark chaos scenario files with the dedicated `chaos` marker (pyproject.toml), so
# `make test`'s default deselection skips them without every scenario file having to
# remember `pytestmark = pytest.mark.chaos` on its own. Files under tests/chaos/ that are
# NOT scenario files (test_invariants.py, test_conftest.py) are untouched and keep running
# under `make test`, since they need no Toxiproxy.
# ---------------------------------------------------------------------------


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    del config  # required by pytest's hookspec; unused here
    for item in items:
        if item.path.name.startswith("test_chaos_"):
            item.add_marker(pytest.mark.chaos)
