"""A local TCP proxy in front of Redis, for tests that need to kill it without touching the
shared container.

``docker stop sankalp-redis`` (docs/spec.md's own chaos-table answer) kills Redis for the whole
pytest session -- ``tests/test_outbox_drain.py`` shares that container, ``restart:
unless-stopped`` means a manual stop stays stopped, and a failed assertion mid-test would leave
the developer's Redis down with no cleanup. Monkeypatching a client to raise (the
``RaisingPublisher`` pattern in ``tests/test_outbox_drain.py``) proves bookkeeping and nothing
about real sockets or real redis-py exception types -- and cannot reproduce a *hang* at all,
which is the one failure mode the circuit breaker exists to make fast.

This is the third option: a real ``asyncio.start_server`` on an ephemeral port, forwarding bytes
to the real Redis on 6379. The limiter under test connects to the proxy, not to Redis directly,
so killing it is scoped to exactly one test -- a new port and a fresh proxy every time, same
spirit as ``tests/conftest.py``'s ``event_stream`` fixture using a unique key instead of a
guard: there is nothing shared left to protect against.
"""

from __future__ import annotations

import asyncio

__all__ = ["RedisProxy"]

_REDIS_HOST = "127.0.0.1"
_REDIS_PORT = 6379

#: Piping chunk size. Not performance-sensitive -- this proxy carries a handful of RESP
#: commands per test, never a real workload.
_CHUNK = 65536


class RedisProxy:
    """Forwards TCP connections to the real Redis, until told to misbehave.

    Three states, chosen to model exactly the two ways a dependency actually fails
    (``tests/test_ratelimit.py`` uses each once):

      * ``open()``      -- forward normally. The starting state.
      * ``blackhole()``  -- accept the TCP connection, then never read or write again. This is
        what a hung Redis (GC pause, a stuck fsync, a network partition that drops packets
        without resetting) looks like to a client: the socket exists, nothing on it ever
        answers, and the client only escapes via its own timeout.
      * ``close()``      -- stop accepting new connections outright, so the client sees
        ``ConnectionRefusedError``. This is what a Redis process that is simply gone looks like.

    ``connections`` counts accepted sockets since the last :meth:`reset_count`, and is the
    latency assertion this module exists for: after the breaker opens, the test asserts this
    stops advancing -- proving no socket was even attempted, rather than inferring it from wall
    -clock timing (which flakes on a loaded CI box).
    """

    def __init__(self) -> None:
        self._server: asyncio.Server | None = None
        self._blackholed = False
        self.connections = 0

    async def start(self) -> int:
        """Bind an ephemeral port and start forwarding. Returns the port."""
        self._server = await asyncio.start_server(self._handle, _REDIS_HOST, 0)
        port: int = self._server.sockets[0].getsockname()[1]
        return port

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()

    def open(self) -> None:
        """Forward normally."""
        self._blackholed = False

    def blackhole(self) -> None:
        """Accept connections, forward nothing. Models a hung Redis."""
        self._blackholed = True

    def reset_count(self) -> None:
        self.connections = 0

    async def _handle(
        self, client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter
    ) -> None:
        self.connections += 1
        if self._blackholed:
            # Never read, never write, never close -- the client's own socket_timeout /
            # asyncio.timeout() is the only thing that ever ends this. Just wait for the client
            # to give up and close its side; nothing here should ever complete on its own.
            try:
                await client_reader.read()
            except OSError, asyncio.CancelledError:
                pass
            finally:
                client_writer.close()
            return

        try:
            redis_reader, redis_writer = await asyncio.open_connection(_REDIS_HOST, _REDIS_PORT)
        except OSError:
            client_writer.close()
            return

        async def pipe(src: asyncio.StreamReader, dst: asyncio.StreamWriter) -> None:
            try:
                while chunk := await src.read(_CHUNK):
                    dst.write(chunk)
                    await dst.drain()
            except OSError, asyncio.CancelledError:
                pass
            finally:
                dst.close()

        await asyncio.gather(
            pipe(client_reader, redis_writer),
            pipe(redis_reader, client_writer),
            return_exceptions=True,
        )
