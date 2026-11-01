from __future__ import annotations

import asyncio
import logging
from contextlib import suppress

from python_socks import ProxyError, ProxyType
from python_socks.async_.asyncio import Proxy as SocksProxy

from proxyhunter.models import Proxy

log = logging.getLogger(__name__)
BUF_SIZE = 65536
MAX_HEAD_BYTES = 1 << 20
CONNECT_HEAD_BYTES = 65536
CONNECTION_ERRORS = (OSError, ProxyError, asyncio.TimeoutError)


class RelayCancelledError(asyncio.CancelledError):
    """Cancellation carrying the number of payload bytes drained before shutdown."""

    def __init__(self, transferred: int):
        super().__init__()
        self.transferred = transferred


async def close_writer(writer: asyncio.StreamWriter) -> None:
    writer.close()
    with suppress(OSError, asyncio.TimeoutError):
        await asyncio.wait_for(writer.wait_closed(), 1.0)


def authority(host: str, port: int) -> str:
    return f"[{host}]:{port}" if ":" in host else f"{host}:{port}"


async def connect_via_upstream(
    upstream: Proxy,
    target_host: str,
    target_port: int,
    timeout: float = 10.0,
) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    """Return asyncio streams tunneled via an upstream, with a whole-handshake deadline.

    SOCKS4 uses SOCKS4a for names and SOCKS5 uses remote DNS (PySocks' old
    default). The https protocol tag means an HTTPS-capable *plaintext* HTTP
    proxy, not TLS to the proxy. readuntil preserves any CONNECT-buffered data.
    The caller owns the returned streams; failure/cancellation closes them.
    """
    if (
        not target_host
        or any(c.isspace() or ord(c) < 32 or ord(c) == 127 for c in target_host)
        or not 0 < target_port < 65536
    ):
        raise ValueError("invalid tunnel target")
    target_host = target_host.encode("idna").decode("ascii")

    async def connect():
        if upstream.protocol in ("socks4", "socks5"):
            proxy = SocksProxy(
                proxy_type=ProxyType.SOCKS4 if upstream.protocol == "socks4" else ProxyType.SOCKS5,
                host=upstream.ip,
                port=upstream.port,
                rdns=True,
            )
            sock = await proxy.connect(dest_host=target_host, dest_port=target_port, timeout=timeout)
            try:
                return await asyncio.open_connection(sock=sock)
            except BaseException:
                sock.close()
                raise
        if upstream.protocol not in ("http", "https"):
            raise ValueError(f"unsupported upstream protocol: {upstream.protocol}")
        reader, writer = await asyncio.open_connection(upstream.ip, upstream.port, limit=CONNECT_HEAD_BYTES)
        try:
            target = authority(target_host, target_port)
            writer.write(
                (
                    f"CONNECT {target} HTTP/1.1\r\nHost: {target}\r\nProxy-Connection: keep-alive\r\n\r\n"
                ).encode("ascii")
            )
            await writer.drain()
            headers = await reader.readuntil(b"\r\n\r\n")
            if len(headers) > CONNECT_HEAD_BYTES:
                raise ConnectionError("upstream CONNECT headers too large")
            status = headers.split(b"\r\n", 1)[0].split()
            if len(status) < 2 or status[0] not in (b"HTTP/1.0", b"HTTP/1.1") or status[1] != b"200":
                raise ConnectionError(f"upstream refused CONNECT: {headers.splitlines()[0]!r}")
            return reader, writer
        except BaseException:
            await close_writer(writer)
            raise

    try:
        return await asyncio.wait_for(connect(), timeout)
    except (asyncio.IncompleteReadError, asyncio.LimitOverrunError) as exc:
        raise ConnectionError("invalid upstream CONNECT response") from exc


async def relay(
    a_reader: asyncio.StreamReader,
    a_writer: asyncio.StreamWriter,
    b_reader: asyncio.StreamReader,
    b_writer: asyncio.StreamWriter,
    idle_timeout: float = 120.0,
) -> int:
    """Backpressured duplex relay, returning bytes successfully written+drained.

    Inactivity is shared by both directions, including stalled drains. Normal
    EOF half-closes the opposite writer and lets the reverse direction finish.
    Transports without half-close support are closed instead. An abrupt reset
    aborts both directions and returns the bytes drained before the reset; it
    does not claim delivery to the remote application. Cancellation propagates
    after cancelling both pumps and closing both streams, as RelayCancelledError
    with a transferred count for forwarder pool bookkeeping.
    """
    loop = asyncio.get_running_loop()
    last_activity = loop.time()
    total = 0

    async def pump(reader, writer):
        nonlocal last_activity, total
        while True:
            data = await reader.read(BUF_SIZE)
            if not data:
                if writer.can_write_eof():
                    writer.write_eof()
                    await writer.drain()
                else:
                    writer.close()
                return
            writer.write(data)
            await writer.drain()
            total += len(data)
            last_activity = loop.time()

    async def watch_idle():
        while True:
            remaining = idle_timeout - (loop.time() - last_activity)
            if remaining <= 0:
                return
            await asyncio.sleep(remaining)

    pumps = {asyncio.create_task(pump(a_reader, b_writer)), asyncio.create_task(pump(b_reader, a_writer))}
    timer = asyncio.create_task(watch_idle())
    tasks = pumps | {timer}
    try:
        while pumps:
            done, _ = await asyncio.wait(pumps | {timer}, return_when=asyncio.FIRST_COMPLETED)
            if timer in done:
                break
            for task in done:
                task.result()
                pumps.remove(task)
    except OSError:
        pass
    except asyncio.CancelledError as exc:
        raise RelayCancelledError(total) from exc
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.gather(close_writer(a_writer), close_writer(b_writer))
    return total


class AsyncProxyServer:
    """Shared forwarder lifecycle: constructors do no I/O; shutdown owns handlers."""

    def __init__(self, pool, settings, host: str, port: int):
        self.pool, self.settings = pool, settings
        self.host, self.port = host, port
        self._server: asyncio.Server | None = None
        self._handlers: set[asyncio.Task] = set()
        self._clients: set[asyncio.StreamWriter] = set()
        self._closing = False
        self._lifecycle_lock = asyncio.Lock()

    @property
    def timeout(self) -> float:
        return max(0.001, float(self.settings.get("timeout", 8.0)))

    @property
    def max_retries(self) -> int:
        return max(0, int(self.settings.get("pool_max_retries", 3)))

    @property
    def idle_timeout(self) -> float:
        return max(0.001, float(self.settings.get("forward_idle_timeout", 120.0)))

    async def start(self):
        async with self._lifecycle_lock:
            if self._server is None:
                self._closing = False
                self._server = await asyncio.start_server(
                    self._accept, self.host, self.port, limit=MAX_HEAD_BYTES
                )
                self.host, self.port = self._server.sockets[0].getsockname()[:2]
        return self

    def _accept(self, reader, writer):
        overloaded = len(self._clients) >= int(self.settings.get("forward_max_connections", 256))
        self._clients.add(writer)
        task = asyncio.create_task(self._serve(reader, writer, overloaded))
        self._handlers.add(task)
        task.add_done_callback(self._handlers.discard)

    async def _serve(self, reader, writer, overloaded):
        try:
            if not self._closing and not overloaded:
                await self._handle(reader, writer)
        except CONNECTION_ERRORS + (
            ValueError,
            asyncio.IncompleteReadError,
            asyncio.LimitOverrunError,
        ) as exc:
            log.debug("forwarding connection failed: %s", exc)
        except Exception:
            log.exception("unexpected forwarding handler failure")
        finally:
            await close_writer(writer)
            self._clients.discard(writer)

    async def shutdown(self) -> None:
        async with self._lifecycle_lock:
            await self._shutdown()

    async def _shutdown(self) -> None:
        self._closing = True
        server, self._server = self._server, None
        if server is not None:
            server.close()
        clients = list(self._clients)
        for writer in clients:
            writer.close()
        while self._handlers:
            tasks = list(self._handlers)
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self._handlers.difference_update(tasks)
        # A handler cancelled before its first instruction never runs its finally.
        await asyncio.gather(*(close_writer(writer) for writer in clients))
        self._clients.difference_update(clients)
        # Python 3.12+ also waits for accepted transports here. Do this last.
        if server is not None:
            await server.wait_closed()
