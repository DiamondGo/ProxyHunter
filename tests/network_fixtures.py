"""Reusable loopback fixtures for networking and parent integration tests.

No public network or runtime files. Every server owns accepted tasks/writers.
Use @bounded on async tests so protocol regressions cannot hang the suite.
"""

from __future__ import annotations

import asyncio
import functools
import ipaddress
import json
import ssl
import struct
from contextlib import AsyncExitStack, suppress
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

import pytest_asyncio
from aiohttp import web

from proxyhunter.models import Proxy


def bounded(function):
    @functools.wraps(function)
    async def run(*args, **kwargs):
        return await asyncio.wait_for(function(*args, **kwargs), 8)

    return run


async def close(writer):
    writer.close()
    with suppress(OSError, asyncio.TimeoutError):
        await asyncio.wait_for(writer.wait_closed(), 0.5)


async def pipe(ar, aw, br, bw):
    """Independent fixture tunnel, preserving half-close (not the production relay)."""

    async def copy(reader, writer):
        while chunk := await reader.read(16384):
            writer.write(chunk)
            await writer.drain()
        if writer.can_write_eof():
            writer.write_eof()
            await writer.drain()

    tasks = [asyncio.create_task(copy(ar, bw)), asyncio.create_task(copy(br, aw))]
    try:
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


class TcpEndpoint:
    def __init__(self, handler):
        self.handler = handler
        self.tasks = set()
        self.writers = set()
        self.errors = []
        self.connections = 0
        self.host = "127.0.0.1"
        self.port = 0

    async def __aenter__(self):
        self.server = await asyncio.start_server(self._accept, self.host, 0)
        self.port = self.server.sockets[0].getsockname()[1]
        return self

    def _accept(self, reader, writer):
        self.connections += 1
        self.writers.add(writer)
        task = asyncio.create_task(self._run(reader, writer))
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)

    async def _run(self, reader, writer):
        try:
            await self.handler(reader, writer)
        except (OSError, asyncio.IncompleteReadError):
            pass
        except Exception as exc:  # noqa: BLE001 - surface every server failure at fixture teardown
            self.errors.append(exc)
        finally:
            await close(writer)
            self.writers.discard(writer)

    async def __aexit__(self, *args):
        self.server.close()
        writers = list(self.writers)
        for writer in writers:
            writer.close()
        tasks = list(self.tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.gather(*(close(writer) for writer in writers))
        await self.server.wait_closed()
        assert not self.errors, self.errors

    def proxy(self, protocol="http", **kwargs):
        return Proxy(self.host, self.port, protocol, "loopback", **kwargs)


@dataclass
class Destination:
    host: str
    port: int
    atyp: int | None = None


def http_response(status=200, body=b"", headers=None):
    if status in (204, 304) or 100 <= status < 200:
        body = b""
    if isinstance(body, (dict, list)):
        body = json.dumps(body).encode()
    elif isinstance(body, str):
        body = body.encode()
    head = f"HTTP/1.1 {status} Test\r\nContent-Length: {len(body)}\r\nConnection: close\r\n"
    for key, value in (headers or {}).items():
        head += f"{key}: {value}\r\n"
    return head.encode() + b"\r\n" + body


class FakeUpstream(TcpEndpoint):
    """Real HTTP CONNECT/SOCKS4(a)/SOCKS5 handshakes and real TCP tunnels.

    plain(head, body) optionally supplies an HTTP forward-proxy response.
    map_host explicitly maps remote names to loopback without external DNS.
    """

    def __init__(
        self,
        protocol="http",
        *,
        reject=False,
        banner=b"",
        plain=None,
        map_host=None,
        connect_reply=None,
        stall=False,
    ):
        super().__init__(self._protocol)
        self.protocol = protocol
        self.reject = reject
        self.banner = banner
        self.plain = plain
        self.map_host = map_host or {}
        self.connect_reply = connect_reply
        self.stall = stall
        self.destinations = []
        self.requests = []
        self.active = 0
        self.max_active = 0
        self.entered = asyncio.Event()

    def proxy(self, protocol=None, **kwargs):
        return super().proxy(protocol or self.protocol, **kwargs)

    async def _protocol(self, reader, writer):
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        self.entered.set()
        try:
            if self.stall:
                await reader.read()
                return
            if self.protocol in ("http", "https"):
                dest = await self._http(reader, writer)
            elif self.protocol == "socks5":
                dest = await self._socks5(reader, writer)
            else:
                dest = await self._socks4(reader, writer)
            if dest is None:
                return
            host, port = dest.host, dest.port
            self.destinations.append(dest)
            try:
                rr, rw = await asyncio.open_connection(self.map_host.get(host, host), port)
            except OSError:
                if self.protocol in ("http", "https"):
                    writer.write(http_response(502))
                elif self.protocol == "socks5":
                    writer.write(b"\x05\x05\x00\x01" + b"\x00" * 6)
                else:
                    writer.write(b"\x00\x5b" + b"\x00" * 6)
                await writer.drain()
                return
            try:
                if self.protocol in ("http", "https"):
                    writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n" + self.banner)
                elif self.protocol == "socks5":
                    writer.write(b"\x05\x00\x00\x01" + b"\x00" * 6)
                else:
                    writer.write(b"\x00\x5a" + b"\x00" * 6)
                await writer.drain()
                await pipe(reader, writer, rr, rw)
            finally:
                await close(rw)
        finally:
            self.active -= 1

    async def _http(self, reader, writer):
        head = await reader.readuntil(b"\r\n\r\n")
        method, target, _ = head.split(b"\r\n", 1)[0].split(b" ")
        length = 0
        for line in head.split(b"\r\n")[1:]:
            if line.lower().startswith(b"content-length:"):
                length = int(line.split(b":", 1)[1])
        body = await reader.readexactly(length)
        self.requests.append((head, body))
        if method != b"CONNECT":
            response = (
                await self.plain(head, body) if self.plain else http_response(200, {"origin": "198.51.100.8"})
            )
            writer.write(response)
            await writer.drain()
            return None
        if self.connect_reply is not None:
            writer.write(self.connect_reply)
            await writer.drain()
            return None
        if self.reject:
            writer.write(http_response(403))
            await writer.drain()
            return None
        parsed = urlsplit("//" + target.decode())
        return Destination(parsed.hostname, parsed.port)

    async def _socks5(self, reader, writer):
        version, count = await reader.readexactly(2)
        assert version == 5
        assert 0 in await reader.readexactly(count)
        writer.write(b"\x05\x00")
        await writer.drain()
        version, command, reserved, atyp = await reader.readexactly(4)
        assert (version, command, reserved) == (5, 1, 0)
        if atyp == 3:
            length = (await reader.readexactly(1))[0]
            host = (await reader.readexactly(length)).decode("ascii")
        else:
            host = str(ipaddress.ip_address(await reader.readexactly(4 if atyp == 1 else 16)))
        port = struct.unpack("!H", await reader.readexactly(2))[0]
        if self.reject:
            writer.write(b"\x05\x05\x00\x01" + b"\x00" * 6)
            await writer.drain()
            return None
        return Destination(host, port, atyp)

    async def _socks4(self, reader, writer):
        version, command, port, packed = struct.unpack("!BBH4s", await reader.readexactly(8))
        assert (version, command) == (4, 1)
        await reader.readuntil(b"\x00")  # user id
        if packed[:3] == b"\x00\x00\x00" and packed[3] != 0:
            host = (await reader.readuntil(b"\x00"))[:-1].decode("ascii")
            atyp = 3
        else:
            host = str(ipaddress.ip_address(packed))
            atyp = 1
        if self.reject:
            writer.write(b"\x00\x5b" + b"\x00" * 6)
            await writer.drain()
            return None
        return Destination(host, port, atyp)


@pytest_asyncio.fixture
async def tcp_server():
    async with AsyncExitStack() as stack:

        async def factory(handler):
            return await stack.enter_async_context(TcpEndpoint(handler))

        yield factory


@pytest_asyncio.fixture
async def upstream_server():
    async with AsyncExitStack() as stack:

        async def factory(protocol="http", **kwargs):
            return await stack.enter_async_context(FakeUpstream(protocol, **kwargs))

        yield factory


@pytest_asyncio.fixture
async def http_server():
    """factory(handler) -> base URL for a wildcard aiohttp.web endpoint."""
    async with AsyncExitStack() as stack:

        async def factory(handler, ssl_context=None):
            app = web.Application()
            app.router.add_route("*", "/{path:.*}", handler)
            runner = web.AppRunner(app, shutdown_timeout=0.2)
            await runner.setup()
            stack.push_async_callback(runner.cleanup)
            site = web.TCPSite(runner, "127.0.0.1", 0, ssl_context=ssl_context)
            await site.start()
            port = site._server.sockets[0].getsockname()[1]
            scheme = "https" if ssl_context else "http"
            return f"{scheme}://127.0.0.1:{port}"

        yield factory


@pytest_asyncio.fixture
async def tls_context(monkeypatch):
    """Trust only the committed, public localhost test certificate in this test."""
    import aiohttp.connector

    directory = Path(__file__).parent / "certs"
    server = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server.load_cert_chain(directory / "localhost.crt", directory / "localhost.key")
    client = ssl.create_default_context(cafile=str(directory / "localhost.crt"))
    monkeypatch.setattr(aiohttp.connector, "_SSL_CONTEXT_VERIFIED", client)
    return server
