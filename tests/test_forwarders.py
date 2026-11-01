import asyncio
import ipaddress
import struct
from contextlib import asynccontextmanager

import pytest

from proxyhunter.forwarder.http_proxy import LocalHttpProxy
from proxyhunter.forwarder.pool import ProxyPool
from proxyhunter.forwarder.socks_proxy import LocalSocks5Proxy
from proxyhunter.forwarder.tunnel import close_writer
from tests.network_fixtures import bounded, http_response
from tests.test_tunnel import echo


@asynccontextmanager
async def forwarder(kind, proxies=(), **settings):
    pool = ProxyPool()
    pool.set_proxies(list(proxies))
    server = kind(pool, {"timeout": 0.2, **settings}, "127.0.0.1", 0)
    assert server.port == 0
    assert server._server is None
    assert await server.start() is server
    assert server.port > 0
    try:
        yield server, pool
    finally:
        await server.shutdown()
        assert not server._handlers
        assert not server._clients
        await server.shutdown()


async def connect_client(server):
    return await asyncio.open_connection(server.host, server.port)


async def wait_reports(pool, count):
    for _ in range(100):
        if sum(s["request_count"] for s in pool.get_status().values()) >= count:
            return
        await asyncio.sleep(0.002)
    raise AssertionError(pool.get_status())


@pytest.mark.parametrize("protocol", ["http", "https", "socks4", "socks5"])
@bounded
async def test_http_plain_post_same_packet_body_and_target_rewrite(tcp_server, upstream_server, protocol):
    seen = []

    async def origin(reader, writer):
        head = await reader.readuntil(b"\r\n\r\n")
        body = await reader.readexactly(11)
        seen.append((head, body))
        writer.write(http_response(200, body))
        await writer.drain()

    remote = await tcp_server(origin)

    async def plain(head, body):
        seen.append((head, body))
        return http_response(200, body)

    upstream = await upstream_server(protocol, plain=plain)
    async with forwarder(LocalHttpProxy, [upstream.proxy()]) as (server, pool):
        reader, writer = await connect_client(server)
        target = f"http://{remote.host}:{remote.port}?q=1"
        try:
            writer.write(
                (
                    f"POST {target} HTTP/1.1\r\nHost: ignored\r\nContent-Length: 11\r\n"
                    "Proxy-Connection: keep-alive\r\n\r\n"
                ).encode()
                + b"hello\x00world"
            )
            await writer.drain()
            response = await reader.read()
            assert response.endswith(b"hello\x00world")
            assert seen[0][1] == b"hello\x00world"
            expected = f"POST {target} HTTP/1.1" if protocol in ("http", "https") else "POST /?q=1 HTTP/1.1"
            assert seen[0][0].startswith(expected.encode() + b"\r\n")
            assert b"Proxy-Connection:" not in seen[0][0]
            writer.write_eof()
            await wait_reports(pool, 1)
            assert pool.get_status()[upstream.proxy().key_str()]["success_count"] == 1
        finally:
            await close_writer(writer)


@pytest.mark.parametrize("protocol", ["http", "socks4", "socks5"])
@bounded
async def test_http_connect_retry_and_buffered_bidirectional_data(tcp_server, upstream_server, protocol):
    remote = await tcp_server(echo)
    bad = await upstream_server(protocol, reject=True)
    good = await upstream_server(protocol, banner=b"server-early" if protocol == "http" else b"")
    async with forwarder(LocalHttpProxy, [bad.proxy(), good.proxy()]) as (server, pool):
        reader, writer = await connect_client(server)
        try:
            writer.write(
                f"CONNECT {remote.host}:{remote.port} HTTP/1.1\r\nHost: x\r\n\r\n".encode() + b"client-early"
            )
            await writer.drain()
            assert await reader.readuntil(b"\r\n\r\n") == b"HTTP/1.1 200 Connection Established\r\n\r\n"
            expected = (b"server-early" if protocol == "http" else b"") + b"client-early"
            assert await reader.readexactly(len(expected)) == expected
            writer.write_eof()
            assert await reader.read() == b""
            await wait_reports(pool, 2)
            assert pool.get_status()[bad.proxy().key_str()]["failure_count"] == 1
            assert pool.get_status()[good.proxy().key_str()]["success_count"] == 1
        finally:
            await close_writer(writer)


@bounded
async def test_connect_does_not_retry_after_success_or_claim_handshake_as_payload(
    upstream_server, tcp_server
):
    async def closes(reader, writer):
        return

    remote = await tcp_server(closes)
    first = await upstream_server()
    unused = await upstream_server()
    async with forwarder(LocalHttpProxy, [first.proxy(), unused.proxy()]) as (server, pool):
        reader, writer = await connect_client(server)
        try:
            writer.write(f"CONNECT {remote.host}:{remote.port} HTTP/1.1\r\n\r\n".encode())
            await writer.drain()
            assert b" 200 " in await reader.readuntil(b"\r\n\r\n")
            writer.write_eof()
            assert await reader.read() == b""
            await wait_reports(pool, 1)
            assert pool.get_status()[first.proxy().key_str()]["success_count"] == 0
            assert unused.connections == 0
        finally:
            await close_writer(writer)


@bounded
async def test_plain_no_retry_after_request_bytes(tcp_server, upstream_server):
    async def closes(reader, writer):
        await reader.readuntil(b"\r\n\r\n")

    first = await tcp_server(closes)
    unused = await upstream_server()
    async with forwarder(LocalHttpProxy, [first.proxy(), unused.proxy()]) as (server, pool):
        reader, writer = await connect_client(server)
        try:
            writer.write(b"GET http://example.invalid/ HTTP/1.1\r\n\r\n")
            await writer.drain()
            writer.write_eof()
            assert await reader.read() == b""
            await wait_reports(pool, 1)
            assert pool.get_status()[first.proxy().key_str()]["failure_count"] == 1
            assert unused.connections == 0
        finally:
            await close_writer(writer)


@pytest.mark.parametrize(
    "packet",
    [
        b"GET / HTTP/1.1\r\n\r\n",
        b"BAD\r\n\r\n",
        b"GET http://x:bad/ HTTP/1.1\r\n\r\n",
        b"GET http://x/ HTTP/2\r\n\r\n",
        b"CONNECT x:0 HTTP/1.1\r\n\r\n",
        b"CONNECT [::1 HTTP/1.1\r\n\r\n",
        b"CONNECT x:443/foo HTTP/1.1\r\n\r\n",
        b"GET http://x/ HTTP/1.1\r\nBad header\r\n\r\n",
        b"GET http://x/ HTTP/1.1\r\nX: abc\x00\r\n\r\n",
    ],
)
@bounded
async def test_http_malformed_400(packet):
    async with forwarder(LocalHttpProxy) as (server, _pool):
        reader, writer = await connect_client(server)
        try:
            writer.write(packet)
            await writer.drain()
            assert (await reader.read()).startswith(b"HTTP/1.1 400 ")
        finally:
            await close_writer(writer)


@pytest.mark.parametrize(
    "mode,status", [("empty_pool", 502), ("timeout", 408), ("oversize", 431), ("incomplete", 400)]
)
@bounded
async def test_http_error_paths(mode, status):
    async with forwarder(LocalHttpProxy, timeout=0.03) as (server, _pool):
        reader, writer = await connect_client(server)
        try:
            if mode == "empty_pool":
                writer.write(b"CONNECT x:443 HTTP/1.1\r\n\r\n")
            elif mode == "oversize":
                writer.write(b"G" * ((1 << 20) + 10))
            elif mode == "incomplete":
                writer.write(b"GET / HTTP/1.1\r\n")
                writer.write_eof()
            await writer.drain()
            assert (await reader.read()).startswith(f"HTTP/1.1 {status} ".encode())
        finally:
            await close_writer(writer)


@bounded
async def test_configurable_retry_limit(upstream_server):
    bad = await upstream_server(reject=True)
    unused = await upstream_server()
    async with forwarder(LocalHttpProxy, [bad.proxy(), unused.proxy()], pool_max_retries=0) as (
        server,
        _pool,
    ):
        reader, writer = await connect_client(server)
        try:
            writer.write(b"CONNECT x.invalid:443 HTTP/1.1\r\n\r\n")
            await writer.drain()
            assert (await reader.read()).startswith(b"HTTP/1.1 502 ")
            assert bad.connections == 1
            assert unused.connections == 0
        finally:
            await close_writer(writer)


def socks_request(host, port, atyp):
    if atyp == 3:
        packed = host.encode("ascii")
        address = bytes([len(packed)]) + packed
    else:
        address = ipaddress.ip_address(host).packed
    return b"\x05\x01\x00" + bytes([atyp]) + address + struct.pack("!H", port)


@pytest.mark.parametrize("atyp,host", [(1, "127.0.0.1"), (3, "remote.invalid"), (4, "::1")])
@pytest.mark.parametrize("protocol", ["http", "socks4", "socks5"])
@bounded
async def test_socks_incoming_addresses_retry_and_pipelined_data(
    tcp_server, upstream_server, atyp, host, protocol
):
    remote = await tcp_server(echo)
    bad = await upstream_server(protocol, reject=True)
    good = await upstream_server(protocol, map_host={host: remote.host})
    async with forwarder(LocalSocks5Proxy, [bad.proxy(), good.proxy()]) as (server, pool):
        reader, writer = await connect_client(server)
        try:
            writer.write(b"\x05\x01\x00" + socks_request(host, remote.port, atyp) + b"early-payload")
            await writer.drain()
            assert await reader.readexactly(2) == b"\x05\x00"
            assert await reader.readexactly(10) == b"\x05\x00\x00\x01" + b"\x00" * 6
            assert await reader.readexactly(13) == b"early-payload"
            writer.write_eof()
            assert await reader.read() == b""
            await wait_reports(pool, 2)
            assert pool.get_status()[good.proxy().key_str()]["success_count"] == 1
        finally:
            await close_writer(writer)


@pytest.mark.parametrize(
    "packet,expected",
    [
        (b"\x05\x01\x02", b"\x05\xff"),
        (b"\x05\x00", b"\x05\xff"),
        (b"\x04\x01\x00", b""),
        (b"\x05\x01\x00\x05\x02\x00\x01", b"\x05\x00\x05\x07\x00\x01" + b"\x00" * 6),
        (b"\x05\x01\x00\x05\x01\x00\x09", b"\x05\x00\x05\x08\x00\x01" + b"\x00" * 6),
        (b"\x05\x01\x00\x05\x01\x01\x01", b"\x05\x00\x05\x01\x00\x01" + b"\x00" * 6),
        (b"\x05\x01\x00\x05\x01\x00\x03\x00", b"\x05\x00\x05\x08\x00\x01" + b"\x00" * 6),
        (b"\x05\x01\x00" + socks_request("127.0.0.1", 0, 1), b"\x05\x00\x05\x01\x00\x01" + b"\x00" * 6),
    ],
)
@bounded
async def test_socks_malformed_handshake(packet, expected):
    async with forwarder(LocalSocks5Proxy) as (server, _pool):
        reader, writer = await connect_client(server)
        try:
            writer.write(packet)
            await writer.drain()
            assert await reader.read() == expected
        finally:
            await close_writer(writer)


@bounded
async def test_socks_empty_pool_and_handshake_timeout():
    async with forwarder(LocalSocks5Proxy, timeout=0.02) as (server, _pool):
        reader, writer = await connect_client(server)
        try:
            writer.write(b"\x05\x01\x00" + socks_request("example.invalid", 80, 3))
            await writer.drain()
            assert await reader.read() == b"\x05\x00\x05\x01\x00\x01" + b"\x00" * 6
        finally:
            await close_writer(writer)
        reader, writer = await connect_client(server)
        try:
            writer.write(b"\x05\x01")
            await writer.drain()
            assert await reader.read() == b""
        finally:
            await close_writer(writer)


@pytest.mark.parametrize("kind", [LocalHttpProxy, LocalSocks5Proxy])
@pytest.mark.parametrize("stage", ["handshake", "connecting", "relaying"])
@bounded
async def test_shutdown_owns_all_accepted_handlers(tcp_server, upstream_server, kind, stage):
    remote = await tcp_server(echo)
    upstream = await upstream_server(stall=stage == "connecting")
    async with forwarder(kind, [upstream.proxy()], timeout=5) as (server, _pool):
        reader, writer = await connect_client(server)
        try:
            if stage != "handshake":
                if kind is LocalHttpProxy:
                    writer.write(f"CONNECT {remote.host}:{remote.port} HTTP/1.1\r\n\r\n".encode())
                else:
                    writer.write(b"\x05\x01\x00" + socks_request(remote.host, remote.port, 1))
                await writer.drain()
                await upstream.entered.wait()
                if stage == "relaying":
                    if kind is LocalHttpProxy:
                        await reader.readuntil(b"\r\n\r\n")
                    else:
                        await reader.readexactly(12)
            await server.shutdown()
            assert not server._handlers
            assert await reader.read() in (b"", b"\x05\x00")
        finally:
            await close_writer(writer)
