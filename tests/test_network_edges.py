import asyncio
import socket

import aiohttp
import pytest
from aiohttp import web

import proxyhunter.validator as validator_module
from proxyhunter.forwarder.http_proxy import LocalHttpProxy
from proxyhunter.forwarder.socks_proxy import LocalSocks5Proxy
from proxyhunter.forwarder.tunnel import BUF_SIZE, close_writer, connect_via_upstream, relay
from proxyhunter.scrapers import fallback
from proxyhunter.validator import ProxyValidator
from tests.network_fixtures import bounded, http_response
from tests.test_forwarders import connect_client, forwarder, socks_request
from tests.test_tunnel import echo


@pytest.mark.parametrize("kind", [LocalHttpProxy, LocalSocks5Proxy])
@bounded
async def test_shutdown_preserves_success_after_payload(kind, tcp_server, upstream_server):
    origin = await tcp_server(echo)
    upstream = await upstream_server()
    async with forwarder(kind, [upstream.proxy()]) as (server, pool):
        reader, writer = await connect_client(server)
        try:
            if kind is LocalHttpProxy:
                writer.write(f"CONNECT {origin.host}:{origin.port} HTTP/1.1\r\n\r\n".encode())
                await writer.drain()
                await reader.readuntil(b"\r\n\r\n")
            else:
                writer.write(b"\x05\x01\x00" + socks_request(origin.host, origin.port, 1))
                await writer.drain()
                await reader.readexactly(12)
            writer.write(b"useful-payload")
            await writer.drain()
            assert await reader.readexactly(14) == b"useful-payload"
            await server.shutdown()
            assert pool.get_status()[upstream.proxy().key_str()]["success_count"] == 1
            assert await reader.read() == b""
        finally:
            await close_writer(writer)


@pytest.mark.parametrize("kind", [LocalHttpProxy, LocalSocks5Proxy])
@bounded
async def test_lifecycle_restart_capacity_and_start_idempotence(kind):
    async with forwarder(kind, timeout=5, forward_max_connections=1) as (server, _pool):
        assert await asyncio.gather(server.start(), server.start()) == [server, server]
        first_reader, first_writer = await connect_client(server)
        try:
            # Wait until the first accepted connection consumes the one slot.
            for _ in range(30):
                if server._clients:
                    break
                await asyncio.sleep(0)
            second_reader, second_writer = await connect_client(server)
            try:
                assert await second_reader.read() == b""
            finally:
                await close_writer(second_writer)
            await asyncio.gather(server.shutdown(), server.shutdown())
            assert await first_reader.read() == b""
            assert await asyncio.gather(server.start(), server.start()) == [server, server]
            third_reader, third_writer = await connect_client(server)
            try:
                await server.shutdown()
                assert await third_reader.read() == b""
            finally:
                await close_writer(third_writer)
        finally:
            await close_writer(first_writer)


@pytest.mark.parametrize(
    "headers",
    [
        b"Content-Length: -1",
        b"Content-Length: 1\r\nContent-Length: 1",
        b"Content-Length: 1\r\nTransfer-Encoding: chunked",
        b"Transfer-Encoding: gzip",
        b"Host: a\r\nHost: b",
        b"X: \x7f",
    ],
)
@bounded
async def test_http_rejects_ambiguous_framing(headers):
    async with forwarder(LocalHttpProxy) as (server, _pool):
        reader, writer = await connect_client(server)
        try:
            writer.write(b"POST http://example.invalid/ HTTP/1.1\r\n" + headers + b"\r\n\r\n")
            await writer.drain()
            assert (await reader.read()).startswith(b"HTTP/1.1 400 ")
        finally:
            await close_writer(writer)


@bounded
async def test_http_chunked_payload_is_not_discarded(tcp_server, upstream_server):
    payload = b"5\r\nhello\r\n0\r\n\r\n"

    async def origin(reader, writer):
        head = await reader.readuntil(b"\r\n\r\n")
        assert b"Transfer-Encoding: chunked" in head
        assert b"Proxy-Authorization:" not in head
        assert await reader.readexactly(len(payload)) == payload
        writer.write(http_response(200, b"ok"))
        await writer.drain()

    remote = await tcp_server(origin)
    upstream = await upstream_server("socks5")
    async with forwarder(LocalHttpProxy, [upstream.proxy()]) as (server, _pool):
        reader, writer = await connect_client(server)
        try:
            writer.write(
                f"POST http://{remote.host}:{remote.port}/ HTTP/1.1\r\n".encode()
                + b"Transfer-Encoding: chunked\r\nProxy-Authorization: must-not-leak\r\n\r\n"
                + payload
            )
            await writer.drain()
            writer.write_eof()
            assert (await reader.read()).endswith(b"ok")
        finally:
            await close_writer(writer)


@bounded
async def test_relay_large_payload_with_slow_receiver(tcp_server):
    payload = bytes(range(256)) * 4096
    result = asyncio.get_running_loop().create_future()

    async def slow_echo(reader, writer):
        while data := await reader.read(8192):
            await asyncio.sleep(0.001)
            writer.write(data)
            await writer.drain()

    origin = await tcp_server(slow_echo)

    async def handler(reader, writer):
        rr, rw = await asyncio.open_connection(origin.host, origin.port)
        rw.get_extra_info("socket").setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
        result.set_result(await relay(reader, writer, rr, rw, 1))

    local = await tcp_server(handler)
    reader, writer = await asyncio.open_connection(local.host, local.port)
    try:

        async def send():
            writer.write(payload)
            await writer.drain()
            writer.write_eof()

        sender = asyncio.create_task(send())
        assert await reader.read() == payload
        await sender
        assert await result == len(payload) * 2
    finally:
        await close_writer(writer)


@bounded
async def test_stalled_drain_is_bounded_and_not_counted():
    class Writer:
        def __init__(self):
            self.closed = False
            self.data = bytearray()
            self.entered = asyncio.Event()

        def write(self, data):
            self.data.extend(data)

        async def drain(self):
            self.entered.set()
            await asyncio.Event().wait()

        def close(self):
            self.closed = True

        async def wait_closed(self):
            pass

        def can_write_eof(self):
            return True

        def write_eof(self):
            pass

    ar, br = asyncio.StreamReader(), asyncio.StreamReader()
    ar.feed_data(b"x" * BUF_SIZE * 3)
    aw, bw = Writer(), Writer()
    task = asyncio.create_task(relay(ar, aw, br, bw, 0.03))
    await bw.entered.wait()
    assert len(bw.data) == BUF_SIZE  # no second read/write until drain completes
    assert await task == 0
    assert aw.closed and bw.closed


@pytest.mark.parametrize("host,port", [("", 80), ("bad\r\nHost: bad", 80), ("bad host", 80), ("x", 0)])
@bounded
async def test_invalid_tunnel_target_before_io(upstream_server, host, port):
    upstream = await upstream_server()
    with pytest.raises(ValueError):
        await connect_via_upstream(upstream.proxy(), host, port)
    assert upstream.connections == 0


@bounded
async def test_fallback_socks5h_remote_dns_and_session_isolation(http_server, upstream_server, monkeypatch):
    calls = 0

    async def proxied(request):
        nonlocal calls
        calls += 1
        if calls == 1:
            response = web.Response(status=503)
            response.set_cookie("direct-only", "secret")
            return response
        assert "Cookie" not in request.headers
        return web.Response(text="proxied")

    origin = await http_server(proxied)
    port = int(origin.rsplit(":", 1)[1])
    first = await upstream_server("socks5", reject=True)
    second = await upstream_server("socks5", map_host={"fallback.invalid": "127.0.0.1"})
    loop = asyncio.get_running_loop()
    original = loop.getaddrinfo

    async def getaddrinfo(host, port, *args, **kwargs):
        return await original("127.0.0.1" if host == "fallback.invalid" else host, port, *args, **kwargs)

    monkeypatch.setattr(loop, "getaddrinfo", getaddrinfo)
    async with aiohttp.ClientSession() as session:
        response = await fallback.get_with_fallback(
            session,
            f"http://fallback.invalid:{port}/",
            fallback_proxies=[first.proxy(), second.proxy()],
        )
        assert response.text == "proxied"
        assert not session.closed
    assert second.destinations[0].host == "fallback.invalid"
    assert second.destinations[0].atyp == 3
    assert calls == 2


@pytest.mark.parametrize(
    "geo_body",
    [b"not-json", b"[]", b'{"status":"fail"}', b'{"status":"success","country":null,"city":[],"isp":""}'],
)
@bounded
async def test_geo_through_proxy_bad_response_does_not_overwrite(upstream_server, monkeypatch, geo_body):
    from unittest.mock import AsyncMock

    monkeypatch.setattr(ProxyValidator, "_get_real_ip", AsyncMock(return_value=None))
    monkeypatch.setattr(validator_module, "PRIMARY_HTTP_URL", "http://fake.test/http")
    monkeypatch.setattr(validator_module, "PRIMARY_HTTPS_URL", "http://fake.test/https")
    monkeypatch.setattr(validator_module, "GEO_VERIFY_URL", "http://fake.test/geo")

    async def plain(head, body):
        return http_response(200, geo_body if b"/geo " in head else b"{}")

    upstream = await upstream_server(plain=plain, reject=True)
    p = upstream.proxy(country="Original", city="Original", isp="Original")
    await ProxyValidator(secondary_check=False, geo_verify_via_proxy=True).validate_all([p])
    assert p.alive
    assert (p.country, p.city, p.isp) == ("Original", "Original", "Original")


@pytest.mark.parametrize("protocol", ["socks4", "socks5"])
@pytest.mark.parametrize("failure", ["reject", "timeout", "closed"])
@bounded
async def test_validator_contains_http_socks_connector_errors(
    upstream_server, monkeypatch, protocol, failure
):
    from unittest.mock import AsyncMock

    monkeypatch.setattr(ProxyValidator, "_get_real_ip", AsyncMock(return_value=None))
    monkeypatch.setattr(validator_module, "PRIMARY_HTTP_URL", "http://127.0.0.1:1/http")
    monkeypatch.setattr(validator_module, "PRIMARY_HTTPS_URL", "http://127.0.0.1:1/https")
    upstream = await upstream_server(protocol, reject=failure == "reject", stall=failure == "timeout")
    if failure == "closed":
        upstream.server.close()
        await upstream.server.wait_closed()
    p = upstream.proxy()
    assert await ProxyValidator(timeout=0.03, secondary_check=False).validate_all([p]) == [p]
    assert not p.alive
    assert p.checked_at


@bounded
async def test_invalid_proxy_is_returned_dead_without_stopping_workers(upstream_server, monkeypatch):
    from unittest.mock import AsyncMock

    from proxyhunter.models import Proxy

    monkeypatch.setattr(ProxyValidator, "_get_real_ip", AsyncMock(return_value=None))
    monkeypatch.setattr(validator_module, "PRIMARY_HTTP_URL", "http://fake.test/http")
    monkeypatch.setattr(validator_module, "PRIMARY_HTTPS_URL", "http://fake.test/https")
    upstream = await upstream_server(reject=True)
    bad = Proxy("127.0.0.1", 80, "socks6", "invalid")
    good = upstream.proxy()
    assert await ProxyValidator(secondary_check=False, workers=2).validate_all([bad, good]) == [bad, good]
    assert not bad.alive and bad.checked_at
    assert good.alive


@pytest.mark.parametrize("status,alive", [(200, True), (204, True), (302, True), (500, False)])
@bounded
async def test_custom_secondary_url_uses_requests_ok_semantics(upstream_server, monkeypatch, status, alive):
    from unittest.mock import AsyncMock

    monkeypatch.setattr(ProxyValidator, "_get_real_ip", AsyncMock(return_value=None))
    monkeypatch.setattr(validator_module, "PRIMARY_HTTP_URL", "http://fake.test/http")
    monkeypatch.setattr(validator_module, "PRIMARY_HTTPS_URL", "http://fake.test/https")

    async def plain(head, body):
        return http_response(status if b"/custom-check " in head else 200)

    upstream = await upstream_server(plain=plain, reject=True)
    p = upstream.proxy()
    await ProxyValidator(secondary_url="http://fake.test/custom-check").validate_all([p])
    assert p.alive is alive
