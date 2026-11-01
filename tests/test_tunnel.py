import asyncio
import socket

import pytest

from proxyhunter.forwarder.tunnel import close_writer, connect_via_upstream, relay
from tests.network_fixtures import bounded


async def echo(reader, writer):
    while data := await reader.read(65536):
        writer.write(data)
        await writer.drain()


@pytest.mark.parametrize("protocol", ["http", "https", "socks4", "socks5"])
@bounded
async def test_connect_payload_integrity_and_remote_dns(tcp_server, upstream_server, protocol):
    origin = await tcp_server(echo)
    upstream = await upstream_server(protocol, map_host={"not-resolvable.invalid": origin.host})
    reader, writer = await connect_via_upstream(upstream.proxy(), "not-resolvable.invalid", origin.port, 1)
    try:
        payload = bytes(range(256)) * 1024
        writer.write(payload)
        await writer.drain()
        assert await reader.readexactly(len(payload)) == payload
        writer.write_eof()
        assert await reader.read() == b""
        assert upstream.destinations[0].host == "not-resolvable.invalid"
        if protocol in ("socks4", "socks5"):
            assert upstream.destinations[0].atyp == 3
    finally:
        await close_writer(writer)


@bounded
async def test_connect_keeps_payload_after_header(tcp_server, upstream_server):
    origin = await tcp_server(echo)
    upstream = await upstream_server(banner=b"buffered-tunnel-data")
    reader, writer = await connect_via_upstream(upstream.proxy(), origin.host, origin.port, 1)
    try:
        assert await reader.readexactly(20) == b"buffered-tunnel-data"
    finally:
        await close_writer(writer)


@pytest.mark.parametrize(
    "reply", [b"HTTP/1.1 403 Forbidden\r\n\r\n", b"garbage\r\n\r\n", b"HTTP/1.1 200 OK\r\n", b"x" * 70000]
)
@bounded
async def test_connect_malformed_response_closes(upstream_server, reply):
    upstream = await upstream_server(connect_reply=reply)
    with pytest.raises((OSError, ConnectionError)):
        await connect_via_upstream(upstream.proxy(), "example.invalid", 80, 0.2)


@pytest.mark.parametrize("protocol", ["http", "socks4", "socks5"])
@bounded
async def test_connect_rejection(upstream_server, protocol):
    from python_socks import ProxyError

    upstream = await upstream_server(protocol, reject=True)
    with pytest.raises((OSError, ProxyError)):
        await connect_via_upstream(upstream.proxy(), "example.invalid", 80, 0.2)


@pytest.mark.parametrize("protocol", ["http", "socks4", "socks5"])
@pytest.mark.parametrize("cancel", [False, True])
@bounded
async def test_connect_timeout_cancellation_closes_socket(upstream_server, protocol, cancel):
    upstream = await upstream_server(protocol, stall=True)
    task = asyncio.create_task(connect_via_upstream(upstream.proxy(), "example.invalid", 80, 0.05))
    await upstream.entered.wait()
    if cancel:
        task.cancel()
    from python_socks import ProxyError

    # asyncio.TimeoutError only became an alias of builtin TimeoutError
    # (and consequently an OSError subclass) in Python 3.11.
    with pytest.raises((asyncio.CancelledError,) if cancel else (OSError, ProxyError, asyncio.TimeoutError)):
        await task
    for _ in range(50):
        if upstream.active == 0:
            break
        await asyncio.sleep(0.002)
    assert upstream.active == 0


@bounded
async def test_relay_half_close_and_byte_count(tcp_server):
    result = asyncio.get_running_loop().create_future()

    async def origin(reader, writer):
        payload = await reader.read()  # requires relay half-close to finish
        assert payload == b"request"
        writer.write(b"response-after-eof")
        await writer.drain()

    remote = await tcp_server(origin)

    async def forward(reader, writer):
        rr, rw = await asyncio.open_connection(remote.host, remote.port)
        result.set_result(await relay(reader, writer, rr, rw, 0.5))

    local = await tcp_server(forward)
    reader, writer = await asyncio.open_connection(local.host, local.port)
    try:
        writer.write(b"request")
        await writer.drain()
        writer.write_eof()
        assert await reader.read() == b"response-after-eof"
        assert await result == len(b"requestresponse-after-eof")
    finally:
        await close_writer(writer)


@bounded
async def test_relay_shared_idle_timeout_and_one_way_activity(tcp_server):
    result = asyncio.get_running_loop().create_future()

    async def origin(reader, writer):
        for _ in range(8):
            writer.write(b"x")
            await writer.drain()
            await asyncio.sleep(0.015)
        await reader.read()  # no EOF until idle timeout

    remote = await tcp_server(origin)

    async def forward(reader, writer):
        rr, rw = await asyncio.open_connection(remote.host, remote.port)
        result.set_result(await relay(reader, writer, rr, rw, 0.06))

    local = await tcp_server(forward)
    reader, writer = await asyncio.open_connection(local.host, local.port)
    try:
        assert await reader.read() == b"x" * 8
        assert await result == 8
    finally:
        await close_writer(writer)


@bounded
async def test_relay_cancellation_closes_both_ends(tcp_server):
    origin_eof = asyncio.Event()
    entered = asyncio.Event()
    tasks = []

    async def origin(reader, writer):
        assert await reader.read() == b""
        origin_eof.set()

    remote = await tcp_server(origin)

    async def forward(reader, writer):
        rr, rw = await asyncio.open_connection(remote.host, remote.port)
        tasks.append(asyncio.current_task())
        entered.set()
        await relay(reader, writer, rr, rw)

    local = await tcp_server(forward)
    reader, writer = await asyncio.open_connection(local.host, local.port)
    try:
        await entered.wait()
        tasks[0].cancel()
        assert await reader.read() == b""
        await origin_eof.wait()
    finally:
        await close_writer(writer)


@bounded
async def test_relay_abrupt_reset_is_contained(tcp_server):
    result = asyncio.get_running_loop().create_future()

    async def origin(reader, writer):
        writer.get_extra_info("socket").setsockopt(
            socket.SOL_SOCKET, socket.SO_LINGER, __import__("struct").pack("ii", 1, 0)
        )
        writer.transport.abort()

    remote = await tcp_server(origin)

    async def forward(reader, writer):
        rr, rw = await asyncio.open_connection(remote.host, remote.port)
        result.set_result(await relay(reader, writer, rr, rw, 0.2))

    local = await tcp_server(forward)
    reader, writer = await asyncio.open_connection(local.host, local.port)
    try:
        try:
            await reader.read()
        except ConnectionResetError:
            pass
        assert await result == 0
    finally:
        await close_writer(writer)
