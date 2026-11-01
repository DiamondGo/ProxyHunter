"""Whole-application tests: only endpoint routing is patched; all I/O is real.

The fixture HTTP proxy forwards requests to an actual local origin, rather than
manufacturing successful validation/payload responses. TLS uses the trusted
localhost certificate from network_fixtures. Every async test has an 8s bound.
"""

from __future__ import annotations

import asyncio
import json
import math
from dataclasses import dataclass, field
from datetime import datetime
from urllib.parse import urlsplit

import aiohttp
import pytest
import pytest_asyncio
from aiohttp import web

import proxyhunter.scrapers.freeproxy_world as world
from proxyhunter import validator
from proxyhunter.lifecycle import run_server, start_server
from proxyhunter.scrapers import proxyscrape
from tests.network_fixtures import FakeUpstream, TcpEndpoint, bounded, close, http_response

PORT_ZERO = {
    "ui_host": "127.0.0.1",
    "forward_host": "127.0.0.1",
    "ui_port": 0,
    "http_proxy_port": 0,
    "socks_port": 0,
}
PAYLOAD = bytes(range(256)) * 289 + b"\x00integration\r\n\xff"
HTTP_REPLY = b"origin received: " + PAYLOAD[::-1]
TCP_REPLY = b"binary received: " + PAYLOAD[::-1]


@dataclass
class LocalNetwork:
    protocol: str
    http_url: str = ""
    https_url: str = ""
    upstream: FakeUpstream | None = None
    rejected: FakeUpstream | None = None
    target: TcpEndpoint | None = None
    calls: list = field(default_factory=list)
    binary: list = field(default_factory=list)
    binary_started: asyncio.Queue = field(default_factory=asyncio.Queue)
    binary_release: asyncio.Event = field(default_factory=asyncio.Event)
    block_path: str | None = None
    entered: asyncio.Event = field(default_factory=asyncio.Event)
    release: asyncio.Event = field(default_factory=asyncio.Event)

    @property
    def sources(self):
        # ProxyScrape has no HTTPS-tagged list; the HTML source does.
        return ["freeproxy_world"] + ([] if self.protocol == "https" else ["proxyscrape"])

    @property
    def summary(self):
        return {
            "scraped": 3 if self.protocol == "https" else 6,
            "unique": 2,
            "reused_alive": 0,
            "freshly_checked": 2,
            "alive": 1,
        }

    @property
    def key(self):
        return f"{self.protocol}|127.0.0.1|{self.upstream.port}"

    @property
    def rejected_key(self):
        return f"{self.protocol}|127.0.0.1|{self.rejected.port}"

    def block(self, path):
        self.block_path = path
        self.entered.clear()
        self.release.clear()

    async def handle(self, request):
        body = await request.read()
        self.calls.append(
            (
                request.scheme,
                request.method,
                request.path_qs,
                request.headers.get("X-Integration-Trace"),
                body,
            )
        )
        if request.path == self.block_path:
            self.entered.set()
            await self.release.wait()
        if request.path == "/world":
            rows = "".join(
                f"<tr><td>127.0.0.1</td><td>{endpoint.port}</td>"
                '<td><a title="Advertised country">XX</a></td><td>Advertised city</td>'
                f"<td>1</td><td><a>{self.protocol}</a></td><td>yes</td></tr>"
                for endpoint in (self.upstream, self.upstream, self.rejected)
            )
            response = web.Response(
                text='<table class="table"><tbody>' + rows + "<tr><td>malformed row</td></tr></tbody></table>"
            )
        elif request.path == "/list":
            response = web.Response(
                text=(
                    f"127.0.0.1:{self.upstream.port}\r\n127.0.0.1:{self.upstream.port}\n"
                    f"127.0.0.1:{self.rejected.port}\ninvalid\n127.0.0.1:bad\n"
                )
            )
        elif request.path == "/ip":
            response = web.json_response({"ip": "203.0.113.17"})
        elif request.path in ("/primary/http", "/primary/https"):
            response = web.json_response({"origin": "198.51.100.23", "headers": {"Via": "integration"}})
        elif request.path == "/generate_204":
            response = web.Response(status=204)
        elif request.path == "/geo":
            response = web.json_response(
                {"status": "success", "country": "Canada", "city": "Ottawa", "isp": "loopback network"}
            )
        elif request.path == "/payload":
            response = web.Response(
                body=b"origin received: " + body[::-1],
                headers={"X-Origin-Trace": request.headers["X-Integration-Trace"]},
            )
        else:
            raise web.HTTPNotFound()
        # Deterministic connection accounting, including SOCKS HTTP sessions.
        response.force_close()
        return response

    async def forward_plain(self, head, body):
        """Independent fixture HTTP transport, with an explicit loopback-only target."""
        lines = head[:-4].split(b"\r\n")
        method, target, version = lines[0].split(b" ")
        parsed = urlsplit(target.decode("ascii"))
        assert parsed.scheme == "http"
        assert (parsed.hostname, parsed.port) == ("127.0.0.1", urlsplit(self.http_url).port)
        path = (parsed.path or "/") + ("?" + parsed.query if parsed.query else "")
        reader, writer = await asyncio.open_connection(parsed.hostname, parsed.port)
        try:
            headers = [
                line
                for line in lines[1:]
                if line.split(b":", 1)[0].lower() not in (b"connection", b"proxy-connection")
            ]
            writer.write(
                b" ".join((method, path.encode("ascii"), version))
                + b"\r\n"
                + b"\r\n".join(headers)
                + b"\r\nConnection: close\r\n\r\n"
                + body
            )
            await writer.drain()
            return await reader.read()
        finally:
            await close(writer)

    async def receive_binary(self, reader, writer):
        data = await reader.readexactly(len(PAYLOAD))
        assert await reader.read() == b""  # Reply only after the client half-close traverses both hops.
        self.binary.append(data)
        self.binary_started.put_nowait(data)
        await self.binary_release.wait()
        writer.write(b"binary received: " + data[::-1])
        await writer.drain()


@pytest_asyncio.fixture
async def local_network(http_server, upstream_server, tcp_server, tls_context, monkeypatch):
    networks = []

    async def factory(protocol="http"):
        network = LocalNetwork(protocol)
        networks.append(network)
        network.binary_release.set()
        network.http_url = await http_server(network.handle)
        network.https_url = await http_server(network.handle, ssl_context=tls_context)
        network.upstream = await upstream_server(protocol, plain=network.forward_plain)

        async def reject_http(head, body):
            return http_response(503, b"rejected by local fixture")

        network.rejected = await upstream_server(protocol, reject=True, plain=reject_http)
        network.target = await tcp_server(network.receive_binary)
        monkeypatch.setattr(world, "BASE_URL", network.http_url + "/world")
        monkeypatch.setattr(proxyscrape, "API_URL", network.https_url + "/list")
        monkeypatch.setattr(validator, "REAL_IP_URL", network.https_url + "/ip")
        monkeypatch.setattr(validator, "PRIMARY_HTTP_URL", network.http_url + "/primary/http")
        monkeypatch.setattr(validator, "PRIMARY_HTTPS_URL", network.https_url + "/primary/https")
        monkeypatch.setattr(validator, "SECONDARY_URL", network.http_url + "/generate_204")
        monkeypatch.setattr(validator, "GEO_VERIFY_URL", network.http_url + "/geo")
        monkeypatch.setattr(validator, "PORT_PROBE_HOST", "127.0.0.1")
        monkeypatch.setattr(validator, "PORT_PROBE_PORT", urlsplit(network.http_url).port)
        # __init__ captured SECONDARY_URL at definition time. Change only that
        # endpoint default, not the constructor, validator, sessions, or network I/O.
        defaults = list(validator.ProxyValidator.__init__.__defaults__)
        defaults[3] = validator.SECONDARY_URL
        monkeypatch.setattr(validator.ProxyValidator.__init__, "__defaults__", tuple(defaults))
        return network

    try:
        yield factory
    finally:
        for network in networks:
            network.release.set()
            network.binary_release.set()


def seed(tmp_path, network):
    return {
        "state_file": str(tmp_path / "state.json"),
        "sources": network.sources,
        "pages": 1,
        "protocols": [network.protocol],
        "timeout": 1.5,
        "workers": 2,
        "secondary_check": True,
        "geo_lookup": False,
        "geo_verify_via_proxy": True,
        "recheck_after": 6,
    }


async def api(client, server, method, path, status=200, **kwargs):
    async with client.request(method, server.ui_url + path, **kwargs) as response:
        body = await response.json()
        assert response.status == status, body
        return body


async def finished_job(client, server):
    while True:
        status = await api(client, server, "GET", "/api/job")
        if not status["running"]:
            assert status["kind"] == "scrape"
            assert status["message"] == "done"
            assert status["error"] is None
            assert status["started_at"] <= status["finished_at"]
            return status
        await asyncio.sleep(0.001)


async def scrape(client, server):
    assert await api(client, server, "POST", "/api/scrape", 202, json={}) == {"started": True}
    return await finished_job(client, server)


def assert_records(body, network):
    assert body["count"] == 2
    rows = {row["key"]: row for row in body["proxies"]}
    assert set(rows) == {network.key, network.rejected_key}
    records = {}
    for alive, key, endpoint in (
        (True, network.key, network.upstream),
        (False, network.rejected_key, network.rejected),
    ):
        row = rows[key]
        checked = datetime.fromisoformat(row["checked_at"])
        assert checked.tzinfo is not None
        latency = row["latency_ms"]
        if alive:
            assert isinstance(latency, (int, float)) and math.isfinite(latency) and latency >= 0
            assert round(latency, 1) == latency
        else:
            assert latency is None
        record = {
            "ip": "127.0.0.1",
            "port": endpoint.port,
            "protocol": network.protocol,
            "source": "freeproxy_world" if network.protocol == "https" else "proxyscrape",
            "country": "Canada" if alive else ("Advertised country" if network.protocol == "https" else None),
            "city": "Ottawa" if alive else ("Advertised city" if network.protocol == "https" else None),
            "isp": "loopback network" if alive else None,
            "alive": alive,
            "latency_ms": latency,
            "supports_https": alive,
            "anonymity": "anonymous" if alive else None,
            "checked_at": row["checked_at"],
            "https_only": False if alive and network.protocol in ("http", "https") else None,
        }
        assert row == {
            **record,
            "key": key,
            "selected": False,
            "pool_failed": False,
            "pool_fail_count": 0,
            "pool_request_count": 0,
            "pool_success_count": 0,
        }
        records[key] = record
    return records


async def pool_status(client, server, network, requests):
    while True:
        status = await api(client, server, "GET", "/api/forward/status")
        if status["selected"] and status["selected"][0]["request_count"] >= requests:
            break
        await asyncio.sleep(0.001)
    assert status == {
        "selected": [
            {
                "key": network.key,
                "failed": False,
                "fail_count": 0,
                "request_count": requests,
                "success_count": requests,
                "failure_count": 0,
            }
        ],
        "count": 1,
        "usable_count": 1,
        "has_usable": True,
        "http_proxy": f"127.0.0.1:{server.http_address[1]}",
        "socks5_proxy": f"127.0.0.1:{server.socks_address[1]}",
    }


async def open_socks(server, target):
    reader, writer = await asyncio.open_connection(*server.socks_address)
    try:
        writer.write(b"\x05\x01\x00")
        await writer.drain()
        assert await reader.readexactly(2) == b"\x05\x00"
        writer.write(b"\x05\x01\x00\x01\x7f\x00\x00\x01" + target.port.to_bytes(2, "big"))
        await writer.drain()
        assert await reader.readexactly(10) == b"\x05\x00\x00\x01" + b"\x00" * 6
        return reader, writer
    except BaseException:
        await close(writer)
        raise


async def forward_payloads(client, server, network, phase):
    proxy_url = "http://127.0.0.1:" + str(server.http_address[1])
    for scheme, origin in (("http", network.http_url), ("https", network.https_url)):
        trace = phase + "-" + scheme
        async with client.post(
            origin + "/payload?phase=" + phase,
            proxy=proxy_url,
            data=PAYLOAD,
            headers={"X-Integration-Trace": trace},
        ) as response:
            assert response.status == 200
            assert response.headers["X-Origin-Trace"] == trace
            assert await response.read() == HTTP_REPLY
    reader, writer = await open_socks(server, network.target)
    try:
        writer.write(PAYLOAD)
        await writer.drain()
        writer.write_eof()
        assert await reader.readexactly(len(TCP_REPLY)) == TCP_REPLY
        assert await reader.read() == b""
    finally:
        await close(writer)
    await pool_status(client, server, network, 3)


def assert_validation_io(network):
    source_calls = [("http", "GET", "/world?page=1", None, b"")]
    if network.protocol != "https":
        source_calls.append(
            (
                "https",
                "GET",
                "/list?request=getproxies&protocol="
                + network.protocol
                + "&timeout=10000&country=all&ssl=all&anonymity=all",
                None,
                b"",
            )
        )
    assert network.calls == source_calls + [
        ("https", "GET", "/ip", None, b""),
        ("http", "GET", "/primary/http", None, b""),
        ("https", "GET", "/primary/https", None, b""),
        ("http", "GET", "/generate_204", None, b""),
        ("http", "GET", "/geo", None, b""),
    ]
    direct_port, tls_port = urlsplit(network.http_url).port, urlsplit(network.https_url).port
    ports = (
        [tls_port, direct_port]
        if network.protocol in ("http", "https")
        else [
            direct_port,
            tls_port,
            direct_port,
            direct_port,
        ]
    )
    assert [(d.host, d.port) for d in network.upstream.destinations] == [("127.0.0.1", p) for p in ports]
    assert network.upstream.connections == (5 if network.protocol in ("http", "https") else 4)
    assert network.rejected.connections == 2
    assert not network.rejected.destinations
    if network.protocol in ("http", "https"):
        assert [(head.split(b"\r\n", 1)[0], body) for head, body in network.upstream.requests] == [
            (f"GET {network.http_url}/primary/http HTTP/1.1".encode(), b""),
            (f"CONNECT 127.0.0.1:{tls_port} HTTP/1.1".encode(), b""),
            (f"GET {network.http_url}/generate_204 HTTP/1.1".encode(), b""),
            (f"CONNECT 127.0.0.1:{direct_port} HTTP/1.1".encode(), b""),
            (f"GET {network.http_url}/geo HTTP/1.1".encode(), b""),
        ]


@pytest.mark.parametrize("protocol", ["http", "https", "socks4", "socks5"])
@bounded
async def test_api_scrape_real_validation_forwarders_and_restart_restore(tmp_path, local_network, protocol):
    network = await local_network(protocol)
    settings_file, state_file = tmp_path / "settings.json", tmp_path / "state.json"
    config = seed(tmp_path, network)
    client_options = {"timeout": aiohttp.ClientTimeout(total=2), "trust_env": False}
    server = await start_server(settings_file, config, network_overrides=PORT_ZERO)
    try:
        async with aiohttp.ClientSession(**client_options) as client:
            assert await api(client, server, "GET", "/api/proxies") == {"proxies": [], "count": 0}
            assert (await scrape(client, server))["summary"] == network.summary
            records = assert_records(await api(client, server, "GET", "/api/proxies"), network)
            assert json.loads(state_file.read_text()) == records
            assert_validation_io(network)
            assert await api(client, server, "POST", "/api/forward/select", json={"keys": [network.key]}) == {
                "count": 1
            }
            await pool_status(client, server, network, 0)
            assert json.loads(settings_file.read_text())["selected_keys"] == [network.key]
            await forward_payloads(client, server, network, "first")
            rows = (await api(client, server, "GET", "/api/proxies"))["proxies"]
            assert {
                r["key"]: (r["selected"], r["pool_request_count"], r["pool_success_count"]) for r in rows
            } == {network.key: (True, 3, 3), network.rejected_key: (False, 0, 0)}
    finally:
        await server.close()

    calls_before_restart = list(network.calls)
    connections_before_restart = network.upstream.connections, network.rejected.connections
    # A new owner must read disk, not inherit the previous pool/store objects.
    restored = await start_server(settings_file, {"state_file": str(state_file)}, network_overrides=PORT_ZERO)
    try:
        assert restored.pool is not server.pool and restored.store is not server.store
        assert network.calls == calls_before_restart
        assert (network.upstream.connections, network.rejected.connections) == connections_before_restart
        assert restored.settings.get("sources") == network.sources
        async with aiohttp.ClientSession(**client_options) as client:
            await pool_status(client, restored, network, 0)
            rows = (await api(client, restored, "GET", "/api/proxies"))["proxies"]
            assert {r["key"]: {k: r[k] for k in records[r["key"]]} for r in rows} == records
            assert {r["key"]: r["selected"] for r in rows} == {network.key: True, network.rejected_key: False}
            # Persisted fresh alive AND dead results suppress all validation I/O.
            assert (await scrape(client, restored))["summary"] == {
                **network.summary,
                "freshly_checked": 0,
                "reused_alive": 1,
            }
            assert network.calls[len(calls_before_restart) :] == calls_before_restart[: len(network.sources)]
            assert (network.upstream.connections, network.rejected.connections) == connections_before_restart
            await forward_payloads(client, restored, network, "restored")
            assert json.loads(state_file.read_text()) == records
            assert json.loads(settings_file.read_text())["selected_keys"] == [network.key]
    finally:
        await restored.close()

    assert [call for call in network.calls if call[2].startswith("/payload")] == [
        (scheme, "POST", "/payload?phase=" + phase, phase + "-" + scheme, PAYLOAD)
        for phase in ("first", "restored")
        for scheme in ("http", "https")
    ]
    assert network.binary == [PAYLOAD, PAYLOAD]
    assert network.upstream.connections == (11 if protocol in ("http", "https") else 10)
    assert network.rejected.connections == 2
    direct_port, tls_port = urlsplit(network.http_url).port, urlsplit(network.https_url).port
    forwarded_ports = (
        [tls_port, network.target.port]
        if protocol in ("http", "https")
        else [direct_port, tls_port, network.target.port]
    )
    assert [(d.host, d.port) for d in network.upstream.destinations[-2 * len(forwarded_ports) :]] == [
        ("127.0.0.1", port) for port in forwarded_ports * 2
    ]
    if protocol in ("http", "https"):
        assert [(head.split(b"\r\n", 1)[0], body) for head, body in network.upstream.requests[-6:]] == [
            item
            for phase in ("first", "restored")
            for item in (
                (f"POST {network.http_url}/payload?phase={phase} HTTP/1.1".encode(), PAYLOAD),
                (f"CONNECT 127.0.0.1:{tls_port} HTTP/1.1".encode(), b""),
                (f"CONNECT 127.0.0.1:{network.target.port} HTTP/1.1".encode(), b""),
            )
        ]


@pytest.mark.parametrize("blocked_path", ["/world", "/primary/http"], ids=["scraping", "validating"])
@bounded
async def test_api_job_and_manual_validation_remain_responsive_during_real_io(
    tmp_path,
    local_network,
    blocked_path,
):
    network = await local_network()
    network.block(blocked_path)
    server = await start_server(
        tmp_path / "settings.json", seed(tmp_path, network), network_overrides=PORT_ZERO
    )
    manual = None
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=3), trust_env=False) as client:
            assert await api(client, server, "POST", "/api/scrape", 202, json={}) == {"started": True}
            await network.entered.wait()
            for _ in range(3):
                status = await api(client, server, "GET", "/api/job")
                assert status["running"] is True and status["finished_at"] is None
                assert status["summary"] == {} and status["error"] is None
                assert status["message"].startswith(
                    "scraping" if blocked_path == "/world" else "validating 2"
                )
                assert await api(client, server, "GET", "/api/proxies") == {"proxies": [], "count": 0}
                settings = await api(client, server, "GET", "/api/settings")
                assert settings["settings"]["sources"] == network.sources
                assert settings["restart_pending"] is False
                assert not network.release.is_set()
            assert await api(client, server, "POST", "/api/scrape", 409, json={}) == {
                "error": "a job is already running",
            }
            assert not (tmp_path / "state.json").exists()
            network.release.set()
            assert (await finished_job(client, server))["summary"] == network.summary
            records = assert_records(await api(client, server, "GET", "/api/proxies"), network)
            assert_validation_io(network)

            network.block("/primary/http")
            manual = asyncio.create_task(
                api(client, server, "POST", "/api/validate", json={"keys": [network.key]})
            )
            await network.entered.wait()
            assert not manual.done()
            assert (await api(client, server, "GET", "/api/job"))["running"] is False
            assert (await api(client, server, "GET", "/api/proxies"))["count"] == 2
            assert (await api(client, server, "GET", "/api/settings"))["settings"]["workers"] == 2
            assert not manual.done() and not network.release.is_set()
            network.release.set()
            result = await manual
            assert set(result) == {"checked", "alive", "results"}
            assert result["checked"] == result["alive"] == len(result["results"]) == 1
            updated = result["results"][0]
            assert {k: v for k, v in updated.items() if k not in ("latency_ms", "checked_at")} == {
                k: v for k, v in records[network.key].items() if k not in ("latency_ms", "checked_at")
            }
            assert updated["latency_ms"] >= 0 and datetime.fromisoformat(updated["checked_at"]).tzinfo
            assert json.loads((tmp_path / "state.json").read_text()) == {**records, network.key: updated}
            assert [call[2] for call in network.calls[-5:]] == [
                "/ip",
                "/primary/http",
                "/primary/https",
                "/generate_204",
                "/geo",
            ]
    finally:
        network.release.set()
        if manual is not None:
            manual.cancel()
            await asyncio.gather(manual, return_exceptions=True)
        await server.close()


@pytest.mark.parametrize("action", ["restart", "stop"])
@bounded
async def test_run_server_drains_real_job_and_both_active_payload_tunnels(tmp_path, local_network, action):
    network = await local_network()
    ready, stop = asyncio.Event(), asyncio.Event()
    servers, callbacks, clients = [], [], []
    state_file, settings_file = tmp_path / "state.json", tmp_path / "settings.json"

    def started(server):
        servers.append(server)
        ready.set()

    def restarted():
        assert servers[0].closed
        assert not servers[0].jobs._tasks
        assert not servers[0].http_proxy._handlers and not servers[0].socks_proxy._handlers
        assert json.loads(settings_file.read_text())["selected_keys"] == [network.key]
        callbacks.append("after real I/O drained")

    runner = asyncio.create_task(
        run_server(
            settings_file,
            seed(tmp_path, network),
            network_overrides=PORT_ZERO,
            on_started=started,
            stop_event=stop,
            restart_callback=restarted,
            handle_signals=False,
        )
    )
    try:
        await ready.wait()
        server = servers[0]
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=2), trust_env=False) as client:
            assert (await scrape(client, server))["summary"] == network.summary
            records = assert_records(await api(client, server, "GET", "/api/proxies"), network)
            assert await api(client, server, "POST", "/api/forward/select", json={"keys": [network.key]}) == {
                "count": 1
            }
            network.binary_release.clear()
            reader, writer = await asyncio.open_connection(*server.http_address)
            clients.append((reader, writer))
            writer.write(f"CONNECT 127.0.0.1:{network.target.port} HTTP/1.1\r\n\r\n".encode())
            await writer.drain()
            assert await reader.readuntil(b"\r\n\r\n") == b"HTTP/1.1 200 Connection Established\r\n\r\n"
            clients.append(await open_socks(server, network.target))
            for _, writer in clients:
                writer.write(PAYLOAD)
                await writer.drain()
                writer.write_eof()
            for _ in clients:
                assert await network.binary_started.get() == PAYLOAD
            assert server.http_proxy._handlers and server.socks_proxy._handlers
            network.block("/world")
            assert await api(client, server, "POST", "/api/scrape", 202, json={}) == {"started": True}
            await network.entered.wait()
            assert (await api(client, server, "GET", "/api/job"))["running"] is True
            if action == "restart":
                assert await api(client, server, "POST", "/api/restart") == {"restarting": True}
            else:
                stop.set()
            await runner
            assert callbacks == (["after real I/O drained"] if action == "restart" else [])
            assert server.closed and not server.jobs._tasks
            assert server.jobs.status()["message"] == "cancelled"
            assert not server.http_proxy._handlers and not server.socks_proxy._handlers
            for reader, _ in clients:
                assert await reader.read() == b""  # No origin reply was released before shutdown.
            assert json.loads(state_file.read_text()) == records
            assert json.loads(settings_file.read_text())["selected_keys"] == [network.key]
            assert network.binary == [PAYLOAD, PAYLOAD]
            assert not network.release.is_set() and not network.binary_release.is_set()
    finally:
        network.release.set()
        network.binary_release.set()
        stop.set()
        runner.cancel()
        await asyncio.gather(runner, return_exceptions=True)
        await asyncio.gather(*(close(writer) for _, writer in clients))
