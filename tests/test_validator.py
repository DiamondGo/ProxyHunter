import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock
from urllib.parse import urlsplit

import pytest
from aiohttp import web

import proxyhunter.validator as module
from proxyhunter.models import Proxy
from proxyhunter.validator import ProxyValidator, _CheckResult
from tests.network_fixtures import bounded, http_response


def test_validator_constructor_no_io(monkeypatch):
    get_ip = AsyncMock(return_value="203.0.113.1")
    monkeypatch.setattr(ProxyValidator, "_get_real_ip", get_ip)
    validator = ProxyValidator()
    assert validator.real_ip is None
    get_ip.assert_not_called()


@pytest.mark.parametrize("protocol", ["http", "https", "socks4", "socks5"])
@bounded
async def test_validator_real_http_https_secondary_geo_and_connect_probe(
    http_server,
    upstream_server,
    tls_context,
    monkeypatch,
    protocol,
):
    counts = {"ip": 0, "secondary": 0, "geo": 0}

    def data(path):
        if path == "/ip":
            counts["ip"] += 1
            return 200, {"ip": "203.0.113.1"}
        if path == "/generate_204":
            counts["secondary"] += 1
            return 204, b""
        if path == "/geo":
            counts["geo"] += 1
            return 200, {"status": "success", "country": "Canada", "city": "Ottawa", "isp": "loopback"}
        return 200, {"origin": "198.51.100.8", "headers": {"Via": "fixture"}}

    async def handler(request):
        status, body = data(request.path)
        return (
            web.json_response(body, status=status) if isinstance(body, dict) else web.Response(status=status)
        )

    direct = await http_server(handler)
    secure = await http_server(handler, ssl_context=tls_context)
    monkeypatch.setattr(module, "REAL_IP_URL", direct + "/ip")
    monkeypatch.setattr(module, "PRIMARY_HTTP_URL", direct + "/http")
    monkeypatch.setattr(module, "PRIMARY_HTTPS_URL", secure + "/https")
    monkeypatch.setattr(module, "GEO_VERIFY_URL", direct + "/geo")
    monkeypatch.setattr(module, "PORT_PROBE_HOST", "127.0.0.1")
    monkeypatch.setattr(module, "PORT_PROBE_PORT", urlsplit(direct).port)

    async def plain(head, body):
        path = urlsplit(head.split(b" ")[1].decode()).path
        return http_response(*data(path))

    upstream = await upstream_server(protocol, plain=plain)
    proxy = upstream.proxy(country="Advertised", city="Old")
    validator = ProxyValidator(timeout=1, secondary_url=direct + "/generate_204", geo_verify_via_proxy=True)
    assert await validator.validate_all([]) == []
    assert counts["ip"] == 0
    assert await validator.validate_all([proxy]) == [proxy]
    assert proxy.alive and proxy.supports_https
    assert proxy.anonymity == "anonymous"
    assert proxy.latency_ms is not None and proxy.latency_ms >= 0
    assert proxy.https_only is (False if protocol in ("http", "https") else None)
    assert (proxy.country, proxy.city, proxy.isp) == ("Canada", "Ottawa", "loopback")
    assert proxy.checked_at
    await validator.validate_all([proxy])
    assert counts == {"ip": 1, "secondary": 2, "geo": 2}


@pytest.mark.parametrize(
    "http_status,https_status,secondary_status,alive,supports",
    [
        (500, 502, 204, False, False),
        (200, 502, 204, True, False),
        (500, 200, 204, True, True),
        (200, 200, 200, False, True),
        (302, 502, 204, True, False),
    ],
)
@bounded
async def test_validator_status_and_secondary_semantics(
    http_server,
    upstream_server,
    monkeypatch,
    http_status,
    https_status,
    secondary_status,
    alive,
    supports,
):
    async def handler(request):
        return web.json_response({"ip": "203.0.113.1"})

    monkeypatch.setattr(module, "REAL_IP_URL", await http_server(handler))
    monkeypatch.setattr(module, "PRIMARY_HTTP_URL", "http://fake.test/http")
    monkeypatch.setattr(module, "PRIMARY_HTTPS_URL", "http://fake.test/https")

    async def plain(head, body):
        path = urlsplit(head.split(b" ")[1].decode()).path
        status = {"/http": http_status, "/https": https_status, "/generate_204": secondary_status}[path]
        return http_response(status, b"not-json", headers={"Location": "http://must-not-follow.invalid"})

    upstream = await upstream_server(plain=plain, reject=True)
    p = upstream.proxy(alive=True, latency_ms=999, anonymity="elite", https_only=False)
    validator = ProxyValidator(timeout=0.2, secondary_url="http://fake.test/generate_204")
    assert await validator.validate_all([p]) == [p]
    assert p.alive is alive
    assert p.supports_https is supports
    if alive:
        assert p.anonymity == "elite"  # non-JSON was accepted by Requests, keep that semantic
        assert p.https_only is (True if supports else None)
    else:
        assert p.latency_ms is None
        assert p.anonymity is None


@pytest.mark.parametrize("body", [[], None, {"origin": [], "headers": []}, {"origin": 7, "headers": None}])
@bounded
async def test_validator_malformed_json_shapes_do_not_drop_results(
    http_server, upstream_server, monkeypatch, body
):
    async def handler(request):
        return web.json_response({"ip": "203.0.113.1"})

    monkeypatch.setattr(module, "REAL_IP_URL", await http_server(handler))
    monkeypatch.setattr(module, "PRIMARY_HTTP_URL", "http://fake.test/http")
    monkeypatch.setattr(module, "PRIMARY_HTTPS_URL", "http://fake.test/https")

    async def plain(head, _body):
        import json

        return http_response(200, json.dumps(body))

    upstream = await upstream_server(plain=plain, reject=True)
    p = upstream.proxy()
    assert await ProxyValidator(secondary_check=False).validate_all([p]) == [p]
    assert p.alive


@pytest.mark.parametrize(
    "headers,origin,expected",
    [
        ({}, "203.0.113.1, 198.51.100.1", "transparent"),
        ({"X-Real-IP": "203.0.113.1"}, "198.51.100.1", "transparent"),
        ({"Forwarded": "for=203.0.113.1"}, "", "transparent"),
        ({"Via": "proxy"}, "198.51.100.1", "anonymous"),
        ({}, "198.51.100.1", "elite"),
    ],
)
def test_anonymity_classification(headers, origin, expected):
    validator = ProxyValidator()
    assert validator._classify_anonymity(_CheckResult(True)) == "unknown"
    validator.real_ip = "203.0.113.1"
    assert validator._classify_anonymity(_CheckResult(True, origin=origin, headers=headers)) == expected


@bounded
async def test_minimum_successful_primary_latency(upstream_server, monkeypatch):
    monkeypatch.setattr(ProxyValidator, "_get_real_ip", AsyncMock(return_value=None))
    monkeypatch.setattr(module, "PRIMARY_HTTP_URL", "http://fake.test/http")
    monkeypatch.setattr(module, "PRIMARY_HTTPS_URL", "http://fake.test/https")
    values = iter([0.0, 0.02346, 1.0, 1.01])
    monkeypatch.setattr(module, "time", SimpleNamespace(perf_counter=lambda: next(values)))
    upstream = await upstream_server(reject=True)
    p = upstream.proxy()
    await ProxyValidator(secondary_check=False).validate_all([p])
    assert p.latency_ms == 10.0


@bounded
async def test_validation_network_concurrency_responsiveness_and_dead_results(upstream_server, monkeypatch):
    monkeypatch.setattr(ProxyValidator, "_get_real_ip", AsyncMock(return_value=None))
    monkeypatch.setattr(module, "PRIMARY_HTTP_URL", "http://fake.test/http")
    monkeypatch.setattr(module, "PRIMARY_HTTPS_URL", "http://fake.test/https")
    active = 0
    peak = 0

    async def plain(head, body):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        try:
            await asyncio.sleep(0.01)
            return http_response(200)
        finally:
            active -= 1

    upstream = await upstream_server(plain=plain, reject=True)
    dead = await upstream_server(stall=True)
    proxies = [upstream.proxy() for _ in range(30)] + [dead.proxy()]
    task = asyncio.create_task(
        ProxyValidator(timeout=0.15, workers=3, secondary_check=False).validate_all(proxies)
    )
    ticks = 0
    while not task.done():
        ticks += 1
        await asyncio.sleep(0.003)
    results = await task
    assert results == proxies
    assert sum(p.alive for p in results) == 30
    assert all(p.checked_at for p in results)
    assert 1 < peak <= 3
    assert ticks >= 10


@bounded
async def test_large_list_creates_only_bounded_worker_tasks(monkeypatch):
    monkeypatch.setattr(ProxyValidator, "_get_real_ip", AsyncMock(return_value=None))

    async def check(self, p):
        await asyncio.sleep(0)
        return p

    monkeypatch.setattr(ProxyValidator, "_check_one", check)
    original = asyncio.create_task
    workers = []

    def create_task(coro, *args, **kwargs):
        task = original(coro, *args, **kwargs)
        if "validate_all.<locals>.worker" in coro.__qualname__:
            workers.append(task)
        return task

    monkeypatch.setattr(asyncio, "create_task", create_task)
    proxies = [Proxy("127.0.0.1", i + 1, "http", "test") for i in range(10000)]
    assert await ProxyValidator(workers=4).validate_all(proxies) == proxies
    assert len(workers) == 4
    assert all(t.done() for t in workers)


@bounded
async def test_cancel_validation_joins_workers_and_closes_sessions(upstream_server, monkeypatch):
    monkeypatch.setattr(ProxyValidator, "_get_real_ip", AsyncMock(return_value=None))
    monkeypatch.setattr(module, "PRIMARY_HTTP_URL", "http://fake.test/http")
    upstream = await upstream_server(stall=True)
    proxies = [upstream.proxy() for _ in range(100)]
    task = asyncio.create_task(ProxyValidator(timeout=10, workers=4).validate_all(proxies))
    await upstream.entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    for _ in range(100):
        if not upstream.active:
            break
        await asyncio.sleep(0.002)
    assert upstream.active == 0
    assert upstream.connections <= 4


@bounded
async def test_real_ip_failure_once_even_with_concurrent_validate_calls(
    http_server, upstream_server, monkeypatch
):
    calls = 0

    async def handler(request):
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.01)
        return web.Response(status=500)

    monkeypatch.setattr(module, "REAL_IP_URL", await http_server(handler))
    monkeypatch.setattr(module, "PRIMARY_HTTP_URL", "http://fake.test/http")
    monkeypatch.setattr(module, "PRIMARY_HTTPS_URL", "http://fake.test/https")
    upstream = await upstream_server(reject=True)
    validator = ProxyValidator(secondary_check=False)
    a, b = upstream.proxy(), upstream.proxy()
    await asyncio.gather(validator.validate_all([a]), validator.validate_all([b]))
    assert calls == 1
    assert a.anonymity == b.anonymity == "unknown"
