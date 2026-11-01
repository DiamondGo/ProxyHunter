import asyncio
from urllib.parse import urlsplit

import aiohttp
import pytest
from aiohttp import web

import proxyhunter.geolocate as geo
import proxyhunter.scrapers.freeproxy_world as world
from proxyhunter import scrapers
from proxyhunter.models import Proxy
from proxyhunter.scrapers import fallback, proxyscrape
from tests.network_fixtures import bounded, http_response


@bounded
async def test_get_direct_redirects_params_and_detached_response(http_server, upstream_server):
    async def handler(request):
        if request.path == "/redirect":
            raise web.HTTPFound("/final?q=yes")
        assert request.query["q"] == "yes"
        return web.json_response({"ok": True})

    url = await http_server(handler)
    unused = await upstream_server()
    async with aiohttp.ClientSession() as session:
        response = await fallback.get_with_fallback(
            session, url + "/redirect", params={"start": "1"}, fallback_proxies=[unused.proxy()]
        )
    assert response.status == response.status_code == 200
    assert response.json() == {"ok": True}
    assert response.text == '{"ok": true}'
    assert unused.connections == 0


@bounded
async def test_fallback_direct_then_distinct_up_to_three_and_last_error(http_server, upstream_server):
    calls = []

    async def direct(request):
        calls.append("direct")
        return web.Response(status=503)

    url = await http_server(direct)
    proxies = []
    for i in range(4):

        async def plain(head, body, i=i):
            calls.append(i)
            return http_response(502 if i < 3 else 200)

        proxies.append(await upstream_server(plain=plain))
    candidates = [proxies[0].proxy(), proxies[0].proxy()] + [p.proxy() for p in proxies[1:]]
    async with aiohttp.ClientSession() as session:
        with pytest.raises(aiohttp.ClientResponseError) as exc:
            await fallback.get_with_fallback(
                session, url, fallback_proxies=candidates, max_fallback_attempts=100
            )
    assert exc.value.status == 502
    assert calls == ["direct", 0, 1, 2]


@bounded
async def test_fallback_success_and_headers_are_preserved(http_server, upstream_server):
    async def direct(request):
        return web.Response(status=403)

    url = await http_server(direct)

    async def plain(head, body):
        assert b"User-Agent: fixture-agent" in head
        assert b"?page=2 " in head
        return http_response(200, "fallback succeeded")

    upstream = await upstream_server(plain=plain)
    async with aiohttp.ClientSession(headers={"User-Agent": "fixture-agent"}) as session:
        response = await fallback.get_with_fallback(
            session, url, params={"page": 2}, fallback_proxies=[upstream.proxy()]
        )
        assert not session.closed
    assert response.text == "fallback succeeded"


@pytest.mark.parametrize("scheme,remote", [("socks4", False), ("socks5", False), ("socks5h", True)])
@bounded
async def test_http_socks_explicit_dns_semantics(http_server, upstream_server, monkeypatch, scheme, remote):
    async def handler(request):
        return web.json_response({"path": request.path})

    url = await http_server(handler)
    target = urlsplit(url)
    loop = asyncio.get_running_loop()
    original = loop.getaddrinfo
    resolved = []

    async def getaddrinfo(host, port, *args, **kwargs):
        if host == "dns-test.invalid":
            resolved.append(host)
            host = "127.0.0.1"
        return await original(host, port, *args, **kwargs)

    monkeypatch.setattr(loop, "getaddrinfo", getaddrinfo)
    upstream = await upstream_server(
        "socks4" if scheme == "socks4" else "socks5", map_host={"dns-test.invalid": "127.0.0.1"}
    )
    async with fallback.proxy_session(f"{scheme}://{upstream.host}:{upstream.port}", 1) as (session, kwargs):
        response = await fallback._get(session, f"http://dns-test.invalid:{target.port}/test", **kwargs)
    assert response.json() == {"path": "/test"}
    assert bool(resolved) is not remote
    dest = upstream.destinations[0]
    assert dest.atyp == (3 if remote else 1)
    assert dest.host == ("dns-test.invalid" if remote else "127.0.0.1")


@bounded
async def test_fallback_cancellation_does_not_attempt_proxy(http_server, upstream_server):
    entered = asyncio.Event()
    release = asyncio.Event()

    async def handler(request):
        entered.set()
        await release.wait()
        return web.Response()

    url = await http_server(handler)
    upstream = await upstream_server()
    async with aiohttp.ClientSession() as session:
        task = asyncio.create_task(
            fallback.get_with_fallback(session, url, fallback_proxies=[upstream.proxy()])
        )
        await entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    release.set()
    assert upstream.connections == 0


@bounded
async def test_response_size_limit(http_server, monkeypatch):
    async def handler(request):
        return web.Response(body=b"x" * 100)

    url = await http_server(handler)
    monkeypatch.setattr(fallback, "MAX_RESPONSE_BYTES", 10)
    async with aiohttp.ClientSession() as session:
        with pytest.raises(aiohttp.ClientPayloadError):
            await fallback.get_with_fallback(session, url)


@bounded
async def test_scrapers_real_http_source_isolation_and_nonblocking_delay(http_server, monkeypatch):
    pages = []

    async def handler(request):
        if request.path == "/world":
            pages.append(int(request.query["page"]))
            if request.query["page"] == "1":
                return web.Response(status=503)
            return web.Response(
                text="""<table class="table"><tbody><tr><td>1.2.3.4</td><td>80</td>
            <td></td><td></td><td></td><td><a>http</a></td><td>yes</td></tr></tbody></table>"""
            )
        assert request.query["request"] == "getproxies"
        if request.query["protocol"] == "socks4":
            return web.Response(status=503)
        return web.Response(text="2.3.4.5:1080\ninvalid\n3.4.5.6:bad")

    url = await http_server(handler)
    monkeypatch.setattr(world, "BASE_URL", url + "/world")
    monkeypatch.setattr(proxyscrape, "API_URL", url + "/api")
    scraper = world.FreeProxyWorldScraper(pages=3, delay=0.04)
    task = asyncio.create_task(scraper.fetch())
    ticks = 0
    while not task.done():
        ticks += 1
        await asyncio.sleep(0.005)
    assert len(await task) == 2
    assert ticks >= 5
    assert pages == [1, 2, 3]
    results = await scrapers.scrape_all(
        ["nonexistent", "proxyscrape"], protocols=["http", "socks4", "socks5"]
    )
    assert [p.protocol for p in results] == ["http", "socks5"]


@bounded
async def test_scrape_all_isolates_constructor_and_fetch_failures(monkeypatch):
    class BadConstructor:
        def __init__(self):
            raise RuntimeError("bad constructor")

    class BadFetch:
        async def fetch(self, **kwargs):
            raise RuntimeError("bad fetch")

    class Good:
        async def fetch(self, **kwargs):
            return [Proxy("1.2.3.4", 80, "http", "good")]

    monkeypatch.setattr(scrapers, "SCRAPERS", {"a": BadConstructor, "b": BadFetch, "c": Good})
    assert len(await scrapers.scrape_all(["a", "b", "c"])) == 1


@bounded
async def test_geo_batch_mutation_and_bad_responses(http_server, monkeypatch):
    calls = []

    async def handler(request):
        values = await request.json()
        calls.append(values)
        if len(calls) == 1:
            return web.Response(text="not json")
        if len(calls) == 2:
            return web.json_response({"bad": "shape"})
        if len(calls) == 3:
            return web.Response(status=429)
        return web.json_response(
            [None, 4]
            + [
                {"status": "success", "query": ip, "country": "Canada", "city": "Vancouver", "isp": "local"}
                for ip in values
                if ip != "5.0.0.1"
            ]
        )

    url = await http_server(handler)
    monkeypatch.setattr(geo, "BATCH_URL", url)
    monkeypatch.setattr(geo, "BATCH_SIZE", 1)
    proxies = [Proxy(f"{i}.0.0.1", 80, "http", "test") for i in range(1, 7)]
    proxies.append(Proxy("7.0.0.1", 80, "http", "test", country="Original"))
    await geo.fill_missing_geo(proxies)
    assert len(calls) == 6
    assert [p.country for p in proxies] == [None, None, None, "Canada", None, "Canada", "Original"]
    assert proxies[3].city == "Vancouver"
    assert proxies[3].isp == "local"


@bounded
async def test_geo_empty_no_request_and_duplicates(http_server, monkeypatch):
    calls = []

    async def handler(request):
        calls.append(await request.json())
        return web.json_response([{"status": "success", "query": "1.2.3.4", "country": "Canada"}])

    monkeypatch.setattr(geo, "BATCH_URL", await http_server(handler))
    await geo.fill_missing_geo([])
    assert calls == []
    proxies = [Proxy("1.2.3.4", p, "http", "test") for p in (80, 8080)]
    await geo.fill_missing_geo(proxies)
    assert [p.country for p in proxies] == ["Canada", "Canada"]
