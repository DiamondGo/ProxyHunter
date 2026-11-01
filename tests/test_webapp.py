"""Browser contracts against real local aiohttp sockets, without public I/O."""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from aiohttp.test_utils import TestClient, TestServer

import proxyhunter.jobs as jobs_module
from proxyhunter import webapp
from proxyhunter.forwarder.pool import ProxyPool
from proxyhunter.jobs import JobRunner
from proxyhunter.models import Proxy
from proxyhunter.settings import NETWORK_KEYS, SCRAPE_KEYS, SettingsStore
from proxyhunter.store import ProxyStore
from tests.network_fixtures import bounded

KEY_ROUTES = (
    "/api/proxies/remove",
    "/api/validate",
    "/api/forward/select",
    "/api/forward/add",
    "/api/forward/remove",
    "/api/forward/reactivate",
)
POST_ROUTES = (*KEY_ROUTES, "/api/scrape", "/api/settings", "/api/restart")


@pytest_asyncio.fixture
async def api(tmp_path):
    settings = SettingsStore(tmp_path / "settings.json")
    store = ProxyStore(tmp_path / "state.json")
    await settings.load()
    await store.load()
    await settings.update({"geo_lookup": False, "pool_fail_threshold": 1})
    proxies = [
        Proxy("127.0.0.1", 10001, "http", "local", alive=True, latency_ms=20),
        Proxy("127.0.0.1", 10002, "socks5", "local", alive=False, latency_ms=1),
        Proxy("127.0.0.1", 10003, "http", "local", alive=True, latency_ms=5),
        Proxy("127.0.0.1", 10004, "http", "local", alive=True),
    ]
    store.upsert_all(proxies)
    await store.save()
    pool = ProxyPool(settings)
    jobs = JobRunner(store, settings)
    active = {key: settings.get(key) for key in NETWORK_KEYS}
    app = webapp.create_app(store, pool, jobs, settings, active)
    async with TestClient(TestServer(app)) as client:
        yield SimpleNamespace(
            client=client,
            app=app,
            store=store,
            pool=pool,
            jobs=jobs,
            settings=settings,
            proxies=proxies,
            active=active,
        )
    await jobs.shutdown()
    await store.flush()
    await settings.flush()
    assert not app[webapp.WEB_STATE].requests


async def post(api, route, keys):
    response = await api.client.post(route, json={"keys": keys})
    assert response.status == 200, await response.text()
    return await response.json()


@bounded
async def test_pages_and_every_static_asset_are_unchanged(api):
    for route, name in (("/", "index.html"), ("/settings", "settings.html")):
        async with api.client.get(route) as response:
            assert response.status == 200
            assert response.content_type == "text/html"
            assert await response.read() == (webapp.BASE_DIR / "templates" / name).read_bytes()
    for path in (webapp.BASE_DIR / "static").rglob("*"):
        if path.is_file():
            async with api.client.get(
                "/static/" + str(path.relative_to(webapp.BASE_DIR / "static"))
            ) as response:
                assert response.status == 200
                assert await response.read() == path.read_bytes()
    for route in ("/missing", "/static/missing", "/static/", "/static/../templates/index.html"):
        response = await api.client.get(route)
        assert response.status in (403, 404)
    assert (await api.client.get("/api/restart")).status == 405


@bounded
async def test_list_sort_pool_statistics_and_reactivation(api):
    p = api.proxies[0]
    assert await post(api, "/api/forward/select", [p.key_str()]) == {"count": 1}
    api.pool.report_result(p, False)
    response = await api.client.get("/api/proxies")
    body = await response.json()
    assert body["count"] == 4
    assert [row["port"] for row in body["proxies"]] == [10003, 10001, 10004, 10002]
    row = body["proxies"][1]
    assert row == {
        **p.to_dict(),
        "key": p.key_str(),
        "selected": True,
        "pool_failed": True,
        "pool_fail_count": 1,
        "pool_request_count": 1,
        "pool_success_count": 0,
    }
    status = await (await api.client.get("/api/forward/status")).json()
    assert status == {
        "count": 1,
        "usable_count": 0,
        "has_usable": False,
        "http_proxy": "127.0.0.1:9528",
        "socks5_proxy": "127.0.0.1:9529",
        "selected": [{"key": p.key_str(), **api.pool.get_status()[p.key_str()]}],
    }
    body = await post(api, "/api/forward/reactivate", [p.key_str(), "unknown"])
    assert body["status"][p.key_str()]["failed"] is False
    assert body["status"][p.key_str()]["fail_count"] == 0
    assert body["status"][p.key_str()]["request_count"] == 1
    status = await (await api.client.get("/api/forward/status")).json()
    assert status["has_usable"] is True
    assert status["usable_count"] == 1


@bounded
async def test_selection_merge_removal_and_persistence(api):
    a, b = [p.key_str() for p in api.proxies[:2]]
    assert await post(api, "/api/forward/select", [a, "unknown"]) == {"count": 1}
    assert await post(api, "/api/forward/add", [b, a, b]) == {"count": 2}
    assert json.loads(api.settings.path.read_text())["selected_keys"] == [a, b]
    assert await post(api, "/api/forward/remove", [a, "unknown"]) == {"count": 1}
    assert api.settings.get("selected_keys") == [b]
    assert await post(api, "/api/proxies/remove", [b, b, "unknown"]) == {"removed": 1}
    assert api.store.get_by_key(b) is None
    assert b not in json.loads(api.store.path.read_text())
    assert not api.pool.get_proxies()
    assert json.loads(api.settings.path.read_text())["selected_keys"] == []
    assert await post(api, "/api/forward/select", [a]) == {"count": 1}
    assert await post(api, "/api/forward/select", []) == {"count": 0}


@bounded
async def test_settings_contract_pending_and_permissive_coercion(api):
    before = await (await api.client.get("/api/settings")).json()
    assert before["scrape_keys"] == list(SCRAPE_KEYS)
    assert before["network_keys"] == list(NETWORK_KEYS)
    assert before["active_network"] == api.active
    assert before["restart_pending"] is False
    assert "selected_keys" not in before["settings"]
    patch = {
        "workers": "10001",
        "pages": 1001,
        "timeout": "0.01",
        "ui_port": "0",
        "forward_host": "",
        "pool_max_retries": 0,
        "recheck_after": 0,
        "limit": 0,
        "sched_pool_refresh_interval_hours": 0,
        "sources": [],
        "protocols": "http,socks5",
        "secondary_check": "off",
        "ui_language": "custom-language",
        "unknown": "ignored",
        "sched_full_scrape_last_run": 1234,
    }
    response = await api.client.post("/api/settings", json=patch)
    assert response.status == 200
    assert await response.json() == {
        "saved": True,
        "restart_required": True,
        "changed_network_fields": ["ui_port", "forward_host"],
    }
    body = await (await api.client.get("/api/settings")).json()
    assert body["restart_pending"] is True
    assert body["active_network"] == before["active_network"]
    assert body["settings"]["timeout"] == 0.01
    assert body["settings"]["workers"] == 10001
    assert body["settings"]["protocols"] == ["http", "socks5"]
    assert body["settings"]["secondary_check"] is False
    assert body["settings"]["sched_full_scrape_last_run"] == 0
    assert "unknown" not in body["settings"]
    saved = json.loads(api.settings.path.read_text())
    assert saved["ui_port"] == 0
    assert saved["forward_host"] == ""
    response = await api.client.post(
        "/api/settings", json={"ui_port": api.active["ui_port"], "forward_host": api.active["forward_host"]}
    )
    assert response.status == 200
    assert (await (await api.client.get("/api/settings")).json())["restart_pending"] is False
    assert (await (await api.client.post("/api/settings", json={})).json())["restart_required"] is False


@pytest.mark.parametrize("route", POST_ROUTES)
@pytest.mark.parametrize("raw", ["{broken", "[]", "null", "false", '"text"', "42", '{"x": NaN}', b"\xff"])
@bounded
async def test_invalid_json_and_roots_are_json400(api, route, raw):
    before = api.settings.snapshot()
    response = await api.client.post(route, data=raw, headers={"Content-Type": "application/json"})
    assert response.status == 400
    assert isinstance((await response.json())["error"], str)
    assert api.settings.snapshot() == before
    assert not api.app[webapp.WEB_STATE].restart_event.is_set()


@pytest.mark.parametrize("route", KEY_ROUTES)
@pytest.mark.parametrize("keys", [None, "http://host:80", {}, 1, [None], [False], [[]], [1], [""]])
@bounded
async def test_invalid_keys_are_json400(api, route, keys):
    response = await api.client.post(route, json={"keys": keys})
    assert response.status == 400
    assert "error" in await response.json()
    assert len(api.store.records) == 4


@pytest.mark.parametrize("route", ["/api/settings", "/api/scrape"])
@pytest.mark.parametrize(
    "patch",
    [
        {"workers": 0},
        {"workers": True},
        {"pages": 1.5},
        {"timeout": "nan"},
        {"timeout": 0},
        {"secondary_check": "maybe"},
        {"ui_port": 65536},
        {"http_proxy_port": -1},
        {"ui_host": []},
        {"sources": [1]},
        {"protocols": {}},
        {"limit": -1},
    ],
)
@bounded
async def test_invalid_config_is_atomic_json400(api, route, patch):
    before = api.settings.snapshot()
    response = await api.client.post(route, json=patch)
    assert response.status == 400
    assert "error" in await response.json()
    assert api.settings.snapshot() == before
    assert not api.jobs.status()["running"]


@bounded
async def test_manual_validate_waits_for_results_geo_and_persistence(api, monkeypatch):
    entered, release, saving, saved = (asyncio.Event() for _ in range(4))
    options = []

    class Validator:
        def __init__(self, **kwargs):
            options.append(kwargs)

        async def validate_all(self, proxies):
            entered.set()
            await release.wait()
            proxies[0].alive = True
            proxies[1].alive = False
            return proxies

    async def geo(proxies):
        assert len(proxies) == 1
        proxies[0].country = "Local"

    save = api.store.save

    async def gated_save():
        saving.set()
        await saved.wait()
        await save()

    monkeypatch.setattr(webapp, "ProxyValidator", Validator)
    monkeypatch.setattr(webapp, "fill_missing_geo", AsyncMock(side_effect=geo))
    monkeypatch.setattr(api.store, "save", gated_save)
    await api.settings.update({"geo_lookup": True, "geo_verify_via_proxy": True})
    keys = [p.key_str() for p in api.proxies[:2]]
    api.pool.set_proxies(api.proxies[:2])
    for p in api.proxies[:2]:
        api.pool.report_result(p, False)
    task = asyncio.create_task(api.client.post("/api/validate", json={"keys": keys}))
    try:
        await entered.wait()
        assert not task.done()
        assert (await api.client.get("/api/job")).status == 200
        release.set()
        await saving.wait()
        assert not task.done()
        saved.set()
        response = await task
        assert response.status == 200  # Never a background 202.
        body = await response.json()
        assert body["checked"] == 2
        assert body["alive"] == 1
        assert body["results"][0]["country"] == "Local"
        assert api.store.get_by_key(keys[0]).country == "Local"
        assert json.loads(api.store.path.read_text())[keys[0]]["country"] == "Local"
        assert not api.pool.get_status()[keys[0]]["failed"]
        assert api.pool.get_status()[keys[1]]["failed"]
        assert options == [
            {"timeout": 8.0, "workers": 2, "secondary_check": True, "geo_verify_via_proxy": True}
        ]
        webapp.fill_missing_geo.assert_awaited_once()
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@bounded
async def test_manual_validate_no_matches(api):
    for keys in ([], ["unknown"]):
        response = await api.client.post("/api/validate", json={"keys": keys})
        assert response.status == 400
        assert await response.json() == {"error": "no known proxies matched the given keys"}


@bounded
async def test_scrape_acceptance_conflict_and_responsive_status(api, monkeypatch):
    entered, release = asyncio.Event(), asyncio.Event()

    async def scrape(sources, **kwargs):
        assert sources == ["local"]
        assert kwargs["pages"] == 2000
        entered.set()
        await release.wait()
        return []

    monkeypatch.setattr(jobs_module, "scrape_all", scrape)
    response = await api.client.post("/api/scrape", json={"sources": "local", "pages": "2000"})
    assert response.status == 202
    assert await response.json() == {"started": True}
    await entered.wait()
    for route in ("/api/job", "/api/proxies", "/api/forward/status", "/api/settings"):
        response = await asyncio.wait_for(api.client.get(route), 0.5)
        assert response.status == 200
    assert (await (await api.client.get("/api/job")).json())["running"]
    response = await api.client.post("/api/scrape", json={})
    assert response.status == 409
    assert await response.json() == {"error": "a job is already running"}
    release.set()
    await api.jobs.wait()
    status = await (await api.client.get("/api/job")).json()
    assert not status["running"]
    assert status["summary"] == {
        "scraped": 0,
        "unique": 0,
        "reused_alive": 0,
        "freshly_checked": 0,
        "alive": 0,
    }


@bounded
async def test_restart_signal_only_after_response_write(api):
    response = await api.client.post("/api/restart")  # Existing browser sends no body.
    assert response.status == 200
    assert await response.json() == {"restarting": True}
    assert api.app[webapp.WEB_STATE].restart_event.is_set()


@bounded
async def test_injected_restart_signal_and_closed_app(api):
    callback = AsyncMock()
    app = webapp.create_app(
        api.store, api.pool, api.jobs, api.settings, api.active, restart_callback=callback
    )
    async with TestClient(TestServer(app)) as client:
        response = await client.post("/api/restart", json={})
        assert await response.json() == {"restarting": True}
        callback.assert_awaited_once()
        app[webapp.WEB_STATE].closing = True
        response = await client.get("/api/job")
        assert response.status == 503
        assert "error" in await response.json()


@bounded
async def test_oversized_and_missing_payloads_are_json400(api):
    async def body():
        for _ in range(17):
            yield b"x" * 65536

    response = await api.client.post("/api/settings", data=body())
    assert response.status == 400
    assert await response.json() == {"error": "request body is too large"}
    for route in POST_ROUTES[:-1]:
        response = await api.client.post(route)
        assert response.status == 400
        assert "error" in await response.json()


@bounded
async def test_standalone_app_cleanup_also_cancels_manual_validation(api, monkeypatch):
    entered, cancelled = asyncio.Event(), asyncio.Event()

    async def validate(self, proxies):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    monkeypatch.setattr(webapp.ProxyValidator, "validate_all", validate)
    request = asyncio.create_task(api.client.post("/api/validate", json={"keys": [api.proxies[0].key_str()]}))
    try:
        await entered.wait()
        await api.client.close()
        assert cancelled.is_set()
        assert not api.app[webapp.WEB_STATE].requests
    finally:
        request.cancel()
        await asyncio.gather(request, return_exceptions=True)
