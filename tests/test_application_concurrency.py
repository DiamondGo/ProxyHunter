"""Removal wins over older async work, without suppressing later discovery.

The application regressions block real loopback validator/scraper/geo sockets.
Only remote endpoint addresses are patched; validator I/O is never replaced.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime

import aiohttp
import pytest
from aiohttp import web

from proxyhunter import geolocate
from proxyhunter.lifecycle import start_server
from proxyhunter.models import Proxy
from proxyhunter.store import ProxyStore
from tests.network_fixtures import bounded
from tests.test_integration import PORT_ZERO, api, scrape, seed
from tests.test_integration import local_network as local_network


async def assert_removed(client, server, key):
    assert server.store.get_by_key(key) is None
    assert key not in json.loads(server.store.path.read_text())
    assert key not in {p.key_str() for p in server.pool.get_proxies()}
    rows = await api(client, server, "GET", "/api/proxies")
    assert key not in {p["key"] for p in rows["proxies"]}
    status = await api(client, server, "GET", "/api/forward/status")
    assert key not in {p["key"] for p in status["selected"]}
    assert server.settings.get("selected_keys") == []


async def rediscover(client, server, network):
    assert (await scrape(client, server))["summary"]["alive"] == 1
    proxy = server.store.get_by_key(network.key)
    assert proxy is not None and proxy.alive
    assert json.loads(server.store.path.read_text())[network.key] == proxy.to_dict()
    # Rediscovery does not implicitly restore a removed forwarding selection.
    assert server.pool.get_proxies() == []
    assert server.settings.get("selected_keys") == []


@pytest.mark.parametrize("operation", ["manual", "scrape", "revalidate"])
@pytest.mark.parametrize("blocked_stage", ["validation", "geo"])
@bounded
async def test_remove_during_real_io_suppresses_stale_validation(
    tmp_path, local_network, http_server, monkeypatch, operation, blocked_stage
):
    network = await local_network()
    config = seed(tmp_path, network)
    if blocked_stage == "validation":
        network.block("/primary/http")
    else:
        config.update(geo_lookup=True, geo_verify_via_proxy=False)

        async def geo(request):
            assert request.method == "POST"
            addresses = await request.json()
            network.entered.set()
            await network.release.wait()
            return web.json_response(
                [
                    {"query": ip, "status": "success", "country": "Canada", "city": "Ottawa", "isp": "local"}
                    for ip in addresses
                ]
            )

        monkeypatch.setattr(geolocate, "BATCH_URL", await http_server(geo))

    server = await start_server(tmp_path / "settings.json", config, network_overrides=PORT_ZERO)
    manual = None
    try:
        # No checked_at: even a scrape must validate this already-known proxy.
        original = network.upstream.proxy(alive=True)
        server.store.upsert_all([original])
        await server.store.save()
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=3), trust_env=False) as client:
            assert await api(client, server, "POST", "/api/forward/select", json={"keys": [network.key]}) == {
                "count": 1
            }
            if operation == "manual":
                manual = asyncio.create_task(
                    api(client, server, "POST", "/api/validate", json={"keys": [network.key]})
                )
            elif operation == "scrape":
                assert await api(client, server, "POST", "/api/scrape", 202, json={}) == {"started": True}
            else:
                assert server.jobs.start_revalidate_all({})

            await network.entered.wait()
            assert not network.release.is_set()
            assert (not manual.done()) if manual is not None else server.jobs.status()["running"]
            assert network.upstream.connections > 0
            assert await api(client, server, "POST", "/api/proxies/remove", json={"keys": [network.key]}) == {
                "removed": 1
            }
            # Deletion and both persistence operations complete while I/O is held.
            await assert_removed(client, server, network.key)
            assert json.loads((tmp_path / "settings.json").read_text())["selected_keys"] == []
            assert not network.release.is_set()
            network.release.set()

            if manual is not None:
                result = await manual
                assert set(result) == {"checked", "alive", "results"}
                assert result["checked"] == result["alive"] == len(result["results"]) == 1
                row = result["results"][0]
                assert set(row) == set(original.to_dict())
                assert (row["protocol"], row["ip"], row["port"]) == original.key()
                assert row["alive"] is True and row["supports_https"] is True
                assert row["country"] == "Canada"
                assert row["latency_ms"] >= 0
                assert datetime.fromisoformat(row["checked_at"]).tzinfo is not None
            else:
                await server.jobs.wait()
                status = await api(client, server, "GET", "/api/job")
                assert status["kind"] == operation
                assert status["running"] is False and status["error"] is None
                assert status["message"] == "done"
                assert status["started_at"] <= status["finished_at"]
                assert status["summary"] == (
                    network.summary if operation == "scrape" else {"checked": 1, "alive": 1}
                )

            await assert_removed(client, server, network.key)
            await rediscover(client, server, network)
    finally:
        network.release.set()
        if manual is not None:
            manual.cancel()
            await asyncio.gather(manual, return_exceptions=True)
        await server.close()


@pytest.mark.parametrize("already_known", [False, True], ids=["absent-key", "known-key"])
@bounded
async def test_remove_during_scraping_suppresses_result_captured_later(
    tmp_path, local_network, already_known
):
    network = await local_network()
    network.block("/world")
    server = await start_server(
        tmp_path / "settings.json", seed(tmp_path, network), network_overrides=PORT_ZERO
    )
    try:
        if already_known:
            server.store.upsert_all([network.upstream.proxy()])
            await server.store.save()
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=3), trust_env=False) as client:
            assert await api(client, server, "POST", "/api/scrape", 202, json={}) == {"started": True}
            await network.entered.wait()
            assert server.jobs.status()["running"]
            assert await api(client, server, "POST", "/api/proxies/remove", json={"keys": [network.key]}) == {
                "removed": int(already_known)
            }
            await assert_removed(client, server, network.key)
            assert not network.release.is_set()
            network.release.set()
            await server.jobs.wait()
            status = await api(client, server, "GET", "/api/job")
            assert status["message"] == "done" and status["error"] is None
            assert status["summary"] == network.summary
            await assert_removed(client, server, network.key)
            await rediscover(client, server, network)
    finally:
        network.release.set()
        await server.close()


@bounded
async def test_remove_during_cached_scrape_geo_does_not_restore_reused_alive(
    tmp_path, local_network, http_server, monkeypatch
):
    network = await local_network()

    async def geo(request):
        assert await request.json() == ["127.0.0.1"]
        network.entered.set()
        await network.release.wait()
        return web.json_response([{"query": "127.0.0.1", "status": "success", "country": "Canada"}])

    monkeypatch.setattr(geolocate, "BATCH_URL", await http_server(geo))
    config = {**seed(tmp_path, network), "geo_lookup": True}
    server = await start_server(tmp_path / "settings.json", config, network_overrides=PORT_ZERO)
    try:
        cached = network.upstream.proxy(alive=True)
        cached.mark_checked()
        rejected = network.rejected.proxy()
        rejected.mark_checked()
        server.store.upsert_all([cached, rejected])
        await server.store.save()
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=3), trust_env=False) as client:
            assert await api(client, server, "POST", "/api/scrape", 202, json={}) == {"started": True}
            await network.entered.wait()
            assert network.upstream.connections == network.rejected.connections == 0
            assert await api(client, server, "POST", "/api/proxies/remove", json={"keys": [network.key]}) == {
                "removed": 1
            }
            await assert_removed(client, server, network.key)
            assert not network.release.is_set()
            network.release.set()
            await server.jobs.wait()
            status = await api(client, server, "GET", "/api/job")
            assert status["message"] == "done" and status["error"] is None
            assert status["summary"] == {**network.summary, "reused_alive": 1, "freshly_checked": 0}
            await assert_removed(client, server, network.key)
            await rediscover(client, server, network)
    finally:
        network.release.set()
        await server.close()


def test_store_revision_is_readonly_and_only_removals_advance_it(tmp_path):
    store = ProxyStore(tmp_path / "state.json")
    initial = store.revision
    assert isinstance(initial, int)
    proxy = Proxy("127.0.0.1", 8080, "http", "test")
    store.upsert_all([proxy])
    assert store.revision == initial
    with pytest.raises(AttributeError):
        store.revision = initial + 1
    assert store.remove([proxy.key_str(), proxy.key_str()]) == 1
    assert store.revision > initial
    after_removal = store.revision
    assert store.remove([proxy.key_str()]) == 0
    assert store.revision > after_removal


def test_store_conditional_upsert_scopes_tokens_to_removed_keys(tmp_path):
    store = ProxyStore(tmp_path / "state.json")
    proxies = [Proxy("127.0.0.1", port, "http", "original") for port in (8080, 8081, 8082)]
    first, second, untouched = proxies
    store.upsert_all(proxies)
    before = store.revision
    assert store.remove([first.key_str()]) == 1
    between = store.revision
    assert store.remove([second.key_str()]) == 1
    after = store.revision

    store.upsert_all(proxies, since=before)
    assert set(store.records) == {untouched.key_str()}
    store.upsert_all(proxies, since=between)
    assert set(store.records) == {first.key_str(), untouched.key_str()}
    fresh = [Proxy(p.ip, p.port, p.protocol, "fresh") for p in proxies]
    store.upsert_all(fresh, since=after)
    assert set(store.records) == {p.key_str() for p in proxies}
    # Even after fresh rediscovery, older completions must not overwrite it.
    store.upsert_all(proxies, since=before)
    assert store.get(first)["source"] == store.get(second)["source"] == "fresh"
    assert store.get(untouched)["source"] == "original"
    assert store.revision == after


def test_store_absent_removal_blocks_stale_but_not_unconditional_or_future_upserts(tmp_path):
    store = ProxyStore(tmp_path / "state.json")
    proxy = Proxy("127.0.0.1", 8080, "http", "test")
    before = store.revision
    assert store.remove([proxy.key_str()]) == 0
    store.upsert_all([proxy], since=before)
    assert store.records == {}
    store.upsert_all([proxy])
    assert store.get_by_key(proxy.key_str()) == proxy
    # An unconditional write cannot erase the protection for older operations.
    stale = Proxy(proxy.ip, proxy.port, proxy.protocol, "stale")
    store.upsert_all([stale], since=before)
    assert store.get(proxy)["source"] == "test"
    assert store.remove([proxy.key_str()]) == 1
    store.upsert_all([proxy], since=store.revision)
    assert store.get_by_key(proxy.key_str()) == proxy


@bounded
async def test_store_tombstones_are_process_local_and_state_schema_is_unchanged(tmp_path):
    path = tmp_path / "state.json"
    store = ProxyStore(path)
    await store.load()
    removed = Proxy("127.0.0.1", 8080, "http", "test")
    retained = Proxy("127.0.0.1", 8081, "http", "test")
    store.upsert_all([removed, retained])
    before = store.revision
    assert store.remove([removed.key_str()]) == 1
    await store.save()
    assert json.loads(path.read_text()) == {retained.key_str(): retained.to_dict()}
    await store.load()
    store.upsert_all([removed], since=before)
    assert store.get_by_key(removed.key_str()) is None
    restored = ProxyStore(path)
    await restored.load()
    assert restored.records == {retained.key_str(): retained.to_dict()}
    assert restored.revision == before
    restored.upsert_all([removed], since=restored.revision)
    await restored.save()
    assert json.loads(path.read_text()) == {p.key_str(): p.to_dict() for p in (removed, retained)}
