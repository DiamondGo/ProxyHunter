"""CLI integration: real stores/exports, scoped network doubles, and real processes."""

from __future__ import annotations

import asyncio
import csv
import json
import os
import signal
import subprocess
import sys
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from urllib.parse import urlsplit

import pytest
from aiohttp import ClientSession, web

from proxyhunter import cli, geolocate, validator, webapp
from proxyhunter.models import Proxy
from proxyhunter.scrapers import proxyscrape
from proxyhunter.store import ProxyStore
from tests.network_fixtures import bounded, http_response

CSV_HEADER = [
    "ip",
    "port",
    "protocol",
    "source",
    "country",
    "city",
    "isp",
    "latency_ms",
    "supports_https",
    "https_only",
    "anonymity",
    "checked_at",
]
PROJECT = Path(__file__).resolve().parents[1]


def proxy(index: int, **kwargs) -> Proxy:
    return Proxy(
        f"192.0.2.{index}",
        8000 + index,
        kwargs.pop("protocol", "http"),
        kwargs.pop("source", "fixture"),
        **kwargs,
    )


def checked(hours: float) -> str:
    return (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()


@pytest.fixture
def paths(tmp_path):
    state = tmp_path / "state.json"
    output = tmp_path / "nested" / "output"
    return SimpleNamespace(
        state=state, output=output, argv=["--state-file", str(state), "--output-dir", str(output)]
    )


@pytest.fixture
def network(monkeypatch):
    """Only CLI-level doubles; never patch transports or the async runtime globally."""
    scrape = AsyncMock(side_effect=AssertionError("unexpected scrape"))
    validate = AsyncMock(side_effect=AssertionError("unexpected validation"))
    geo = AsyncMock(side_effect=AssertionError("unexpected geolocation"))
    constructor = Mock(return_value=SimpleNamespace(validate_all=validate))
    monkeypatch.setattr(cli, "scrape_all", scrape)
    monkeypatch.setattr(cli, "ProxyValidator", constructor)
    monkeypatch.setattr(cli, "fill_missing_geo", geo)
    return SimpleNamespace(scrape=scrape, validate=validate, geo=geo, constructor=constructor)


async def seed(path: Path, proxies: list[Proxy]) -> None:
    store = ProxyStore(path)
    await store.load()
    store.upsert_all(proxies)
    await store.save()
    await store.flush()


async def records(path: Path) -> dict:
    store = ProxyStore(path)
    await store.load()
    return store.records


def mark_alive(proxies):
    for p in proxies:
        p.alive = True
        p.latency_ms = 12.5
        p.mark_checked()
    return proxies


def test_parser_preserves_all_defaults():
    assert vars(cli.build_parser().parse_args([])) == {
        "sources": "all",
        "pages": 3,
        "protocols": "http,socks4,socks5",
        "workers": 50,
        "timeout": 8.0,
        "limit": None,
        "no_secondary_check": False,
        "no_geo_lookup": False,
        "geo_verify_via_proxy": False,
        "output_dir": "./output",
        "state_file": "proxyhunter_state.json",
        "recheck_after": 6.0,
        "revalidate": False,
        "min_age": 0.0,
        "serve": False,
        "ui_host": "127.0.0.1",
        "ui_port": 9527,
        "forward_host": "127.0.0.1",
        "http_proxy_port": 9528,
        "socks_port": 9529,
        "settings_file": "proxyhunter_settings.json",
        "verbose": False,
    }


@pytest.mark.parametrize("empty", [False, True])
def test_write_outputs_exact_schema_sorting_text_and_empty_files(tmp_path, empty):
    slow = proxy(
        1,
        latency_ms=50,
        protocol="https",
        country="日本",
        city="東京",
        isp="a,b",
        alive=True,
        supports_https=True,
        https_only=False,
        anonymity="elite",
        checked_at="stamp",
    )
    missing = proxy(2, protocol="socks4", alive=True)
    fast = proxy(3, protocol="socks5", alive=True, latency_ms=0)
    tied = proxy(4, alive=True, latency_ms=50)
    missing_tied = proxy(5, alive=True)
    proxies = [] if empty else [slow, missing, fast, tied, missing_tied]
    ordered = [] if empty else [fast, slow, tied, missing, missing_tied]
    output = tmp_path / "parents" / "exports"
    cli.write_outputs(proxies, output)
    assert proxies == ([] if empty else [slow, missing, fast, tied, missing_tied])
    assert {p.name for p in output.iterdir()} == {
        "proxies_valid.json",
        "proxies_valid.csv",
        "proxies_valid.txt",
    }
    text = (output / "proxies_valid.json").read_text(encoding="utf-8")
    assert text == json.dumps([p.to_dict() for p in ordered], indent=2, ensure_ascii=False)
    with (output / "proxies_valid.csv").open(newline="", encoding="utf-8") as stream:
        rows = list(csv.reader(stream))
    assert rows == [CSV_HEADER] + [
        [
            "" if value is None else str(value)
            for value in (
                p.ip,
                p.port,
                p.protocol,
                p.source,
                p.country,
                p.city,
                p.isp,
                p.latency_ms,
                p.supports_https,
                p.https_only,
                p.anonymity,
                p.checked_at,
            )
        ]
        for p in ordered
    ]
    assert (output / "proxies_valid.txt").read_text() == (
        ""
        if empty
        else "socks5://192.0.2.3:8003\nhttp://192.0.2.1:8001\n"
        "http://192.0.2.4:8004\nsocks4://192.0.2.2:8002\nhttp://192.0.2.5:8005\n"
    )


@bounded
async def test_scrape_cache_dead_skip_dedupe_fallback_geo_and_summary(paths, network, capsys, caplog):
    recent_alive = proxy(
        1, alive=True, checked_at=checked(1), latency_ms=50, supports_https=True, anonymity="elite"
    )
    recent_dead = proxy(2, checked_at=checked(1))
    stale_dead = proxy(3, checked_at=checked(24))
    unrelated = proxy(5, alive=True, protocol="socks4", latency_ms=10)
    await seed(paths.state, [recent_alive, recent_dead, stale_dead, unrelated])
    scraped = [proxy(1), proxy(2), proxy(3), proxy(4, source="first"), proxy(4, source="last")]
    network.scrape.side_effect = lambda *a, **kw: scraped

    def validate(proxies):
        assert [p.key_str() for p in proxies] == [stale_dead.key_str(), scraped[-1].key_str()]
        assert proxies[-1].source == "last"
        proxies[0].alive = True
        proxies[0].latency_ms = 8
        for p in proxies:
            p.mark_checked()
        return proxies

    def geo(proxies):
        assert [p.ip for p in proxies] == ["192.0.2.3", "192.0.2.1"]
        for p in proxies:
            p.country, p.city, p.isp = "Canada", "Vancouver", "fixture ISP"

    network.validate.side_effect = validate
    network.geo.side_effect = geo
    with caplog.at_level("INFO", logger="proxyhunter"):
        result = await cli.async_main(paths.argv)
    assert result == 0
    network.scrape.assert_awaited_once_with(
        ["freeproxy_world", "proxyscrape"],
        pages=3,
        protocols=["http", "socks4", "socks5"],
        fallback_proxies=[unrelated, recent_alive],
    )
    network.constructor.assert_called_once_with(
        timeout=8.0, workers=50, secondary_check=True, geo_verify_via_proxy=False
    )
    network.validate.assert_awaited_once()
    network.geo.assert_awaited_once()
    saved = await records(paths.state)
    assert len(saved) == 5
    assert saved[recent_dead.key_str()] == recent_dead.to_dict()
    assert saved[unrelated.key_str()] == unrelated.to_dict()
    for index in (1, 3):
        row = saved[proxy(index).key_str()]
        assert (row["country"], row["city"], row["isp"]) == ("Canada", "Vancouver", "fixture ISP")
    assert saved[recent_alive.key_str()]["checked_at"] == recent_alive.checked_at
    assert saved[proxy(4).key_str()]["alive"] is False
    assert saved[proxy(4).key_str()]["checked_at"] is not None
    output = json.loads((paths.output / "proxies_valid.json").read_text())
    assert output == [saved[proxy(i).key_str()] for i in (3, 1)]
    assert "1 skipped (recently confirmed dead)" in caplog.text
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == (
        "\n=== Summary ===\nscraped: 5  unique: 4\n"
        "reused from cache without re-checking: 1\n"
        "freshly checked: 2 (alive: 1, dead: 1)\ntotal alive in output: 2\n"
        "https support: 1/2\nanonymity: {'unknown': 1, 'elite': 1}\n"
        f"state file: {paths.state} (5 known proxies total)\n"
    )


@bounded
async def test_cache_only_run_never_validates_or_geolocates_when_disabled(paths, network):
    cached = proxy(1, alive=True, checked_at=checked(1), latency_ms=10)
    await seed(paths.state, [cached])
    network.scrape.side_effect = lambda *a, **kw: [proxy(1)]
    assert await cli.async_main(paths.argv + ["--no-geo-lookup"]) == 0
    network.validate.assert_not_called()
    network.geo.assert_not_called()
    assert json.loads((paths.output / "proxies_valid.json").read_text()) == [cached.to_dict()]
    assert await records(paths.state) == {cached.key_str(): cached.to_dict()}


@pytest.mark.parametrize("mode", ["scrape-empty", "cache-dead", "revalidate-empty", "min-age"])
@bounded
async def test_no_candidates_preserves_outputs_and_error_messages(paths, network, mode, caplog, capsys):
    paths.output.mkdir(parents=True)
    output = paths.output / "proxies_valid.json"
    output.write_text("keep previous output")
    args = paths.argv.copy()
    message = "nothing to validate"
    before = None
    if mode == "scrape-empty":
        network.scrape.side_effect = lambda *a, **kw: []
        message = "no proxies scraped, nothing to validate"
    elif mode == "cache-dead":
        dead = proxy(1, checked_at=checked(1))
        await seed(paths.state, [dead])
        before = paths.state.read_bytes()
        network.scrape.side_effect = lambda *a, **kw: [proxy(1)]
    else:
        args += ["--revalidate"]
        if mode == "min-age":
            await seed(paths.state, [proxy(1, checked_at=checked(1))])
            before = paths.state.read_bytes()
            args += ["--min-age", "100"]
    assert await cli.async_main(args) == 1
    assert message in caplog.text
    assert output.read_text() == "keep previous output"
    if before is not None:
        assert paths.state.read_bytes() == before
    else:
        assert not paths.state.exists()
    assert capsys.readouterr().err == ""
    network.validate.assert_not_called()
    network.geo.assert_not_called()
    if mode.startswith("revalidate") or mode == "min-age":
        network.scrape.assert_not_called()


@bounded
async def test_all_dead_results_still_write_empty_outputs_and_persist_dead(paths, network, capsys):
    network.scrape.side_effect = lambda *a, **kw: [proxy(1)]

    def dead(proxies):
        for p in proxies:
            p.mark_checked()
        return proxies

    network.validate.side_effect = dead
    network.geo.side_effect = lambda proxies: None
    assert await cli.async_main(paths.argv) == 0
    network.geo.assert_awaited_once_with([])
    assert (paths.output / "proxies_valid.json").read_text() == "[]"
    assert (paths.output / "proxies_valid.txt").read_text() == ""
    with (paths.output / "proxies_valid.csv").open(newline="") as stream:
        assert list(csv.reader(stream)) == [CSV_HEADER]
    saved = await records(paths.state)
    assert saved[proxy(1).key_str()]["alive"] is False
    assert saved[proxy(1).key_str()]["checked_at"] is not None
    assert "freshly checked: 1 (alive: 0, dead: 1)" in capsys.readouterr().err


@pytest.mark.parametrize("min_age,expected", [(0, [1, 2, 3, 4]), (6, [2, 4])])
@bounded
async def test_revalidate_min_age_including_missing_timestamps(paths, network, capsys, min_age, expected):
    known = [
        proxy(1, checked_at=checked(1), alive=True),
        proxy(2, checked_at=checked(24)),
        proxy(3),
        proxy(4, checked_at=checked(48), alive=True),
    ]
    await seed(paths.state, known)
    network.validate.side_effect = mark_alive
    assert (
        await cli.async_main(
            paths.argv
            + [
                "--revalidate",
                "--min-age",
                str(min_age),
                "--no-geo-lookup",
                "--sources",
                "ignored",
                "--pages",
                "99",
                "--protocols",
                "ignored",
            ]
        )
        == 0
    )
    network.scrape.assert_not_called()
    network.validate.assert_awaited_once()
    assert [p.ip for p in network.validate.await_args.args[0]] == [f"192.0.2.{i}" for i in expected]
    saved = await records(paths.state)
    for i, original in enumerate(known, 1):
        if i in expected:
            assert saved[original.key_str()]["alive"] is True
        else:
            assert saved[original.key_str()] == original.to_dict()
    summary = capsys.readouterr().err
    assert "scraped:" not in summary
    assert "reused from cache without re-checking: 0" in summary
    assert f"total alive in output: {len(expected)}" in summary


@pytest.mark.parametrize("limit,expected", [(None, 3), (0, 3), (1, 1), (-1, 2)])
@pytest.mark.parametrize("revalidate", [False, True])
@bounded
async def test_limit_preserves_legacy_truthy_slice_semantics(paths, network, limit, expected, revalidate):
    proxies = [proxy(i) for i in (1, 2, 3)]
    args = paths.argv + ["--no-geo-lookup"]
    if revalidate:
        await seed(paths.state, proxies)
        args += ["--revalidate"]
    else:
        network.scrape.side_effect = lambda *a, **kw: proxies
    if limit is not None:
        args += ["--limit", str(limit)]
    network.validate.side_effect = mark_alive
    assert await cli.async_main(args) == 0
    assert len(network.validate.await_args.args[0]) == expected
    output = json.loads((paths.output / "proxies_valid.json").read_text())
    assert [p["ip"] for p in output] == [f"192.0.2.{i}" for i in range(1, expected + 1)]


@bounded
async def test_zero_recheck_validates_recent_alive_and_dead_and_forwards_options(paths, network):
    await seed(paths.state, [proxy(1, alive=True, checked_at=checked(1)), proxy(2, checked_at=checked(1))])
    network.scrape.side_effect = lambda *a, **kw: [proxy(1), proxy(2)]
    network.validate.side_effect = mark_alive
    assert (
        await cli.async_main(
            paths.argv
            + [
                "--sources",
                "proxyscrape,unknown",
                "--pages",
                "7",
                "--protocols",
                " socks5, ,http ",
                "--workers",
                "3",
                "--timeout",
                "0.25",
                "--no-secondary-check",
                "--no-geo-lookup",
                "--geo-verify-via-proxy",
                "--recheck-after",
                "0",
            ]
        )
        == 0
    )
    network.scrape.assert_awaited_once_with(
        ["proxyscrape", "unknown"], pages=7, protocols=["socks5", "http"], fallback_proxies=[]
    )
    network.constructor.assert_called_once_with(
        timeout=0.25, workers=3, secondary_check=False, geo_verify_via_proxy=True
    )
    assert len(network.validate.await_args.args[0]) == 2
    network.geo.assert_not_called()


@bounded
async def test_limit_does_not_truncate_reused_alive(paths, network):
    cached = proxy(1, alive=True, checked_at=checked(1))
    await seed(paths.state, [cached])
    network.scrape.side_effect = lambda *a, **kw: [proxy(1), proxy(2), proxy(3)]
    network.validate.side_effect = mark_alive
    assert await cli.async_main(paths.argv + ["--limit", "1", "--no-geo-lookup"]) == 0
    assert [p.ip for p in network.validate.await_args.args[0]] == ["192.0.2.2"]
    output = json.loads((paths.output / "proxies_valid.json").read_text())
    assert {p["ip"] for p in output} == {"192.0.2.1", "192.0.2.2"}


@pytest.mark.parametrize("custom", [False, True])
@bounded
async def test_serve_awaits_server_and_forwards_every_seed_option(paths, network, monkeypatch, custom):
    entered, release = asyncio.Event(), asyncio.Event()

    async def server(**kwargs):
        entered.set()
        await release.wait()

    serve = AsyncMock(side_effect=server)
    monkeypatch.setattr(webapp, "run_server", serve)
    settings = paths.state.parent / "settings.json"
    argv = paths.argv + ["--serve", "--settings-file", str(settings)]
    expected = {
        "sources": ["freeproxy_world", "proxyscrape"],
        "pages": 3,
        "protocols": ["http", "socks4", "socks5"],
        "workers": 50,
        "timeout": 8.0,
        "secondary_check": True,
        "geo_lookup": True,
        "geo_verify_via_proxy": False,
        "recheck_after": 6.0,
        "limit": None,
        "ui_host": "127.0.0.1",
        "ui_port": 9527,
        "forward_host": "127.0.0.1",
        "http_proxy_port": 9528,
        "socks_port": 9529,
        "state_file": str(paths.state),
    }
    if custom:
        argv += [
            "--sources",
            "proxyscrape,unknown",
            "--pages",
            "5",
            "--protocols",
            " socks5,,http ",
            "--workers",
            "7",
            "--timeout",
            "0.5",
            "--no-secondary-check",
            "--no-geo-lookup",
            "--geo-verify-via-proxy",
            "--recheck-after",
            "2.5",
            "--limit",
            "9",
            "--ui-host",
            "::1",
            "--ui-port",
            "0",
            "--forward-host",
            "127.0.0.2",
            "--http-proxy-port",
            "0",
            "--socks-port",
            "0",
            "--revalidate",
            "--min-age",
            "20",
            "-v",
        ]
        expected.update(
            sources=["proxyscrape", "unknown"],
            pages=5,
            protocols=["socks5", "http"],
            workers=7,
            timeout=0.5,
            secondary_check=False,
            geo_lookup=False,
            geo_verify_via_proxy=True,
            recheck_after=2.5,
            limit=9,
            ui_host="::1",
            ui_port=0,
            forward_host="127.0.0.2",
            http_proxy_port=0,
            socks_port=0,
        )
    task = asyncio.create_task(cli.async_main(argv))
    try:
        await entered.wait()
        assert not task.done()
        serve.assert_awaited_once_with(settings_file=settings, cli_seed=expected)
    finally:
        release.set()
    assert await task == 0
    network.scrape.assert_not_called()
    network.validate.assert_not_called()
    network.geo.assert_not_called()
    assert not paths.state.exists()


@pytest.mark.parametrize(
    "error", [ValueError("invalid config"), TypeError("invalid seed"), OSError("bind failed")]
)
@pytest.mark.parametrize("verbose", [False, True])
@bounded
async def test_serve_errors_are_logged_and_nonzero(paths, monkeypatch, caplog, error, verbose):
    serve = AsyncMock(side_effect=error)
    monkeypatch.setattr(webapp, "run_server", serve)
    assert await cli.async_main(paths.argv + ["--serve"] + (["-v"] if verbose else [])) == 1
    serve.assert_awaited_once()
    assert str(error) in caplog.text
    assert bool(caplog.records[-1].exc_info) is verbose


@bounded
async def test_state_load_filesystem_error_flushes_and_returns_nonzero(paths, network, monkeypatch, caplog):
    paths.state.mkdir()
    flush = AsyncMock()

    class Store(ProxyStore):
        async def flush(self):
            await flush()
            await super().flush()

    monkeypatch.setattr(cli, "ProxyStore", Store)
    assert await cli.async_main(paths.argv) == 1
    flush.assert_awaited_once()
    network.scrape.assert_not_called()
    assert "could not complete run:" in caplog.text
    assert not paths.output.exists()


@bounded
async def test_output_filesystem_error_returns_nonzero_without_saving(paths, network, caplog, capsys):
    paths.output.parent.mkdir(parents=True)
    paths.output.write_text("not a directory")
    network.scrape.side_effect = lambda *a, **kw: [proxy(1)]
    network.validate.side_effect = mark_alive
    assert await cli.async_main(paths.argv + ["--no-geo-lookup"]) == 1
    assert "could not complete run:" in caplog.text
    assert not paths.state.exists()
    assert paths.output.read_text() == "not a directory"
    assert capsys.readouterr().err == ""


@bounded
async def test_save_error_is_awaited_logged_and_flushed(paths, network, monkeypatch, caplog):
    events = []

    class Store(ProxyStore):
        async def save(self):
            await asyncio.sleep(0)
            events.append("save")
            raise PermissionError("read-only state")

        async def flush(self):
            await asyncio.sleep(0)
            events.append("flush")
            await super().flush()

    monkeypatch.setattr(cli, "ProxyStore", Store)
    network.scrape.side_effect = lambda *a, **kw: [proxy(1)]
    network.validate.side_effect = mark_alive
    assert await cli.async_main(paths.argv + ["--no-geo-lookup"]) == 1
    assert events == ["save", "flush"]
    assert "read-only state" in caplog.text
    assert (paths.output / "proxies_valid.json").exists()


@pytest.mark.parametrize("stage", ["scrape", "validate", "geo"])
@bounded
async def test_network_cancellation_propagates_after_cleanup_and_flush(paths, network, monkeypatch, stage):
    entered, cleaned = asyncio.Event(), asyncio.Event()
    flush = AsyncMock()

    class Store(ProxyStore):
        async def flush(self):
            await flush()
            await super().flush()

    async def blocked(*args, **kwargs):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            await asyncio.sleep(0)
            cleaned.set()

    monkeypatch.setattr(cli, "ProxyStore", Store)
    network.scrape.side_effect = lambda *a, **kw: [proxy(1)]
    network.validate.side_effect = mark_alive
    getattr(network, stage).side_effect = blocked
    task = asyncio.create_task(cli.async_main(paths.argv))
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert cleaned.is_set()
    flush.assert_awaited_once()
    assert not paths.state.exists()
    assert not paths.output.exists()


@pytest.mark.parametrize("fail_worker", [False, True])
@bounded
async def test_export_serialization_off_loop_and_repeated_cancel_joins_worker(
    paths,
    network,
    monkeypatch,
    fail_worker,
):
    loop = asyncio.get_running_loop()
    thread = threading.get_ident()
    entered, finished = asyncio.Event(), asyncio.Event()
    release = threading.Event()
    original = cli.write_outputs

    def write(proxies, output):
        assert threading.get_ident() != thread
        loop.call_soon_threadsafe(entered.set)
        try:
            assert release.wait(3), "test did not release disk worker"
            if fail_worker:
                raise OSError("worker failed after cancellation")
            original(proxies, output)
        finally:
            loop.call_soon_threadsafe(finished.set)

    monkeypatch.setattr(cli, "write_outputs", write)
    network.scrape.side_effect = lambda *a, **kw: [proxy(1)]
    network.validate.side_effect = mark_alive
    task = asyncio.create_task(cli.async_main(paths.argv + ["--no-geo-lookup"]))
    try:
        await entered.wait()
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        assert not finished.is_set()
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert finished.is_set()
    assert not paths.state.exists()
    assert (paths.output / "proxies_valid.json").exists() is not fail_worker


def test_main_uses_one_asyncio_run_and_propagates_result(paths, monkeypatch):
    original = asyncio.run
    loops = []

    async def entry(argv):
        assert argv == paths.argv
        loops.append(asyncio.get_running_loop())
        return 7

    run = Mock(wraps=original)
    monkeypatch.setattr(cli, "async_main", entry)
    monkeypatch.setattr(cli.asyncio, "run", run)
    assert cli.main(paths.argv) == 7
    assert len(loops) == 1
    run.assert_called_once()
    assert loops[0].is_closed()


@pytest.mark.parametrize("error", [KeyboardInterrupt, asyncio.CancelledError])
def test_main_interruption_returns_130_without_traceback(monkeypatch, caplog, error):
    def interrupt(coroutine):
        coroutine.close()
        raise error()

    monkeypatch.setattr(cli.asyncio, "run", interrupt)
    with caplog.at_level("INFO", logger="proxyhunter"):
        assert cli.main([]) == 130
    assert "interrupted" in caplog.text
    assert caplog.records[-1].exc_info is None


@bounded
async def test_real_cli_scraper_validator_geo_and_exports_use_only_loopback(
    paths,
    http_server,
    upstream_server,
    tls_context,
    monkeypatch,
):
    async def forward(head, body):
        method, target, _ = head.split(b"\r\n", 1)[0].split(b" ")
        assert method == b"GET" and body == b""
        assert urlsplit(target.decode()).hostname == "127.0.0.1"
        async with ClientSession(trust_env=False) as session, session.get(target.decode()) as response:
            return http_response(response.status, await response.read())

    upstream = await upstream_server(plain=forward)
    calls = []

    async def handler(request):
        calls.append(request.path)
        if request.path == "/api":
            assert request.query["protocol"] == "http"
            return web.Response(text=f"{upstream.host}:{upstream.port}\n{upstream.host}:{upstream.port}\n")
        if request.path == "/ip":
            return web.json_response({"ip": "198.51.100.9"})
        if request.path == "/batch":
            ips = await request.json()
            return web.json_response(
                [
                    {
                        "status": "success",
                        "query": ip,
                        "country": "Canada",
                        "city": "Vancouver",
                        "isp": "local",
                    }
                    for ip in ips
                ]
            )
        assert request.path in ("/http", "/https")
        return web.json_response({"origin": "198.51.100.10", "headers": {}})

    url = await http_server(handler)
    secure_url = await http_server(handler, ssl_context=tls_context)
    monkeypatch.setattr(proxyscrape, "API_URL", url + "/api")
    monkeypatch.setattr(validator, "REAL_IP_URL", url + "/ip")
    monkeypatch.setattr(validator, "PRIMARY_HTTP_URL", url + "/http")
    monkeypatch.setattr(validator, "PRIMARY_HTTPS_URL", secure_url + "/https")
    monkeypatch.setattr(validator, "PORT_PROBE_HOST", "127.0.0.1")
    monkeypatch.setattr(validator, "PORT_PROBE_PORT", urlsplit(url).port)
    monkeypatch.setattr(geolocate, "BATCH_URL", url + "/batch")
    assert (
        await cli.async_main(
            paths.argv
            + [
                "--sources",
                "proxyscrape",
                "--protocols",
                "http",
                "--workers",
                "1",
                "--no-secondary-check",
            ]
        )
        == 0
    )
    assert calls == ["/api", "/ip", "/http", "/https", "/batch"]
    saved = await records(paths.state)
    assert len(saved) == 1
    row = next(iter(saved.values()))
    assert row["alive"] and row["supports_https"]
    assert row["https_only"] is False
    assert row["anonymity"] == "elite"
    assert (row["country"], row["city"], row["isp"]) == ("Canada", "Vancouver", "local")
    assert row["latency_ms"] is not None and row["checked_at"] is not None
    assert json.loads((paths.output / "proxies_valid.json").read_text()) == [row]
    assert (paths.output / "proxies_valid.txt").read_text() == f"http://{upstream.host}:{upstream.port}\n"
    assert upstream.connections == 3


@pytest.mark.parametrize(
    "mode,code,fragment",
    [
        ("unknown", 1, "no proxies scraped, nothing to validate"),
        ("empty", 1, "nothing to validate"),
        ("filesystem", 1, "could not complete run:"),
        ("config", 1, "workers must be positive"),
        ("usage", 2, "unrecognized arguments"),
        ("help", 0, "usage: proxyhunter"),
    ],
)
def test_real_module_subprocess_exit_codes_without_external_services(paths, mode, code, fragment):
    args = paths.argv.copy()
    if mode == "unknown":
        args += ["--sources", "unknown-test-source"]
    elif mode == "empty":
        args += ["--revalidate"]
    elif mode == "filesystem":
        paths.state.mkdir()
        args += ["--revalidate"]
    elif mode == "config":
        args += ["--serve", "--settings-file", str(paths.state.parent / "settings.json"), "--workers", "0"]
    elif mode == "usage":
        args += ["--invalid-option"]
    else:
        args += ["--help"]
    result = subprocess.run(
        [sys.executable, "-W", "error", "-m", "proxyhunter", *args],
        cwd=PROJECT,
        env={**os.environ, "PYTHONASYNCIODEBUG": "1"},
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert result.returncode == code, result.stdout + result.stderr
    assert fragment in result.stdout + result.stderr
    assert "Traceback" not in result.stderr
    assert "RuntimeWarning" not in result.stderr
    assert "ResourceWarning" not in result.stderr
    assert not paths.output.exists()


@bounded
async def test_real_serve_subprocess_awaits_startup_and_stops_cleanly(paths):
    settings = paths.state.parent / "settings.json"
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-W",
        "error",
        "-m",
        "proxyhunter",
        *paths.argv,
        "--serve",
        "--settings-file",
        str(settings),
        "--ui-port",
        "0",
        "--http-proxy-port",
        "0",
        "--socks-port",
        "0",
        cwd=PROJECT,
        env={**os.environ, "PYTHONASYNCIODEBUG": "1"},
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    startup = bytearray()
    try:
        while b"SOCKS5 proxy listening on" not in startup:
            line = await process.stderr.readline()
            assert line, startup.decode()
            startup.extend(line)
        process.send_signal(signal.SIGINT)
        stdout, stderr = await process.communicate()
        assert process.returncode == 0, (startup + stderr).decode()
        assert stdout == b""
        assert b"127.0.0.1:" in startup
        assert b"Traceback" not in stderr
        assert b"Warning" not in stderr
        saved = json.loads(settings.read_text())
        assert saved["ui_host"] == saved["forward_host"] == "127.0.0.1"
        assert saved["ui_port"] == saved["http_proxy_port"] == saved["socks_port"] == 0
    finally:
        if process.returncode is None:
            process.kill()
        await process.communicate()
