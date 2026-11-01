"""Offline contracts for ordered, cancellation-safe async persistence."""

from __future__ import annotations

import asyncio
import inspect
import json
import threading
import time
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import MappingProxyType

import pytest

from proxyhunter import persistence
from proxyhunter.models import Proxy
from proxyhunter.settings import DEFAULTS, SettingsStore, coerce_settings
from proxyhunter.store import ProxyStore
from tests.network_fixtures import bounded


def proxy(port=8000, **values):
    return Proxy("127.0.0.1", port, "http", "test", **values)


class WorkerGate:
    """Block an actual worker, without sleeping or blocking the event loop."""

    def __init__(self):
        self.loop = asyncio.get_running_loop()
        self.entered = asyncio.Event()
        self.release = threading.Event()
        self.tasks = []
        self.worker_thread = None

    def block(self):
        self.worker_thread = threading.get_ident()
        self.loop.call_soon_threadsafe(self.entered.set)
        if not self.release.wait(4):
            raise AssertionError("test did not release disk worker")

    def start(self, coroutine):
        task = asyncio.create_task(coroutine)
        self.tasks.append(task)
        return task

    async def wait(self):
        await asyncio.wait_for(self.entered.wait(), 1)


@pytest.fixture
async def gate():
    gate = WorkerGate()
    try:
        yield gate
    finally:
        gate.release.set()
        await asyncio.wait_for(asyncio.gather(*gate.tasks, return_exceptions=True), 5)


def test_constructors_are_lazy_and_api_is_explicit(tmp_path, monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("constructor performed I/O")

    with monkeypatch.context() as patch:
        for method in ("exists", "stat", "open", "read_text", "write_text", "resolve"):
            patch.setattr(Path, method, forbidden)
        patch.setattr(persistence, "_read_json", forbidden)
        patch.setattr(persistence, "_atomic_write", forbidden)
        settings = SettingsStore(str(tmp_path / "settings.json"))
        store = ProxyStore(str(tmp_path / "state.json"))
    assert store.records == {}
    assert settings.snapshot() == DEFAULTS
    assert settings.is_new is None
    for instance in (settings, store):
        for method in ("load", "save", "flush"):
            assert inspect.iscoroutinefunction(getattr(instance, method))
    for method in ("update", "seed_from"):
        assert inspect.iscoroutinefunction(getattr(settings, method))
    for method in ("get", "snapshot"):
        assert not inspect.iscoroutinefunction(getattr(settings, method))
    for method in ("get", "get_by_key", "upsert_all", "remove", "all_known_proxies", "split_fresh"):
        assert not inspect.iscoroutinefunction(getattr(store, method))


def test_original_defaults_and_detached_reads(tmp_path):
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
        "selected_keys": [],
        "sched_full_scrape_enabled": False,
        "sched_full_scrape_interval_hours": 24.0,
        "sched_full_scrape_last_run": 0,
        "sched_pool_refresh_enabled": False,
        "sched_pool_refresh_interval_hours": 1.0,
        "sched_pool_top_n": 5,
        "sched_pool_refresh_last_run": 0,
        "sched_pool_topup_enabled": False,
        "sched_pool_topup_interval_hours": 0.5,
        "sched_pool_topup_min_count": 3,
        "sched_pool_topup_last_run": 0,
        "sched_pool_https_only": True,
        "sched_revalidate_enabled": False,
        "sched_revalidate_interval_hours": 12.0,
        "sched_revalidate_last_run": 0,
        "pool_fail_threshold": 10,
        "pool_max_retries": 3,
        "ui_language": "zh",
    }
    assert DEFAULTS == expected
    left, right = SettingsStore(tmp_path / "left"), SettingsStore(tmp_path / "right")
    left.get("sources").append("outside")
    left.snapshot()["selected_keys"].append("outside")
    fallback = {"nested": [[]]}
    left.get("missing", fallback)["nested"][0].append("outside")
    assert fallback == {"nested": [[]]}
    left.data["protocols"].append("only-left")
    assert DEFAULTS == expected
    assert right.snapshot() == expected
    assert left.get("sources") == expected["sources"]
    assert left.get("selected_keys") == []


@bounded
async def test_settings_original_schema_seed_update_and_roundtrip(tmp_path):
    path = tmp_path / "settings.json"
    settings = SettingsStore(path)
    await settings.load()
    assert settings.is_new is True
    assert not path.exists()
    await settings.seed_from({"pages": "7", "sources": ["custom"], "timeout": None, "ui_language": "中文"})
    assert settings.is_new is False
    changes = {"selected_keys": ["http|127.0.0.1|8000"], "extension": {"nested": [["original"]]}}
    await settings.update(changes)
    changes["extension"]["nested"][0].append("outside")
    settings.get("extension")["nested"][0].append("outside")
    settings.snapshot()["extension"]["nested"][0].append("outside")
    timestamps = {key: 1234.5 for key in DEFAULTS if key.endswith("_last_run")}
    await settings.update(timestamps)
    await settings.update(coerce_settings({**{key: 9999 for key in timestamps}, "pages": "9", "bad": 42}))
    await settings.flush()
    expected = {
        **deepcopy(DEFAULTS),
        "pages": 9,
        "sources": ["custom"],
        "ui_language": "中文",
        "selected_keys": changes["selected_keys"],
        "extension": {"nested": [["original"]]},
        **timestamps,
    }
    assert settings.snapshot() == expected
    assert json.loads(path.read_text()) == expected
    assert "中文" in path.read_text(encoding="utf-8")
    loaded = SettingsStore(str(path))
    await loaded.load()
    assert loaded.is_new is False
    assert loaded.snapshot() == expected


@bounded
async def test_settings_patch_validation_is_all_or_nothing(tmp_path):
    settings = SettingsStore(tmp_path / "settings.json")
    await settings.load()
    await settings.save()
    before = settings.path.read_bytes()
    with pytest.raises((TypeError, ValueError)):
        await settings.update({"pages": 10, "timeout": -1})
    assert settings.snapshot() == DEFAULTS
    assert settings.path.read_bytes() == before
    with pytest.raises(TypeError, match="mapping"):
        await settings.seed_from([])


@bounded
async def test_proxy_original_schema_queries_and_roundtrip(tmp_path):
    path = tmp_path / "state.json"
    store = ProxyStore(path)
    await store.load()
    assert store.records == {}
    assert not path.exists()
    alive = proxy(
        alive=True,
        latency_ms=12.5,
        country="中国",
        city="北京",
        isp="ISP",
        supports_https=True,
        https_only=False,
        anonymity="elite",
    )
    alive.mark_checked()
    dead = proxy(8001)
    store.upsert_all([alive, dead])
    expected = {p.key_str(): p.to_dict() for p in (alive, dead)}
    alive.country = "external mutation"
    store.get(alive)["country"] = "external mutation"
    store.get_by_key(alive.key_str()).country = "external mutation"
    store.all_known_proxies()[0].country = "external mutation"
    assert store.records == expected
    assert store.get(proxy(9000)) is None
    assert store.get_by_key("absent") is None
    await store.save()
    await store.flush()
    assert json.loads(path.read_text()) == expected
    assert "中国" in path.read_text(encoding="utf-8")
    loaded = ProxyStore(str(path))
    await loaded.load()
    assert loaded.records == expected
    assert loaded.all_known_proxies() == [Proxy(**row) for row in expected.values()]
    assert loaded.remove([dead.key_str(), "absent", dead.key_str()]) == 1
    await loaded.save()
    await store.load()
    assert store.all_known_proxies() == [Proxy(**expected[alive.key_str()])]


@bounded
async def test_proxy_fresh_dead_stale_and_unchecked(tmp_path):
    now = datetime.now(timezone.utc)
    fresh = proxy(alive=True, checked_at=now.isoformat())
    dead = proxy(8001, checked_at=now.isoformat())
    stale = proxy(8002, alive=True, checked_at=(now - timedelta(hours=10)).isoformat())
    never = proxy(8003)
    store = ProxyStore(tmp_path / "state.json")
    store.upsert_all([fresh, dead, stale, never])
    candidates = [proxy(p.port) for p in (fresh, dead, stale, never)]
    assert store.split_fresh(candidates, 6) == ([candidates[2], candidates[3]], [fresh])
    assert store.split_fresh(candidates, 0) == (candidates, [])
    assert store.split_fresh(candidates, -1) == (candidates, [])
    assert store.age_hours(never) is None
    assert store.age_hours(proxy(9999)) is None
    assert 0 <= store.age_hours(fresh) < 0.01


@pytest.mark.parametrize("stamp", ["not-a-date", "", 123, [], {}, "2020-99-99T00:00:00"])
@bounded
async def test_proxy_bad_timestamps_are_stale(tmp_path, stamp):
    store = ProxyStore(tmp_path / "state.json")
    item = proxy(alive=True, checked_at=stamp)
    store.upsert_all([item])
    assert store.age_hours(item) is None
    await store.save()
    await store.load()
    assert store.get_by_key(item.key_str()).checked_at is None
    assert store.split_fresh([item], 6) == ([item], [])


@pytest.mark.parametrize("suffix", ["", "+00:00", "Z"])
@bounded
async def test_proxy_legacy_naive_and_z_dates(tmp_path, suffix):
    checked = datetime.now(timezone.utc).replace(tzinfo=None).isoformat() + suffix
    item = proxy(alive=True, checked_at=checked)
    path = tmp_path / "state.json"
    path.write_text(json.dumps({item.key_str(): item.to_dict()}))
    store = ProxyStore(path)
    await store.load()
    assert 0 <= store.age_hours(item) < 0.01
    assert store.split_fresh([item], 1) == ([], [item])


@pytest.mark.parametrize("store_type", [ProxyStore, SettingsStore])
@pytest.mark.parametrize(
    "content", [b"{broken", b"[]", b"null", b"42", b'"string"', b"\xff", b'{"x": NaN}', b"[" * 1200]
)
@bounded
async def test_corrupt_json_falls_back_without_overwriting(tmp_path, store_type, content):
    path = tmp_path / "data.json"
    path.write_bytes(content)
    store = store_type(path)
    await store.load()
    if store_type is SettingsStore:
        assert store.is_new is False
        assert store.snapshot() == DEFAULTS
    else:
        assert store.records == {}
    assert path.read_bytes() == content
    await store.save()
    assert isinstance(json.loads(path.read_text()), dict)


@bounded
async def test_settings_salvages_valid_values_and_preserves_extensions(tmp_path):
    path = tmp_path / "settings.json"
    path.write_text(
        json.dumps(
            {
                "pages": 17,
                "timeout": "oops",
                "workers": [],
                "sources": {"bad": 1},
                "geo_lookup": "false",
                "sched_pool_topup_last_run": "123.5",
                "sched_full_scrape_last_run": -1,
                "extension": {"a": [1, 2]},
            }
        )
    )
    settings = SettingsStore(path)
    await settings.load()
    expected = {
        **deepcopy(DEFAULTS),
        "pages": 17,
        "geo_lookup": False,
        "sched_pool_topup_last_run": 123.5,
        "extension": {"a": [1, 2]},
    }
    assert settings.snapshot() == expected
    settings.get("extension")["a"].append(3)
    assert settings.snapshot() == expected


@bounded
async def test_proxy_salvages_partial_records_and_old_optional_fields(tmp_path):
    good = proxy(alive=True)
    old = {"ip": "127.0.0.1", "port": 8001, "protocol": "socks5", "source": "old", "future": 1}
    rows = {
        good.key_str(): {**good.to_dict(), "future": "ignored"},
        "socks5|127.0.0.1|8001": old,
        "missing": {},
        "list": [],
        "null": None,
        "wrong-key": good.to_dict(),
    }
    for port, patch in enumerate(
        [
            {"port": True},
            {"port": -1},
            {"port": 65536},
            {"port": "8000"},
            {"ip": []},
            {"country": []},
            {"alive": "true"},
            {"supports_https": 1},
            {"https_only": "false"},
            {"latency_ms": "bad"},
            {"latency_ms": -1},
            {"latency_ms": True},
            {"protocol": ""},
            {"source": None},
        ],
        8100,
    ):
        item = proxy(port)
        rows[item.key_str()] = {**item.to_dict(), **patch}
    path = tmp_path / "state.json"
    path.write_text(json.dumps(rows))
    store = ProxyStore(path)
    await store.load()
    assert store.all_known_proxies() == [good, Proxy("127.0.0.1", 8001, "socks5", "old")]


@pytest.mark.parametrize("store_type", [ProxyStore, SettingsStore])
@bounded
async def test_missing_parent_loads_but_save_errors_and_does_not_create_dirs(tmp_path, store_type):
    store = store_type(tmp_path / "missing" / "data.json")
    await store.load()
    with pytest.raises(FileNotFoundError):
        await store.save()
    assert not store.path.parent.exists()
    await store.flush()


@pytest.mark.parametrize("store_type", [ProxyStore, SettingsStore])
@bounded
async def test_directory_path_errors_propagate(tmp_path, store_type):
    store = store_type(tmp_path)
    with pytest.raises(IsADirectoryError):
        await store.load()
    with pytest.raises(IsADirectoryError):
        await store.save()
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("store_type", [ProxyStore, SettingsStore])
@bounded
async def test_non_directory_ancestor_errors_propagate(tmp_path, store_type):
    ancestor = tmp_path / "file"
    ancestor.write_text("not a directory")
    store = store_type(ancestor / "data.json")
    with pytest.raises(NotADirectoryError):
        await store.load()
    with pytest.raises(NotADirectoryError):
        await store.save()
    assert ancestor.read_text() == "not a directory"
    assert list(tmp_path.iterdir()) == [ancestor]


@pytest.mark.parametrize("store_type", [ProxyStore, SettingsStore])
@bounded
async def test_read_os_errors_do_not_replace_in_memory_data(tmp_path, monkeypatch, store_type):
    path = tmp_path / "data.json"
    store = store_type(path)
    if store_type is SettingsStore:
        await store.update({"pages": 17})
        before = store.snapshot()
    else:
        store.upsert_all([proxy()])
        before = deepcopy(store.records)

    def denied(*args, **kwargs):
        raise PermissionError("read denied")

    monkeypatch.setattr(Path, "read_text", denied)
    with pytest.raises(PermissionError, match="read denied"):
        await store.load()
    assert (store.snapshot() if store_type is SettingsStore else store.records) == before


@pytest.mark.parametrize(
    "changes, expected",
    [
        (
            {"pages": "12", "workers": 501.0, "timeout": "0.125"},
            {"pages": 12, "workers": 501, "timeout": 0.125},
        ),
        (
            {"pages": 2000, "workers": 5000, "timeout": 1000},
            {"pages": 2000, "workers": 5000, "timeout": 1000.0},
        ),
        (
            {"sources": " custom, proxyscrape,, ", "protocols": ["https", "socks5h"]},
            {"sources": ["custom", "proxyscrape"], "protocols": ["https", "socks5h"]},
        ),
        ({"sources": [], "selected_keys": (" http|a|80 ",)}, {"sources": [], "selected_keys": ["http|a|80"]}),
        (
            {"ui_host": " ::1 ", "ui_language": "custom-language"},
            {"ui_host": "::1", "ui_language": "custom-language"},
        ),
        ({"ui_host": "", "forward_host": " "}, {"ui_host": "", "forward_host": ""}),
        (
            {"ui_port": 0, "http_proxy_port": "65535", "pool_max_retries": 0, "limit": 0},
            {"ui_port": 0, "http_proxy_port": 65535, "pool_max_retries": 0, "limit": 0},
        ),
        (
            {"recheck_after": 0, "sched_pool_topup_interval_hours": 0},
            {"recheck_after": 0.0, "sched_pool_topup_interval_hours": 0.0},
        ),
        ({"unknown": object(), "sched_full_scrape_last_run": object()}, {}),
    ],
)
def test_coerce_supported_legacy_values(changes, expected):
    assert coerce_settings(MappingProxyType(changes)) == expected


@pytest.mark.parametrize("value", [None, "", "null", " NULL "])
def test_coerce_optional_limit(value):
    assert coerce_settings({"limit": value}) == {"limit": None}


@pytest.mark.parametrize("value", [True, 1, 1.0, "true", "TRUE", "yes", " On ", "1"])
def test_coerce_true(value):
    assert coerce_settings({"secondary_check": value}) == {"secondary_check": True}


@pytest.mark.parametrize("value", [False, 0, 0.0, "false", "FALSE", "no", " Off ", "0"])
def test_coerce_false(value):
    assert coerce_settings({"secondary_check": value}) == {"secondary_check": False}


@pytest.mark.parametrize("changes", [None, [], "pages", 123, True])
def test_coerce_rejects_non_mapping(changes):
    with pytest.raises(TypeError, match="mapping"):
        coerce_settings(changes)


@pytest.mark.parametrize(
    "key,value",
    [
        ("pages", True),
        ("pages", 1.5),
        ("pages", "1.5"),
        ("pages", []),
        ("pages", None),
        ("pages", 0),
        ("pages", -1),
        ("workers", 0),
        ("timeout", 0),
        ("timeout", -0.5),
        ("timeout", float("nan")),
        ("timeout", float("inf")),
        ("timeout", "-inf"),
        ("timeout", "1e9999"),
        ("workers", float("inf")),
        ("workers", float("nan")),
        ("limit", -1),
        ("limit", True),
        ("ui_port", 65536),
        ("socks_port", -1),
        ("http_proxy_port", False),
        ("pool_fail_threshold", 0),
        ("pool_max_retries", -1),
        ("recheck_after", -1),
        ("sched_pool_top_n", 0),
        ("sched_pool_topup_min_count", -1),
        ("sched_full_scrape_interval_hours", -1),
        ("sched_revalidate_interval_hours", "NaN"),
        ("geo_lookup", "not-bool"),
        ("geo_lookup", 2),
        ("geo_lookup", []),
        ("geo_lookup", None),
        ("geo_lookup", float("nan")),
        ("sources", {"x": 1}),
        ("sources", 1),
        ("sources", [1]),
        ("protocols", ["http", None]),
        ("selected_keys", {"key"}),
        ("ui_host", None),
        ("ui_host", 1),
        ("ui_language", " "),
        ("ui_language", []),
    ],
)
def test_coerce_rejects_invalid_types_and_ranges(key, value):
    with pytest.raises((TypeError, ValueError)):
        coerce_settings({key: value})


def test_coerce_detaches_lists():
    changes = {"sources": ["custom"]}
    result = coerce_settings(changes)
    result["sources"].append("outside")
    assert changes == {"sources": ["custom"]}


@pytest.mark.parametrize("store_type", [ProxyStore, SettingsStore])
@pytest.mark.parametrize("stage", ["serialize", "create", "write", "fsync", "replace"])
@bounded
async def test_atomic_failures_preserve_old_file_and_clean_temps(tmp_path, monkeypatch, store_type, stage):
    path = tmp_path / "data.json"
    store = store_type(path)
    await store.save()
    before = path.read_bytes()
    if store_type is SettingsStore:
        store.data["pages"] = 99
    else:
        store.upsert_all([proxy()])

    def fail(*args, **kwargs):
        raise OSError(f"{stage} failed")

    if stage == "serialize":
        monkeypatch.setattr(persistence.json, "dumps", fail)
    elif stage == "create":
        monkeypatch.setattr(persistence.tempfile, "NamedTemporaryFile", fail)
    elif stage == "write":
        original = persistence.tempfile.NamedTemporaryFile

        def bad_writer(*args, **kwargs):
            stream = original(*args, **kwargs)
            stream.write = fail
            return stream

        monkeypatch.setattr(persistence.tempfile, "NamedTemporaryFile", bad_writer)
    else:
        monkeypatch.setattr(persistence.os, stage, fail)
    with pytest.raises(OSError, match=f"{stage} failed"):
        await store.save()
    await store.flush()
    assert path.read_bytes() == before
    assert list(tmp_path.iterdir()) == [path]
    monkeypatch.undo()
    await store.save()  # A failed operation must not poison the writer lock.
    assert path.read_bytes() != before


@bounded
async def test_serialization_error_does_not_touch_existing_file(tmp_path):
    settings = SettingsStore(tmp_path / "settings.json")
    await settings.save()
    before = settings.path.read_bytes()
    settings.data["extension"] = object()
    with pytest.raises(TypeError):
        await settings.save()
    settings.data["extension"] = float("nan")
    with pytest.raises(ValueError):
        await settings.save()
    assert settings.path.read_bytes() == before
    assert list(tmp_path.iterdir()) == [settings.path]


@bounded
async def test_atomic_replace_uses_complete_temp_in_destination_directory(tmp_path, monkeypatch):
    path = tmp_path / "settings.json"
    path.write_text('{"pages": 1}')
    original = persistence.os.replace
    observed = []

    def replace(source, destination):
        assert Path(source).parent == path.parent
        assert Path(destination) == path
        assert json.loads(path.read_text()) == {"pages": 1}
        assert json.loads(Path(source).read_text()) == DEFAULTS
        observed.append(Path(source))
        original(source, destination)

    monkeypatch.setattr(persistence.os, "replace", replace)
    await SettingsStore(path).save()
    assert len(observed) == 1
    assert not observed[0].exists()
    assert json.loads(path.read_text()) == DEFAULTS


@pytest.mark.parametrize("store_type", [ProxyStore, SettingsStore])
@pytest.mark.parametrize("cancel_old", [False, True])
@bounded
async def test_ordered_snapshots_flush_and_repeated_cancellation(
    tmp_path,
    monkeypatch,
    gate,
    store_type,
    cancel_old,
):
    path = tmp_path / "data.json"
    (tmp_path / "sub").mkdir()
    first, second = store_type(path), store_type(tmp_path / "sub" / ".." / "data.json")
    if store_type is SettingsStore:
        first.data["pages"], second.data["pages"] = 1, 2
        first.data["extension"] = {"nested": ["old"]}
    else:
        first.upsert_all([proxy(8001)])
        second.upsert_all([proxy(8002)])
    old_snapshot = deepcopy(first.data if store_type is SettingsStore else first.records)
    new_snapshot = deepcopy(second.data if store_type is SettingsStore else second.records)
    original = persistence._atomic_write
    writes = []

    def delayed_write(target, snapshot):
        if not writes:
            gate.block()
        original(target, snapshot)
        writes.append(deepcopy(snapshot))

    monkeypatch.setattr(persistence, "_atomic_write", delayed_write)
    old = gate.start(first.save())
    await gate.wait()
    if store_type is SettingsStore:
        first.data["pages"] = 99
        first.data["extension"]["nested"].append("outside")
    else:
        first.records.clear()
    newer = gate.start(second.save())
    await asyncio.sleep(0)
    barrier = gate.start(first.flush())
    for _ in range(3):
        if cancel_old:
            old.cancel()
        await asyncio.sleep(0.01)
        assert not old.done()
        assert not newer.done()
        assert not barrier.done()
        assert writes == []
    gate.release.set()
    if cancel_old:
        with pytest.raises(asyncio.CancelledError):
            await old
    else:
        await old
    await newer
    await barrier
    assert writes == [old_snapshot, new_snapshot]
    assert json.loads(path.read_text()) == new_snapshot


@bounded
async def test_cancelled_failed_worker_is_retrieved_before_next_writer(tmp_path, monkeypatch, gate):
    path = tmp_path / "data.json"
    settings = SettingsStore(path)
    original = persistence._atomic_write
    calls = []
    cancellation_causes = []

    def write(target, snapshot):
        calls.append(snapshot["pages"])
        if snapshot["pages"] == 3:
            gate.block()
            raise OSError("old write failed")
        original(target, snapshot)

    async def save():
        try:
            await settings.save()
        except asyncio.CancelledError as exc:
            # Check before crossing a Task boundary: Python 3.10 may replace
            # the cancellation exception and discard its cause when joined.
            cancellation_causes.append(exc.__cause__)
            raise

    monkeypatch.setattr(persistence, "_atomic_write", write)
    old = gate.start(save())
    await gate.wait()
    old.cancel()
    settings.data["pages"] = 7
    newer = gate.start(settings.save())
    await asyncio.sleep(0.01)
    assert not old.done()
    assert calls == [3]
    gate.release.set()
    with pytest.raises(asyncio.CancelledError):
        await old
    assert len(cancellation_causes) == 1
    assert isinstance(cancellation_causes[0], OSError)
    await newer
    assert json.loads(path.read_text())["pages"] == 7


@bounded
async def test_cancelled_queued_save_never_starts_and_flush_does_not_save(tmp_path, monkeypatch, gate):
    settings = SettingsStore(tmp_path / "settings.json")
    original = persistence._atomic_write
    snapshots = []

    def write(target, snapshot):
        gate.block()
        original(target, snapshot)
        snapshots.append(snapshot)

    monkeypatch.setattr(persistence, "_atomic_write", write)
    active = gate.start(settings.save())
    await gate.wait()
    settings.data["pages"] = 19
    queued = gate.start(settings.save())
    await asyncio.sleep(0)
    queued.cancel()
    with pytest.raises(asyncio.CancelledError):
        await queued
    barrier = gate.start(settings.flush())
    await asyncio.sleep(0.01)
    assert not barrier.done()
    gate.release.set()
    await active
    await barrier
    assert len(snapshots) == 1
    assert json.loads(settings.path.read_text())["pages"] == 3
    assert settings.get("pages") == 19


@bounded
async def test_burst_settings_updates_capture_each_patch_without_losing_last_run(tmp_path, monkeypatch):
    settings = SettingsStore(tmp_path / "settings.json")
    await settings.load()
    original = persistence._atomic_write
    snapshots = []

    def write(target, snapshot):
        snapshots.append(deepcopy(snapshot))
        original(target, snapshot)

    monkeypatch.setattr(persistence, "_atomic_write", write)
    timestamp = 123456.5
    patches = [{"sched_full_scrape_last_run": timestamp}] + [
        {"pages": index + 1, f"extension_{index}": {"value": [index]}} for index in range(30)
    ]
    await asyncio.gather(*(settings.update(patch) for patch in patches))
    await settings.flush()
    expected = deepcopy(DEFAULTS)
    for patch, snapshot in zip(patches, snapshots):
        expected.update(patch)
        assert snapshot == expected
    assert len(snapshots) == len(patches)
    assert settings.snapshot() == expected
    assert json.loads(settings.path.read_text()) == expected


@bounded
async def test_burst_proxy_upserts_and_saves_keep_all_records(tmp_path):
    store = ProxyStore(tmp_path / "state.json")
    await store.load()
    items = [proxy(8000 + index, alive=bool(index % 2)) for index in range(30)]

    async def upsert_and_save(item):
        store.upsert_all([item])
        await store.save()

    await asyncio.gather(*(upsert_and_save(item) for item in items))
    await store.flush()
    expected = {item.key_str(): item.to_dict() for item in items}
    assert store.records == expected
    assert json.loads(store.path.read_text()) == expected


@bounded
async def test_cancelled_load_drains_worker_and_does_not_apply_stale_result(tmp_path, monkeypatch, gate):
    settings = SettingsStore(tmp_path / "settings.json")
    settings.path.write_text('{"pages": 17}')
    original = Path.read_text

    def read(path, *args, **kwargs):
        gate.block()
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", read)
    loading = gate.start(settings.load())
    await gate.wait()
    settings.data["pages"] = 19
    saving = gate.start(settings.save())
    for _ in range(3):
        loading.cancel()
        await asyncio.sleep(0.01)
        assert not loading.done()
        assert not saving.done()
    gate.release.set()
    with pytest.raises(asyncio.CancelledError):
        await loading
    await saving
    assert settings.is_new is None
    assert settings.get("pages") == 19
    assert json.loads(original(settings.path))["pages"] == 19


@bounded
async def test_overflowed_json_numbers_are_not_applied(tmp_path):
    settings_path, state_path = tmp_path / "settings.json", tmp_path / "state.json"
    settings_path.write_text('{"pages": 9, "timeout": 1e400}')
    settings = SettingsStore(settings_path)
    await settings.load()
    assert settings.get("pages") == 9
    assert settings.get("timeout") == DEFAULTS["timeout"]
    good, bad = proxy(), proxy(8001, latency_ms="overflow")
    state_path.write_text(
        json.dumps({item.key_str(): item.to_dict() for item in (good, bad)}).replace('"overflow"', "1e400")
    )
    store = ProxyStore(state_path)
    await store.load()
    assert store.all_known_proxies() == [good]


@bounded
async def test_load_waits_for_outstanding_write(tmp_path, monkeypatch, gate):
    path = tmp_path / "data.json"
    original = persistence._atomic_write

    def write(target, snapshot):
        gate.block()
        original(target, snapshot)

    monkeypatch.setattr(persistence, "_atomic_write", write)
    writer, reader = SettingsStore(path), SettingsStore(path)
    writer.data["pages"] = 10
    task = gate.start(writer.save())
    await gate.wait()
    loading = gate.start(reader.load())
    await asyncio.sleep(0.01)
    assert not loading.done()
    gate.release.set()
    await task
    await loading
    assert reader.get("pages") == 10


@bounded
async def test_unrelated_paths_can_write_while_one_worker_is_blocked(tmp_path, monkeypatch, gate):
    original = persistence._atomic_write
    slow = SettingsStore(tmp_path / "slow.json")
    fast = SettingsStore(tmp_path / "fast.json")

    def write(target, snapshot):
        if target == slow.path:
            gate.block()
        original(target, snapshot)

    monkeypatch.setattr(persistence, "_atomic_write", write)
    task = gate.start(slow.save())
    await gate.wait()
    await asyncio.wait_for(fast.save(), 1)
    assert not task.done()
    assert json.loads(fast.path.read_text()) == DEFAULTS


@pytest.mark.parametrize("operation", ["read", "decode", "serialize", "write", "normalize"])
@bounded
async def test_disk_and_json_work_are_offloaded_and_loop_keeps_ticking(
    tmp_path, monkeypatch, gate, operation
):
    settings = SettingsStore(tmp_path / "settings.json")
    settings.path.write_text(json.dumps(DEFAULTS))
    if operation == "read":
        original = Path.read_text

        def read(path, *args, **kwargs):
            gate.block()
            return original(path, *args, **kwargs)

        monkeypatch.setattr(Path, "read_text", read)
        coroutine = settings.load()
    elif operation in ("decode", "serialize"):
        method = "loads" if operation == "decode" else "dumps"
        original = getattr(persistence.json, method)

        def serialize(*args, **kwargs):
            gate.block()
            return original(*args, **kwargs)

        monkeypatch.setattr(persistence.json, method, serialize)
        coroutine = settings.load() if operation == "decode" else settings.save()
    elif operation == "write":
        original = persistence.os.replace

        def replace(*args):
            gate.block()
            return original(*args)

        monkeypatch.setattr(persistence.os, "replace", replace)
        coroutine = settings.save()
    else:

        def normalize(value):
            gate.block()
            return value

        coroutine = settings._file.load(normalize)
    task = gate.start(coroutine)
    await gate.wait()
    assert gate.worker_thread != threading.get_ident()
    started = time.monotonic()
    for _ in range(8):
        await asyncio.sleep(0.005)
        assert not task.done()
    assert time.monotonic() - started < 0.5
    gate.release.set()
    await task
