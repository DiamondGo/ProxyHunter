"""Deterministic application-layer tests; no public network access or wall-clock waits."""

from __future__ import annotations

import asyncio
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, call

import pytest

import proxyhunter.jobs as jobs_module
import proxyhunter.scheduler as scheduler_module
from proxyhunter.forwarder.pool import ProxyPool
from proxyhunter.jobs import JobRunner
from proxyhunter.models import Proxy
from proxyhunter.scheduler import Scheduler
from proxyhunter.settings import SettingsStore
from proxyhunter.store import ProxyStore
from proxyhunter.validator import ProxyValidator
from tests.network_fixtures import bounded as bounded_test


def proxy(port=8000, **kwargs):
    return Proxy("127.0.0.1", port, "http", "test", **kwargs)


async def bounded(awaitable):
    return await asyncio.wait_for(awaitable, 2)


async def turn():
    """Yield through the ready queue without a timing-dependent sleep."""
    loop = asyncio.get_running_loop()
    ready = loop.create_future()
    loop.call_soon(ready.set_result, None)
    await ready


@pytest.fixture(autouse=True)
async def task_guard():
    loop = asyncio.get_running_loop()
    before = asyncio.all_tasks()
    errors = []
    previous = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: errors.append(context))
    try:
        yield
        await turn()
        await turn()
        leaked = asyncio.all_tasks() - before - {asyncio.current_task()}
        for task in leaked:
            task.cancel()
        if leaked:
            await bounded(asyncio.gather(*leaked, return_exceptions=True))
        assert not leaked, f"Leaked tasks: {leaked}"
        assert not errors, f"Event-loop errors: {errors}"
    finally:
        loop.set_exception_handler(previous)


@pytest.fixture
async def rig(tmp_path, monkeypatch):
    settings = SettingsStore(tmp_path / "settings.json")
    store = ProxyStore(tmp_path / "state.json")
    await settings.load()
    await store.load()
    scrape = AsyncMock(return_value=[])
    geo = AsyncMock()
    validate = AsyncMock(side_effect=lambda proxies: proxies)
    validator = Mock(return_value=SimpleNamespace(validate_all=validate))
    monkeypatch.setattr(jobs_module, "scrape_all", scrape)
    monkeypatch.setattr(jobs_module, "fill_missing_geo", geo)
    monkeypatch.setattr(jobs_module, "ProxyValidator", validator)
    pool = ProxyPool(settings)
    jobs = JobRunner(store, settings)
    scheduler = Scheduler(store, pool, jobs, settings)
    result = SimpleNamespace(
        store=store,
        settings=settings,
        pool=pool,
        jobs=jobs,
        scheduler=scheduler,
        scrape=scrape,
        geo=geo,
        validate=validate,
        validator=validator,
    )
    try:
        yield result
    finally:
        await bounded(result.scheduler.shutdown())
        await bounded(jobs.shutdown())
        await bounded(store.flush())
        await bounded(settings.flush())
        assert not jobs._tasks
        assert result.scheduler._task is None


def start(rig, kind, overrides=None):
    method = rig.jobs.start_full_scrape if kind == "scrape" else rig.jobs.start_revalidate_all
    return method(overrides or {})


@pytest.mark.asyncio
async def test_constructors_are_idle_and_status_is_detached(tmp_path):
    store = ProxyStore(tmp_path / "missing-state.json")
    settings = SettingsStore(tmp_path / "missing-settings.json")
    before = asyncio.all_tasks()
    jobs = JobRunner(store, settings)
    scheduler = Scheduler(store, ProxyPool(settings), jobs, settings)
    assert asyncio.all_tasks() == before
    assert not store.path.exists() and not settings.path.exists()
    assert jobs.status() == {
        "running": False,
        "kind": None,
        "message": "",
        "started_at": None,
        "finished_at": None,
        "error": None,
        "summary": {},
    }
    jobs._state.summary = {"nested": [{"value": 1}]}
    snapshot = jobs.status()
    snapshot["summary"]["nested"][0]["value"] = 99
    snapshot["running"] = True
    assert jobs.status()["summary"] == {"nested": [{"value": 1}]}
    assert not jobs.status()["running"]
    await jobs.wait()
    await jobs.shutdown()
    await scheduler.shutdown()


async def test_full_scrape_cache_dedupe_limit_geo_and_persistence(rig):
    cached = proxy(2, alive=True, supports_https=True, latency_ms=10)
    dead = proxy(3)
    cached.mark_checked()
    dead.mark_checked()
    stale = proxy(4, checked_at="2000-01-01T00:00:00+00:00")
    rig.store.upsert_all([cached, dead, stale])
    first, last, excluded = proxy(1, country="old"), proxy(1), proxy(5)
    rig.scrape.return_value = [first, proxy(2), proxy(3), last, proxy(4), excluded]

    async def validate(proxies):
        assert [p.port for p in proxies] == [1, 4]
        for p in proxies:
            p.alive = p.port == 1
            p.mark_checked()
        return proxies

    async def geo(proxies):
        assert [p.port for p in proxies] == [1, 2]
        for p in proxies:
            p.country = "Canada"

    rig.validate.side_effect = validate
    rig.geo.side_effect = geo
    assert rig.jobs.start_full_scrape({"limit": 2, "geo_verify_via_proxy": True})
    await bounded(rig.jobs.wait())
    status = rig.jobs.status()
    assert not status["running"] and status["error"] is None and status["message"] == "done"
    assert status["finished_at"] >= status["started_at"]
    assert status["summary"] == {
        "scraped": 6,
        "unique": 5,
        "reused_alive": 1,
        "freshly_checked": 2,
        "alive": 2,
    }
    rig.scrape.assert_awaited_once_with(
        rig.settings.get("sources"),
        pages=3,
        protocols=rig.settings.get("protocols"),
        fallback_proxies=[cached],
    )
    rig.validator.assert_called_once_with(
        timeout=8.0, workers=50, secondary_check=True, geo_verify_via_proxy=True
    )
    assert rig.validate.await_args.args[0][0] is last
    loaded = ProxyStore(rig.store.path)
    await loaded.load()
    assert loaded.get_by_key(last.key_str()).country == "Canada"
    assert loaded.get_by_key(cached.key_str()).country == "Canada"
    assert not loaded.get_by_key(stale.key_str()).alive
    assert loaded.get_by_key(excluded.key_str()) is None
    assert loaded.get_by_key(dead.key_str()).checked_at == dead.checked_at


@pytest.mark.parametrize("kind", ["scrape", "revalidate"])
@pytest.mark.parametrize("geo_lookup", [False, True])
async def test_empty_job_still_awaits_save_without_validation(rig, kind, geo_lookup):
    assert start(rig, kind, {"geo_lookup": geo_lookup})
    await bounded(rig.jobs.wait())
    rig.validate.assert_not_awaited()
    if geo_lookup:
        rig.geo.assert_awaited_once_with([])
    else:
        rig.geo.assert_not_awaited()
    expected = (
        {"checked": 0, "alive": 0}
        if kind == "revalidate"
        else {
            "scraped": 0,
            "unique": 0,
            "reused_alive": 0,
            "freshly_checked": 0,
            "alive": 0,
        }
    )
    assert rig.jobs.status()["summary"] == expected
    assert rig.store.path.read_text() == "{}"


async def test_full_scrape_reuses_alive_and_skips_fresh_dead(rig):
    alive, dead = proxy(1, alive=True), proxy(2)
    alive.mark_checked()
    dead.mark_checked()
    rig.store.upsert_all([alive, dead])
    rig.scrape.return_value = [proxy(1), proxy(2)]
    assert rig.jobs.start_full_scrape({})
    await bounded(rig.jobs.wait())
    rig.validate.assert_not_awaited()
    rig.geo.assert_awaited_once_with([alive])
    assert rig.jobs.status()["summary"] == {
        "scraped": 2,
        "unique": 2,
        "reused_alive": 1,
        "freshly_checked": 0,
        "alive": 1,
    }


async def test_zero_limit_and_recheck_disable_cache_without_limiting(rig):
    proxies = [proxy(1), proxy(2, alive=True)]
    for p in proxies:
        p.mark_checked()
    rig.store.upsert_all(proxies)
    rig.scrape.return_value = proxies
    assert rig.jobs.start_full_scrape({"limit": 0, "recheck_after": 0, "geo_lookup": False})
    await bounded(rig.jobs.wait())
    rig.validate.assert_awaited_once_with(proxies)
    assert rig.jobs.status()["summary"]["freshly_checked"] == 2
    rig.geo.assert_not_awaited()


async def test_revalidate_ignores_cache_and_limit_and_keeps_dead_results(rig):
    proxies = [proxy(1), proxy(2, alive=True), proxy(3)]
    for p in proxies:
        p.mark_checked()
    rig.store.upsert_all(proxies)

    async def validate(known):
        assert known == proxies
        for p in known:
            p.alive = p.port != 2
        return known

    rig.validate.side_effect = validate
    assert rig.jobs.start_revalidate_all({"limit": 1, "recheck_after": 99999})
    await bounded(rig.jobs.wait())
    rig.scrape.assert_not_awaited()
    assert rig.jobs.status()["summary"] == {"checked": 3, "alive": 2}
    assert [p.port for p in rig.geo.await_args.args[0]] == [1, 3]
    loaded = ProxyStore(rig.store.path)
    await loaded.load()
    assert [p.alive for p in loaded.all_known_proxies()] == [True, False, True]


@pytest.mark.parametrize(
    "kind,stage",
    [
        ("scrape", "scrape"),
        ("scrape", "validate"),
        ("scrape", "geo"),
        ("scrape", "save"),
        ("revalidate", "validate"),
        ("revalidate", "geo"),
        ("revalidate", "save"),
    ],
)
async def test_errors_surface_in_status_and_next_job_resets_error(rig, monkeypatch, kind, stage):
    rig.store.upsert_all([proxy(alive=True)])
    rig.scrape.return_value = [proxy(alive=True)]
    target = {"scrape": rig.scrape, "validate": rig.validate, "geo": rig.geo}.get(stage)
    if stage == "save":
        target = AsyncMock()
        monkeypatch.setattr(rig.store, "save", target)
    target.side_effect = RuntimeError(f"{stage} failed")
    assert start(rig, kind, {"recheck_after": 0})
    await bounded(rig.jobs.wait())
    status = rig.jobs.status()
    assert not status["running"] and status["kind"] == kind
    assert status["error"] == f"{stage} failed"
    assert status["finished_at"] >= status["started_at"]
    assert status["summary"] == {}
    target.side_effect = (lambda proxies: proxies) if stage == "validate" else None
    assert start(rig, kind, {"recheck_after": 0})
    assert rig.jobs.status()["error"] is None
    await bounded(rig.jobs.wait())
    assert rig.jobs.status()["message"] == "done"


@pytest.mark.parametrize("first", ["scrape", "revalidate"])
@pytest.mark.parametrize("second", ["scrape", "revalidate"])
async def test_atomic_acceptance_excludes_both_job_kinds(rig, first, second):
    entered, release = asyncio.Event(), asyncio.Event()
    rig.store.upsert_all([proxy()])

    async def block(*args, **kwargs):
        entered.set()
        await release.wait()
        return []

    target = rig.scrape if first == "scrape" else rig.validate
    target.side_effect = block
    assert start(rig, first)
    assert not any(start(rig, second) for _ in range(20))
    assert rig.jobs.status()["running"]
    await bounded(entered.wait())
    assert not start(rig, second)
    release.set()
    await bounded(rig.jobs.wait())
    assert start(rig, second)
    await bounded(rig.jobs.wait())


async def test_options_are_detached_and_each_job_reads_dynamic_settings(rig):
    overrides = {"sources": ["override"], "protocols": ["socks5"]}
    assert rig.jobs.start_full_scrape(overrides)
    overrides["sources"].append("mutated")
    overrides["protocols"].clear()
    await rig.settings.update({"sources": ["new"], "pages": 9, "workers": 4, "timeout": 2.5})
    await bounded(rig.jobs.wait())
    assert rig.scrape.await_args.args == (["override"],)
    assert rig.scrape.await_args.kwargs["protocols"] == ["socks5"]
    assert rig.scrape.await_args.kwargs["pages"] == 3
    assert rig.validator.call_args.kwargs["workers"] == 50
    assert rig.jobs.start_full_scrape({})
    await bounded(rig.jobs.wait())
    assert rig.scrape.await_args.args == (["new"],)
    assert rig.scrape.await_args.kwargs["pages"] == 9
    assert rig.validator.call_args.kwargs["workers"] == 4
    assert rig.validator.call_args.kwargs["timeout"] == 2.5


@pytest.mark.parametrize("kind", ["scrape", "revalidate"])
@bounded_test
async def test_immediate_shutdown_cancels_before_first_instruction(rig, kind):
    assert start(rig, kind)
    # Bound the whole test, not shutdown: Python 3.10 wait_for creates a task
    # and yields before shutdown runs, allowing the job's first instruction.
    await rig.jobs.shutdown()
    status = rig.jobs.status()
    assert not status["running"] and status["message"] == "cancelled"
    assert status["error"] is None and status["summary"] == {}
    assert status["finished_at"] >= status["started_at"]
    rig.scrape.assert_not_awaited()
    rig.validate.assert_not_awaited()
    assert not start(rig, kind)
    await rig.jobs.shutdown()


@pytest.mark.parametrize(
    "kind,stage",
    [
        ("scrape", "scrape"),
        ("scrape", "validate"),
        ("scrape", "geo"),
        ("scrape", "save"),
        ("revalidate", "validate"),
        ("revalidate", "geo"),
        ("revalidate", "save"),
    ],
)
async def test_shutdown_awaits_child_cleanup_and_is_concurrent_safe(rig, monkeypatch, kind, stage):
    rig.store.upsert_all([proxy(alive=True)])
    rig.scrape.return_value = [proxy(alive=True)]
    entered, cleaning, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    children = []

    async def block(*args, **kwargs):
        child = asyncio.create_task(asyncio.Event().wait())
        children.append(child)
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            child.cancel()
            await asyncio.gather(child, return_exceptions=True)
            cleaning.set()
            await release.wait()

    target = {"scrape": rig.scrape, "validate": rig.validate, "geo": rig.geo}.get(stage)
    if stage == "save":
        target = AsyncMock()
        monkeypatch.setattr(rig.store, "save", target)
    target.side_effect = block
    assert start(rig, kind, {"recheck_after": 0})
    await bounded(entered.wait())
    stop1 = asyncio.create_task(rig.jobs.shutdown())
    await bounded(cleaning.wait())
    stop2 = asyncio.create_task(rig.jobs.shutdown())
    await turn()
    assert not stop1.done() and not stop2.done()
    assert rig.jobs.status()["running"]
    assert not start(rig, kind)
    release.set()
    await bounded(asyncio.gather(stop1, stop2))
    assert all(task.done() for task in children)
    assert not rig.jobs.status()["running"]
    assert rig.jobs.status()["message"] == "cancelled"
    assert rig.jobs.status()["error"] is None


async def test_cancelled_waiter_does_not_cancel_job_and_save_is_awaited(rig, monkeypatch):
    entered, release = asyncio.Event(), asyncio.Event()

    async def save():
        entered.set()
        await release.wait()

    monkeypatch.setattr(rig.store, "save", save)
    assert rig.jobs.start_full_scrape({})
    await bounded(entered.wait())
    waiter = asyncio.create_task(rig.jobs.wait())
    await turn()
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    assert rig.jobs.status()["running"] and rig.jobs.status()["finished_at"] is None
    release.set()
    await bounded(rig.jobs.wait())
    assert rig.jobs.status()["message"] == "done"


async def test_cancelled_shutdown_waiter_leaves_owned_cleanup_joinable(rig):
    entered, cleaning, release = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def scrape(*args, **kwargs):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cleaning.set()
            await release.wait()

    rig.scrape.side_effect = scrape
    assert rig.jobs.start_full_scrape({})
    await bounded(entered.wait())
    stopper = asyncio.create_task(rig.jobs.shutdown())
    await bounded(cleaning.wait())
    stopper.cancel()
    with pytest.raises(asyncio.CancelledError):
        await stopper
    assert rig.jobs.status()["running"] and rig.jobs._tasks
    release.set()
    await bounded(rig.jobs.shutdown())
    assert not rig.jobs.status()["running"] and not rig.jobs._tasks


async def test_shutdown_cleans_real_validator_worker_tasks(rig, monkeypatch):
    entered, cleaned = asyncio.Event(), asyncio.Event()
    rig.store.upsert_all([proxy(1), proxy(2)])
    monkeypatch.setattr(jobs_module, "ProxyValidator", ProxyValidator)
    monkeypatch.setattr(ProxyValidator, "_get_real_ip", AsyncMock(return_value=None))

    async def check(_self, _proxy):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            await turn()
            cleaned.set()

    monkeypatch.setattr(ProxyValidator, "_check_one", check)
    assert rig.jobs.start_revalidate_all({"workers": 2})
    await bounded(entered.wait())
    await bounded(rig.jobs.shutdown())
    assert cleaned.is_set()
    assert not rig.jobs.status()["running"]


OPERATIONS = ("full_scrape", "pool_refresh", "pool_topup", "revalidate")


class FakeJobs:
    def __init__(self, events):
        self.events = events
        self.busy = False

    def _accept(self, kind, overrides):
        assert overrides == {}
        self.events.append(("job", kind, not self.busy))
        if self.busy:
            return False
        self.busy = True
        return True

    def start_full_scrape(self, overrides):
        return self._accept("scrape", overrides)

    def start_revalidate_all(self, overrides):
        return self._accept("revalidate", overrides)


@pytest.fixture
def scheduled(rig, monkeypatch):
    events = []
    jobs = FakeJobs(events)
    clock = SimpleNamespace(now=100_000.0)
    monkeypatch.setattr(scheduler_module, "time", SimpleNamespace(time=lambda: clock.now))
    original_update = rig.settings.update

    async def update(changes):
        events.append(("settings", deepcopy(changes)))
        await original_update(changes)

    monkeypatch.setattr(rig.settings, "update", AsyncMock(side_effect=update))
    rig.scheduler = Scheduler(rig.store, rig.pool, jobs, rig.settings)
    return SimpleNamespace(**vars(rig), fake_jobs=jobs, events=events, clock=clock)


async def enable(rig, *operations, **changes):
    patch = {}
    for operation in operations:
        patch[f"sched_{operation}_enabled"] = True
        patch[f"sched_{operation}_interval_hours"] = 1
    await rig.settings.update({**patch, **changes})


@pytest.mark.parametrize(
    "enabled,interval,last,now,expected",
    [
        (False, 1, 0, 3600, False),
        (True, None, 0, 3600, False),
        (True, 0, 0, 3600, False),
        (True, -1, 0, 3600, False),
        (True, 1, None, 3600, True),
        (True, 1, 0, 3599.99, False),
        (True, 1, 0, 3600, True),
        (True, 1, 100, 3700, True),
        (True, 1, 100, 3699, False),
        (True, 0.5, 10, 1810, True),
        (True, 1, 4000, 3600, False),
    ],
)
def test_due_boundaries(enabled, interval, last, now, expected):
    assert Scheduler._due({"e": enabled, "i": interval, "l": last}, "e", "i", "l", now) is expected
    assert not Scheduler._due({}, "e", "i", "l", now)


@pytest.mark.parametrize("operation", OPERATIONS)
@pytest.mark.parametrize("reason", ["disabled", "not_due", "zero_interval"])
async def test_scheduler_skips_disabled_not_due_and_zero_interval(scheduled, operation, reason):
    r = scheduled
    await enable(r, operation)
    if reason == "disabled":
        await r.settings.update({f"sched_{operation}_enabled": False})
    elif reason == "not_due":
        await r.settings.update({f"sched_{operation}_last_run": r.clock.now - 3599})
    else:
        await r.settings.update({f"sched_{operation}_interval_hours": 0})
    r.events.clear()
    await r.scheduler._tick()
    assert r.events == []


@pytest.mark.parametrize("operation", OPERATIONS)
async def test_scheduler_due_operation_persists_timestamp(scheduled, operation):
    r = scheduled
    await enable(r, operation)
    r.events.clear()
    await r.scheduler._tick()
    key = f"sched_{operation}_last_run"
    assert r.settings.get(key) == r.clock.now
    loaded = SettingsStore(r.settings.path)
    await loaded.load()
    assert loaded.get(key) == r.clock.now
    events = deepcopy(r.events)
    await r.scheduler._tick()
    assert r.events == events


async def test_restored_scheduler_honors_persisted_intervals(scheduled):
    r = scheduled
    await enable(r, *OPERATIONS)
    await r.settings.update({f"sched_{operation}_last_run": r.clock.now for operation in OPERATIONS})
    restored = SettingsStore(r.settings.path)
    await restored.load()
    events = []
    scheduler = Scheduler(r.store, r.pool, FakeJobs(events), restored)
    await scheduler._tick()
    assert events == []
    assert restored.snapshot() == r.settings.snapshot()
    await scheduler.shutdown()


@pytest.mark.parametrize("operation", ["full_scrape", "revalidate"])
async def test_busy_job_does_not_advance_timestamp_and_retries(scheduled, operation):
    r = scheduled
    await enable(r, operation)
    r.fake_jobs.busy = True
    await r.scheduler._tick()
    assert r.settings.get(f"sched_{operation}_last_run") == 0
    r.fake_jobs.busy = False
    await r.scheduler._tick()
    assert r.settings.get(f"sched_{operation}_last_run") == r.clock.now


async def test_all_four_operations_keep_order_and_failed_pool_semantics(scheduled):
    r = scheduled
    await enable(r, *OPERATIONS, sched_pool_top_n=1, sched_pool_topup_min_count=3, pool_fail_threshold=1)
    failed = proxy(100, alive=True, supports_https=True, latency_ms=500)
    healthy = proxy(101, alive=True, supports_https=True, latency_ms=500)
    fast = proxy(1, alive=True, supports_https=True, latency_ms=10)
    next_best = proxy(2, alive=True, supports_https=True, latency_ms=20)
    r.pool.set_proxies([failed, healthy])
    r.pool.report_result(failed, False)
    r.store.upsert_all(
        [
            fast,
            next_best,
            proxy(3, alive=True, latency_ms=1),
            proxy(4, supports_https=True, latency_ms=2),
            proxy(5, alive=True, supports_https=True),
        ]
    )
    r.events.clear()
    await r.scheduler._tick()
    refresh_keys = [p.key_str() for p in [failed, healthy, fast]]
    final_keys = refresh_keys + [next_best.key_str()]
    assert r.events == [
        ("job", "scrape", True),
        ("settings", {"sched_full_scrape_last_run": r.clock.now}),
        ("settings", {"sched_pool_refresh_last_run": r.clock.now}),
        ("settings", {"selected_keys": refresh_keys}),
        ("settings", {"sched_pool_topup_last_run": r.clock.now}),
        ("settings", {"selected_keys": final_keys}),
        ("job", "revalidate", False),
    ]
    assert [p.key_str() for p in r.pool.get_proxies()] == final_keys
    assert r.pool.get_status()[failed.key_str()]["failed"]
    assert r.settings.get("sched_revalidate_last_run") == 0
    r.fake_jobs.busy = False
    await r.scheduler._tick()
    assert r.events[-2:] == [
        ("job", "revalidate", True),
        ("settings", {"sched_revalidate_last_run": r.clock.now}),
    ]


async def test_refresh_merges_dedupes_sorts_and_keeps_pool_counters(scheduled):
    r = scheduled
    await enable(r, "pool_refresh", sched_pool_top_n=2)
    old = proxy(1, alive=True, supports_https=True, latency_ms=100)
    replacement = proxy(1, alive=True, supports_https=True, latency_ms=1, country="Canada")
    fast = proxy(2, alive=True, supports_https=True, latency_ms=2)
    slow = proxy(3, alive=True, supports_https=True, latency_ms=50)
    r.pool.set_proxies([old])
    r.pool.report_result(old, True)
    r.store.upsert_all([slow, fast, replacement])
    await r.scheduler._tick()
    assert r.pool.get_proxies() == [replacement, fast]
    assert r.pool.get_status()[old.key_str()]["success_count"] == 1
    assert r.settings.get("selected_keys") == [replacement.key_str(), fast.key_str()]


@pytest.mark.parametrize("existing_count,min_count", [(3, 3), (4, 3), (0, 3)])
async def test_topup_checks_usable_threshold_and_no_candidates_still_marks_run(
    scheduled, existing_count, min_count
):
    r = scheduled
    await enable(r, "pool_topup", sched_pool_topup_min_count=min_count)
    r.pool.set_proxies([proxy(port) for port in range(1, existing_count + 1)])
    r.events.clear()
    await r.scheduler._tick()
    assert r.events == [("settings", {"sched_pool_topup_last_run": r.clock.now})]
    assert len(r.pool.get_proxies()) == existing_count


async def test_topup_excludes_existing_failed_proxies_and_ineligible_candidates(scheduled):
    r = scheduled
    await enable(r, "pool_topup", sched_pool_topup_min_count=2, pool_fail_threshold=1)
    failed = proxy(1, alive=True, supports_https=True, latency_ms=1)
    eligible = proxy(5, alive=True, supports_https=True, latency_ms=20)
    r.pool.set_proxies([failed])
    r.pool.report_result(failed, False)
    r.store.upsert_all(
        [
            failed,
            proxy(2, alive=True, latency_ms=2),
            proxy(3, supports_https=True, latency_ms=3),
            proxy(4, alive=True, supports_https=True),
            eligible,
        ]
    )
    await r.scheduler._tick()
    assert r.pool.get_proxies() == [failed, eligible]
    assert r.pool.get_status()[failed.key_str()]["failed"]
    assert r.settings.get("selected_keys") == [failed.key_str(), eligible.key_str()]


async def test_scheduler_reads_dynamic_enabled_interval_filter_and_pool_options(scheduled):
    r = scheduled
    https = proxy(1, alive=True, supports_https=True, latency_ms=20)
    http = proxy(2, alive=True, latency_ms=1)
    extra = proxy(3, alive=True, latency_ms=30)
    r.store.upsert_all([https, http, extra])
    await enable(r, "pool_refresh", sched_pool_top_n=1)
    await r.scheduler._tick()
    assert r.pool.get_proxies() == [https]
    await r.settings.update({"sched_pool_refresh_interval_hours": 2, "sched_pool_https_only": False})
    r.clock.now += 3600
    await r.scheduler._tick()
    assert r.pool.get_proxies() == [https]
    await r.settings.update({"sched_pool_refresh_interval_hours": 1, "sched_pool_top_n": 2})
    await r.scheduler._tick()
    assert r.pool.get_proxies() == [https, http]
    await r.settings.update({"sched_pool_refresh_enabled": False})
    await enable(r, "pool_topup", sched_pool_topup_min_count=3)
    await r.scheduler._tick()
    assert r.pool.get_proxies() == [https, http, extra]
    r.clock.now += 3600
    r.events.clear()
    await r.settings.update({"sched_pool_topup_enabled": False})
    r.events.clear()
    await r.scheduler._tick()
    assert r.events == []


async def test_pool_threshold_reads_async_settings_updates_synchronously(rig):
    p = proxy()
    await rig.settings.update({"pool_fail_threshold": 2})
    rig.pool.set_proxies([p])
    rig.pool.report_result(p, False)
    assert rig.pool.pick() == p
    await rig.settings.update({"pool_fail_threshold": 1})
    rig.pool.report_result(p, False)
    assert rig.pool.pick() is None
    rig.pool.report_result(p, True)
    assert rig.pool.pick_many(3) == [p]


async def test_concurrent_ticks_are_serialized_across_settings_writes(scheduled, monkeypatch):
    r = scheduled
    await enable(r, "pool_refresh")
    entered, release = asyncio.Event(), asyncio.Event()
    original = r.settings.update

    async def update(changes):
        if "sched_pool_refresh_last_run" in changes:
            entered.set()
            await release.wait()
        await original(changes)

    patch = AsyncMock(side_effect=update)
    monkeypatch.setattr(r.settings, "update", patch)
    tick1 = asyncio.create_task(r.scheduler._tick())
    await bounded(entered.wait())
    tick2 = asyncio.create_task(r.scheduler._tick())
    await turn()
    assert not tick2.done()
    release.set()
    await bounded(asyncio.gather(tick1, tick2))
    assert patch.await_args_list == [
        call({"sched_pool_refresh_last_run": r.clock.now}),
        call({"selected_keys": []}),
    ]


async def test_scheduler_lifecycle_is_idempotent_responsive_and_restartable(rig, monkeypatch):
    entered, cleaned = asyncio.Event(), asyncio.Event()

    async def tick():
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            await turn()
            cleaned.set()

    patch = AsyncMock(side_effect=tick)
    monkeypatch.setattr(rig.scheduler, "_tick", patch)
    results = await bounded(asyncio.gather(rig.scheduler.start(), rig.scheduler.start()))
    assert results == [rig.scheduler, rig.scheduler]
    await bounded(entered.wait())
    assert patch.await_count == 1
    await turn()  # The scheduler cannot block unrelated loop work.
    await bounded(asyncio.gather(rig.scheduler.shutdown(), rig.scheduler.shutdown()))
    assert cleaned.is_set() and rig.scheduler._task is None
    entered.clear()
    assert await rig.scheduler.start() is rig.scheduler
    await bounded(entered.wait())
    assert patch.await_count == 2
    await bounded(rig.scheduler.shutdown())
    await rig.scheduler.start()
    await bounded(rig.scheduler.shutdown())  # Includes pre-first-instruction cancellation.


async def test_scheduler_timer_is_cancellable_without_waiting_for_interval(rig, monkeypatch):
    entered = asyncio.Event()

    async def tick():
        entered.set()

    monkeypatch.setattr(rig.scheduler, "_tick", tick)
    monkeypatch.setattr(scheduler_module, "CHECK_INTERVAL_SECONDS", 3600)
    await rig.scheduler.start()
    await bounded(entered.wait())
    await turn()
    task = rig.scheduler._task
    assert not task.done()
    await bounded(rig.scheduler.shutdown())
    assert task.done()


async def test_scheduler_logs_tick_error_and_continues(rig, monkeypatch, caplog):
    entered = asyncio.Event()
    calls = 0

    async def tick():
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("tick failed")
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(rig.scheduler, "_tick", tick)
    monkeypatch.setattr(scheduler_module, "CHECK_INTERVAL_SECONDS", 0)
    await rig.scheduler.start()
    await bounded(entered.wait())
    assert calls == 2 and "scheduler tick failed" in caplog.text
    await bounded(rig.scheduler.shutdown())


async def test_tick_write_error_preserves_original_order_and_retry_points(scheduled, monkeypatch):
    r = scheduled
    await enable(r, *OPERATIONS)
    update = AsyncMock(side_effect=OSError("disk unavailable"))
    monkeypatch.setattr(r.settings, "update", update)
    with pytest.raises(OSError, match="disk unavailable"):
        await r.scheduler._tick()
    update.assert_awaited_once_with({"sched_full_scrape_last_run": r.clock.now})
    assert r.events[-1] == ("job", "scrape", True)
    assert r.settings.get("sched_pool_refresh_last_run") == 0
    assert r.settings.get("sched_full_scrape_last_run") == 0


async def test_scheduler_shutdown_does_not_cancel_separately_owned_jobs(rig):
    entered = asyncio.Event()

    async def scrape(*args, **kwargs):
        entered.set()
        await asyncio.Event().wait()

    rig.scrape.side_effect = scrape
    await enable(rig, "full_scrape")
    await rig.scheduler.start()
    await bounded(entered.wait())
    await bounded(rig.scheduler.shutdown())
    assert rig.jobs.status()["running"]
    await bounded(rig.jobs.shutdown())
    assert not rig.jobs.status()["running"]
