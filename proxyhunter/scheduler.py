from __future__ import annotations

import asyncio
import logging
import time

from proxyhunter.forwarder.pool import ProxyPool
from proxyhunter.jobs import JobRunner
from proxyhunter.settings import SettingsStore
from proxyhunter.store import ProxyStore

log = logging.getLogger(__name__)

CHECK_INTERVAL_SECONDS = 5


class Scheduler:
    """Event-loop service that periodically triggers four kinds of tasks, all
    configured (enabled + interval, persisted in settings.json so they survive
    a restart) from the settings page:

      - a full scrape + validate (same job the "发起全量抓取" button starts)
      - unconditionally merging the lowest-latency known proxies into the pool
      - topping the pool back up to a minimum usable count, only when it has
        fallen short (e.g. after proxies died and got marked failed)
      - a full re-validation of every already-known proxy

    Each task remembers its own last-run timestamp in settings so intervals
    are honored across restarts instead of resetting to "due immediately".
    """

    def __init__(self, store: ProxyStore, pool: ProxyPool, jobs: JobRunner, settings: SettingsStore):
        self._store = store
        self._pool = pool
        self._jobs = jobs
        self._settings = settings
        self._task: asyncio.Task | None = None
        self._lifecycle_lock = asyncio.Lock()
        self._tick_lock = asyncio.Lock()

    async def start(self) -> Scheduler:
        """Start once (or restart after shutdown); constructors perform no I/O."""
        async with self._lifecycle_lock:
            if self._task is None:
                self._task = asyncio.create_task(self._loop(), name="proxyhunter-scheduler")
        return self

    async def shutdown(self) -> None:
        """Stop and join the scheduler, not the separately owned JobRunner."""
        async with self._lifecycle_lock:
            if self._task is not None:
                self._task.cancel()
                await asyncio.gather(self._task, return_exceptions=True)
                self._task = None

    async def _loop(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            try:
                await self._tick()
            except Exception:  # One bad tick shouldn't kill the scheduler.
                log.exception("scheduler tick failed")
            # A cancellable loop timer, with no polling thread or blocking wait.
            ready = loop.create_future()
            timer = loop.call_later(CHECK_INTERVAL_SECONDS, ready.set_result, None)
            try:
                await ready
            finally:
                timer.cancel()

    @staticmethod
    def _due(s: dict, enabled_key: str, interval_key: str, last_run_key: str, now: float) -> bool:
        if not s.get(enabled_key):
            return False
        interval_hours = s.get(interval_key) or 0
        if interval_hours <= 0:
            return False
        last_run = s.get(last_run_key) or 0
        return (now - last_run) >= interval_hours * 3600

    async def _tick(self) -> None:
        """Run one deterministic, serialized tick using fresh settings."""
        async with self._tick_lock:
            now = time.time()
            s = self._settings.snapshot()
            await self._maybe_full_scrape(s, now)
            await self._maybe_pool_refresh(s, now)
            await self._maybe_pool_topup(s, now)
            await self._maybe_revalidate(s, now)

    async def _maybe_full_scrape(self, s: dict, now: float) -> None:
        if not self._due(
            s,
            "sched_full_scrape_enabled",
            "sched_full_scrape_interval_hours",
            "sched_full_scrape_last_run",
            now,
        ):
            return
        if self._jobs.start_full_scrape({}):
            log.info("scheduled full scrape started")
            await self._settings.update({"sched_full_scrape_last_run": now})
        else:
            log.info("scheduled full scrape skipped: a job is already running")

    async def _maybe_revalidate(self, s: dict, now: float) -> None:
        if not self._due(
            s, "sched_revalidate_enabled", "sched_revalidate_interval_hours", "sched_revalidate_last_run", now
        ):
            return
        if self._jobs.start_revalidate_all({}):
            log.info("scheduled revalidate-all started")
            await self._settings.update({"sched_revalidate_last_run": now})
        else:
            log.info("scheduled revalidate-all skipped: a job is already running")

    async def _maybe_pool_refresh(self, s: dict, now: float) -> None:
        if not self._due(
            s,
            "sched_pool_refresh_enabled",
            "sched_pool_refresh_interval_hours",
            "sched_pool_refresh_last_run",
            now,
        ):
            return
        await self._settings.update({"sched_pool_refresh_last_run": now})

        top_n = int(s.get("sched_pool_top_n") or 5)
        https_only = s.get("sched_pool_https_only", True)
        candidates = [
            p
            for p in self._store.all_known_proxies()
            if p.alive and p.latency_ms is not None and (not https_only or p.supports_https)
        ]
        candidates.sort(key=lambda p: p.latency_ms)
        top = candidates[:top_n]

        # Merge into the existing pool rather than replacing it, deduped by key.
        merged = {p.key_str(): p for p in self._pool.get_proxies()}
        for p in top:
            merged[p.key_str()] = p
        result = list(merged.values())

        self._pool.set_proxies(result)
        await self._settings.update({"selected_keys": [p.key_str() for p in result]})
        log.info("scheduled pool refresh: considered top %d, pool now has %d proxies", top_n, len(result))

    async def _maybe_pool_topup(self, s: dict, now: float) -> None:
        if not self._due(
            s, "sched_pool_topup_enabled", "sched_pool_topup_interval_hours", "sched_pool_topup_last_run", now
        ):
            return
        await self._settings.update({"sched_pool_topup_last_run": now})

        min_count = int(s.get("sched_pool_topup_min_count") or 3)
        pool_status = self._pool.get_status()
        usable_count = sum(1 for st in pool_status.values() if not st["failed"])
        if usable_count >= min_count:
            return

        need = min_count - usable_count
        https_only = s.get("sched_pool_https_only", True)
        existing_keys = {p.key_str() for p in self._pool.get_proxies()}
        candidates = [
            p
            for p in self._store.all_known_proxies()
            if p.alive
            and p.latency_ms is not None
            and p.key_str() not in existing_keys
            and (not https_only or p.supports_https)
        ]
        candidates.sort(key=lambda p: p.latency_ms)
        to_add = candidates[:need]
        if not to_add:
            log.info(
                "scheduled pool top-up: only %d/%d usable but no more candidates available",
                usable_count,
                min_count,
            )
            return

        result = self._pool.get_proxies() + to_add
        self._pool.set_proxies(result)
        await self._settings.update({"selected_keys": [p.key_str() for p in result]})
        log.info(
            "scheduled pool top-up: usable was %d/%d, added %d proxies, pool now has %d",
            usable_count,
            min_count,
            len(to_add),
            len(result),
        )
