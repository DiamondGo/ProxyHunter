from __future__ import annotations

import asyncio
import logging
import time
from copy import deepcopy
from dataclasses import asdict, dataclass, field

from proxyhunter.geolocate import fill_missing_geo
from proxyhunter.scrapers import dedupe, scrape_all
from proxyhunter.scrapers.fallback import select_fallback_candidates
from proxyhunter.settings import SettingsStore
from proxyhunter.store import ProxyStore
from proxyhunter.validator import ProxyValidator

log = logging.getLogger(__name__)


@dataclass
class JobState:
    running: bool = False
    kind: str | None = None
    message: str = ""
    started_at: float | None = None
    finished_at: float | None = None
    error: str | None = None
    summary: dict = field(default_factory=dict)


class JobRunner:
    """Own at most one job at a time on the application's event loop.

    Acceptance and status reads are synchronous, with no intervening await.
    Shutdown is terminal: once begun, new jobs are rejected. The application
    owns loading/flushing the shared store and settings around this lifecycle.
    Accepted work retains the store's removal revision across all network awaits.
    """

    def __init__(self, store: ProxyStore, settings: SettingsStore):
        self._store = store
        self._settings = settings
        self._state = JobState()
        self._tasks: set[asyncio.Task] = set()
        self._current_task: asyncio.Task | None = None
        self._closed = False

    def status(self) -> dict:
        return asdict(self._state)

    def start_full_scrape(self, overrides: dict) -> bool:
        return self._start("scrape", overrides)

    def start_revalidate_all(self, overrides: dict) -> bool:
        """Re-check every proxy already known to the store, ignoring recheck_after -
        the caller (a scheduled task, typically) is what controls the cadence here."""
        return self._start("revalidate", overrides)

    def _start(self, kind: str, overrides: dict) -> bool:
        if self._closed or self._state.running:
            return False
        loop = asyncio.get_running_loop()
        opts = {**self._settings.snapshot(), **deepcopy(overrides)}
        since = self._store.revision
        self._state = JobState(running=True, kind=kind, message="starting...", started_at=time.time())
        task = loop.create_task(self._run(kind, opts, since=since), name=f"proxyhunter-job-{kind}")
        self._tasks.add(task)
        self._current_task = task
        task.add_done_callback(self._task_done)
        return True

    async def wait(self) -> None:
        """Join currently accepted work; cancelling a waiter does not cancel a job."""
        await asyncio.gather(*(asyncio.shield(task) for task in tuple(self._tasks)), return_exceptions=True)

    async def shutdown(self) -> None:
        """Reject new work, cancel accepted jobs once, and await their cleanup."""
        if not self._closed:
            self._closed = True
            for task in self._tasks:
                task.cancel()
        await self.wait()

    def _task_done(self, task: asyncio.Task) -> None:
        self._tasks.discard(task)
        if task.cancelled():
            # Also covers cancellation before _run gets its first instruction.
            if self._current_task is task:
                self._state.running = False
                self._state.message = "cancelled"
                self._state.finished_at = time.time()
        else:
            task.exception()  # Retrieve even unexpected failures; never leak task exceptions.
        if self._current_task is task:
            self._current_task = None

    async def _run(self, kind: str, opts: dict, *, since: int) -> None:
        try:
            if kind == "scrape":
                summary = await self._run_full_scrape(opts, since=since)
            else:
                summary = await self._run_revalidate_all(opts, since=since)
            self._finish_success(summary)
        except Exception as exc:  # Surface failures to the UI, not the event loop.
            log.exception("background %s job failed", kind)
            self._finish_error(exc)

    def _set_message(self, message: str) -> None:
        self._state.message = message

    def _finish_success(self, summary: dict) -> None:
        self._state.running = False
        self._state.message = "done"
        self._state.finished_at = time.time()
        self._state.summary = summary

    def _finish_error(self, exc: Exception) -> None:
        self._state.running = False
        self._state.error = str(exc)
        self._state.finished_at = time.time()

    async def _run_full_scrape(self, opts: dict, *, since: int) -> dict:
        self._set_message(f"scraping {opts['sources']}...")
        fallback_proxies = select_fallback_candidates(self._store.all_known_proxies())
        scraped = await scrape_all(
            opts["sources"],
            pages=opts["pages"],
            protocols=opts["protocols"],
            fallback_proxies=fallback_proxies,
        )
        unique = dedupe(scraped)

        to_check, reused_alive = self._store.split_fresh(unique, opts["recheck_after"])
        if opts.get("limit"):
            to_check = to_check[: opts["limit"]]

        self._set_message(f"validating {len(to_check)} proxies...")
        validator = ProxyValidator(
            timeout=opts["timeout"],
            workers=opts["workers"],
            secondary_check=opts["secondary_check"],
            geo_verify_via_proxy=opts.get("geo_verify_via_proxy", False),
        )
        freshly_checked = await validator.validate_all(to_check) if to_check else []

        alive = [p for p in freshly_checked if p.alive] + reused_alive
        if opts.get("geo_lookup", True):
            self._set_message("filling in geo data...")
            # Cached alive proxies may still be missing geo data.
            await fill_missing_geo(alive)

        self._store.upsert_all(freshly_checked + reused_alive, since=since)
        await self._store.save()

        return {
            "scraped": len(scraped),
            "unique": len(unique),
            "reused_alive": len(reused_alive),
            "freshly_checked": len(freshly_checked),
            "alive": len(alive),
        }

    async def _run_revalidate_all(self, opts: dict, *, since: int) -> dict:
        known = self._store.all_known_proxies()
        self._set_message(f"revalidating {len(known)} proxies...")
        validator = ProxyValidator(
            timeout=opts["timeout"],
            workers=opts["workers"],
            secondary_check=opts["secondary_check"],
            geo_verify_via_proxy=opts.get("geo_verify_via_proxy", False),
        )
        results = await validator.validate_all(known) if known else []

        if opts.get("geo_lookup", True):
            self._set_message("filling in geo data...")
            await fill_missing_geo([p for p in results if p.alive])

        self._store.upsert_all(results, since=since)
        await self._store.save()

        return {"checked": len(results), "alive": sum(1 for p in results if p.alive)}
