from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field

from proxyhunter.geolocate import fill_missing_geo
from proxyhunter.scrapers import dedupe, scrape_all
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
    """Runs at most one background scrape+validate job at a time, triggered from the UI."""

    def __init__(self, store: ProxyStore, settings: SettingsStore):
        self._store = store
        self._settings = settings
        self._lock = threading.Lock()
        self._state = JobState()

    def status(self) -> dict:
        with self._lock:
            s = self._state
            return {
                "running": s.running,
                "kind": s.kind,
                "message": s.message,
                "started_at": s.started_at,
                "finished_at": s.finished_at,
                "error": s.error,
                "summary": s.summary,
            }

    def start_full_scrape(self, overrides: dict) -> bool:
        with self._lock:
            if self._state.running:
                return False
            self._state = JobState(running=True, kind="scrape", message="starting...", started_at=time.time())
        opts = {**self._settings.snapshot(), **overrides}
        threading.Thread(target=self._run_full_scrape, args=(opts,), daemon=True).start()
        return True

    def start_revalidate_all(self, overrides: dict) -> bool:
        """Re-check every proxy already known to the store, ignoring recheck_after -
        the caller (a scheduled task, typically) is what controls the cadence here."""
        with self._lock:
            if self._state.running:
                return False
            self._state = JobState(running=True, kind="revalidate", message="starting...", started_at=time.time())
        opts = {**self._settings.snapshot(), **overrides}
        threading.Thread(target=self._run_revalidate_all, args=(opts,), daemon=True).start()
        return True

    def _set_message(self, message: str) -> None:
        with self._lock:
            self._state.message = message

    def _finish_success(self, summary: dict) -> None:
        with self._lock:
            self._state.running = False
            self._state.message = "done"
            self._state.finished_at = time.time()
            self._state.summary = summary

    def _finish_error(self, exc: Exception) -> None:
        with self._lock:
            self._state.running = False
            self._state.error = str(exc)
            self._state.finished_at = time.time()

    def _run_full_scrape(self, opts: dict) -> None:
        try:
            self._set_message(f"scraping {opts['sources']}...")
            scraped = scrape_all(opts["sources"], pages=opts["pages"], protocols=opts["protocols"])
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
            freshly_checked = validator.validate_all(to_check) if to_check else []

            alive = [p for p in freshly_checked if p.alive] + reused_alive
            if opts.get("geo_lookup", True):
                self._set_message("filling in geo data...")
                # Also covers reused_alive: a cached proxy carried over from a
                # previous run may still be missing geo data itself.
                fill_missing_geo(alive)

            self._store.upsert_all(freshly_checked + reused_alive)
            self._store.save()

            self._finish_success(
                {
                    "scraped": len(scraped),
                    "unique": len(unique),
                    "reused_alive": len(reused_alive),
                    "freshly_checked": len(freshly_checked),
                    "alive": len(alive),
                }
            )
        except Exception as exc:  # noqa: BLE001 - surface any failure to the UI instead of crashing the thread silently
            log.exception("background scrape job failed")
            self._finish_error(exc)

    def _run_revalidate_all(self, opts: dict) -> None:
        try:
            known = self._store.all_known_proxies()
            self._set_message(f"revalidating {len(known)} proxies...")
            validator = ProxyValidator(
                timeout=opts["timeout"],
                workers=opts["workers"],
                secondary_check=opts["secondary_check"],
                geo_verify_via_proxy=opts.get("geo_verify_via_proxy", False),
            )
            results = validator.validate_all(known) if known else []

            if opts.get("geo_lookup", True):
                self._set_message("filling in geo data...")
                fill_missing_geo([p for p in results if p.alive])

            self._store.upsert_all(results)
            self._store.save()

            self._finish_success({"checked": len(results), "alive": sum(1 for p in results if p.alive)})
        except Exception as exc:  # noqa: BLE001 - surface any failure to the UI instead of crashing the thread silently
            log.exception("background revalidate job failed")
            self._finish_error(exc)
