from __future__ import annotations

import itertools
import threading

from proxyhunter.models import Proxy

DEFAULT_FAIL_THRESHOLD = 3


class ProxyPool:
    """Thread-safe round-robin pool of upstream proxies used by the local forwarders.

    The UI mutates this via /api/forward/select; the HTTP and SOCKS5 forward
    servers pull from it (via .pick()) for every new incoming connection, and
    report back whether each forwarding attempt succeeded (via .report_result()).
    A proxy that fails `fail_threshold` times in a row is marked "failed" and
    skipped by .pick() - but it stays in the pool (and in the UI) until the
    user re-validates it or removes it explicitly.
    """

    def __init__(self, fail_threshold: int = DEFAULT_FAIL_THRESHOLD):
        self.fail_threshold = fail_threshold
        self._lock = threading.Lock()
        self._proxies: list[Proxy] = []
        self._fail_counts: dict[str, int] = {}
        self._failed: set[str] = set()
        self._cycle = iter(())

    def _rebuild_cycle_locked(self) -> None:
        usable = [p for p in self._proxies if p.key_str() not in self._failed]
        self._cycle = itertools.cycle(usable) if usable else iter(())

    def set_proxies(self, proxies: list[Proxy]) -> None:
        with self._lock:
            self._proxies = list(proxies)
            keys = {p.key_str() for p in self._proxies}
            self._fail_counts = {k: v for k, v in self._fail_counts.items() if k in keys}
            self._failed = {k for k in self._failed if k in keys}
            self._rebuild_cycle_locked()

    def get_proxies(self) -> list[Proxy]:
        with self._lock:
            return list(self._proxies)

    def get_status(self) -> dict[str, dict]:
        """key_str() -> {"failed": bool, "fail_count": int} for every proxy currently in the pool."""
        with self._lock:
            return {
                p.key_str(): {
                    "failed": p.key_str() in self._failed,
                    "fail_count": self._fail_counts.get(p.key_str(), 0),
                }
                for p in self._proxies
            }

    def has_usable_proxy(self) -> bool:
        with self._lock:
            return any(p.key_str() not in self._failed for p in self._proxies)

    def pick(self) -> Proxy | None:
        with self._lock:
            try:
                return next(self._cycle)
            except StopIteration:
                return None

    def report_result(self, proxy: Proxy, success: bool) -> None:
        key = proxy.key_str()
        with self._lock:
            if key not in {p.key_str() for p in self._proxies}:
                return  # proxy was removed/replaced since this attempt started; ignore stale report

            if success:
                if self._fail_counts.get(key) or key in self._failed:
                    self._fail_counts.pop(key, None)
                    was_failed = key in self._failed
                    self._failed.discard(key)
                    if was_failed:
                        self._rebuild_cycle_locked()
                return

            count = self._fail_counts.get(key, 0) + 1
            self._fail_counts[key] = count
            if count >= self.fail_threshold and key not in self._failed:
                self._failed.add(key)
                self._rebuild_cycle_locked()

    def clear_failed(self, keys: list[str]) -> None:
        """Reset failure bookkeeping for specific proxies, e.g. after they're manually re-validated."""
        with self._lock:
            changed = False
            for key in keys:
                self._fail_counts.pop(key, None)
                if key in self._failed:
                    self._failed.discard(key)
                    changed = True
            if changed:
                self._rebuild_cycle_locked()

    def remove_proxies(self, keys: list[str]) -> None:
        with self._lock:
            key_set = set(keys)
            self._proxies = [p for p in self._proxies if p.key_str() not in key_set]
            for key in keys:
                self._fail_counts.pop(key, None)
                self._failed.discard(key)
            self._rebuild_cycle_locked()
