from __future__ import annotations

import itertools
import threading

from proxyhunter.models import Proxy

DEFAULT_FAIL_THRESHOLD = 10


class ProxyPool:
    """Thread-safe round-robin pool of upstream proxies used by the local forwarders.

    The UI mutates this via /api/forward/select; the HTTP and SOCKS5 forward
    servers pull from it (via .pick()) for every new incoming connection, and
    report back whether each forwarding attempt succeeded (via .report_result()).
    A proxy that fails `fail_threshold` times in a row is marked "failed" and
    skipped by .pick() - but it stays in the pool (and in the UI) until the
    user re-validates it or removes it explicitly.
    """

    def __init__(self, settings=None, fail_threshold: int = DEFAULT_FAIL_THRESHOLD):
        # When a SettingsStore is given, fail_threshold is read fresh from it
        # on every use (via the property below) so the user's "pool_fail_threshold"
        # setting applies immediately, without a service restart. Otherwise
        # (e.g. in tests) it falls back to the fixed value passed in here.
        self._settings = settings
        self._fixed_fail_threshold = fail_threshold
        self._lock = threading.Lock()
        self._proxies: list[Proxy] = []
        self._fail_counts: dict[str, int] = {}
        self._failed: set[str] = set()
        self._request_counts: dict[str, int] = {}
        self._success_counts: dict[str, int] = {}
        self._cycle = iter(())

    @property
    def fail_threshold(self) -> int:
        if self._settings is not None:
            return int(self._settings.get("pool_fail_threshold", DEFAULT_FAIL_THRESHOLD))
        return self._fixed_fail_threshold

    def _rebuild_cycle_locked(self) -> None:
        usable = [p for p in self._proxies if p.key_str() not in self._failed]
        self._cycle = itertools.cycle(usable) if usable else iter(())

    def set_proxies(self, proxies: list[Proxy]) -> None:
        with self._lock:
            self._proxies = list(proxies)
            keys = {p.key_str() for p in self._proxies}
            self._fail_counts = {k: v for k, v in self._fail_counts.items() if k in keys}
            self._failed = {k for k in self._failed if k in keys}
            self._request_counts = {k: v for k, v in self._request_counts.items() if k in keys}
            self._success_counts = {k: v for k, v in self._success_counts.items() if k in keys}
            self._rebuild_cycle_locked()

    def get_proxies(self) -> list[Proxy]:
        with self._lock:
            return list(self._proxies)

    def get_status(self) -> dict[str, dict]:
        """key_str() -> stats dict for every proxy currently in the pool.

        request_count/success_count/failure_count track forwarding attempts
        since this pool was created (or since the proxy was last removed and
        re-added) - they are in-memory only, like fail_count/failed, and
        reset on process restart.
        """
        with self._lock:
            return {
                p.key_str(): {
                    "failed": p.key_str() in self._failed,
                    "fail_count": self._fail_counts.get(p.key_str(), 0),
                    "request_count": self._request_counts.get(p.key_str(), 0),
                    "success_count": self._success_counts.get(p.key_str(), 0),
                    "failure_count": self._request_counts.get(p.key_str(), 0)
                    - self._success_counts.get(p.key_str(), 0),
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

    def pick_many(self, max_count: int) -> list[Proxy]:
        """Return up to max_count distinct usable proxies, in round-robin order.

        Used by the forwarders to build a retry sequence: if the first proxy
        fails, they fall through to the next one returned here instead of
        failing the client's request outright. Naturally returns fewer than
        max_count (down to a single proxy, or zero) when the pool doesn't
        have that many usable proxies - retrying stops once every usable
        proxy has been tried.
        """
        with self._lock:
            usable = [p for p in self._proxies if p.key_str() not in self._failed]
            if not usable:
                return []
            target = min(max_count, len(usable))
            result: list[Proxy] = []
            seen: set[str] = set()
            # Bounded by 2x usable size so a pathological _cycle state can
            # never spin forever; every usable proxy is reachable within one
            # full lap of the cycle.
            for _ in range(len(usable) * 2):
                if len(result) >= target:
                    break
                try:
                    p = next(self._cycle)
                except StopIteration:
                    break
                if p.key_str() in seen:
                    continue
                seen.add(p.key_str())
                result.append(p)
            return result

    def report_result(self, proxy: Proxy, success: bool) -> None:
        key = proxy.key_str()
        with self._lock:
            if key not in {p.key_str() for p in self._proxies}:
                return  # proxy was removed/replaced since this attempt started; ignore stale report

            self._request_counts[key] = self._request_counts.get(key, 0) + 1
            if success:
                self._success_counts[key] = self._success_counts.get(key, 0) + 1
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
                self._request_counts.pop(key, None)
                self._success_counts.pop(key, None)
            self._rebuild_cycle_locked()
