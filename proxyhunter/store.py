from __future__ import annotations

import logging
import math
from copy import deepcopy
from dataclasses import fields
from datetime import datetime, timezone
from pathlib import Path

from proxyhunter.models import Proxy
from proxyhunter.persistence import JSONFile

log = logging.getLogger(__name__)

DEFAULT_STATE_FILE = "proxyhunter_state.json"

_PROXY_FIELDS = {field.name for field in fields(Proxy)}
_TEXT_FIELDS = ("country", "city", "isp", "anonymity")


def _loaded_records(loaded: object) -> dict[str, dict]:
    """Salvage valid rows from older/partly corrupt state, in the I/O worker."""
    records = {}
    if not isinstance(loaded, dict):
        if loaded is not None:
            log.warning("state JSON must be an object, starting fresh")
        return records
    for key, row in loaded.items():
        try:
            if not isinstance(row, dict):
                raise TypeError("record must be an object")
            proxy = Proxy(**{name: value for name, value in row.items() if name in _PROXY_FIELDS})
            if not all(isinstance(value, str) for value in (proxy.ip, proxy.protocol, proxy.source)):
                raise ValueError("ip, protocol and source must be strings")
            if not proxy.ip or not proxy.protocol or key != proxy.key_str():
                raise ValueError("record identity does not match its key")
            if (
                isinstance(proxy.port, bool)
                or not isinstance(proxy.port, int)
                or not 1 <= proxy.port <= 65535
            ):
                raise ValueError("invalid proxy port")
            if any(
                value is not None and not isinstance(value, str)
                for value in (getattr(proxy, name) for name in _TEXT_FIELDS)
            ):
                raise ValueError("invalid proxy metadata")
            if not isinstance(proxy.alive, bool) or not isinstance(proxy.supports_https, bool):
                raise TypeError("invalid proxy boolean")
            if proxy.https_only is not None and not isinstance(proxy.https_only, bool):
                raise ValueError("invalid https_only")
            latency = proxy.latency_ms
            if latency is not None and (
                isinstance(latency, bool)
                or not isinstance(latency, (int, float))
                or not math.isfinite(latency)
                or latency < 0
            ):
                raise ValueError("invalid proxy latency")
            # Invalid dates mean stale, not a reason to discard the proxy.
            try:
                if proxy.checked_at is not None:
                    _checked_time(proxy.checked_at)
            except (TypeError, ValueError):
                proxy.checked_at = None
            records[key] = proxy.to_dict()
        except (TypeError, ValueError, OverflowError) as exc:
            log.warning("ignoring invalid state record %s: %s", key, exc)
    return records


def _checked_time(value: str) -> datetime:
    if not isinstance(value, str):
        raise TypeError("checked_at must be an ISO timestamp")
    # Python 3.10's fromisoformat does not support a trailing Z.
    checked = datetime.fromisoformat(value[:-1] + "+00:00" if value.endswith("Z") else value)
    return checked if checked.tzinfo is not None else checked.replace(tzinfo=timezone.utc)


class ProxyStore:
    """Persists every proxy ever checked, keyed by (protocol, ip, port), so
    repeated runs can skip re-validating proxies that were checked recently
    (whether they were found alive or dead).

    Await load before use. In-memory operations are synchronous and confined to
    the application event loop; save writes a detached snapshot. Concurrent
    instances have ordered whole-file writes, not merged transactions.

    Capture revision before async work and pass it to upsert_all(since=...) to
    suppress results for keys removed in the meantime. Removal revisions are
    local to this instance's lifetime, never persisted or cleared by load.
    """

    def __init__(self, path: Path | str):
        self.path = Path(path)
        self.records: dict[str, dict] = {}
        self._file = JSONFile(self.path)
        self._revision = 0
        self._removal_revisions: dict[str, int] = {}

    @property
    def revision(self) -> int:
        """Read-only removal sequence; capture on this store before async work."""
        return self._revision

    async def load(self) -> None:
        self.records, _ = await self._file.load(_loaded_records)

    async def save(self) -> None:
        await self._file.save(deepcopy(self.records))

    async def flush(self) -> None:
        """Wait for already-started saves; upserts/removals still require save."""
        await self._file.flush()

    @staticmethod
    def _key(proxy: Proxy) -> str:
        return proxy.key_str()

    def get(self, proxy: Proxy) -> dict | None:
        return deepcopy(self.records.get(self._key(proxy)))

    def get_by_key(self, key: str) -> Proxy | None:
        rec = self.records.get(key)
        return Proxy(**rec) if rec else None

    def upsert_all(self, proxies: list[Proxy], *, since: int | None = None) -> None:
        """Write results unless their key was removed after the captured revision.

        Omit since for unconditional legacy writes. A current revision permits
        fresh rediscovery; neither kind of write clears older removal guards.
        """
        for p in proxies:
            key = self._key(p)
            if since is not None and self._removal_revisions.get(key, 0) > since:
                continue
            self.records[key] = p.to_dict()

    def remove(self, keys: list[str]) -> int:
        """Remove and guard every requested key, including keys not yet stored."""
        self._revision += 1
        removed = 0
        for key in keys:
            self._removal_revisions[key] = self._revision
            if self.records.pop(key, None) is not None:
                removed += 1
        return removed

    def all_known_proxies(self) -> list[Proxy]:
        return [Proxy(**rec) for rec in self.records.values()]

    def age_hours(self, proxy: Proxy) -> float | None:
        rec = self.get(proxy)
        checked = rec and rec.get("checked_at")
        if not checked:
            return None
        try:
            checked_at = _checked_time(checked)
        except (TypeError, ValueError):
            return None
        return (datetime.now(timezone.utc) - checked_at).total_seconds() / 3600

    def split_fresh(
        self, proxies: list[Proxy], recheck_after_hours: float
    ) -> tuple[list[Proxy], list[Proxy]]:
        """Split proxies into (need_check, reused_alive).

        Proxies whose cached result is younger than `recheck_after_hours` are
        skipped entirely - no request is made for them this run. If the cached
        result was alive it's carried through into the output unchanged; if it
        was dead it's simply dropped (this is what avoids re-validating proxies
        that recently failed during a fresh full scrape).
        """
        if recheck_after_hours <= 0:
            return list(proxies), []

        need_check: list[Proxy] = []
        reused_alive: list[Proxy] = []
        for p in proxies:
            age = self.age_hours(p)
            if age is not None and age <= recheck_after_hours:
                cached = Proxy(**self.get(p))
                if cached.alive:
                    reused_alive.append(cached)
                continue
            need_check.append(p)
        return need_check, reused_alive
