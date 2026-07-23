from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from proxyhunter.models import Proxy

log = logging.getLogger(__name__)

DEFAULT_STATE_FILE = "proxyhunter_state.json"


class ProxyStore:
    """Persists every proxy ever checked, keyed by (protocol, ip, port), so
    repeated runs can skip re-validating proxies that were checked recently
    (whether they were found alive or dead)."""

    def __init__(self, path: Path):
        self.path = path
        self.records: dict[str, dict] = {}
        if self.path.exists():
            try:
                self.records = json.loads(self.path.read_text())
            except (json.JSONDecodeError, OSError) as exc:
                log.warning("could not read state file %s, starting fresh: %s", self.path, exc)
                self.records = {}

    def save(self) -> None:
        self.path.write_text(json.dumps(self.records, indent=2, ensure_ascii=False))

    @staticmethod
    def _key(proxy: Proxy) -> str:
        return proxy.key_str()

    def get(self, proxy: Proxy) -> dict | None:
        return self.records.get(self._key(proxy))

    def get_by_key(self, key: str) -> Proxy | None:
        rec = self.records.get(key)
        return Proxy(**rec) if rec else None

    def upsert_all(self, proxies: list[Proxy]) -> None:
        for p in proxies:
            self.records[self._key(p)] = p.to_dict()

    def remove(self, keys: list[str]) -> int:
        removed = 0
        for key in keys:
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
        checked_at = datetime.fromisoformat(checked)
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
