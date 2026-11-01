from __future__ import annotations

import logging
import math
from collections.abc import Mapping
from copy import deepcopy
from pathlib import Path

from proxyhunter.persistence import JSONFile

log = logging.getLogger(__name__)

DEFAULT_SETTINGS_FILE = "proxyhunter_settings.json"

# Settings the running process can apply immediately (read fresh on every job/request).
SCRAPE_KEYS = (
    "sources",
    "pages",
    "protocols",
    "workers",
    "timeout",
    "secondary_check",
    "geo_lookup",
    "geo_verify_via_proxy",
    "recheck_after",
    "limit",
    "sched_full_scrape_enabled",
    "sched_full_scrape_interval_hours",
    "sched_pool_refresh_enabled",
    "sched_pool_refresh_interval_hours",
    "sched_pool_top_n",
    "sched_pool_topup_enabled",
    "sched_pool_topup_interval_hours",
    "sched_pool_topup_min_count",
    "sched_pool_https_only",
    "sched_revalidate_enabled",
    "sched_revalidate_interval_hours",
    "pool_fail_threshold",
    "pool_max_retries",
)
# Settings baked into already-bound sockets at process start - changing these
# only takes effect after the process restarts.
NETWORK_KEYS = ("ui_host", "ui_port", "forward_host", "http_proxy_port", "socks_port")

# selected_keys isn't user-editable settings-page content; it's just how the
# forward pool selection survives a restart.
INTERNAL_KEYS = ("selected_keys",)

DEFAULTS = {
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

_FIELD_TYPES = {
    "sources": "list",
    "pages": "int",
    "protocols": "list",
    "workers": "int",
    "timeout": "float",
    "secondary_check": "bool",
    "geo_lookup": "bool",
    "geo_verify_via_proxy": "bool",
    "recheck_after": "float",
    "limit": "int_or_none",
    "ui_host": "str",
    "ui_port": "int",
    "forward_host": "str",
    "http_proxy_port": "int",
    "socks_port": "int",
    "selected_keys": "list",
    "sched_full_scrape_enabled": "bool",
    "sched_full_scrape_interval_hours": "float",
    "sched_pool_refresh_enabled": "bool",
    "sched_pool_refresh_interval_hours": "float",
    "sched_pool_top_n": "int",
    "sched_pool_topup_enabled": "bool",
    "sched_pool_topup_interval_hours": "float",
    "sched_pool_topup_min_count": "int",
    "sched_pool_https_only": "bool",
    "sched_revalidate_enabled": "bool",
    "sched_revalidate_interval_hours": "float",
    "pool_fail_threshold": "int",
    "pool_max_retries": "int",
    "ui_language": "str",
    # sched_*_last_run deliberately excluded: it's written only by the
    # scheduler itself (via SettingsStore.update), never accepted from the
    # settings-page POST.
}


_LAST_RUN_KEYS = tuple(key for key in DEFAULTS if key.endswith("_last_run"))
_PORT_KEYS = {"ui_port", "http_proxy_port", "socks_port"}
_ZERO_ALLOWED = {"limit", "recheck_after", "pool_max_retries", *_PORT_KEYS, *_LAST_RUN_KEYS}
_ZERO_ALLOWED.update(key for key in DEFAULTS if key.endswith("_interval_hours"))


def _number(value, *, integer: bool):
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        raise TypeError("expected a number")
    try:
        if integer:
            if isinstance(value, float) and (not math.isfinite(value) or not value.is_integer()):
                raise ValueError("expected a finite whole number")
            return int(value)
        result = float(value)
    except (ValueError, OverflowError) as exc:
        raise ValueError("expected a finite number" if not integer else "expected a whole number") from exc
    if not math.isfinite(result):
        raise ValueError("expected a finite number")
    return result


def _coerce_value(key: str, value, kind: str):
    if kind == "int_or_none":
        if value is None or (isinstance(value, str) and value.strip().lower() in ("", "null")):
            return None
        kind = "int"
    if kind in ("int", "float"):
        result = _number(value, integer=kind == "int")
        minimum = 0 if key in _ZERO_ALLOWED else 1
        # Floats such as timeout may be any positive fraction, not just >= 1.
        if result < 0 or (minimum and result == 0):
            raise ValueError(f"{key} must be {'nonnegative' if minimum == 0 else 'positive'}")
        if key in _PORT_KEYS and result > 65535:
            raise ValueError(f"{key} must be between 0 and 65535")
        return result
    if kind == "bool":
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)) and value in (0, 1):
            return bool(value)
        if isinstance(value, str):
            token = value.strip().lower()
            if token in ("true", "1", "yes", "on"):
                return True
            if token in ("false", "0", "no", "off"):
                return False
        raise ValueError(f"{key} must be a boolean (true/false or 1/0)")
    if kind == "list":
        if isinstance(value, str):
            value = value.split(",")
        if not isinstance(value, (list, tuple)) or any(not isinstance(item, str) for item in value):
            raise TypeError(f"{key} must be a string list or comma-separated string")
        return [item.strip() for item in value if item.strip()]
    if not isinstance(value, str):
        raise TypeError(f"{key} must be a string")
    # An empty bind host is the socket API's wildcard address, not invalid input.
    if not value.strip() and key not in ("ui_host", "forward_host"):
        raise ValueError(f"{key} must not be empty")
    return value.strip()


def coerce_settings(changes: Mapping) -> dict:
    """Validate an API patch; ignore unknown fields and internal last-run keys.

    Do not enforce UI suggestions (worker/page maxima, enumerated languages or
    source names). Port zero, zero intervals/retries/limit, empty selections and
    fractional positive timeouts remain usable by CLI/API clients.
    """
    if not isinstance(changes, Mapping):
        raise TypeError("settings must be a mapping")
    return {
        key: _coerce_value(key, value, _FIELD_TYPES[key])
        for key, value in changes.items()
        if key in _FIELD_TYPES
    }


def _trusted_changes(changes: Mapping) -> dict:
    """Internal callers may also set scheduler timestamps and extension keys."""
    result = coerce_settings(changes)
    for key, value in changes.items():
        if key in _LAST_RUN_KEYS:
            result[key] = _coerce_value(key, value, "float")
        elif key not in _FIELD_TYPES:
            result[key] = deepcopy(value)
    return result


def _loaded_settings(loaded: object) -> dict:
    result = deepcopy(DEFAULTS)
    if not isinstance(loaded, dict):
        if loaded is not None:
            log.warning("settings JSON must be an object, using defaults")
        return result
    for key, value in loaded.items():
        try:
            result.update(_trusted_changes({key: value}))
        except (TypeError, ValueError) as exc:
            log.warning("ignoring invalid setting %s: %s", key, exc)
    return result


class SettingsStore:
    """Persists proxyhunter's --serve settings to a JSON file.

    Once this file exists it's the source of truth for `--serve` runs; CLI
    flags only seed it the first time it's created.

    Await load before inspecting is_new or applying changes. In-memory access
    belongs to the application event loop. Concurrent saves are ordered whole-
    file snapshots, not cross-instance read/modify/write transactions. A failed
    save propagates its error without rolling back the in-memory patch.
    """

    def __init__(self, path: Path | str):
        self.path = Path(path)
        self.data = deepcopy(DEFAULTS)
        self.is_new: bool | None = None  # Known only after load; no constructor I/O.
        self._file = JSONFile(self.path)

    async def load(self) -> None:
        """Load explicitly; missing/corrupt content uses independent defaults."""
        data, exists = await self._file.load(_loaded_settings)
        self.data = data
        self.is_new = not exists

    async def save(self) -> None:
        await self._file.save(deepcopy(self.data))

    async def flush(self) -> None:
        """Wait for already-started saves, without writing unsaved changes."""
        await self._file.flush()

    def get(self, key: str, default=None):
        return deepcopy(self.data.get(key, default))

    def snapshot(self) -> dict:
        return deepcopy(self.data)

    async def update(self, changes: Mapping) -> None:
        """Apply a trusted patch (including scheduler timestamps), then persist."""
        self.data.update(_trusted_changes(changes))
        await self.save()

    async def seed_from(self, values: Mapping) -> None:
        """Bootstrap the file from CLI flags. Only meaningful the first time it's created."""
        if not isinstance(values, Mapping):
            raise TypeError("settings seed must be a mapping")
        self.data.update(_trusted_changes({k: v for k, v in values.items() if v is not None}))
        await self.save()
        self.is_new = False
