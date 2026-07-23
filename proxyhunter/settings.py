from __future__ import annotations

import json
import threading
from pathlib import Path

DEFAULT_SETTINGS_FILE = "proxyhunter_settings.json"

# Settings the running process can apply immediately (read fresh on every job/request).
SCRAPE_KEYS = (
    "sources", "pages", "protocols", "workers", "timeout",
    "secondary_check", "geo_lookup", "geo_verify_via_proxy", "recheck_after", "limit",
    "sched_full_scrape_enabled", "sched_full_scrape_interval_hours",
    "sched_pool_refresh_enabled", "sched_pool_refresh_interval_hours", "sched_pool_top_n",
    "sched_pool_topup_enabled", "sched_pool_topup_interval_hours", "sched_pool_topup_min_count",
    "sched_pool_https_only",
    "sched_revalidate_enabled", "sched_revalidate_interval_hours",
    "pool_fail_threshold", "pool_max_retries",
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


def coerce_settings(changes: dict) -> dict:
    """Coerce incoming (e.g. JSON-from-UI) values to the expected type for each known key.
    Unknown keys are dropped rather than silently corrupting the settings file."""
    coerced = {}
    for key, value in changes.items():
        kind = _FIELD_TYPES.get(key)
        if kind is None:
            continue
        if kind == "int":
            coerced[key] = int(value)
        elif kind == "float":
            coerced[key] = float(value)
        elif kind == "bool":
            coerced[key] = bool(value)
        elif kind == "list":
            if isinstance(value, str):
                coerced[key] = [v.strip() for v in value.split(",") if v.strip()]
            else:
                coerced[key] = list(value)
        elif kind == "int_or_none":
            coerced[key] = None if value in (None, "", "null") else int(value)
        else:
            coerced[key] = value
    return coerced


class SettingsStore:
    """Persists proxyhunter's --serve settings to a JSON file.

    Once this file exists it's the source of truth for `--serve` runs; CLI
    flags only seed it the first time it's created.
    """

    def __init__(self, path: Path):
        self.path = path
        self._lock = threading.Lock()
        if path.exists():
            try:
                loaded = json.loads(path.read_text())
            except (json.JSONDecodeError, OSError):
                loaded = {}
            self.data = {**DEFAULTS, **loaded}
            self.is_new = False
        else:
            self.data = dict(DEFAULTS)
            self.is_new = True

    def save(self) -> None:
        with self._lock:
            self.path.write_text(json.dumps(self.data, indent=2, ensure_ascii=False))

    def get(self, key: str, default=None):
        with self._lock:
            return self.data.get(key, default)

    def snapshot(self) -> dict:
        with self._lock:
            return dict(self.data)

    def update(self, changes: dict) -> None:
        with self._lock:
            self.data.update(changes)
        self.save()

    def seed_from(self, values: dict) -> None:
        """Bootstrap the file from CLI flags. Only meaningful the first time it's created."""
        with self._lock:
            self.data.update({k: v for k, v in values.items() if v is not None})
        self.save()
        self.is_new = False
