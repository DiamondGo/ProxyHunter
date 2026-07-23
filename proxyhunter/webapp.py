from __future__ import annotations

import logging
import os
import sys
import threading
import time
from pathlib import Path

from flask import Flask, jsonify, request, send_from_directory

from proxyhunter.forwarder.http_proxy import LocalHttpProxy
from proxyhunter.forwarder.pool import ProxyPool
from proxyhunter.forwarder.socks_proxy import LocalSocks5Proxy
from proxyhunter.geolocate import fill_missing_geo
from proxyhunter.jobs import JobRunner
from proxyhunter.scheduler import Scheduler
from proxyhunter.settings import NETWORK_KEYS, SCRAPE_KEYS, SettingsStore, coerce_settings
from proxyhunter.store import ProxyStore
from proxyhunter.validator import ProxyValidator

log = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent

RESTART_DELAY_SECONDS = 0.5


def _restart_process() -> None:
    time.sleep(RESTART_DELAY_SECONDS)

    # Most sockets (including our own forwarder listeners) are non-inheritable
    # by default in Python 3.4+ (PEP 446), so exec() closes them automatically.
    # Werkzeug's dev server is the exception: it deliberately marks its socket
    # inheritable and remembers the fd via WERKZEUG_SERVER_FD (for its own
    # --reload support), which would otherwise survive into the new process
    # via the inherited environment and keep the old UI port bound. Close it
    # explicitly so the new process can rebind immediately.
    fd = os.environ.pop("WERKZEUG_SERVER_FD", None)
    if fd is not None:
        try:
            os.close(int(fd))
        except OSError:
            pass

    log.info("restarting: python -m proxyhunter %s", " ".join(sys.argv[1:]))
    os.execv(sys.executable, [sys.executable, "-m", "proxyhunter"] + sys.argv[1:])


def create_app(store: ProxyStore, pool: ProxyPool, jobs: JobRunner, settings: SettingsStore, active_network: dict) -> Flask:
    app = Flask(__name__, static_folder=str(BASE_DIR / "static"))

    @app.get("/")
    def index():
        return send_from_directory(BASE_DIR / "templates", "index.html")

    @app.get("/settings")
    def settings_page():
        return send_from_directory(BASE_DIR / "templates", "settings.html")

    @app.get("/api/proxies")
    def list_proxies():
        selected_keys = {p.key_str() for p in pool.get_proxies()}
        pool_status = pool.get_status()
        proxies = store.all_known_proxies()
        proxies.sort(key=lambda p: (not p.alive, p.latency_ms is None, p.latency_ms or 0))
        return jsonify(
            {
                "proxies": [
                    {
                        **p.to_dict(),
                        "key": p.key_str(),
                        "selected": p.key_str() in selected_keys,
                        "pool_failed": pool_status.get(p.key_str(), {}).get("failed", False),
                        "pool_fail_count": pool_status.get(p.key_str(), {}).get("fail_count", 0),
                        "pool_request_count": pool_status.get(p.key_str(), {}).get("request_count", 0),
                        "pool_success_count": pool_status.get(p.key_str(), {}).get("success_count", 0),
                    }
                    for p in proxies
                ],
                "count": len(proxies),
            }
        )

    @app.post("/api/proxies/remove")
    def remove_proxies():
        body = request.get_json(silent=True) or {}
        keys = body.get("keys") or []
        removed = store.remove(keys)
        store.save()
        # A deleted proxy can't stay in the forward pool either.
        pool.remove_proxies(keys)
        settings.update({"selected_keys": [p.key_str() for p in pool.get_proxies()]})
        return jsonify({"removed": removed})

    @app.post("/api/scrape")
    def trigger_scrape():
        overrides = request.get_json(silent=True) or {}
        started = jobs.start_full_scrape(overrides)
        if not started:
            return jsonify({"error": "a job is already running"}), 409
        return jsonify({"started": True}), 202

    @app.get("/api/job")
    def job_status():
        return jsonify(jobs.status())

    @app.post("/api/validate")
    def validate_selected():
        body = request.get_json(silent=True) or {}
        keys = body.get("keys") or []
        proxies = [p for p in (store.get_by_key(k) for k in keys) if p is not None]
        if not proxies:
            return jsonify({"error": "no known proxies matched the given keys"}), 400

        validator = ProxyValidator(
            timeout=settings.get("timeout", 8.0),
            workers=min(settings.get("workers", 50), max(1, len(proxies))),
            secondary_check=settings.get("secondary_check", True),
            geo_verify_via_proxy=settings.get("geo_verify_via_proxy", False),
        )
        results = validator.validate_all(proxies)

        if settings.get("geo_lookup", True):
            fill_missing_geo([p for p in results if p.alive])

        store.upsert_all(results)
        store.save()
        # A proxy that's back up after manual re-validation should be given a
        # fresh start in the forward pool's failure tracking too.
        pool.clear_failed([p.key_str() for p in results if p.alive])
        return jsonify(
            {
                "checked": len(results),
                "alive": sum(1 for p in results if p.alive),
                "results": [p.to_dict() for p in results],
            }
        )

    @app.get("/api/forward/status")
    def forward_status():
        selected = pool.get_proxies()
        pool_status = pool.get_status()
        return jsonify(
            {
                "selected": [
                    {
                        "key": p.key_str(),
                        **pool_status.get(
                            p.key_str(),
                            {"failed": False, "fail_count": 0, "request_count": 0, "success_count": 0, "failure_count": 0},
                        ),
                    }
                    for p in selected
                ],
                "count": len(selected),
                "usable_count": sum(1 for s in pool_status.values() if not s["failed"]),
                "has_usable": pool.has_usable_proxy(),
                "http_proxy": f"{active_network['forward_host']}:{active_network['http_proxy_port']}",
                "socks5_proxy": f"{active_network['forward_host']}:{active_network['socks_port']}",
            }
        )

    @app.post("/api/forward/select")
    def forward_select():
        body = request.get_json(silent=True) or {}
        keys = body.get("keys") or []
        proxies = [p for p in (store.get_by_key(k) for k in keys) if p is not None]
        pool.set_proxies(proxies)
        settings.update({"selected_keys": [p.key_str() for p in proxies]})
        return jsonify({"count": len(proxies)})

    @app.post("/api/forward/add")
    def forward_add():
        """Merge the given proxies into the existing pool, deduped by key -
        unlike /api/forward/select this never drops proxies already in the pool."""
        body = request.get_json(silent=True) or {}
        keys = body.get("keys") or []
        proxies = [p for p in (store.get_by_key(k) for k in keys) if p is not None]
        merged = {p.key_str(): p for p in pool.get_proxies()}
        for p in proxies:
            merged[p.key_str()] = p
        result = list(merged.values())
        pool.set_proxies(result)
        settings.update({"selected_keys": [p.key_str() for p in result]})
        return jsonify({"count": len(result)})

    @app.post("/api/forward/remove")
    def forward_remove():
        body = request.get_json(silent=True) or {}
        keys = body.get("keys") or []
        pool.remove_proxies(keys)
        settings.update({"selected_keys": [p.key_str() for p in pool.get_proxies()]})
        return jsonify({"count": len(pool.get_proxies())})

    @app.post("/api/forward/reactivate")
    def forward_reactivate():
        """Manually clear a pool proxy's failed/fail-count state without
        re-validating it, so it's picked by the load balancer again."""
        body = request.get_json(silent=True) or {}
        keys = body.get("keys") or []
        pool.clear_failed(keys)
        return jsonify({"status": pool.get_status()})

    @app.get("/api/settings")
    def get_settings():
        current = settings.snapshot()
        current.pop("selected_keys", None)
        return jsonify(
            {
                "settings": current,
                "scrape_keys": list(SCRAPE_KEYS),
                "network_keys": list(NETWORK_KEYS),
                "active_network": active_network,
                "restart_pending": any(current.get(k) != active_network.get(k) for k in NETWORK_KEYS),
            }
        )

    @app.post("/api/settings")
    def update_settings():
        body = request.get_json(silent=True) or {}
        try:
            changes = coerce_settings(body)
        except (TypeError, ValueError) as exc:
            return jsonify({"error": f"invalid settings value: {exc}"}), 400
        settings.update(changes)
        restart_required = any(k in NETWORK_KEYS for k in changes)
        return jsonify(
            {
                "saved": True,
                "restart_required": restart_required,
                "changed_network_fields": [k for k in changes if k in NETWORK_KEYS],
            }
        )

    @app.post("/api/restart")
    def restart():
        threading.Thread(target=_restart_process, daemon=True).start()
        return jsonify({"restarting": True})

    return app


def run_server(settings_file: Path, cli_seed: dict) -> None:
    settings = SettingsStore(settings_file)
    if settings.is_new:
        settings.seed_from({k: v for k, v in cli_seed.items() if k != "state_file"})

    active_network = {k: settings.get(k) for k in NETWORK_KEYS}

    store_path = Path(cli_seed.get("state_file")) if cli_seed.get("state_file") else Path("proxyhunter_state.json")
    store = ProxyStore(store_path)
    pool = ProxyPool()
    pool.set_proxies([p for p in (store.get_by_key(k) for k in settings.get("selected_keys", [])) if p is not None])
    jobs = JobRunner(store, settings)
    scheduler = Scheduler(store, pool, jobs, settings)
    scheduler.start_in_thread()

    http_proxy = LocalHttpProxy(
        pool, settings, host=active_network["forward_host"], port=active_network["http_proxy_port"]
    )
    socks_proxy = LocalSocks5Proxy(
        pool, settings, host=active_network["forward_host"], port=active_network["socks_port"]
    )
    http_proxy.start_in_thread()
    socks_proxy.start_in_thread()
    log.info("local HTTP forward proxy listening on %s:%d", active_network["forward_host"], active_network["http_proxy_port"])
    log.info("local SOCKS5 forward proxy listening on %s:%d", active_network["forward_host"], active_network["socks_port"])

    app = create_app(store, pool, jobs, settings, active_network)

    log.info("UI listening on http://%s:%d", active_network["ui_host"], active_network["ui_port"])
    log.info("settings file: %s", settings_file)
    app.run(host=active_network["ui_host"], port=active_network["ui_port"], threaded=True)
