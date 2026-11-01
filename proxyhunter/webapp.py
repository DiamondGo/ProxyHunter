"""Browser-compatible aiohttp API; all shared state belongs to one event loop."""

from __future__ import annotations

import asyncio
import inspect
import json
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

from aiohttp import web

from proxyhunter.forwarder.pool import ProxyPool
from proxyhunter.geolocate import fill_missing_geo
from proxyhunter.jobs import JobRunner
from proxyhunter.settings import NETWORK_KEYS, SCRAPE_KEYS, SettingsStore, coerce_settings
from proxyhunter.store import ProxyStore
from proxyhunter.validator import ProxyValidator

BASE_DIR = Path(__file__).resolve().parent


@dataclass
class WebState:
    """Request ownership, also usable when create_app is hosted independently."""

    requests: set[asyncio.Task] = field(default_factory=set)
    restart_event: asyncio.Event = field(default_factory=asyncio.Event)
    closing: bool = False

    async def shutdown(self) -> None:
        self.closing = True
        tasks = tuple(task for task in self.requests if task is not asyncio.current_task())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


WEB_STATE = web.AppKey("proxyhunter.web_state", WebState)


class InvalidPayload(ValueError):
    pass


def _reject_constant(value: str):
    raise InvalidPayload(f"non-finite JSON number: {value}")


async def _object(request: web.Request, *, allow_empty: bool = False) -> dict:
    try:
        raw = await request.read()
        body = {} if allow_empty and not raw else json.loads(raw, parse_constant=_reject_constant)
    except (ValueError, UnicodeError, RecursionError) as exc:
        raise InvalidPayload("request body must be valid JSON") from exc
    if not isinstance(body, dict):
        raise InvalidPayload("request body must be a JSON object")
    return body


async def _keys(request: web.Request) -> list[str]:
    body = await _object(request)
    keys = body.get("keys", [])
    if not isinstance(keys, list) or any(not isinstance(key, str) or not key for key in keys):
        raise InvalidPayload("keys must be a list of nonempty strings")
    return keys


def _settings_patch(body: dict) -> dict:
    try:
        return coerce_settings(body)
    except (TypeError, ValueError, OverflowError) as exc:
        raise InvalidPayload(f"invalid settings value: {exc}") from exc


def _address(host: str, port: int) -> str:
    return f"[{host}]:{port}" if ":" in host else f"{host}:{port}"


def create_app(
    store: ProxyStore,
    pool: ProxyPool,
    jobs: JobRunner,
    settings: SettingsStore,
    active_network: dict,
    *,
    restart_callback: Callable | None = None,
    configured_network: dict | None = None,
) -> web.Application:
    """Create an app without starting listeners or owning the supplied services.

    restart_callback is a *signal*, never a process replacement: it is invoked
    after the restart response has been written. By default it sets the exposed
    WEB_STATE.restart_event. run_server owns draining and the actual restart.
    configured_network is the persisted startup snapshot; active_network holds
    actual bound addresses (which differ for port zero and wildcard hosts).
    """
    state = WebState()
    configured = dict(active_network if configured_network is None else configured_network)
    signal_restart = restart_callback or state.restart_event.set

    @web.middleware
    async def managed_requests(request, handler):
        if state.closing:
            return web.json_response({"error": "server is shutting down"}, status=503)
        task = asyncio.current_task()
        state.requests.add(task)
        try:
            try:
                return await handler(request)
            except InvalidPayload as exc:
                return web.json_response({"error": str(exc)}, status=400)
            except web.HTTPRequestEntityTooLarge:
                return web.json_response({"error": "request body is too large"}, status=400)
        finally:
            state.requests.discard(task)

    app = web.Application(middlewares=[managed_requests])
    app[WEB_STATE] = state

    async def shutdown(_app):
        await state.shutdown()

    app.on_shutdown.append(shutdown)

    async def index(request):
        return web.FileResponse(BASE_DIR / "templates" / "index.html")

    async def settings_page(request):
        return web.FileResponse(BASE_DIR / "templates" / "settings.html")

    async def list_proxies(request):
        selected_keys = {p.key_str() for p in pool.get_proxies()}
        pool_status = pool.get_status()
        proxies = store.all_known_proxies()
        proxies.sort(key=lambda p: (not p.alive, p.latency_ms is None, p.latency_ms or 0))
        return web.json_response(
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

    async def persist_selection():
        await settings.update({"selected_keys": [p.key_str() for p in pool.get_proxies()]})

    async def remove_proxies(request):
        keys = await _keys(request)
        removed = store.remove(keys)
        pool.remove_proxies(keys)
        await store.save()
        await persist_selection()
        return web.json_response({"removed": removed})

    async def trigger_scrape(request):
        overrides = _settings_patch(await _object(request))
        if not jobs.start_full_scrape(overrides):
            return web.json_response({"error": "a job is already running"}, status=409)
        return web.json_response({"started": True}, status=202)

    async def job_status(request):
        return web.json_response(jobs.status())

    async def validate_selected(request):
        keys = await _keys(request)
        since = store.revision
        proxies = [p for p in (store.get_by_key(k) for k in keys) if p is not None]
        if not proxies:
            return web.json_response({"error": "no known proxies matched the given keys"}, status=400)
        validator = ProxyValidator(
            timeout=settings.get("timeout", 8.0),
            workers=min(settings.get("workers", 50), max(1, len(proxies))),
            secondary_check=settings.get("secondary_check", True),
            geo_verify_via_proxy=settings.get("geo_verify_via_proxy", False),
        )
        results = await validator.validate_all(proxies)
        if settings.get("geo_lookup", True):
            await fill_missing_geo([p for p in results if p.alive])
        store.upsert_all(results, since=since)
        await store.save()
        pool.clear_failed([p.key_str() for p in results if p.alive])
        return web.json_response(
            {
                "checked": len(results),
                "alive": sum(1 for p in results if p.alive),
                "results": [p.to_dict() for p in results],
            }
        )

    async def forward_status(request):
        selected = pool.get_proxies()
        pool_status = pool.get_status()
        return web.json_response(
            {
                "selected": [{"key": p.key_str(), **pool_status[p.key_str()]} for p in selected],
                "count": len(selected),
                "usable_count": sum(1 for s in pool_status.values() if not s["failed"]),
                "has_usable": pool.has_usable_proxy(),
                "http_proxy": _address(active_network["forward_host"], active_network["http_proxy_port"]),
                "socks5_proxy": _address(active_network["forward_host"], active_network["socks_port"]),
            }
        )

    def known(keys):
        return [p for p in (store.get_by_key(k) for k in keys) if p is not None]

    async def forward_select(request):
        pool.set_proxies(known(await _keys(request)))
        await persist_selection()
        return web.json_response({"count": len(pool.get_proxies())})

    async def forward_add(request):
        proxies = known(await _keys(request))
        merged = {p.key_str(): p for p in pool.get_proxies()}
        merged.update((p.key_str(), p) for p in proxies)
        pool.set_proxies(list(merged.values()))
        await persist_selection()
        return web.json_response({"count": len(merged)})

    async def forward_remove(request):
        pool.remove_proxies(await _keys(request))
        await persist_selection()
        return web.json_response({"count": len(pool.get_proxies())})

    async def forward_reactivate(request):
        pool.clear_failed(await _keys(request))
        return web.json_response({"status": pool.get_status()})

    async def get_settings(request):
        current = settings.snapshot()
        current.pop("selected_keys", None)
        return web.json_response(
            {
                "settings": current,
                "scrape_keys": list(SCRAPE_KEYS),
                "network_keys": list(NETWORK_KEYS),
                "active_network": active_network,
                "restart_pending": any(current.get(k) != configured.get(k) for k in NETWORK_KEYS),
            }
        )

    async def update_settings(request):
        changes = _settings_patch(await _object(request))
        await settings.update(changes)
        return web.json_response(
            {
                "saved": True,
                "restart_required": any(k in NETWORK_KEYS for k in changes),
                "changed_network_fields": [k for k in changes if k in NETWORK_KEYS],
            }
        )

    async def restart(request):
        await _object(request, allow_empty=True)
        response = web.json_response({"restarting": True})
        await response.prepare(request)
        await response.write_eof()
        result = signal_restart()
        if inspect.isawaitable(result):
            await result
        return response

    app.add_routes(
        [
            web.get("/", index),
            web.get("/settings", settings_page),
            web.get("/api/proxies", list_proxies),
            web.post("/api/proxies/remove", remove_proxies),
            web.post("/api/scrape", trigger_scrape),
            web.get("/api/job", job_status),
            web.post("/api/validate", validate_selected),
            web.get("/api/forward/status", forward_status),
            web.post("/api/forward/select", forward_select),
            web.post("/api/forward/add", forward_add),
            web.post("/api/forward/remove", forward_remove),
            web.post("/api/forward/reactivate", forward_reactivate),
            web.get("/api/settings", get_settings),
            web.post("/api/settings", update_settings),
            web.post("/api/restart", restart),
            web.static("/static/", BASE_DIR / "static", show_index=False, follow_symlinks=False),
        ]
    )
    return app


async def start_server(settings_file: Path, cli_seed: dict, *, network_overrides=None, **kwargs):
    """Start a managed server; see lifecycle.start_server for the ownership API."""
    from proxyhunter.lifecycle import start_server as start

    return await start(settings_file, cli_seed, network_overrides=network_overrides, **kwargs)


async def run_server(
    settings_file: Path,
    cli_seed: dict,
    *,
    stop_event=None,
    restart_callback=None,
    network_overrides=None,
    **kwargs,
) -> None:
    """Await stop/restart and drain all resources before returning or reexecing."""
    from proxyhunter.lifecycle import run_server as run

    await run(
        settings_file,
        cli_seed,
        stop_event=stop_event,
        restart_callback=restart_callback,
        network_overrides=network_overrides,
        **kwargs,
    )
