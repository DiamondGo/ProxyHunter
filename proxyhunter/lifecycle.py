"""Single-loop application lifecycle, including failure-safe startup and restart."""

from __future__ import annotations

import asyncio
import inspect
import logging
import os
import signal
import sys
from collections.abc import Callable, Mapping
from pathlib import Path

from aiohttp import web

from proxyhunter.forwarder.http_proxy import LocalHttpProxy
from proxyhunter.forwarder.pool import ProxyPool
from proxyhunter.forwarder.socks_proxy import LocalSocks5Proxy
from proxyhunter.jobs import JobRunner
from proxyhunter.scheduler import Scheduler
from proxyhunter.settings import NETWORK_KEYS, SettingsStore, coerce_settings
from proxyhunter.store import DEFAULT_STATE_FILE, ProxyStore
from proxyhunter.webapp import WEB_STATE, _address, create_app

log = logging.getLogger(__name__)


async def _drain(task: asyncio.Task):
    """Join owned cleanup even if its caller is cancelled repeatedly (3.10)."""
    cancelled = None
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError as exc:
            cancelled = exc
        except Exception:  # noqa: BLE001 - retrieve the owned task's error below, after joining it
            break
    try:
        result = task.result()
    except BaseException:
        if cancelled is not None:
            raise cancelled
        raise
    if cancelled is not None:
        raise cancelled
    return result


class ApplicationServer:
    """Lazy owner of settings, state, jobs, scheduler, UI, and both forwarders.

    Await start(), then close() (or use async with). close is terminal,
    idempotent, shared by concurrent callers, and finishes despite cancellation
    before propagating it. Startup failures/cancellation also finish cleanup.
    No process replacement occurs here; restart_event is only a signal.

    configured_network: persisted startup settings; requested_network: effective
    bind settings after nonpersistent overrides; active_network: actual bound
    addresses. addresses contains all socket addresses, including dual-stack
    binds. Port-zero requests stay zero in settings and requested_network.
    """

    def __init__(
        self,
        settings_file: Path,
        cli_seed: dict,
        *,
        network_overrides: Mapping | None = None,
        shutdown_timeout: float = 5.0,
    ):
        self.settings = SettingsStore(settings_file)
        self.cli_seed = dict(cli_seed)
        self.store = ProxyStore(Path(self.cli_seed.get("state_file") or DEFAULT_STATE_FILE))
        self.pool = ProxyPool(settings=self.settings)
        self.jobs = JobRunner(self.store, self.settings)
        self.scheduler = Scheduler(self.store, self.pool, self.jobs, self.settings)
        if network_overrides is not None and (
            not isinstance(network_overrides, Mapping) or set(network_overrides) - set(NETWORK_KEYS)
        ):
            raise ValueError("network_overrides must contain only network settings")
        self.network_overrides = coerce_settings(network_overrides or {})
        self.shutdown_timeout = shutdown_timeout
        self.configured_network: dict = {}
        self.requested_network: dict = {}
        self.active_network: dict = {}
        self.addresses: dict[str, list[tuple]] = {}
        self.restart_event = asyncio.Event()
        self.app: web.Application | None = None
        self.runner: web.AppRunner | None = None
        self.site: web.TCPSite | None = None
        self.http_proxy: LocalHttpProxy | None = None
        self.socks_proxy: LocalSocks5Proxy | None = None
        self._lock = asyncio.Lock()
        self._close_task: asyncio.Task | None = None
        self._started = False
        self._closing = False
        self.closed = False

    @property
    def ui_address(self) -> tuple[str, int]:
        return self.addresses["ui"][0][:2]

    @property
    def http_address(self) -> tuple[str, int]:
        return self.addresses["http"][0][:2]

    @property
    def socks_address(self) -> tuple[str, int]:
        return self.addresses["socks"][0][:2]

    @property
    def ui_url(self) -> str:
        return "http://" + _address(*self.ui_address)

    async def start(self) -> ApplicationServer:
        try:
            async with self._lock:
                if self._closing:
                    raise RuntimeError("server is closed")
                if self._started:
                    return self
                await self.settings.load()
                if self.settings.is_new:
                    await self.settings.seed_from(
                        {k: v for k, v in self.cli_seed.items() if k != "state_file"}
                    )
                self.configured_network = {k: self.settings.get(k) for k in NETWORK_KEYS}
                self.requested_network = {**self.configured_network, **self.network_overrides}
                self.active_network = dict(self.requested_network)
                await self.store.load()
                self.pool.set_proxies(
                    [
                        p
                        for p in (self.store.get_by_key(k) for k in self.settings.get("selected_keys", []))
                        if p is not None
                    ]
                )
                net = self.requested_network
                self.http_proxy = LocalHttpProxy(
                    self.pool, self.settings, host=net["forward_host"], port=net["http_proxy_port"]
                )
                await self.http_proxy.start()
                self.socks_proxy = LocalSocks5Proxy(
                    self.pool, self.settings, host=net["forward_host"], port=net["socks_port"]
                )
                await self.socks_proxy.start()
                for name, proxy in (("http", self.http_proxy), ("socks", self.socks_proxy)):
                    self.addresses[name] = [sock.getsockname() for sock in proxy._server.sockets]
                self.active_network.update(
                    forward_host=self.http_proxy.host,
                    http_proxy_port=self.http_proxy.port,
                    socks_port=self.socks_proxy.port,
                )
                self.app = create_app(
                    self.store,
                    self.pool,
                    self.jobs,
                    self.settings,
                    self.active_network,
                    configured_network=self.configured_network,
                    restart_callback=self.restart_event.set,
                )
                self.runner = web.AppRunner(self.app, shutdown_timeout=self.shutdown_timeout)
                await self.runner.setup()
                self.site = web.TCPSite(self.runner, net["ui_host"], net["ui_port"])
                await self.site.start()
                self.addresses["ui"] = list(self.runner.addresses)
                self.active_network.update(ui_host=self.ui_address[0], ui_port=self.ui_address[1])
                await self.scheduler.start()
                self._started = True
                log.info("UI listening on %s", self.ui_url)
                log.info("HTTP proxy listening on %s", _address(*self.http_address))
                log.info("SOCKS5 proxy listening on %s", _address(*self.socks_address))
            return self
        except BaseException:
            await self.close()
            raise

    async def close(self) -> None:
        if self._close_task is None:
            self._closing = True
            self._close_task = asyncio.create_task(self._close(), name="proxyhunter-close")
        await _drain(self._close_task)

    shutdown = close

    async def _close(self) -> None:
        errors = []

        async def cleanup(operation):
            try:
                await operation()
            except Exception as exc:
                errors.append(exc)
                log.exception("server cleanup failed")

        async with self._lock:
            try:
                # Reject new requests immediately; close listeners before draining
                # work. Runner.cleanup also stops partial/registered UI sites.
                if self.app is not None:
                    self.app[WEB_STATE].closing = True
                if self.site is not None and self.runner is not None and self.site in self.runner.sites:
                    await cleanup(self.site.stop)
                await cleanup(self.scheduler.shutdown)
                for proxy in (self.http_proxy, self.socks_proxy):
                    if proxy is not None:
                        await cleanup(proxy.shutdown)
                if self.app is not None:
                    await cleanup(self.app[WEB_STATE].shutdown)
                await cleanup(self.jobs.shutdown)
                if self._started:
                    # Mutation owners have joined, but their queued saves may have
                    # been cancelled. Flush is only a barrier, so checkpoint memory
                    # and the actual pool selection first. Never do this for a
                    # partial startup: unloaded/corrupt originals must stay intact.
                    await cleanup(self.store.save)
                    await cleanup(
                        lambda: self.settings.update(
                            {"selected_keys": [p.key_str() for p in self.pool.get_proxies()]}
                        )
                    )
                await cleanup(self.store.flush)
                await cleanup(self.settings.flush)
                if self.runner is not None:
                    await cleanup(self.runner.cleanup)
            finally:
                self.closed = True
            if errors:
                raise errors[0]

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, *args) -> None:
        await self.close()


async def start_server(
    settings_file: Path,
    cli_seed: dict,
    *,
    network_overrides: Mapping | None = None,
    shutdown_timeout: float = 5.0,
) -> ApplicationServer:
    """Return a started owner. The caller must await server.close()."""
    return await ApplicationServer(
        settings_file,
        cli_seed,
        network_overrides=network_overrides,
        shutdown_timeout=shutdown_timeout,
    ).start()


def _restart_process() -> None:
    # orig_argv preserves interpreter flags, script/console entrypoints and -m.
    # argv[0] may be an alias; exec the actual current interpreter explicitly.
    argv = list(sys.orig_argv)
    os.execv(sys.executable, [sys.executable, *argv[1:]])


def _install_signals(stop_event: asyncio.Event) -> Callable[[], None]:
    loop = asyncio.get_running_loop()
    installed = []
    for signum in (signal.SIGINT, signal.SIGTERM):
        # Preserve an existing asyncio handler as well as the process handler.
        previous = signal.getsignal(signum)
        handle = getattr(loop, "_signal_handlers", {}).get(signum)
        try:
            loop.add_signal_handler(signum, stop_event.set)
        except (NotImplementedError, RuntimeError, ValueError):
            continue
        installed.append((signum, previous, handle))

    def restore():
        for signum, previous, handle in installed:
            loop.remove_signal_handler(signum)
            if handle is not None and not handle.cancelled():
                loop.add_signal_handler(signum, handle._callback, *handle._args)
            else:
                signal.signal(signum, previous)

    return restore


async def run_server(
    settings_file: Path,
    cli_seed: dict,
    *,
    stop_event: asyncio.Event | None = None,
    restart_callback: Callable | None = None,
    network_overrides: Mapping | None = None,
    shutdown_timeout: float = 5.0,
    handle_signals: bool = True,
    on_started: Callable[[ApplicationServer], object] | None = None,
) -> None:
    """Own the server until stop, restart, signal, or cancellation.

    on_started receives the started owner (sync or async). restart_callback is
    zero-argument, sync or async, invoked only after complete successful cleanup
    and signal restoration. The default reexec exists ONLY at this outer layer.
    Cancellation or a simultaneous explicit stop suppresses restart.
    """
    server = ApplicationServer(
        settings_file,
        cli_seed,
        network_overrides=network_overrides,
        shutdown_timeout=shutdown_timeout,
    )
    stop = stop_event if stop_event is not None else asyncio.Event()
    restore = _install_signals(stop) if handle_signals else lambda: None
    waiters: list[asyncio.Task] = []
    restart = False
    try:
        await server.start()
        if on_started is not None:
            result = on_started(server)
            if inspect.isawaitable(result):
                await result
        waiters = [
            asyncio.create_task(stop.wait(), name="proxyhunter-stop-wait"),
            asyncio.create_task(server.restart_event.wait(), name="proxyhunter-restart-wait"),
        ]
        await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
        restart = server.restart_event.is_set() and not stop.is_set()
    finally:

        async def finish():
            for task in waiters:
                task.cancel()
            await asyncio.gather(*waiters, return_exceptions=True)
            try:
                await server.close()
            finally:
                restore()

        await _drain(asyncio.create_task(finish(), name="proxyhunter-run-cleanup"))
    if restart and not stop.is_set():
        result = (restart_callback or _restart_process)()
        if inspect.isawaitable(result):
            await result
