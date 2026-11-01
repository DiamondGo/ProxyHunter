"""Managed application ownership and local socket E2E tests."""

from __future__ import annotations

import asyncio
import json
import os
import signal
import sys
import threading
from unittest.mock import AsyncMock

import aiohttp
import pytest
from aiohttp import web

import proxyhunter.jobs as jobs_module
import proxyhunter.validator as validator_module
from proxyhunter import lifecycle, persistence, webapp
from proxyhunter.lifecycle import ApplicationServer, run_server, start_server
from proxyhunter.models import Proxy
from proxyhunter.settings import NETWORK_KEYS, SettingsStore
from proxyhunter.store import ProxyStore
from tests.network_fixtures import bounded, close, http_response

PORT_ZERO = {
    "ui_host": "127.0.0.1",
    "forward_host": "127.0.0.1",
    "ui_port": 0,
    "http_proxy_port": 0,
    "socks_port": 0,
}


def owner(tmp_path, **kwargs):
    return ApplicationServer(
        tmp_path / "settings.json",
        {
            "state_file": str(tmp_path / "state.json"),
            "geo_lookup": False,
            **PORT_ZERO,
        },
        **kwargs,
    )


async def assert_closed(server):
    assert server.closed
    assert not server.jobs._tasks
    assert server.scheduler._task is None
    if server.app is not None:
        assert not server.app[webapp.WEB_STATE].requests
    if server.runner is not None:
        assert not server.runner.sites
        assert server.runner.server is None
    for proxy in (server.http_proxy, server.socks_proxy):
        if proxy is not None:
            assert proxy._server is None
            assert not proxy._handlers
            assert not proxy._clients
    for addresses in server.addresses.values():
        for address in addresses:
            with pytest.raises(OSError):
                await asyncio.open_connection(*address[:2])


@bounded
async def test_lazy_constructor_port_zero_and_snapshots(tmp_path, monkeypatch):
    baseline = asyncio.all_tasks()
    with monkeypatch.context() as patch:
        patch.setattr(SettingsStore, "load", AsyncMock(side_effect=AssertionError("constructor I/O")))
        patch.setattr(ProxyStore, "load", AsyncMock(side_effect=AssertionError("constructor I/O")))
        server = owner(tmp_path)
    assert not list(tmp_path.iterdir())
    assert server.runner is None
    async with server as started:
        assert started is server
        assert await server.start() is server
        assert all(server.requested_network[key] == 0 for key in ("ui_port", "http_proxy_port", "socks_port"))
        for key in ("ui_port", "http_proxy_port", "socks_port"):
            assert server.active_network[key] > 0
            assert server.settings.get(key) == 0
        assert server.http_address == (server.http_proxy.host, server.http_proxy.port)
        assert server.socks_address == (server.socks_proxy.host, server.socks_proxy.port)
        async with aiohttp.ClientSession() as client:
            response = await client.get(server.ui_url + "/api/settings")
            body = await response.json()
            assert body["active_network"] == server.active_network
            assert body["restart_pending"] is False
            response = await client.get(server.ui_url + "/api/forward/status")
            body = await response.json()
            assert body["http_proxy"] == f"127.0.0.1:{server.http_proxy.port}"
            assert body["socks5_proxy"] == f"127.0.0.1:{server.socks_proxy.port}"
    await server.close()
    await server.shutdown()
    await assert_closed(server)
    assert not (asyncio.all_tasks() - baseline)
    with pytest.raises(RuntimeError, match="closed"):
        await server.start()


@bounded
async def test_persisted_settings_selection_and_nonpersistent_overrides(tmp_path):
    settings = SettingsStore(tmp_path / "settings.json")
    store = ProxyStore(tmp_path / "state.json")
    await settings.load()
    await store.load()
    p = Proxy("127.0.0.1", 23456, "http", "local", alive=True)
    store.upsert_all([p])
    await store.save()
    await settings.update({"ui_port": 54321, "selected_keys": [p.key_str(), "unknown"], "workers": 9})
    server = await start_server(
        settings.path, {"state_file": str(store.path), "workers": 77}, network_overrides=PORT_ZERO
    )
    try:
        assert server.settings.get("workers") == 9
        assert server.settings.get("ui_port") == 54321
        assert server.configured_network["ui_port"] == 54321
        assert server.requested_network["ui_port"] == 0
        assert [p.key_str() for p in server.pool.get_proxies()] == [p.key_str()]
        async with aiohttp.ClientSession() as client:
            body = await (await client.get(server.ui_url + "/api/settings")).json()
            assert body["restart_pending"] is False
            await client.post(server.ui_url + "/api/settings", json={"ui_port": 12345})
            body = await (await client.get(server.ui_url + "/api/settings")).json()
            assert body["restart_pending"] is True
        assert json.loads(settings.path.read_text())["ui_port"] == 12345
    finally:
        await server.close()
    await assert_closed(server)


@bounded
async def test_actual_http_and_socks_listeners_forward_locally(tmp_path, upstream_server, tcp_server):
    async def plain(head, body):
        assert b"GET http://local.invalid/data HTTP/1.1" in head
        return http_response(body=b"forwarded locally")

    async def echo(reader, writer):
        writer.write(await reader.readexactly(4))
        await writer.drain()

    upstream = await upstream_server(plain=plain)
    target = await tcp_server(echo)
    async with owner(tmp_path) as server:
        server.store.upsert_all([upstream.proxy(alive=True)])
        async with aiohttp.ClientSession() as client:
            response = await client.post(
                server.ui_url + "/api/forward/select", json={"keys": [upstream.proxy().key_str()]}
            )
            assert await response.json() == {"count": 1}
            response = await client.get(
                "http://local.invalid/data", proxy="http://" + webapp._address(*server.http_address)
            )
            assert await response.read() == b"forwarded locally"
        reader, writer = await asyncio.open_connection(*server.socks_address)
        try:
            writer.write(b"\x05\x01\x00")
            await writer.drain()
            assert await reader.readexactly(2) == b"\x05\x00"
            writer.write(b"\x05\x01\x00\x01\x7f\x00\x00\x01" + target.port.to_bytes(2, "big"))
            await writer.drain()
            assert (await reader.readexactly(10))[:2] == b"\x05\x00"
            writer.write(b"ping")
            await writer.drain()
            assert await reader.readexactly(4) == b"ping"
        finally:
            await close(writer)
    await assert_closed(server)


START_STAGES = (
    (lifecycle.SettingsStore, "load"),
    (lifecycle.SettingsStore, "seed_from"),
    (lifecycle.ProxyStore, "load"),
    (lifecycle.LocalHttpProxy, "start"),
    (lifecycle.LocalSocks5Proxy, "start"),
    (web.AppRunner, "setup"),
    (web.TCPSite, "start"),
    (lifecycle.Scheduler, "start"),
)


@pytest.mark.parametrize("target,method", START_STAGES)
@pytest.mark.parametrize("cancelled", [False, True], ids=["failure", "cancelled"])
@bounded
async def test_every_async_startup_stage_cleans_partial_resources(
    tmp_path, monkeypatch, target, method, cancelled
):
    server = owner(tmp_path)
    original = getattr(target, method)

    async def fail_after(self, *args, **kwargs):
        await original(self, *args, **kwargs)
        if cancelled:
            raise asyncio.CancelledError
        raise RuntimeError("injected startup failure")

    monkeypatch.setattr(target, method, fail_after)
    with pytest.raises(asyncio.CancelledError if cancelled else RuntimeError):
        await server.start()
    await server.close()
    await assert_closed(server)


@bounded
async def test_sync_app_startup_failure_and_real_bind_failure_cleaned(tmp_path, monkeypatch):
    server = owner(tmp_path)
    with monkeypatch.context() as patch:
        patch.setattr(
            lifecycle, "create_app", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("app"))
        )
        with pytest.raises(RuntimeError, match="app"):
            await server.start()
    await assert_closed(server)
    occupied = await asyncio.start_server(lambda r, w: w.close(), "127.0.0.1", 0)
    port = occupied.sockets[0].getsockname()[1]
    try:
        other = owner(tmp_path, network_overrides={"socks_port": port})
        with pytest.raises(OSError):
            await other.start()
        await assert_closed(other)
    finally:
        occupied.close()
        await occupied.wait_closed()


@bounded
async def test_shutdown_cancels_real_manual_validator_sessions(
    tmp_path, monkeypatch, upstream_server, http_server
):
    async def ip(request):
        return web.json_response({"ip": "192.0.2.1"})

    ip_url = await http_server(ip)
    monkeypatch.setattr(validator_module, "REAL_IP_URL", ip_url)
    monkeypatch.setattr(validator_module, "PRIMARY_HTTP_URL", "http://local.invalid/")
    upstream = await upstream_server(stall=True)
    server = await owner(tmp_path).start()
    server.store.upsert_all([upstream.proxy()])
    client = aiohttp.ClientSession()
    task = asyncio.create_task(
        client.post(server.ui_url + "/api/validate", json={"keys": [upstream.proxy().key_str()]})
    )
    try:
        await upstream.entered.wait()
        assert server.app[webapp.WEB_STATE].requests
        await server.close()
        await asyncio.gather(task, return_exceptions=True)
        assert not server.app[webapp.WEB_STATE].requests
        await assert_closed(server)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await client.close()
        await server.close()


@bounded
async def test_cancelled_concurrent_close_waits_for_manual_cleanup(tmp_path, monkeypatch):
    entered, cleaning, release = (asyncio.Event() for _ in range(3))

    async def validate(self, proxies):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cleaning.set()
            await release.wait()

    monkeypatch.setattr(webapp.ProxyValidator, "validate_all", validate)
    server = await owner(tmp_path).start()
    p = Proxy("127.0.0.1", 1234, "http", "local")
    server.store.upsert_all([p])
    async with aiohttp.ClientSession() as client:
        request = asyncio.create_task(
            client.post(server.ui_url + "/api/validate", json={"keys": [p.key_str()]})
        )
        closer = None
        second = None
        try:
            await entered.wait()
            closer = asyncio.create_task(server.close())
            await cleaning.wait()
            closer.cancel()
            second = asyncio.create_task(server.close())
            await asyncio.sleep(0)
            closer.cancel()
            assert not closer.done()
            assert not second.done()
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await closer
            await second
            await asyncio.gather(request, return_exceptions=True)
        finally:
            release.set()
            request.cancel()
            await asyncio.gather(request, *[t for t in (closer, second) if t], return_exceptions=True)
            await server.close()
    await assert_closed(server)


@bounded
async def test_restart_drains_jobs_manual_requests_forwarders_and_flush_before_callback(
    tmp_path, monkeypatch
):
    manual_entered, job_entered, cleaning, release = (asyncio.Event() for _ in range(4))
    holder = []
    callbacks = []
    flushed = []

    async def validate(self, proxies):
        manual_entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cleaning.set()
            await release.wait()

    async def scrape(*args, **kwargs):
        job_entered.set()
        await asyncio.Event().wait()

    async def restarted():
        server = holder[0]
        await assert_closed(server)
        assert flushed == ["store", "settings"]
        callbacks.append(True)

    def started(server):
        holder.append(server)
        p = Proxy("127.0.0.1", 1234, "http", "local")
        server.store.upsert_all([p])
        for name, obj in (("store", server.store), ("settings", server.settings)):
            original = obj.flush

            async def flush(name=name, original=original):
                await original()
                flushed.append(name)

            monkeypatch.setattr(obj, "flush", flush)

    monkeypatch.setattr(webapp.ProxyValidator, "validate_all", validate)
    monkeypatch.setattr(jobs_module, "scrape_all", scrape)
    ready = asyncio.Event()

    def on_started(server):
        started(server)
        ready.set()

    runner = asyncio.create_task(
        run_server(
            tmp_path / "settings.json",
            {
                "state_file": str(tmp_path / "state.json"),
                "geo_lookup": False,
                **PORT_ZERO,
            },
            restart_callback=restarted,
            on_started=on_started,
            handle_signals=False,
        )
    )
    request = None
    writer = None
    try:
        await ready.wait()
        server = holder[0]
        async with aiohttp.ClientSession() as client:
            response = await client.post(server.ui_url + "/api/scrape", json={})
            assert response.status == 202
            await job_entered.wait()
            request = asyncio.create_task(
                client.post(
                    server.ui_url + "/api/validate",
                    json={"keys": [server.store.all_known_proxies()[0].key_str()]},
                )
            )
            await manual_entered.wait()
            _, writer = await asyncio.open_connection(*server.http_address)  # Idle accepted client.
            response = await client.post(server.ui_url + "/api/restart")
            assert response.status == 200
            assert await response.json() == {"restarting": True}
            await cleaning.wait()
            assert not callbacks
            assert not runner.done()
            release.set()
            await runner
            await asyncio.gather(request, return_exceptions=True)
        assert callbacks == [True]
        assert not server.jobs.status()["running"]
    finally:
        release.set()
        runner.cancel()
        await asyncio.gather(runner, return_exceptions=True)
        if request is not None:
            request.cancel()
            await asyncio.gather(request, return_exceptions=True)
        if writer is not None:
            await close(writer)


@pytest.mark.parametrize("cancelled", [False, True])
@bounded
async def test_run_server_explicit_stop_and_cancellation_never_restart(tmp_path, cancelled):
    ready, stop = asyncio.Event(), asyncio.Event()
    servers = []

    def started(server):
        servers.append(server)
        ready.set()

    restart = AsyncMock()
    task = asyncio.create_task(
        webapp.run_server(
            tmp_path / "settings.json",
            {
                "state_file": str(tmp_path / "state.json"),
                **PORT_ZERO,
            },
            stop_event=stop,
            restart_callback=restart,
            on_started=started,
            handle_signals=False,
        )
    )
    await ready.wait()
    if cancelled:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    else:
        stop.set()
        await task
    restart.assert_not_called()
    await assert_closed(servers[0])


@pytest.mark.parametrize("ending", ["restart", "stop", "cancel"])
@bounded
async def test_shutdown_checkpoints_api_mutations_queued_behind_active_writes(tmp_path, monkeypatch, ending):
    ready, stop, quiesced = (asyncio.Event() for _ in range(3))
    release = threading.Event()
    servers, callbacks, tasks = [], [], []
    kept = Proxy("127.0.0.1", 1234, "http", "local", alive=True)
    removed = Proxy("127.0.0.1", 1235, "http", "local", alive=True)
    selection = [removed.key_str(), kept.key_str()]
    expected_store = {kept.key_str(): kept.to_dict()}
    expected_settings = {}

    def started(server):
        servers.append(server)
        ready.set()

    async def assert_checkpoint():
        server = servers[0]
        await assert_closed(server)
        store, settings = ProxyStore(server.store.path), SettingsStore(server.settings.path)
        await asyncio.gather(store.load(), settings.load())
        assert (store.records, settings.snapshot()) == (expected_store, expected_settings)
        assert json.loads(store.path.read_text()) == expected_store
        assert json.loads(settings.path.read_text()) == expected_settings

    async def restarted():
        await assert_checkpoint()
        callbacks.append(True)

    runner = asyncio.create_task(
        run_server(
            tmp_path / "settings.json",
            {"state_file": str(tmp_path / "state.json"), "geo_lookup": False, **PORT_ZERO},
            on_started=started,
            stop_event=stop,
            restart_callback=restarted,
            handle_signals=False,
        )
    )
    tasks.append(runner)
    async with aiohttp.ClientSession() as client:
        try:
            await ready.wait()
            server = servers[0]
            server.store.upsert_all([kept, removed])
            await server.store.save()
            response = await client.post(
                server.ui_url + "/api/forward/select", json={"keys": [removed.key_str()]}
            )
            assert await response.json() == {"count": 1}
            before_store = json.loads(server.store.path.read_text())
            before_settings = json.loads(server.settings.path.read_text())
            entered = {path: asyncio.Event() for path in (server.store.path, server.settings.path)}
            writes = {path: [] for path in entered}
            queued = {name: asyncio.Event() for name in ("selection", "settings", "removal")}
            loop = asyncio.get_running_loop()
            atomic_write, save = persistence._atomic_write, persistence.JSONFile.save

            def gated_write(path, snapshot):
                assert threading.get_ident() != loop_thread
                if not writes[path]:
                    loop.call_soon_threadsafe(entered[path].set)
                    if not release.wait(4):
                        raise AssertionError("test did not release disk writer")
                atomic_write(path, snapshot)
                writes[path].append(snapshot)

            async def observe_save(file, snapshot):
                if file is server.settings._file:
                    if snapshot["selected_keys"] == selection:
                        queued["selection"].set()
                    if snapshot["workers"] == 7:
                        queued["settings"].set()
                elif file is server.store._file and removed.key_str() not in snapshot:
                    queued["removal"].set()
                # No intervening await: the real save queues on the held path lock
                # before the test can resume from its observation event.
                await save(file, snapshot)

            shutdown_jobs = server.jobs.shutdown

            async def join_jobs():
                await shutdown_jobs()
                quiesced.set()

            loop_thread = threading.get_ident()
            monkeypatch.setattr(persistence, "_atomic_write", gated_write)
            monkeypatch.setattr(persistence.JSONFile, "save", observe_save)
            monkeypatch.setattr(server.jobs, "shutdown", join_jobs)
            active = [asyncio.create_task(obj.save()) for obj in (server.store, server.settings)]
            tasks.extend(active)
            await asyncio.gather(*(event.wait() for event in entered.values()))

            for name, path, body in (
                ("selection", "/api/forward/select", {"keys": selection}),
                ("settings", "/api/settings", {"workers": 7}),
                ("removal", "/api/proxies/remove", {"keys": [removed.key_str()]}),
            ):
                tasks.append(asyncio.create_task(client.post(server.ui_url + path, json=body)))
                await queued[name].wait()

            assert server.store.records == expected_store
            assert [p.key_str() for p in server.pool.get_proxies()] == [kept.key_str()]
            # Removal has not reached persist_selection; even settings memory is stale.
            assert server.settings.get("selected_keys") == selection
            assert server.settings.get("workers") == 7
            expected_settings = {**server.settings.snapshot(), "selected_keys": [kept.key_str()]}
            assert json.loads(server.store.path.read_text()) == before_store
            assert json.loads(server.settings.path.read_text()) == before_settings

            if ending == "restart":
                response = await client.post(server.ui_url + "/api/restart")
                assert await response.json() == {"restarting": True}
            elif ending == "stop":
                stop.set()
            else:
                runner.cancel()
            await quiesced.wait()
            assert not server.app[webapp.WEB_STATE].requests
            assert server.scheduler._task is None
            assert not server.jobs._tasks
            assert not runner.done()
            assert not callbacks
            assert all(not task.done() for task in active)
            assert all(not snapshots for snapshots in writes.values())
            if ending == "cancel":
                for _ in range(2):
                    runner.cancel()
                    await asyncio.sleep(0)
                    assert not runner.done()

            release.set()
            await asyncio.gather(*active)
            if ending == "cancel":
                with pytest.raises(asyncio.CancelledError):
                    await runner
            else:
                await runner
            if ending != "restart":
                await assert_checkpoint()
            assert callbacks == ([True] if ending == "restart" else [])
        finally:
            release.set()
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.parametrize("existing", [False, True], ids=["missing", "corrupt"])
@bounded
async def test_close_without_start_never_reads_or_writes_files(tmp_path, monkeypatch, existing):
    paths = [tmp_path / name for name in ("settings.json", "state.json")]
    contents = b"{original corrupt JSON\xff"
    if existing:
        for path in paths:
            path.write_bytes(contents)

    def forbidden(*args, **kwargs):
        raise AssertionError("unstarted server performed disk I/O")

    monkeypatch.setattr(persistence, "_read_json", forbidden)
    monkeypatch.setattr(persistence, "_atomic_write", forbidden)
    server = owner(tmp_path)
    await server.close()
    await server.close()
    await assert_closed(server)
    if existing:
        assert [path.read_bytes() for path in paths] == [contents, contents]
    else:
        assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("stage", ["settings", "store", "listener"])
@pytest.mark.parametrize("corrupt", [False, True], ids=["valid", "corrupt"])
@bounded
async def test_failed_startup_does_not_checkpoint_original_files(tmp_path, monkeypatch, stage, corrupt):
    server = owner(tmp_path, network_overrides=PORT_ZERO)
    p = Proxy("127.0.0.1", 1234, "http", "local", alive=True)
    settings_bytes = json.dumps({**PORT_ZERO, "workers": 7, "selected_keys": [p.key_str()]}).encode()
    store_bytes = json.dumps({p.key_str(): p.to_dict()}).encode()
    if corrupt:
        settings_bytes, store_bytes = b"{original bad settings", b"\xfforiginal bad state"
    server.settings.path.write_bytes(settings_bytes)
    server.store.path.write_bytes(store_bytes)
    target, method = {
        "settings": (server.settings, "load"),
        "store": (server.store, "load"),
        "listener": (lifecycle.LocalHttpProxy, "start"),
    }[stage]
    original = getattr(target, method)

    async def fail_after(*args, **kwargs):
        await original(*args, **kwargs)
        raise OSError("startup before ready")

    monkeypatch.setattr(target, method, fail_after)
    with pytest.raises(OSError, match="startup before ready"):
        await server.start()
    await server.close()
    await assert_closed(server)
    assert server.settings.path.read_bytes() == settings_bytes
    assert server.store.path.read_bytes() == store_bytes


@pytest.mark.parametrize("failing_step", ["scheduler", "store.save", "settings.update"])
@bounded
async def test_checkpoint_and_cleanup_errors_drain_every_resource_and_prevent_restart(
    tmp_path, monkeypatch, failing_step
):
    servers, steps = [], []
    restart = AsyncMock()
    expected_steps = ["scheduler", "store.save", "settings.update", "store.flush", "settings.flush", "runner"]

    def started(server):
        servers.append(server)
        for name, obj, method in (
            ("scheduler", server.scheduler, "shutdown"),
            ("store.save", server.store, "save"),
            ("settings.update", server.settings, "update"),
            ("store.flush", server.store, "flush"),
            ("settings.flush", server.settings, "flush"),
            ("runner", web.AppRunner, "cleanup"),
        ):
            original = getattr(obj, method)

            async def operation(*args, name=name, original=original):
                if name in ("store.save", "settings.update"):
                    assert server.scheduler._task is None
                    assert not server.jobs._tasks
                    assert not server.app[webapp.WEB_STATE].requests
                    assert server.http_proxy._server is None
                    assert server.socks_proxy._server is None
                steps.append(name)
                if name == failing_step and name != "scheduler":
                    raise OSError(f"failed {name}")
                await original(*args)
                if name == failing_step:
                    raise OSError(f"failed {name}")

            monkeypatch.setattr(obj, method, operation)
        server.restart_event.set()

    with pytest.raises(OSError, match=f"failed {failing_step}"):
        await run_server(
            tmp_path / "settings.json",
            {"state_file": str(tmp_path / "state.json"), **PORT_ZERO},
            on_started=started,
            restart_callback=restart,
            handle_signals=False,
        )
    assert steps == expected_steps
    restart.assert_not_called()
    await assert_closed(servers[0])


@pytest.mark.parametrize("signum", [signal.SIGINT, signal.SIGTERM])
@bounded
async def test_signal_graceful_stop_and_restore_previous_loop_handler(tmp_path, signum):
    loop = asyncio.get_running_loop()
    previous = signal.getsignal(signum)
    previous_event = asyncio.Event()
    loop.add_signal_handler(signum, previous_event.set)
    servers = []

    def started(server):
        servers.append(server)
        os.kill(os.getpid(), signum)

    try:
        await run_server(
            tmp_path / "settings.json",
            {
                "state_file": str(tmp_path / "state.json"),
                **PORT_ZERO,
            },
            on_started=started,
        )
        assert not previous_event.is_set()
        await assert_closed(servers[0])
        os.kill(os.getpid(), signum)
        await previous_event.wait()
    finally:
        loop.remove_signal_handler(signum)
        signal.signal(signum, previous)


@bounded
async def test_signal_handlers_restored_on_start_failure(tmp_path, monkeypatch):
    previous = {s: signal.getsignal(s) for s in (signal.SIGINT, signal.SIGTERM)}
    monkeypatch.setattr(lifecycle.SettingsStore, "load", AsyncMock(side_effect=OSError("load")))
    with pytest.raises(OSError, match="load"):
        await run_server(tmp_path / "settings.json", {})
    assert {s: signal.getsignal(s) for s in previous} == previous


def test_reexec_keeps_original_script_flags_and_module_argv(monkeypatch):
    calls = []
    monkeypatch.setattr(lifecycle.os, "execv", lambda executable, argv: calls.append((executable, argv)))
    for original in (
        ["python3", "-X", "dev", "path/to/entry.py", "--serve"],
        ["python3", "-I", "-m", "proxyhunter", "--serve"],
    ):
        monkeypatch.setattr(sys, "orig_argv", original)
        lifecycle._restart_process()
        assert calls[-1] == (sys.executable, [sys.executable, *original[1:]])


@bounded
async def test_cleanup_error_does_not_skip_other_resources(tmp_path, monkeypatch):
    server = await owner(tmp_path).start()
    shutdown = server.scheduler.shutdown

    async def fail():
        await shutdown()
        raise OSError("cleanup")

    monkeypatch.setattr(server.scheduler, "shutdown", fail)
    with pytest.raises(OSError, match="cleanup"):
        await server.close()
    await assert_closed(server)


def test_invalid_network_overrides_rejected_without_io(tmp_path):
    for patch in ([], {"workers": 5}, {"ui_port": -1}, {"ui_port": True}):
        with pytest.raises((TypeError, ValueError)):
            owner(tmp_path, network_overrides=patch)
    assert not list(tmp_path.iterdir())
    assert set(PORT_ZERO) == set(NETWORK_KEYS)


@bounded
async def test_webapp_server_factory_wrapper(tmp_path):
    server = await webapp.start_server(
        tmp_path / "settings.json",
        {
            "state_file": str(tmp_path / "state.json"),
            **PORT_ZERO,
        },
    )
    await server.close()
    await assert_closed(server)


@bounded
async def test_default_reexec_runs_after_draining_with_original_argv(tmp_path, monkeypatch):
    servers, calls = [], []
    original = ["python3", "-X", "dev", "some-script.py", "--serve"]
    monkeypatch.setattr(sys, "orig_argv", original)

    def execv(executable, argv):
        assert servers[0].closed
        assert not servers[0].runner.sites
        assert servers[0].runner.server is None
        assert not servers[0].jobs._tasks
        assert servers[0].http_proxy._server is None
        assert servers[0].socks_proxy._server is None
        calls.append((executable, argv))

    async def started(server):
        servers.append(server)
        async with aiohttp.ClientSession() as client:
            response = await client.post(server.ui_url + "/api/restart")
            assert await response.json() == {"restarting": True}

    monkeypatch.setattr(lifecycle.os, "execv", execv)
    await run_server(
        tmp_path / "settings.json",
        {
            "state_file": str(tmp_path / "state.json"),
            **PORT_ZERO,
        },
        on_started=started,
        handle_signals=False,
    )
    assert calls == [(sys.executable, [sys.executable, *original[1:]])]
    await assert_closed(servers[0])


@bounded
async def test_cleanup_failure_and_repeated_cancellation_still_joins(tmp_path, monkeypatch):
    server = await owner(tmp_path).start()
    entered, release = asyncio.Event(), asyncio.Event()
    original = server.scheduler.shutdown

    async def fail():
        await original()
        entered.set()
        await release.wait()
        raise OSError("late cleanup failure")

    monkeypatch.setattr(server.scheduler, "shutdown", fail)
    task = asyncio.create_task(server.close())
    try:
        await entered.wait()
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        await assert_closed(server)
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


def test_unavailable_signal_registration_is_safe(monkeypatch):
    class Loop:
        def add_signal_handler(self, *args):
            raise NotImplementedError

    monkeypatch.setattr(lifecycle.asyncio, "get_running_loop", Loop)
    restore = lifecycle._install_signals(asyncio.Event())
    restore()
