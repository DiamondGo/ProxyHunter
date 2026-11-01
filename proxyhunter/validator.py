from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass

import aiohttp

from proxyhunter.forwarder.tunnel import CONNECTION_ERRORS, close_writer, connect_via_upstream
from proxyhunter.models import Proxy
from proxyhunter.scrapers.fallback import NETWORK_ERRORS, client_timeout, proxy_session, read_body

log = logging.getLogger(__name__)
PRIMARY_HTTP_URL = "http://httpbin.org/get"
PRIMARY_HTTPS_URL = "https://httpbin.org/get"
SECONDARY_URL = "http://www.gstatic.com/generate_204"
REAL_IP_URL = "https://api.ipify.org?format=json"
PORT_PROBE_HOST = "www.gstatic.com"
PORT_PROBE_PORT = 80
GEO_VERIFY_URL = "http://ip-api.com/json/?fields=status,country,city,isp"
LEAK_HEADERS = ("x-forwarded-for", "x-real-ip", "forwarded", "client-ip")
PROXY_SIGNATURE_HEADERS = ("via", "x-forwarded-for", "forwarded", "proxy-connection", "x-real-ip")


@dataclass
class _CheckResult:
    ok: bool
    latency_ms: float | None = None
    origin: str = ""
    headers: dict | None = None


class ProxyValidator:
    def __init__(
        self,
        timeout: float = 8.0,
        workers: int = 50,
        secondary_check: bool = True,
        secondary_url: str = SECONDARY_URL,
        geo_verify_via_proxy: bool = False,
    ):
        self.timeout = timeout
        self.workers = max(1, workers)
        self.secondary_check = secondary_check
        self.secondary_url = secondary_url
        self.geo_verify_via_proxy = geo_verify_via_proxy
        self.real_ip: str | None = None
        self._real_ip_ready = False
        self._real_ip_lock = asyncio.Lock()

    async def _get_real_ip(self) -> str | None:
        try:
            async with (
                aiohttp.ClientSession(timeout=client_timeout(10), trust_env=False) as session,
                session.get(REAL_IP_URL) as resp,
            ):
                resp.raise_for_status()
                data = json.loads(await read_body(resp))
                ip = data.get("ip") if isinstance(data, dict) else None
                return ip if isinstance(ip, str) else None
        except NETWORK_ERRORS + (ValueError,) as exc:
            log.warning("could not determine real public IP, anonymity checks will be skipped: %s", exc)
            return None

    async def validate_all(self, proxies: list[Proxy]) -> list[Proxy]:
        """Return ALL results, dead included, using at most workers tasks/sessions.

        Public IP is resolved once per validator lifecycle, lazily on first
        nonempty call. Cancellation cancels and joins all workers. Instances
        should be used on one event loop, including repeated validation calls.
        """
        if not proxies:
            return []
        async with self._real_ip_lock:
            if not self._real_ip_ready:
                self.real_ip = await self._get_real_ip()
                self._real_ip_ready = True
        pending = iter(enumerate(proxies))
        results: list[Proxy | None] = [None] * len(proxies)

        async def worker():
            for index, proxy in pending:
                try:
                    results[index] = await self._check_one(proxy)
                except NETWORK_ERRORS + (ValueError,) as exc:
                    log.debug("proxy %s failed validation: %s", proxy.key_str(), exc)
                    proxy.mark_checked()
                    proxy.alive = False
                    results[index] = proxy

        tasks = [asyncio.create_task(worker()) for _ in range(min(self.workers, len(proxies)))]
        try:
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        log.info("validated %d proxies (%d alive)", len(proxies), sum(p.alive for p in proxies))
        return [p for p in results if p is not None]

    async def _check_one(self, proxy: Proxy) -> Proxy:
        proxy.alive = False
        proxy.latency_ms = None
        proxy.anonymity = None
        proxy.supports_https = False
        proxy.https_only = None
        async with proxy_session(proxy.proxy_url(), self.timeout) as (session, kwargs):
            http_result = await self._request(session, PRIMARY_HTTP_URL, kwargs)
            https_result = await self._request(session, PRIMARY_HTTPS_URL, kwargs)
            proxy.mark_checked()
            proxy.supports_https = https_result.ok
            if not http_result.ok and not https_result.ok:
                return proxy
            primary = https_result if https_result.ok else http_result
            if self.secondary_check:
                secondary = await self._request(
                    session,
                    self.secondary_url,
                    kwargs,
                    expect_status=204 if "generate_204" in self.secondary_url else None,
                )
                if not secondary.ok:
                    return proxy
            proxy.alive = True
            latencies = [
                r.latency_ms for r in (http_result, https_result) if r.ok and r.latency_ms is not None
            ]
            proxy.latency_ms = round(min(latencies), 1) if latencies else None
            proxy.anonymity = self._classify_anonymity(primary)
            if proxy.supports_https and proxy.protocol in ("http", "https"):
                proxy.https_only = not await self._probe_connect_any_port(proxy)
            if self.geo_verify_via_proxy:
                await self._verify_geo_via_proxy(proxy, session, kwargs)
        return proxy

    async def _verify_geo_via_proxy(self, proxy: Proxy, session: aiohttp.ClientSession, kwargs: dict) -> None:
        try:
            async with session.get(GEO_VERIFY_URL, **kwargs) as resp:
                resp.raise_for_status()
                data = json.loads(await read_body(resp))
        except NETWORK_ERRORS + (ValueError,):
            return
        if isinstance(data, dict) and data.get("status") == "success":
            for field in ("country", "city", "isp"):
                value = data.get(field)
                if isinstance(value, str) and value:
                    setattr(proxy, field, value)

    async def _probe_connect_any_port(self, proxy: Proxy) -> bool:
        try:
            _, writer = await connect_via_upstream(
                proxy, PORT_PROBE_HOST, PORT_PROBE_PORT, timeout=self.timeout
            )
        except CONNECTION_ERRORS:
            return False
        await close_writer(writer)
        return True

    async def _request(
        self,
        session: aiohttp.ClientSession,
        url: str,
        kwargs: dict,
        expect_status: int | None = None,
    ) -> _CheckResult:
        start = time.perf_counter()
        try:
            async with session.get(url, allow_redirects=False, **kwargs) as resp:
                body = await read_body(resp)
                latency_ms = (time.perf_counter() - start) * 1000
                if expect_status is not None:
                    return _CheckResult(resp.status == expect_status, latency_ms)
                if resp.status >= 400:
                    return _CheckResult(False)
                origin, headers = "", {}
                try:
                    data = json.loads(body)
                    if isinstance(data, dict):
                        origin = data.get("origin", "")
                        headers = data.get("headers", {})
                except ValueError:
                    pass
                return _CheckResult(
                    True,
                    latency_ms,
                    origin if isinstance(origin, str) else "",
                    headers if isinstance(headers, dict) else {},
                )
        except NETWORK_ERRORS:
            return _CheckResult(False)

    def _classify_anonymity(self, result: _CheckResult) -> str:
        if not self.real_ip:
            return "unknown"
        headers = {k.lower(): v for k, v in (result.headers or {}).items()}
        if self.real_ip in result.origin or any(
            self.real_ip in str(headers.get(h, "")) for h in LEAK_HEADERS
        ):
            return "transparent"
        return "anonymous" if any(h in headers for h in PROXY_SIGNATURE_HEADERS) else "elite"
