from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

import requests

from proxyhunter.forwarder.tunnel import connect_via_upstream
from proxyhunter.models import Proxy

log = logging.getLogger(__name__)

PRIMARY_HTTP_URL = "http://httpbin.org/get"
PRIMARY_HTTPS_URL = "https://httpbin.org/get"
SECONDARY_URL = "http://www.gstatic.com/generate_204"
REAL_IP_URL = "https://api.ipify.org?format=json"

# Used to probe whether an http/https proxy's CONNECT support is restricted to
# port 443 (a common ACL policy) or actually allows arbitrary target ports.
PORT_PROBE_HOST = "www.gstatic.com"
PORT_PROBE_PORT = 80

# Called *through* the proxy (no IP argument) so it geolocates whichever IP the
# proxy is actually egressing from, rather than trusting the IP the site listed
# it under - the proxy's real egress point can differ from its advertised address.
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
        self.workers = workers
        self.secondary_check = secondary_check
        self.secondary_url = secondary_url
        self.geo_verify_via_proxy = geo_verify_via_proxy
        self.real_ip = self._get_real_ip()

    def _get_real_ip(self) -> str | None:
        try:
            resp = requests.get(REAL_IP_URL, timeout=10)
            resp.raise_for_status()
            return resp.json().get("ip")
        except (requests.RequestException, ValueError) as exc:
            log.warning("could not determine real public IP, anonymity checks will be skipped: %s", exc)
            return None

    def validate_all(self, proxies: list[Proxy]) -> list[Proxy]:
        """Check every proxy and return ALL results (alive and dead alike) so
        callers can persist dead results too - that's what lets a later run
        skip re-checking a proxy that recently failed."""
        results: list[Proxy] = []
        alive_count = 0
        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            futures = {pool.submit(self._check_one, p): p for p in proxies}
            done = 0
            for future in as_completed(futures):
                done += 1
                proxy = future.result()
                results.append(proxy)
                if proxy.alive:
                    alive_count += 1
                if done % 50 == 0 or done == len(proxies):
                    log.info("validated %d/%d proxies (%d alive so far)", done, len(proxies), alive_count)
        return results

    def _check_one(self, proxy: Proxy) -> Proxy:
        proxy_url = proxy.proxy_url()
        proxies_dict = {"http": proxy_url, "https": proxy_url}

        http_result = self._request(PRIMARY_HTTP_URL, proxies_dict)
        https_result = self._request(PRIMARY_HTTPS_URL, proxies_dict)

        proxy.mark_checked()
        proxy.supports_https = https_result.ok

        if not http_result.ok and not https_result.ok:
            proxy.alive = False
            return proxy

        primary = https_result if https_result.ok else http_result

        secondary_ok = True
        if self.secondary_check:
            secondary_result = self._request(
                self.secondary_url,
                proxies_dict,
                expect_status=204 if "generate_204" in self.secondary_url else None,
            )
            secondary_ok = secondary_result.ok

        proxy.alive = secondary_ok
        if not proxy.alive:
            return proxy

        latencies = [r.latency_ms for r in (http_result, https_result) if r.ok and r.latency_ms is not None]
        proxy.latency_ms = round(min(latencies), 1) if latencies else None
        proxy.anonymity = self._classify_anonymity(primary)

        if proxy.supports_https and proxy.protocol in ("http", "https"):
            proxy.https_only = not self._probe_connect_any_port(proxy)

        if self.geo_verify_via_proxy:
            self._verify_geo_via_proxy(proxy, proxies_dict)

        return proxy

    def _verify_geo_via_proxy(self, proxy: Proxy, proxies_dict: dict) -> None:
        """Confirm country/city/isp by actually routing a geolocation lookup
        through the proxy, overriding whatever the source site or a direct
        (non-proxied) IP lookup said - this is what "confirmed" means here."""
        try:
            resp = requests.get(GEO_VERIFY_URL, proxies=proxies_dict, timeout=(self.timeout, self.timeout))
            resp.raise_for_status()
            data = resp.json()
        except (requests.RequestException, ValueError):
            return
        if data.get("status") != "success":
            return
        proxy.country = data.get("country") or proxy.country
        proxy.city = data.get("city") or proxy.city
        proxy.isp = data.get("isp") or proxy.isp

    def _probe_connect_any_port(self, proxy: Proxy) -> bool:
        """True if this proxy's CONNECT works for a non-443 port too, not just 443."""
        try:
            sock = connect_via_upstream(proxy, PORT_PROBE_HOST, PORT_PROBE_PORT, timeout=self.timeout)
        except (OSError, ConnectionError):
            return False
        sock.close()
        return True

    def _request(self, url: str, proxies_dict: dict, expect_status: int | None = None) -> _CheckResult:
        start = time.perf_counter()
        try:
            resp = requests.get(
                url,
                proxies=proxies_dict,
                timeout=(self.timeout, self.timeout),
                allow_redirects=False,
            )
        except requests.RequestException:
            return _CheckResult(ok=False)

        latency_ms = (time.perf_counter() - start) * 1000

        if expect_status is not None:
            return _CheckResult(ok=resp.status_code == expect_status, latency_ms=latency_ms)

        if not resp.ok:
            return _CheckResult(ok=False)

        origin = ""
        headers = {}
        try:
            data = resp.json()
            origin = data.get("origin", "")
            headers = data.get("headers", {})
        except ValueError:
            pass

        return _CheckResult(ok=True, latency_ms=latency_ms, origin=origin, headers=headers)

    def _classify_anonymity(self, result: _CheckResult) -> str:
        if not self.real_ip:
            return "unknown"

        headers = {k.lower(): v for k, v in (result.headers or {}).items()}

        leaked = self.real_ip in result.origin or any(
            self.real_ip in str(headers.get(h, "")) for h in LEAK_HEADERS
        )
        if leaked:
            return "transparent"

        has_proxy_signature = any(h in headers for h in PROXY_SIGNATURE_HEADERS)
        if has_proxy_signature:
            return "anonymous"

        return "elite"
