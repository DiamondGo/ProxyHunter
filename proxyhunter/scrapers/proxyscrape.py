from __future__ import annotations

import logging
from collections.abc import Iterable

import aiohttp

from proxyhunter.models import Proxy
from proxyhunter.scrapers.base import BaseScraper
from proxyhunter.scrapers.fallback import NETWORK_ERRORS, get_with_fallback

log = logging.getLogger(__name__)

API_URL = "https://api.proxyscrape.com/v2/"
SUPPORTED_PROTOCOLS = ("http", "socks4", "socks5")


class ProxyScrapeScraper(BaseScraper):
    name = "proxyscrape"

    def __init__(self, protocols: list[str] | None = None):
        self.protocols = [p for p in (protocols or ["http", "socks4", "socks5"]) if p in SUPPORTED_PROTOCOLS]

    async def fetch(self, fallback_proxies: list[Proxy] | None = None) -> list[Proxy]:
        results = []
        async with aiohttp.ClientSession(trust_env=False) as session:
            for protocol in self.protocols:
                results.extend(await self._fetch_protocol(protocol, fallback_proxies, session))
        return results

    async def _fetch_protocol(
        self,
        protocol: str,
        fallback_proxies: list[Proxy] | None,
        session: aiohttp.ClientSession,
    ) -> list[Proxy]:
        params = {
            "request": "getproxies",
            "protocol": protocol,
            "timeout": "10000",
            "country": "all",
            "ssl": "all",
            "anonymity": "all",
        }
        try:
            resp = await get_with_fallback(
                session, API_URL, params=params, timeout=15, fallback_proxies=fallback_proxies
            )
        except NETWORK_ERRORS as exc:
            log.warning("proxyscrape %s failed: %s", protocol, exc)
            return []
        return list(self._parse(resp.text, protocol))

    def _parse(self, text: str, protocol: str) -> Iterable[Proxy]:
        for line in text.splitlines():
            line = line.strip()
            if not line or ":" not in line:
                continue
            ip, _, port_text = line.partition(":")
            if not port_text.isdigit():
                continue
            yield Proxy(ip=ip, port=int(port_text), protocol=protocol, source=self.name)
