from __future__ import annotations

import logging
from typing import Iterable

import requests

from proxyhunter.models import Proxy
from proxyhunter.scrapers.base import BaseScraper

log = logging.getLogger(__name__)

API_URL = "https://api.proxyscrape.com/v2/"
SUPPORTED_PROTOCOLS = ("http", "socks4", "socks5")


class ProxyScrapeScraper(BaseScraper):
    name = "proxyscrape"

    def __init__(self, protocols: list[str] | None = None):
        self.protocols = [p for p in (protocols or ["http", "socks4", "socks5"]) if p in SUPPORTED_PROTOCOLS]

    def fetch(self) -> Iterable[Proxy]:
        for protocol in self.protocols:
            yield from self._fetch_protocol(protocol)

    def _fetch_protocol(self, protocol: str) -> Iterable[Proxy]:
        params = {
            "request": "getproxies",
            "protocol": protocol,
            "timeout": "10000",
            "country": "all",
            "ssl": "all",
            "anonymity": "all",
        }
        try:
            resp = requests.get(API_URL, params=params, timeout=15)
            resp.raise_for_status()
        except requests.RequestException as exc:
            log.warning("proxyscrape %s failed: %s", protocol, exc)
            return

        for line in resp.text.splitlines():
            line = line.strip()
            if not line or ":" not in line:
                continue
            ip, _, port_text = line.partition(":")
            if not port_text.isdigit():
                continue
            yield Proxy(ip=ip, port=int(port_text), protocol=protocol, source=self.name)
