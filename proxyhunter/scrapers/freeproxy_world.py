from __future__ import annotations

import logging
import time
from typing import Iterable

import requests
from bs4 import BeautifulSoup

from proxyhunter.models import Proxy
from proxyhunter.scrapers.base import BaseScraper
from proxyhunter.scrapers.fallback import get_with_fallback

log = logging.getLogger(__name__)

BASE_URL = "https://www.freeproxy.world/"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)


class FreeProxyWorldScraper(BaseScraper):
    name = "freeproxy_world"

    def __init__(self, pages: int = 3, delay: float = 1.0):
        self.pages = max(1, pages)
        self.delay = delay

    def fetch(self, fallback_proxies: list[Proxy] | None = None) -> Iterable[Proxy]:
        session = requests.Session()
        session.headers.update({"User-Agent": USER_AGENT})

        for page in range(1, self.pages + 1):
            try:
                resp = get_with_fallback(
                    session, BASE_URL, params={"page": page}, timeout=15, fallback_proxies=fallback_proxies
                )
            except requests.RequestException as exc:
                log.warning("freeproxy.world page %d failed: %s", page, exc)
                continue

            yield from self._parse(resp.text)

            if page < self.pages:
                time.sleep(self.delay)

    def _parse(self, html: str) -> Iterable[Proxy]:
        soup = BeautifulSoup(html, "html.parser")
        rows = soup.select("table.table tbody tr")

        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 7:
                continue

            ip = cells[0].get_text(strip=True)
            port_text = cells[1].get_text(strip=True)
            if not ip or not port_text.isdigit():
                continue
            port = int(port_text)

            country_link = cells[2].find("a")
            country = country_link.get("title") if country_link else None
            city = cells[3].get_text(strip=True) or None

            protocol_tags = cells[5].find_all("a")
            protocols = [
                t.get_text(strip=True).lower()
                for t in protocol_tags
                if t.get_text(strip=True)
            ] or ["http"]

            anonymity_text = cells[6].get_text(strip=True).lower()

            for protocol in protocols:
                yield Proxy(
                    ip=ip,
                    port=port,
                    protocol=protocol,
                    source=self.name,
                    country=country,
                    city=city,
                    anonymity="transparent" if anonymity_text == "no" else None,
                )
