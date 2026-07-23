from __future__ import annotations

import logging

from proxyhunter.models import Proxy
from proxyhunter.scrapers.freeproxy_world import FreeProxyWorldScraper
from proxyhunter.scrapers.proxyscrape import ProxyScrapeScraper

log = logging.getLogger(__name__)

SCRAPERS = {
    "freeproxy_world": FreeProxyWorldScraper,
    "proxyscrape": ProxyScrapeScraper,
}


def scrape_all(
    sources: list[str],
    pages: int = 3,
    protocols: list[str] | None = None,
) -> list[Proxy]:
    protocols = protocols or ["http", "socks4", "socks5"]
    proxies: list[Proxy] = []

    for name in sources:
        scraper_cls = SCRAPERS[name]
        if name == "freeproxy_world":
            scraper = scraper_cls(pages=pages)
        elif name == "proxyscrape":
            scraper = scraper_cls(protocols=protocols)
        else:
            scraper = scraper_cls()

        try:
            found = list(scraper.fetch())
        except Exception as exc:  # noqa: BLE001 - a broken source shouldn't kill the run
            log.warning("scraper %s failed: %s", name, exc)
            continue

        log.info("%s: scraped %d proxies", name, len(found))
        proxies.extend(found)

    return proxies


def dedupe(proxies: list[Proxy]) -> list[Proxy]:
    seen: dict[tuple, Proxy] = {}
    for p in proxies:
        seen[p.key()] = p
    return list(seen.values())
