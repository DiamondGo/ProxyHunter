from __future__ import annotations

import logging

import requests

from proxyhunter.models import Proxy

log = logging.getLogger(__name__)

BATCH_URL = "http://ip-api.com/batch"
BATCH_SIZE = 100


def fill_missing_geo(proxies: list[Proxy]) -> None:
    """Fill country/city/isp for proxies that don't already have a country,
    using ip-api.com's free batch endpoint. Mutates proxies in place."""
    targets = [p for p in proxies if not p.country]
    if not targets:
        return

    for i in range(0, len(targets), BATCH_SIZE):
        chunk = targets[i : i + BATCH_SIZE]
        ips = [p.ip for p in chunk]
        try:
            resp = requests.post(
                BATCH_URL,
                params={"fields": "status,country,city,isp,query"},
                json=ips,
                timeout=15,
            )
            resp.raise_for_status()
            results = resp.json()
        except (requests.RequestException, ValueError) as exc:
            log.warning("geolocation batch lookup failed: %s", exc)
            continue

        by_ip = {r.get("query"): r for r in results if r.get("status") == "success"}
        for p in chunk:
            info = by_ip.get(p.ip)
            if info:
                p.country = info.get("country")
                p.city = info.get("city")
                p.isp = info.get("isp")
