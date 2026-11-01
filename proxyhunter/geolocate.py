from __future__ import annotations

import json
import logging

import aiohttp

from proxyhunter.models import Proxy
from proxyhunter.scrapers.fallback import NETWORK_ERRORS, client_timeout, read_body

log = logging.getLogger(__name__)
BATCH_URL = "http://ip-api.com/batch"
BATCH_SIZE = 100


async def fill_missing_geo(proxies: list[Proxy]) -> None:
    """Mutate missing geo fields in place, in bounded sequential batches."""
    targets = [p for p in proxies if not p.country]
    if not targets:
        return
    async with aiohttp.ClientSession(timeout=client_timeout(15), trust_env=False) as session:
        for i in range(0, len(targets), BATCH_SIZE):
            chunk = targets[i : i + BATCH_SIZE]
            try:
                async with session.post(
                    BATCH_URL,
                    params={"fields": "status,country,city,isp,query"},
                    json=[p.ip for p in chunk],
                ) as resp:
                    resp.raise_for_status()
                    results = json.loads(await read_body(resp))
                if not isinstance(results, list):
                    raise ValueError("geolocation response is not a list")  # noqa: TRY004 - invalid wire data
            except NETWORK_ERRORS + (ValueError,) as exc:
                log.warning("geolocation batch lookup failed: %s", exc)
                continue
            by_ip = {
                r.get("query"): r
                for r in results
                if isinstance(r, dict) and r.get("status") == "success" and isinstance(r.get("query"), str)
            }
            for p in chunk:
                info = by_ip.get(p.ip)
                if info:
                    for field in ("country", "city", "isp"):
                        value = info.get(field)
                        if isinstance(value, str):
                            setattr(p, field, value)
