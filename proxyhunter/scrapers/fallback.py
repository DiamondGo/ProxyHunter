from __future__ import annotations

import logging
from typing import Any

import requests

from proxyhunter.models import Proxy

log = logging.getLogger(__name__)

MAX_FALLBACK_ATTEMPTS = 3


def select_fallback_candidates(known_proxies: list[Proxy]) -> list[Proxy]:
    """Pick already-validated proxies worth trying as a scrape fallback.

    Restricted to proxies capable of reaching an HTTPS URL (both scrape
    sources are https://) - socks4/5 tunnel raw TCP so they're always fine,
    but an http-type proxy needs supports_https, otherwise the CONNECT to
    the scrape site would just fail too. Sorted fastest-first so the
    (at most 3, per get_with_fallback) attempts favor the more reliable ones.
    """
    candidates = [p for p in known_proxies if p.alive and (p.protocol in ("socks4", "socks5") or p.supports_https)]
    candidates.sort(key=lambda p: p.latency_ms if p.latency_ms is not None else float("inf"))
    return candidates


def proxy_url(p: Proxy) -> str:
    if p.protocol == "socks5":
        return f"socks5h://{p.ip}:{p.port}"
    if p.protocol == "socks4":
        return f"socks4://{p.ip}:{p.port}"
    return f"http://{p.ip}:{p.port}"


def get_with_fallback(
    session: Any,
    url: str,
    *,
    params: dict | None = None,
    timeout: float = 15,
    fallback_proxies: list[Proxy] | None = None,
    max_fallback_attempts: int = MAX_FALLBACK_ATTEMPTS,
) -> requests.Response:
    """GET a URL directly first; if that fails, retry through up to
    max_fallback_attempts different already-validated proxies.

    Used so a full scrape can still reach the proxy-list sites even when this
    machine can't reach them directly (e.g. they're blocked/unreachable from
    here), by routing the scrape request itself through a proxy we already
    know works. `session` may be a requests.Session or the requests module
    itself - both expose .get().
    """
    try:
        resp = session.get(url, params=params, timeout=timeout)
        resp.raise_for_status()
        return resp
    except requests.RequestException as direct_exc:
        last_exc: Exception = direct_exc
        if not fallback_proxies:
            raise

        for p in fallback_proxies[:max_fallback_attempts]:
            proxies = {"http": proxy_url(p), "https": proxy_url(p)}
            try:
                resp = session.get(url, params=params, timeout=timeout, proxies=proxies)
                resp.raise_for_status()
                log.info("reached %s via fallback proxy %s after direct access failed", url, p.key_str())
                return resp
            except requests.RequestException as exc:
                log.debug("fallback proxy %s failed for %s: %s", p.key_str(), url, exc)
                last_exc = exc
                continue

        raise last_exc
