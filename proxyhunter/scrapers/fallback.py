from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass

import aiohttp
from aiohttp_socks import ProxyConnectionError as SocksConnectionError
from aiohttp_socks import ProxyConnector, ProxyType
from aiohttp_socks import ProxyError as SocksClientError
from aiohttp_socks import ProxyTimeoutError as SocksTimeoutError
from python_socks import ProxyError
from yarl import URL

from proxyhunter.models import Proxy

log = logging.getLogger(__name__)
MAX_FALLBACK_ATTEMPTS = 3
MAX_RESPONSE_BYTES = 16 * 1024 * 1024
NETWORK_ERRORS = (
    aiohttp.ClientError,
    ProxyError,
    SocksConnectionError,
    SocksClientError,
    SocksTimeoutError,
    OSError,
    asyncio.TimeoutError,
)


def select_fallback_candidates(known_proxies: list[Proxy]) -> list[Proxy]:
    """HTTPS-capable, alive candidates, fastest first; never mutate the input."""
    candidates = [
        p for p in known_proxies if p.alive and (p.protocol in ("socks4", "socks5") or p.supports_https)
    ]
    candidates.sort(key=lambda p: p.latency_ms if p.latency_ms is not None else float("inf"))
    return candidates


def proxy_url(p: Proxy) -> str:
    scheme = "socks5h" if p.protocol == "socks5" else "socks4" if p.protocol == "socks4" else "http"
    host = f"[{p.ip}]" if ":" in p.ip else p.ip
    return f"{scheme}://{host}:{p.port}"


def client_timeout(timeout: float) -> aiohttp.ClientTimeout:
    """Requests-style connect/read deadlines, not a whole-response deadline."""
    return aiohttp.ClientTimeout(total=None, connect=timeout, sock_connect=timeout, sock_read=timeout)


@asynccontextmanager
async def proxy_session(url: str, timeout: float) -> AsyncIterator[tuple[aiohttp.ClientSession, dict]]:
    """One isolated session per proxy. HTTP 'https' tags still mean plaintext HTTP.

    Explicit rdns is important: validator socks5 URLs resolve locally, fallback
    socks5h resolves remotely, and socks4 (not socks4a) resolves locally.
    The caller must keep all requests inside this context.
    """
    parsed = URL(url)
    kwargs = {}
    if parsed.scheme in ("socks4", "socks5", "socks5h"):
        connector = ProxyConnector(
            host=parsed.host,
            port=parsed.port,
            proxy_type=ProxyType.SOCKS4 if parsed.scheme == "socks4" else ProxyType.SOCKS5,
            rdns=parsed.scheme == "socks5h",
            limit=1,
        )
    elif parsed.scheme == "http":
        connector = aiohttp.TCPConnector(limit=1)
        kwargs["proxy"] = url
    else:
        raise ValueError(f"unsupported proxy URL: {url}")
    async with aiohttp.ClientSession(
        connector=connector,
        timeout=client_timeout(timeout),
        trust_env=False,
        cookie_jar=aiohttp.DummyCookieJar(),
    ) as session:
        yield session, kwargs


@dataclass(frozen=True)
class BufferedResponse:
    """Detached response; no open connection escapes get_with_fallback.

    status/status_code, text and json() are synchronous after awaiting the GET.
    """

    status: int
    body: bytes
    encoding: str = "utf-8"

    @property
    def status_code(self) -> int:
        return self.status

    @property
    def text(self) -> str:
        return self.body.decode(self.encoding, errors="replace")

    def json(self):
        return json.loads(self.text)


async def read_body(response: aiohttp.ClientResponse) -> bytes:
    body = bytearray()
    async for chunk in response.content.iter_chunked(65536):
        body.extend(chunk)
        if len(body) > MAX_RESPONSE_BYTES:
            raise aiohttp.ClientPayloadError("response exceeds size limit")
    return bytes(body)


async def _get(session: aiohttp.ClientSession, url: str, **kwargs) -> BufferedResponse:
    async with session.get(url, **kwargs) as resp:
        resp.raise_for_status()
        return BufferedResponse(resp.status, await read_body(resp), resp.charset or "utf-8")


async def get_with_fallback(
    session: aiohttp.ClientSession,
    url: str,
    *,
    params: dict | None = None,
    timeout: float = 15,
    fallback_proxies: list[Proxy] | None = None,
    max_fallback_attempts: int = MAX_FALLBACK_ATTEMPTS,
) -> BufferedResponse:
    """Direct GET, then up to three provided candidates; redirects are followed.

    The supplied session is caller-owned (direct traffic only). SOCKS needs a
    different connector, so each fallback owns and closes an isolated session.
    Cancellation is never converted into a fallback attempt.
    """
    kwargs = {"params": params, "timeout": client_timeout(timeout), "allow_redirects": True}
    try:
        return await _get(session, url, **kwargs)
    except NETWORK_ERRORS as exc:
        last_exc = exc
    attempts = max(0, min(MAX_FALLBACK_ATTEMPTS, max_fallback_attempts))
    seen = set()
    for p in fallback_proxies or []:
        if not attempts:
            break
        if p.key() in seen:
            continue
        seen.add(p.key())
        attempts -= 1
        try:
            async with proxy_session(proxy_url(p), timeout) as (proxied, proxy_kwargs):
                proxied.headers.update(session.headers)
                response = await _get(proxied, url, **kwargs, **proxy_kwargs)
            log.info("reached %s via fallback proxy %s", url, p.key_str())
            return response
        except NETWORK_ERRORS as exc:
            log.debug("fallback proxy %s failed for %s: %s", p.key_str(), url, exc)
            last_exc = exc
    raise last_exc
