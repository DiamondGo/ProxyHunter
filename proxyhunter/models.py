from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone


@dataclass
class Proxy:
    ip: str
    port: int
    protocol: str  # http, https, socks4, socks5
    source: str
    country: str | None = None
    city: str | None = None
    isp: str | None = None
    alive: bool = False
    latency_ms: float | None = None
    supports_https: bool = False
    anonymity: str | None = None  # elite, anonymous, transparent, unknown
    checked_at: str | None = None
    # http/https proxies only: True if CONNECT only works for port 443 (common
    # policy restriction), meaning it can't be used as a general-purpose tunnel
    # for non-HTTPS targets. None = not applicable/not tested (e.g. socks4/5,
    # or supports_https was already False).
    https_only: bool | None = None

    def key(self) -> tuple[str, str, int]:
        return (self.protocol, self.ip, self.port)

    def key_str(self) -> str:
        protocol, ip, port = self.key()
        return f"{protocol}|{ip}|{port}"

    def proxy_url(self) -> str:
        scheme = self.protocol if self.protocol.startswith("socks") else "http"
        return f"{scheme}://{self.ip}:{self.port}"

    def mark_checked(self) -> None:
        self.checked_at = datetime.now(timezone.utc).isoformat(timespec="seconds")

    def to_dict(self) -> dict:
        return asdict(self)
