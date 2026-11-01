from __future__ import annotations

import asyncio
import re
from urllib.parse import urlsplit

from proxyhunter.forwarder.pool import ProxyPool
from proxyhunter.forwarder.tunnel import (
    CONNECTION_ERRORS,
    MAX_HEAD_BYTES,
    AsyncProxyServer,
    RelayCancelledError,
    authority,
    close_writer,
    connect_via_upstream,
    relay,
)
from proxyhunter.settings import SettingsStore

DEFAULT_MAX_RETRIES = 3
TOKEN = re.compile(rb"[!#$%&'*+.^_`|~0-9A-Za-z-]+")


def parse_authority(target: str) -> tuple[str, int]:
    """CONNECT authority, including bracketed IPv6; default port remains 443."""
    parsed = urlsplit("//" + target)
    if (
        not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("invalid CONNECT authority")
    port = parsed.port if parsed.port is not None else 443
    if not 0 < port < 65536:
        raise ValueError("invalid port")
    return parsed.hostname, port


class LocalHttpProxy(AsyncProxyServer):
    """HTTP/1 forwarder. start/shutdown are awaitable; construction does no I/O.

    Retries stop once a success response or any application request is written.
    Pool success means relay payload moved, not merely a successful handshake or
    the initial HTTP request headers. Plain requests request Connection: close;
    SOCKS origin-form rewriting is for one origin/request per connection.
    """

    def __init__(self, pool: ProxyPool, settings: SettingsStore, host: str, port: int):
        super().__init__(pool, settings, host, port)

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), self.timeout)
            if len(head) > MAX_HEAD_BYTES:
                await self._send_error(writer, 431, "request headers too large")
                return
        except asyncio.TimeoutError:
            await self._send_error(writer, 408, "request header timeout")
            return
        except asyncio.LimitOverrunError:
            await self._send_error(writer, 431, "request headers too large")
            return
        except asyncio.IncompleteReadError as exc:
            if exc.partial:
                await self._send_error(writer, 400, "incomplete request headers")
            return

        try:
            lines = head[:-4].split(b"\r\n")
            method, target, version = lines[0].split(b" ")
            if not TOKEN.fullmatch(method) or version not in (b"HTTP/1.0", b"HTTP/1.1"):
                raise ValueError("invalid request line")
            headers = []
            framing = {}
            for line in lines[1:]:
                name, sep, value = line.partition(b":")
                if (
                    not sep
                    or not TOKEN.fullmatch(name)
                    or any(c == 127 or (c < 32 and c != 9) for c in value)
                ):
                    raise ValueError("invalid request header")
                lower = name.lower()
                if lower in (b"content-length", b"transfer-encoding", b"host"):
                    if lower in framing:
                        raise ValueError("duplicate framing/Host header")
                    framing[lower] = value.strip()
                headers.append((name, value.strip()))
            length = framing.get(b"content-length")
            transfer = framing.get(b"transfer-encoding")
            if (length is not None and not length.isdigit()) or (length is not None and transfer is not None):
                raise ValueError("ambiguous/invalid request body length")
            if transfer is not None and transfer.lower() != b"chunked":
                raise ValueError("unsupported transfer encoding")
            text = target.decode("ascii")
            if any(ord(c) <= 32 or ord(c) == 127 for c in text):
                raise ValueError("invalid target")
            if method == b"CONNECT":
                host, port = parse_authority(text)
                path = None
            else:
                parsed = urlsplit(text)
                if (
                    parsed.scheme != "http"
                    or not parsed.hostname
                    or parsed.username is not None
                    or parsed.password is not None
                    or parsed.fragment
                ):
                    raise ValueError("expected absolute-form http URI (use CONNECT for HTTPS)")
                host, port = parsed.hostname, parsed.port if parsed.port is not None else 80
                if not 0 < port < 65536:
                    raise ValueError("invalid port")
                path = (parsed.path or "/") + ("?" + parsed.query if parsed.query else "")
        except (ValueError, UnicodeError) as exc:
            await self._send_error(writer, 400, str(exc))
            return

        candidates = self.pool.pick_many(self.max_retries + 1)
        if not candidates:
            await self._send_error(
                writer, 502, "No upstream proxy selected in the proxyhunter UI forward pool"
            )
            return
        last_exc = None
        for upstream in candidates:
            try:
                if path is None or upstream.protocol in ("socks4", "socks5"):
                    remote_reader, remote_writer = await connect_via_upstream(
                        upstream, host, port, self.timeout
                    )
                elif upstream.protocol in ("http", "https"):
                    remote_reader, remote_writer = await asyncio.wait_for(
                        asyncio.open_connection(upstream.ip, upstream.port),
                        self.timeout,
                    )
                else:
                    raise ValueError("unsupported upstream protocol")
            except CONNECTION_ERRORS + (ValueError,) as exc:
                self.pool.report_result(upstream, False)
                last_exc = exc
                continue

            transferred = 0
            try:
                # No retries beyond here: even a failed drain can have sent bytes.
                if path is None:
                    writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                    await asyncio.wait_for(writer.drain(), self.timeout)
                else:
                    request_target = (
                        path.encode("ascii") if upstream.protocol in ("socks4", "socks5") else target
                    )
                    chunks = [method + b" " + request_target + b" " + version + b"\r\n"]
                    for name, value in headers:
                        if name.lower() not in (
                            b"proxy-connection",
                            b"proxy-authorization",
                            b"connection",
                            b"host",
                        ):
                            chunks.append(name + b": " + value + b"\r\n")
                    chunks.append(
                        f"Host: {authority(host, port)}\r\nConnection: close\r\n\r\n".encode("ascii")
                    )
                    remote_writer.write(b"".join(chunks))
                    await asyncio.wait_for(remote_writer.drain(), self.timeout)
                # readuntil left body/early CONNECT payload in reader; relay owns it.
                transferred = await relay(reader, writer, remote_reader, remote_writer, self.idle_timeout)
            except RelayCancelledError as exc:
                transferred = exc.transferred
                raise
            finally:
                await close_writer(remote_writer)
                self.pool.report_result(upstream, transferred > 0)
            return
        await self._send_error(
            writer, 502, f"upstream connect failed after {len(candidates)} attempt(s): {last_exc}"
        )

    async def _send_error(self, writer: asyncio.StreamWriter, code: int, message: str) -> None:
        body = message.encode("utf-8", errors="replace")
        writer.write(
            (
                f"HTTP/1.1 {code} Proxy Error\r\nContent-Type: text/plain\r\n"
                f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n"
            ).encode()
            + body
        )
        try:
            await asyncio.wait_for(writer.drain(), self.timeout)
        except CONNECTION_ERRORS:
            pass
