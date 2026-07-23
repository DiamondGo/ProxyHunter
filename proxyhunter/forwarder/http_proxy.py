from __future__ import annotations

import logging
import socket
import socketserver
import threading

from proxyhunter.forwarder.pool import ProxyPool
from proxyhunter.forwarder.tunnel import ReusableThreadingTCPServer, connect_via_upstream, relay
from proxyhunter.settings import SettingsStore

log = logging.getLogger(__name__)

MAX_HEAD_BYTES = 1 << 20


class _HttpProxyHandler(socketserver.BaseRequestHandler):
    pool: ProxyPool
    settings: SettingsStore

    @property
    def timeout(self) -> float:
        return self.settings.get("timeout", 8.0)

    def handle(self) -> None:
        client = self.request
        client.settimeout(self.timeout)
        try:
            head, leftover = self._read_until_headers_end(client)
        except OSError:
            return
        if not head:
            return

        try:
            request_line, _, header_block = head.partition(b"\r\n")
            method, target, _ = request_line.split(b" ", 2)
        except ValueError:
            return

        upstream = self.pool.pick()
        if upstream is None:
            self._send_error(client, 502, "No upstream proxy selected in the proxyhunter UI forward pool")
            return

        try:
            if method == b"CONNECT":
                self._handle_connect(client, upstream, target)
            else:
                self._handle_plain(client, upstream, method, target, header_block, leftover)
        except (OSError, ConnectionError) as exc:
            log.debug("proxy handling error via %s:%s: %s", upstream.ip, upstream.port, exc)

    def _handle_connect(self, client: socket.socket, upstream, target: bytes) -> None:
        host_b, _, port_b = target.partition(b":")
        host = host_b.decode()
        port = int(port_b) if port_b else 443
        try:
            remote = connect_via_upstream(upstream, host, port, timeout=self.timeout)
        except (OSError, ConnectionError) as exc:
            self.pool.report_result(upstream, False)
            self._send_error(client, 502, f"upstream connect failed: {exc}")
            return
        self.pool.report_result(upstream, True)
        client.sendall(b"HTTP/1.1 200 Connection Established\r\n\r\n")
        relay(client, remote)

    def _handle_plain(
        self,
        client: socket.socket,
        upstream,
        method: bytes,
        target: bytes,
        header_block: bytes,
        leftover: bytes,
    ) -> None:
        url = target.decode()
        if "://" not in url:
            self._send_error(client, 400, "expected absolute-form request URI")
            return
        _, _, after_scheme = url.partition("://")
        host_port, _, path = after_scheme.partition("/")
        path = "/" + path
        if ":" in host_port:
            host, port_s = host_port.split(":", 1)
            port = int(port_s)
        else:
            host, port = host_port, 80

        if upstream.protocol in ("socks4", "socks5"):
            # SOCKS upstreams only tunnel raw TCP, so we connect straight to the
            # origin server and rewrite the request line to origin-form.
            try:
                remote = connect_via_upstream(upstream, host, port, timeout=self.timeout)
            except (OSError, ConnectionError) as exc:
                self.pool.report_result(upstream, False)
                self._send_error(client, 502, f"upstream connect failed: {exc}")
                return
            self.pool.report_result(upstream, True)
            request_line = f"{method.decode()} {path} HTTP/1.1\r\n".encode()
        else:
            # http/https upstreams are real forward proxies: hand them the
            # original absolute-form request line as-is.
            try:
                remote = socket.create_connection((upstream.ip, upstream.port), timeout=self.timeout)
            except OSError as exc:
                self.pool.report_result(upstream, False)
                self._send_error(client, 502, f"upstream connect failed: {exc}")
                return
            self.pool.report_result(upstream, True)
            request_line = method + b" " + target + b" HTTP/1.1\r\n"

        remote.sendall(request_line + header_block)
        if leftover:
            remote.sendall(leftover)
        relay(client, remote)

    @staticmethod
    def _read_until_headers_end(client: socket.socket) -> tuple[bytes, bytes]:
        """Read until the HTTP header terminator. Returns (head incl. terminator, leftover bytes)."""
        buf = b""
        while b"\r\n\r\n" not in buf:
            chunk = client.recv(65536)
            if not chunk:
                break
            buf += chunk
            if len(buf) > MAX_HEAD_BYTES:
                break
        head, sep, rest = buf.partition(b"\r\n\r\n")
        if sep:
            return head + sep, rest
        return buf, b""

    @staticmethod
    def _send_error(client: socket.socket, code: int, message: str) -> None:
        body = message.encode()
        resp = (
            f"HTTP/1.1 {code} Proxy Error\r\n"
            f"Content-Type: text/plain\r\n"
            f"Content-Length: {len(body)}\r\n"
            f"Connection: close\r\n\r\n"
        ).encode() + body
        try:
            client.sendall(resp)
        except OSError:
            pass
        finally:
            try:
                client.close()
            except OSError:
                pass


class LocalHttpProxy:
    def __init__(self, pool: ProxyPool, settings: SettingsStore, host: str, port: int):
        handler = type("BoundHttpProxyHandler", (_HttpProxyHandler,), {"pool": pool, "settings": settings})
        self._server = ReusableThreadingTCPServer((host, port), handler)
        self.host, self.port = host, port

    def start_in_thread(self) -> threading.Thread:
        t = threading.Thread(target=self._server.serve_forever, daemon=True, name="http-forward-proxy")
        t.start()
        return t

    def shutdown(self) -> None:
        self._server.shutdown()
        self._server.server_close()
