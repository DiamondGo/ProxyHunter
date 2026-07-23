from __future__ import annotations

import select
import socket
import socketserver

from proxyhunter.models import Proxy

BUF_SIZE = 65536


class ReusableThreadingTCPServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


def _read_http_response_headers(sock: socket.socket, timeout: float) -> bytes:
    sock.settimeout(timeout)
    buf = b""
    while b"\r\n\r\n" not in buf:
        chunk = sock.recv(4096)
        if not chunk:
            break
        buf += chunk
        if len(buf) > 65536:
            break
    return buf


def connect_via_upstream(upstream: Proxy, target_host: str, target_port: int, timeout: float = 10.0) -> socket.socket:
    """Return a socket already tunneled through `upstream` to (target_host, target_port).

    http/https upstreams are tunneled with the HTTP CONNECT method; socks4/5
    upstreams use PySocks to open a raw connection through the proxy.
    """
    if upstream.protocol in ("socks4", "socks5"):
        import socks  # PySocks

        sock = socks.socksocket()
        proxy_type = socks.SOCKS4 if upstream.protocol == "socks4" else socks.SOCKS5
        sock.set_proxy(proxy_type, upstream.ip, upstream.port)
        sock.settimeout(timeout)
        sock.connect((target_host, target_port))
        sock.settimeout(None)
        return sock

    sock = socket.create_connection((upstream.ip, upstream.port), timeout=timeout)
    request = (
        f"CONNECT {target_host}:{target_port} HTTP/1.1\r\n"
        f"Host: {target_host}:{target_port}\r\n"
        f"Proxy-Connection: keep-alive\r\n\r\n"
    ).encode()
    sock.sendall(request)
    headers = _read_http_response_headers(sock, timeout)
    status_line = headers.split(b"\r\n", 1)[0]
    if b" 200 " not in status_line:
        sock.close()
        raise ConnectionError(f"upstream {upstream.ip}:{upstream.port} refused CONNECT: {status_line!r}")
    sock.settimeout(None)
    return sock


def relay(a: socket.socket, b: socket.socket, idle_timeout: float = 120.0) -> int:
    """Pipe bytes between two connected sockets until either side closes or goes idle.

    Returns the total number of bytes relayed in either direction, so callers
    can tell an upstream that accepted the connection but never actually
    moved any data (e.g. closed right away) apart from one that did.
    """
    sockets = [a, b]
    total_bytes = 0
    try:
        while True:
            readable, _, exceptional = select.select(sockets, [], sockets, idle_timeout)
            if exceptional or not readable:
                break
            closed = False
            for s in readable:
                other = b if s is a else a
                try:
                    data = s.recv(BUF_SIZE)
                except OSError:
                    closed = True
                    break
                if not data:
                    closed = True
                    break
                try:
                    other.sendall(data)
                except OSError:
                    closed = True
                    break
                total_bytes += len(data)
            if closed:
                break
    finally:
        for s in sockets:
            try:
                s.close()
            except OSError:
                pass
    return total_bytes
