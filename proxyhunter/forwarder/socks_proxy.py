from __future__ import annotations

import logging
import socket
import socketserver
import struct
import threading

from proxyhunter.forwarder.pool import ProxyPool
from proxyhunter.forwarder.tunnel import ReusableThreadingTCPServer, connect_via_upstream, relay
from proxyhunter.settings import SettingsStore

log = logging.getLogger(__name__)

SOCKS_VERSION = 0x05
CMD_CONNECT = 0x01
ATYP_IPV4 = 0x01
ATYP_DOMAIN = 0x03
ATYP_IPV6 = 0x04

REP_SUCCESS = 0x00
REP_GENERAL_FAILURE = 0x01
REP_COMMAND_NOT_SUPPORTED = 0x07
REP_ADDRESS_TYPE_NOT_SUPPORTED = 0x08

MAX_RETRIES = 3


class _Socks5Handler(socketserver.BaseRequestHandler):
    pool: ProxyPool
    settings: SettingsStore

    @property
    def timeout(self) -> float:
        return self.settings.get("timeout", 8.0)

    def handle(self) -> None:
        client = self.request
        client.settimeout(self.timeout)
        try:
            if not self._negotiate_auth(client):
                return
            target_host, target_port = self._read_request(client)
        except (OSError, ValueError):
            return
        if target_host is None:
            return

        candidates = self.pool.pick_many(MAX_RETRIES + 1)
        if not candidates:
            self._reply(client, REP_GENERAL_FAILURE)
            return

        for upstream in candidates:
            try:
                remote = connect_via_upstream(upstream, target_host, target_port, timeout=self.timeout)
            except (OSError, ConnectionError) as exc:
                log.debug("socks upstream connect via %s:%s failed: %s", upstream.ip, upstream.port, exc)
                self.pool.report_result(upstream, False)
                continue
            self.pool.report_result(upstream, True)
            self._reply(client, REP_SUCCESS)
            relay(client, remote)
            return

        self._reply(client, REP_GENERAL_FAILURE)

    def _negotiate_auth(self, client: socket.socket) -> bool:
        header = self._recvn(client, 2)
        version, nmethods = header[0], header[1]
        if version != SOCKS_VERSION:
            return False
        methods = self._recvn(client, nmethods)
        if 0x00 not in methods:
            client.sendall(bytes([SOCKS_VERSION, 0xFF]))
            return False
        client.sendall(bytes([SOCKS_VERSION, 0x00]))
        return True

    def _read_request(self, client: socket.socket) -> tuple[str | None, int]:
        header = self._recvn(client, 4)
        version, cmd, _rsv, atyp = header
        if version != SOCKS_VERSION or cmd != CMD_CONNECT:
            self._reply(client, REP_COMMAND_NOT_SUPPORTED)
            return None, 0

        if atyp == ATYP_IPV4:
            addr = socket.inet_ntoa(self._recvn(client, 4))
        elif atyp == ATYP_DOMAIN:
            length = self._recvn(client, 1)[0]
            addr = self._recvn(client, length).decode()
        elif atyp == ATYP_IPV6:
            addr = socket.inet_ntop(socket.AF_INET6, self._recvn(client, 16))
        else:
            self._reply(client, REP_ADDRESS_TYPE_NOT_SUPPORTED)
            return None, 0

        port = struct.unpack("!H", self._recvn(client, 2))[0]
        return addr, port

    @staticmethod
    def _recvn(sock: socket.socket, n: int) -> bytes:
        buf = b""
        while len(buf) < n:
            chunk = sock.recv(n - len(buf))
            if not chunk:
                raise OSError("connection closed while reading SOCKS5 handshake")
            buf += chunk
        return buf

    @staticmethod
    def _reply(client: socket.socket, rep: int) -> None:
        try:
            client.sendall(
                bytes([SOCKS_VERSION, rep, 0x00, ATYP_IPV4]) + socket.inet_aton("0.0.0.0") + struct.pack("!H", 0)
            )
        except OSError:
            pass


class LocalSocks5Proxy:
    def __init__(self, pool: ProxyPool, settings: SettingsStore, host: str, port: int):
        handler = type("BoundSocks5Handler", (_Socks5Handler,), {"pool": pool, "settings": settings})
        self._server = ReusableThreadingTCPServer((host, port), handler)
        self.host, self.port = host, port

    def start_in_thread(self) -> threading.Thread:
        t = threading.Thread(target=self._server.serve_forever, daemon=True, name="socks5-forward-proxy")
        t.start()
        return t

    def shutdown(self) -> None:
        self._server.shutdown()
        self._server.server_close()
