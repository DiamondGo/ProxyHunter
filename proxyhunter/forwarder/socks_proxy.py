from __future__ import annotations

import asyncio
import ipaddress
import struct

from proxyhunter.forwarder.pool import ProxyPool
from proxyhunter.forwarder.tunnel import (
    CONNECTION_ERRORS,
    AsyncProxyServer,
    RelayCancelledError,
    close_writer,
    connect_via_upstream,
    relay,
)
from proxyhunter.settings import SettingsStore

SOCKS_VERSION = 0x05
CMD_CONNECT = 0x01
ATYP_IPV4 = 0x01
ATYP_DOMAIN = 0x03
ATYP_IPV6 = 0x04
REP_SUCCESS = 0x00
REP_GENERAL_FAILURE = 0x01
REP_COMMAND_NOT_SUPPORTED = 0x07
REP_ADDRESS_TYPE_NOT_SUPPORTED = 0x08
DEFAULT_MAX_RETRIES = 3


class LocalSocks5Proxy(AsyncProxyServer):
    """No-auth SOCKS5 CONNECT listener with tracked, cancellable async handlers."""

    def __init__(self, pool: ProxyPool, settings: SettingsStore, host: str, port: int):
        super().__init__(pool, settings, host, port)

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        # One total deadline prevents byte-at-a-time handshake slowloris attacks.
        target = await asyncio.wait_for(self._handshake(reader, writer), self.timeout)
        if target is None:
            return
        host, port = target
        for upstream in self.pool.pick_many(self.max_retries + 1):
            try:
                remote_reader, remote_writer = await connect_via_upstream(upstream, host, port, self.timeout)
            except CONNECTION_ERRORS + (ValueError,):
                self.pool.report_result(upstream, False)
                continue
            transferred = 0
            try:
                await self._reply(writer, REP_SUCCESS)
                transferred = await relay(reader, writer, remote_reader, remote_writer, self.idle_timeout)
            except RelayCancelledError as exc:
                transferred = exc.transferred
                raise
            finally:
                await close_writer(remote_writer)
                self.pool.report_result(upstream, transferred > 0)
            return
        await self._reply(writer, REP_GENERAL_FAILURE)

    async def _handshake(self, reader, writer) -> tuple[str, int] | None:
        version, count = await reader.readexactly(2)
        if version != SOCKS_VERSION:
            return None
        methods = await reader.readexactly(count)
        if 0 not in methods:
            writer.write(b"\x05\xff")
            await writer.drain()
            return None
        writer.write(b"\x05\x00")
        await writer.drain()
        version, cmd, reserved, atyp = await reader.readexactly(4)
        if version != SOCKS_VERSION or reserved != 0:
            await self._reply(writer, REP_GENERAL_FAILURE)
            return None
        if cmd != CMD_CONNECT:
            await self._reply(writer, REP_COMMAND_NOT_SUPPORTED)
            return None
        if atyp in (ATYP_IPV4, ATYP_IPV6):
            raw = await reader.readexactly(4 if atyp == ATYP_IPV4 else 16)
            host = str(ipaddress.ip_address(raw))
        elif atyp == ATYP_DOMAIN:
            length = (await reader.readexactly(1))[0]
            raw = await reader.readexactly(length)
            try:
                host = raw.decode("idna")
                if not host or any(c in host for c in "\x00\r\n /:"):
                    raise ValueError("invalid domain")
                # Send an ASCII IDNA name on the wire to upstream CONNECT.
                host = host.encode("idna").decode("ascii")
            except UnicodeError:
                await self._reply(writer, REP_ADDRESS_TYPE_NOT_SUPPORTED)
                return None
            except ValueError:
                await self._reply(writer, REP_ADDRESS_TYPE_NOT_SUPPORTED)
                return None
        else:
            await self._reply(writer, REP_ADDRESS_TYPE_NOT_SUPPORTED)
            return None
        port = struct.unpack("!H", await reader.readexactly(2))[0]
        if port == 0:
            await self._reply(writer, REP_GENERAL_FAILURE)
            return None
        return host, port

    async def _reply(self, writer, rep: int) -> None:
        writer.write(bytes([SOCKS_VERSION, rep, 0, ATYP_IPV4]) + b"\x00" * 6)
        await asyncio.wait_for(writer.drain(), self.timeout)
