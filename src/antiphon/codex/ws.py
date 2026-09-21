"""WebSocket client over a Unix socket, as the Codex app-server control socket speaks it.

The daemon accepts an HTTP upgrade on `GET /rpc` with no authentication and
exchanges JSON-RPC as text frames. Client frames are masked (the RFC 6455
requirement), server frames are not. Only text, ping, pong and close frames
are expected; a ping is answered with a pong carrying the same payload.
"""
import asyncio
import base64
import os
import struct

OP_CONTINUATION = 0x0
OP_TEXT = 0x1
OP_CLOSE = 0x8
OP_PING = 0x9
OP_PONG = 0xA


def upgrade_request(key: str, upgrade_path: str = "/rpc") -> bytes:
    return (
        f"GET {upgrade_path} HTTP/1.1\r\nHost: localhost\r\nConnection: Upgrade\r\n"
        "Upgrade: websocket\r\nSec-WebSocket-Version: 13\r\n"
        f"Sec-WebSocket-Key: {key}\r\n\r\n"
    ).encode()


def encode_frame(opcode: int, payload: bytes, mask: bytes | None) -> bytes:
    n = len(payload)
    mask_bit = 0x80 if mask is not None else 0
    header = bytes([0x80 | opcode])
    if n < 126:
        header += bytes([mask_bit | n])
    elif n < 65536:
        header += bytes([mask_bit | 126]) + struct.pack(">H", n)
    else:
        header += bytes([mask_bit | 127]) + struct.pack(">Q", n)
    if mask is None:
        return header + payload
    return header + mask + _xor(payload, mask)


def _xor(payload: bytes, mask: bytes) -> bytes:
    repeated = (mask * (len(payload) // 4 + 1))[: len(payload)]
    return bytes(a ^ b for a, b in zip(payload, repeated))


class TransportClosed(Exception):
    """The connection ended: a close frame, EOF, or a refused upgrade.

    `last_bytes` is the tail of what the peer sent before the end, for the
    person reading the failure.
    """

    def __init__(self, reason: str, last_bytes: bytes = b""):
        super().__init__(reason)
        self.reason = reason
        self.last_bytes = last_bytes


class UnixWebSocket:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self._reader = reader
        self._writer = writer
        self._last_bytes = b""

    @classmethod
    async def connect(cls, path: str, upgrade_path: str = "/rpc") -> "UnixWebSocket":
        reader, writer = await asyncio.open_unix_connection(path)
        key = base64.b64encode(os.urandom(16)).decode()
        writer.write(upgrade_request(key, upgrade_path))
        await writer.drain()
        try:
            head = await reader.readuntil(b"\r\n\r\n")
        except asyncio.IncompleteReadError as e:
            writer.close()
            raise TransportClosed("connection closed during the upgrade", e.partial) from e
        status_line = head.split(b"\r\n", 1)[0]
        if not status_line.startswith(b"HTTP/1.1 101"):
            writer.close()
            raise TransportClosed(f"upgrade refused: {status_line.decode(errors='replace')}", head)
        return cls(reader, writer)

    async def send_text(self, text: str) -> None:
        self._writer.write(encode_frame(OP_TEXT, text.encode(), os.urandom(4)))
        await self._writer.drain()

    async def recv_text(self) -> str:
        while True:
            try:
                opcode, payload = await read_frame(self._reader)
            except asyncio.IncompleteReadError as e:
                raise TransportClosed("connection closed by the peer", self._last_bytes + e.partial) from e
            except ConnectionError as e:
                # A daemon dying with our data unread resets the socket instead of
                # closing it; for the caller it is the same event.
                raise TransportClosed(f"connection reset by the peer: {e}", self._last_bytes) from e
            self._last_bytes = (self._last_bytes + payload)[-512:]
            if opcode == OP_TEXT:
                return payload.decode()
            if opcode == OP_PING:
                self._writer.write(encode_frame(OP_PONG, payload, os.urandom(4)))
                await self._writer.drain()
            elif opcode == OP_CLOSE:
                # First two bytes are the status code, the rest the reason text.
                raise TransportClosed(f"close frame: {payload[2:].decode(errors='replace')}", self._last_bytes)
            elif opcode != OP_PONG:
                raise TransportClosed(f"unexpected frame opcode {opcode:#x}", self._last_bytes)

    async def close(self) -> None:
        if not self._writer.is_closing():
            self._writer.write(encode_frame(OP_CLOSE, b"\x03\xe8", os.urandom(4)))
            self._writer.close()
        # The peer may already be gone; what matters is that our side is released.
        try:
            await self._writer.wait_closed()
        except OSError:
            pass


async def read_frame(reader: asyncio.StreamReader) -> tuple[int, bytes]:
    """One complete message: (opcode, payload), continuation frames joined."""
    opcode = None
    payload = b""
    while True:
        head = await reader.readexactly(2)
        fin = head[0] & 0x80
        frame_opcode = head[0] & 0x0F
        masked = head[1] & 0x80
        n = head[1] & 0x7F
        if n == 126:
            n = struct.unpack(">H", await reader.readexactly(2))[0]
        elif n == 127:
            n = struct.unpack(">Q", await reader.readexactly(8))[0]
        mask = await reader.readexactly(4) if masked else None
        data = await reader.readexactly(n)
        if mask is not None:
            data = _xor(data, mask)
        if frame_opcode != OP_CONTINUATION:
            opcode = frame_opcode
        payload += data
        if fin:
            return opcode, payload
