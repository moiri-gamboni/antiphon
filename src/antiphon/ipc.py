"""The control socket between the CLI and the bridge.

Newline-delimited JSON over a Unix socket, one request per connection:
`{"v": 1, "op": <name>, "args": {...}}` answered by `{"ok": true, "result": ...}`
or `{"ok": false, "error": {"kind", "message", "raw"}}`. The server reads the
calling process's pid from the socket so the bridge can classify who is asking.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import socket
import struct
import sys

log = logging.getLogger("antiphon.ipc")

PROTOCOL_VERSION = 1

# A final answer or a long brief can exceed asyncio's 64 KiB default line limit.
LINE_LIMIT = 16 * 1024 * 1024


class IpcError(Exception):
    """The bridge answered with an error; `kind` is what the CLI maps to an exit code."""

    def __init__(self, kind: str, message: str, raw=None, degraded: list[str] | None = None):
        super().__init__(message)
        self.kind = kind
        self.message = message
        self.raw = raw
        self.degraded = degraded


class BridgeUnreachable(Exception):
    """No bridge answered on the socket."""


class BridgeBusy(BridgeUnreachable):
    """A bridge accepted the connection but did not reply in time: it is alive, just
    slow (a fresh one still making its first daemon connection), not a stale socket."""


def peer_pid(sock: socket.socket) -> int | None:
    """The pid of the process at the other end of a Unix socket, where the OS tells us."""
    if sys.platform.startswith("linux"):
        raw = sock.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i"))
        pid, _uid, _gid = struct.unpack("3i", raw)
        return pid
    if sys.platform == "darwin":
        # SOL_LOCAL (0) / LOCAL_PEERPID (0x002) from the kernel's sys/un.h; the
        # socket module does not name them.
        raw = sock.getsockopt(0, 0x002, struct.calcsize("i"))
        return struct.unpack("i", raw)[0]
    return None


def _error(kind: str, message: str, raw=None) -> dict:
    return {"ok": False, "error": {"kind": kind, "message": message, "raw": raw}}


async def serve(path: str, handler, extra=None) -> asyncio.AbstractServer:
    """Listen on `path`; `handler(op, args, caller_pid)` answers each request.

    `extra()` may return fields merged into every envelope (the degraded banner).
    """

    async def on_connection(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            line = await reader.readline()
            log.debug("ipc request: %s", line)
            reply = await _reply(line, writer)
            if extra is not None:
                reply.update(extra())
            log.debug("ipc reply: %s", reply)
            writer.write(json.dumps(reply).encode() + b"\n")
            await writer.drain()
        finally:
            writer.close()

    async def _reply(line: bytes, writer: asyncio.StreamWriter) -> dict:
        try:
            request = json.loads(line)
            version, op, args = request["v"], request["op"], request["args"]
        except (ValueError, KeyError, TypeError) as e:
            return _error("bad_request", f"request is not a versioned op envelope: {e!r}")
        if version != PROTOCOL_VERSION:
            return _error("version", f"request is protocol version {version}, this bridge speaks {PROTOCOL_VERSION}")
        caller_pid = peer_pid(writer.get_extra_info("socket"))
        try:
            return {"ok": True, "result": await handler(op, args, caller_pid)}
        except IpcError as e:
            return _error(e.kind, e.message, e.raw)
        except Exception as e:
            # A bug in one op must not take the bridge down; the caller gets the
            # exception's name and the traceback goes to the bridge log.
            log.exception("op %s failed", op)
            return _error("internal", f"{op} failed in the bridge: {e!r}")

    server = await asyncio.start_unix_server(on_connection, path=path, limit=LINE_LIMIT)
    os.chmod(path, 0o600)
    return server


def call_raw(path: str, op: str, args: dict, timeout: float = 30) -> dict:
    """One request, the whole reply envelope; raises `IpcError` on an error reply."""
    request = json.dumps({"v": PROTOCOL_VERSION, "op": op, "args": args}).encode() + b"\n"
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
        s.settimeout(timeout)
        try:
            s.connect(path)
            s.sendall(request)
            data = b""
            while not data.endswith(b"\n"):
                chunk = s.recv(65536)
                if not chunk:
                    raise BridgeBusy("the bridge closed the connection without replying")
                data += chunk
        except (FileNotFoundError, ConnectionRefusedError) as e:
            raise BridgeUnreachable(f"no bridge at {path}: {e.strerror}") from e
        except TimeoutError as e:
            raise BridgeBusy(f"the bridge did not reply within {timeout} s") from e
    reply = json.loads(data)
    log.debug("ipc %s -> %s", op, reply)
    if not reply["ok"]:
        error = reply["error"]
        raise IpcError(error["kind"], error["message"], error.get("raw"), reply.get("degraded"))
    return reply


def call(path: str, op: str, args: dict, timeout: float = 30):
    return call_raw(path, op, args, timeout)["result"]
