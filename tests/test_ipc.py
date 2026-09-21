import asyncio
import json
import os
import socket

import pytest

from antiphon import ipc


def run(coro):
    return asyncio.run(coro)


class Served:
    """An IPC server whose handler records what it saw and answers from a table."""

    def __init__(self, tmp_path):
        self.path = str(tmp_path / "bridge.sock")
        self.calls: list[tuple[str, dict, int | None]] = []
        self.results: dict[str, object] = {"ping": {"ok": "yes"}}
        self.server = None

    async def handler(self, op, args, caller_pid):
        self.calls.append((op, args, caller_pid))
        if op == "boom":
            raise ipc.IpcError("delivery_rejected", "the daemon said no", {"code": -32600})
        if op == "crash":
            raise RuntimeError("handler bug")
        if op not in self.results:
            raise ipc.IpcError("unknown_op", f"unknown op {op!r}")
        return self.results[op]

    async def __aenter__(self):
        self.server = await ipc.serve(self.path, self.handler)
        return self

    async def __aexit__(self, *exc):
        self.server.close()
        await self.server.wait_closed()


def raw_exchange(path: str, line: bytes) -> list[bytes]:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
        s.connect(path)
        s.sendall(line)
        s.shutdown(socket.SHUT_WR)
        data = b""
        while chunk := s.recv(65536):
            data += chunk
    return data.split(b"\n")[:-1]


def test_a_request_line_yields_exactly_one_reply_line(tmp_path):
    async def body():
        async with Served(tmp_path) as s:
            request = json.dumps({"v": 1, "op": "ping", "args": {"x": 1}}).encode() + b"\n"
            lines = await asyncio.to_thread(raw_exchange, s.path, request)
            return lines, s.calls

    lines, calls = run(body())
    assert len(lines) == 1
    assert json.loads(lines[0]) == {"ok": True, "result": {"ok": "yes"}}
    assert calls[0][:2] == ("ping", {"x": 1})


def test_the_callers_pid_is_read_from_the_socket(tmp_path):
    async def body():
        async with Served(tmp_path) as s:
            await asyncio.to_thread(ipc.call, s.path, "ping", {})
            return s.calls[0][2]

    assert run(body()) == os.getpid()


def test_unknown_op_is_an_error_reply_of_kind_unknown_op(tmp_path):
    async def body():
        async with Served(tmp_path) as s:
            with pytest.raises(ipc.IpcError) as info:
                await asyncio.to_thread(ipc.call, s.path, "nope", {})
            return info.value

    error = run(body())
    assert error.kind == "unknown_op"
    assert "nope" in error.message


def test_version_mismatch_names_both_versions(tmp_path):
    async def body():
        async with Served(tmp_path) as s:
            request = json.dumps({"v": 7, "op": "ping", "args": {}}).encode() + b"\n"
            [line] = await asyncio.to_thread(raw_exchange, s.path, request)
            return json.loads(line), s.calls

    reply, calls = run(body())
    assert reply["ok"] is False
    assert reply["error"]["kind"] == "version"
    assert "7" in reply["error"]["message"] and "1" in reply["error"]["message"]
    assert calls == []


def test_a_handler_error_carries_kind_message_and_raw(tmp_path):
    async def body():
        async with Served(tmp_path) as s:
            with pytest.raises(ipc.IpcError) as info:
                await asyncio.to_thread(ipc.call, s.path, "boom", {})
            return info.value

    error = run(body())
    assert (error.kind, error.message, error.raw) == ("delivery_rejected", "the daemon said no", {"code": -32600})


def test_a_handler_exception_is_reported_as_an_internal_error_and_the_server_survives(tmp_path, caplog):
    async def body():
        async with Served(tmp_path) as s:
            with pytest.raises(ipc.IpcError) as info:
                await asyncio.to_thread(ipc.call, s.path, "crash", {})
            again = await asyncio.to_thread(ipc.call, s.path, "ping", {})
            return info.value, again

    error, again = run(body())
    assert error.kind == "internal"
    assert "RuntimeError" in error.message
    assert again == {"ok": "yes"}
    assert "handler bug" in caplog.text


def test_a_request_that_is_not_json_gets_a_bad_request_reply(tmp_path):
    async def body():
        async with Served(tmp_path) as s:
            [line] = await asyncio.to_thread(raw_exchange, s.path, b"not json\n")
            return json.loads(line)

    reply = run(body())
    assert reply["ok"] is False
    assert reply["error"]["kind"] == "bad_request"


def test_call_raises_bridge_unreachable_when_nothing_listens(tmp_path):
    with pytest.raises(ipc.BridgeUnreachable):
        ipc.call(str(tmp_path / "missing.sock"), "ping", {})
    stale = tmp_path / "stale.sock"
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
        s.bind(str(stale))
    with pytest.raises(ipc.BridgeUnreachable):
        ipc.call(str(stale), "ping", {})


def test_call_raises_bridge_unreachable_when_the_server_closes_without_replying(tmp_path):
    path = str(tmp_path / "bridge.sock")

    async def body():
        async def hang_up(reader, writer):
            await reader.readline()
            writer.close()

        server = await asyncio.start_unix_server(hang_up, path=path)
        try:
            with pytest.raises(ipc.BridgeUnreachable):
                await asyncio.to_thread(ipc.call, path, "ping", {}, 2)
        finally:
            server.close()
            await server.wait_closed()

    run(body())


def test_a_payload_larger_than_the_default_line_limit_round_trips(tmp_path):
    async def body():
        async with Served(tmp_path) as s:
            big = "z" * 500_000  # well past asyncio's 64 KiB default line limit
            s.results["ping"] = {"echo": big}
            result = await asyncio.to_thread(ipc.call, s.path, "ping", {"blob": big})
            return result, s.calls[0][1]

    result, args = run(body())
    assert result == {"echo": "z" * 500_000}
    assert args["blob"] == "z" * 500_000


def test_the_socket_is_private_to_the_user(tmp_path):
    async def body():
        async with Served(tmp_path) as s:
            return os.stat(s.path).st_mode & 0o777

    assert run(body()) == 0o600


def test_the_envelope_carries_the_extra_fields_the_server_adds(tmp_path):
    path = str(tmp_path / "bridge.sock")

    async def handler(op, args, caller_pid):
        return {"pong": True}

    async def body():
        server = await ipc.serve(path, handler, extra=lambda: {"degraded": ["codex protocol: turn/steer unsupported"]})
        try:
            return await asyncio.to_thread(ipc.call_raw, path, "ping", {})
        finally:
            server.close()
            await server.wait_closed()

    reply = run(body())
    assert reply == {"ok": True, "result": {"pong": True}, "degraded": ["codex protocol: turn/steer unsupported"]}
