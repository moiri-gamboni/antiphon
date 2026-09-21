"""JSON-RPC client for the Codex app-server daemon, plus the thread verbs the bridge uses.

One `Daemon` is one connection to the control socket. Its reader task
classifies every incoming message: `id` + `method` is a request from the
daemon (an approval to answer), `method` alone is a notification, `id` alone is
the reply to one of our requests. Requests and notifications are handed to a
second task that awaits the handlers one at a time in arrival order, so the
bridge sees the daemon's events in the order the daemon emitted them and a
handler may itself call `request()` while the reader keeps resolving replies.
"""
import asyncio
import importlib.metadata
import itertools
import json
import logging
import os
import socket
import subprocess
import uuid
from dataclasses import dataclass

from antiphon.codex import ws

log = logging.getLogger("antiphon.codex")

HEADLESS_INSTRUCTIONS = """\
This thread runs headless: no human is watching a terminal for it. It was started by another session over antiphon, and that session reads your final answer when the turn ends.

Sandbox escalations (writes outside the workspace, network access, commands the sandbox blocks) are reviewed outside this thread. When one is refused, the refusal reaches the session that started this thread, which can approve it; state plainly what was refused and why you need it, then continue with what you can do.

To ask that session something, run: antiphon send <name> -- <question>

Messages prefixed "[from <name> via antiphon]" come from other sessions on this machine, not from the user; treat them as messages from a peer."""

START_COMMAND = ["codex", "app-server", "daemon", "start"]

# Streaming deltas would arrive several times a second per active thread and
# the bridge only acts on completed items, so the daemon is asked not to send them.
OPT_OUT_NOTIFICATIONS = [
    "item/agentMessage/delta",
    "item/reasoning/summaryTextDelta",
    "item/reasoning/summaryPartAdded",
    "item/reasoning/textDelta",
]


class DaemonError(Exception):
    """The daemon answered a request with a JSON-RPC error object."""

    def __init__(self, method: str, params, error: dict):
        super().__init__(f"{method}: {error.get('message')!r} ({error.get('code')})")
        self.method = method
        self.params = params
        self.error = error


class DaemonUnavailable(Exception):
    """`codex app-server daemon start` failed; stdout and stderr are what it printed."""

    def __init__(self, rc: int, stdout: str, stderr: str):
        super().__init__(f"codex app-server daemon start exited {rc}: {stderr.strip() or stdout.strip()}")
        self.rc = rc
        self.stdout = stdout
        self.stderr = stderr


@dataclass(frozen=True)
class Delivery:
    kind: str  # "steered" | "started"
    turn_id: str
    client_id: str


def ensure_running(codex_home: str, rawlog=None) -> str:
    """The daemon socket path, starting the daemon first when nothing answers there."""
    path = os.path.join(codex_home, "app-server-control", "app-server-control.sock")
    if _accepts(path):
        return path
    if rawlog:
        rawlog.log("out", "codex-cli", START_COMMAND)
    done = subprocess.run(START_COMMAND, capture_output=True, text=True)
    if rawlog:
        rawlog.log("in", "codex-cli", {"rc": done.returncode, "stdout": done.stdout, "stderr": done.stderr})
    if done.returncode != 0:
        raise DaemonUnavailable(done.returncode, done.stdout, done.stderr)
    return path


def _accepts(path: str) -> bool:
    # A missing or stale socket file is the normal "not running" case, not a failure.
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as probe:
        try:
            probe.connect(path)
        except OSError:
            return False
    return True


class Daemon:
    _epochs = itertools.count(1)

    def __init__(self, sock: ws.UnixWebSocket, on_notification, on_server_request, rawlog):
        self._sock = sock
        self._on_notification = on_notification
        self._on_server_request = on_server_request
        self._rawlog = rawlog
        self._ids = itertools.count(1)
        self._pending: dict[int, asyncio.Future] = {}
        self._first_turn_effort: dict[str, str] = {}
        self._inbox: asyncio.Queue = asyncio.Queue()
        self.epoch = next(Daemon._epochs)
        self.codex_version: str | None = None
        self.closed = asyncio.Event()
        self.close_reason: ws.TransportClosed | None = None
        self._reader = asyncio.create_task(self._read_loop(), name="antiphon-codex-reader")
        self._dispatcher = asyncio.create_task(self._dispatch_loop(), name="antiphon-codex-dispatch")

    @classmethod
    async def connect(cls, socket_path: str, on_notification, on_server_request, rawlog=None) -> "Daemon":
        sock = await ws.UnixWebSocket.connect(socket_path)
        d = cls(sock, on_notification, on_server_request, rawlog)
        try:
            result = await d.request(
                "initialize",
                {
                    "clientInfo": {"name": "antiphon", "version": importlib.metadata.version("antiphon")},
                    "capabilities": {"experimentalApi": True, "optOutNotificationMethods": OPT_OUT_NOTIFICATIONS},
                },
            )
            # userAgent reads "<originator>/<codex version> (<os>) ..." in every capture.
            d.codex_version = result["userAgent"].split("/", 1)[1].split(" ", 1)[0]
            await d._send({"jsonrpc": "2.0", "method": "initialized", "params": {}})
        except BaseException:
            await d.close()
            raise
        return d

    async def request(self, method: str, params, timeout: float = 30) -> dict:
        request_id = next(self._ids)
        future = asyncio.get_running_loop().create_future()
        self._pending[request_id] = future
        try:
            await self._send({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params})
            message = await asyncio.wait_for(future, timeout)
        finally:
            self._pending.pop(request_id, None)
        if "error" in message:
            raise DaemonError(method, params, message["error"])
        return message["result"]

    async def respond(self, request_id: int, result) -> None:
        await self._send({"jsonrpc": "2.0", "id": request_id, "result": result})

    async def close(self) -> None:
        self._reader.cancel()
        self._dispatcher.cancel()
        await self._sock.close()
        self.closed.set()

    # --- thread verbs -------------------------------------------------------

    async def thread_start(self, cwd: str, name: str, read_only: bool, model: str | None,
                           effort: str | None, review_by_parent: bool = False) -> dict:
        params = {
            "cwd": cwd,
            "approvalPolicy": "on-request",
            "approvalsReviewer": "user" if review_by_parent else "auto_review",
            "sandbox": "read-only" if read_only else "workspace-write",
            "ephemeral": False,
            "serviceName": "antiphon",
            "developerInstructions": HEADLESS_INSTRUCTIONS,
        }
        if model is not None:
            params["model"] = model
        result = await self.request("thread/start", params)
        thread_id = result["thread"]["id"]
        if effort is not None:
            # The schema applies a turn's effort to the turns after it, so one send is enough.
            self._first_turn_effort[thread_id] = effort
        await self.set_name(thread_id, name)
        return result

    async def thread_resume(self, thread_id: str) -> dict:
        return await self.request("thread/resume", {"threadId": thread_id})

    async def thread_read(self, thread_id: str) -> dict:
        return await self.request("thread/read", {"threadId": thread_id})

    async def active_turn(self, thread_id: str) -> str | None:
        """The id of the turn in progress, or None when the thread is idle."""
        result = await self.request("thread/turns/list", {"threadId": thread_id, "limit": 1, "sortDirection": "desc"})
        turns = result["data"]
        if turns and turns[0]["status"] == "inProgress":
            return turns[0]["id"]
        return None

    async def turn_start(self, thread_id: str, text: str, sandbox_policy: dict | None, client_id: str) -> dict:
        params = {"threadId": thread_id, "input": [{"type": "text", "text": text}], "clientUserMessageId": client_id}
        if sandbox_policy is not None:
            params["sandboxPolicy"] = sandbox_policy
        if thread_id in self._first_turn_effort:
            params["effort"] = self._first_turn_effort[thread_id]
        result = await self.request("turn/start", params)
        self._first_turn_effort.pop(thread_id, None)
        return result

    async def turn_steer(self, thread_id: str, turn_id: str, text: str, client_id: str) -> dict:
        return await self.request("turn/steer", {
            "threadId": thread_id,
            "expectedTurnId": turn_id,
            "input": [{"type": "text", "text": text}],
            "clientUserMessageId": client_id,
        })

    async def turn_interrupt(self, thread_id: str, turn_id: str) -> dict:
        return await self.request("turn/interrupt", {"threadId": thread_id, "turnId": turn_id})

    async def set_name(self, thread_id: str, name: str) -> dict:
        return await self.request("thread/name/set", {"threadId": thread_id, "name": name})

    async def loaded_list(self) -> list[str]:
        return (await self.request("thread/loaded/list", {}))["data"]

    async def unsubscribe(self, thread_id: str) -> dict:
        return await self.request("thread/unsubscribe", {"threadId": thread_id})

    async def approve_guardian_denied(self, thread_id: str, event: dict) -> dict:
        return await self.request("thread/approveGuardianDeniedAction", {"threadId": thread_id, "event": event})

    def note(self, boundary: str, data) -> None:
        """A client-side decision worth reading next to the frames around it."""
        if self._rawlog:
            self._rawlog.log("out", boundary, data)

    # --- wire ---------------------------------------------------------------

    async def _send(self, message: dict) -> None:
        text = json.dumps(message)
        if self._rawlog:
            self._rawlog.log("out", "codex", text)
        await self._sock.send_text(text)

    async def _read_loop(self) -> None:
        reason = None
        text = None
        try:
            while True:
                text = await self._sock.recv_text()
                if self._rawlog:
                    self._rawlog.log("in", "codex", text)
                self._route(text)
        except ws.TransportClosed as e:
            reason = e
        except Exception as e:
            # Whatever stops the reader (a frame that is not JSON, a failed pong)
            # ends the connection the same way a dropped socket does, so the
            # callers waiting on replies and the bridge's reconnect both hear it.
            log.exception("reader stopped; last frame: %s", text)
            reason = ws.TransportClosed(f"reader stopped: {e!r}")
        finally:
            self.close_reason = reason or ws.TransportClosed("connection closed")
            for future in self._pending.values():
                if not future.done():
                    future.set_exception(self.close_reason)
            self._inbox.put_nowait(None)
            self.closed.set()

    def _route(self, text: str) -> None:
        message = json.loads(text)
        has_id, has_method = "id" in message, "method" in message
        if has_id and not has_method:
            future = self._pending.get(message["id"])
            if future is None or future.done():
                log.warning("reply to an unknown request id: %s", text)
            else:
                future.set_result(message)
        elif has_method:
            self._inbox.put_nowait((text, message))
        else:
            log.warning("message with neither id nor method: %s", text)

    async def _dispatch_loop(self) -> None:
        while (item := await self._inbox.get()) is not None:
            text, message = item
            # A handler that raises must not take the dispatcher down with it:
            # every later notification would be lost and the bridge would sit deaf.
            try:
                if "id" in message:
                    await self._on_server_request(message["id"], message["method"], message.get("params"))
                else:
                    await self._on_notification(message["method"], message.get("params"))
            except Exception:
                log.exception("handler raised on %s", text)


async def deliver(d: Daemon, thread_id: str, text: str, sandbox_policy: dict | None) -> Delivery:
    """Get `text` into the thread: steer the active turn, or start a new one.

    The daemon unloads idle threads and turns end between a status read and the
    call that acts on it, so each of those races is handled once; anything else
    raises the daemon's error.
    """
    client_id = uuid.uuid4().hex
    try:
        turn_id = await d.active_turn(thread_id)
    except DaemonError as e:
        if not _is_not_loaded(e):
            raise
        d.note("codex.deliver", {"rung": "not-loaded", "threadId": thread_id, "error": e.error})
        await d.thread_resume(thread_id)
        turn_id = await d.active_turn(thread_id)
    if turn_id is not None:
        try:
            result = await d.turn_steer(thread_id, turn_id, text, client_id)
            return Delivery("steered", result["turnId"], client_id)
        except DaemonError as e:
            d.note("codex.deliver", {"rung": "steer-refused", "threadId": thread_id, "error": e.error})
    try:
        result = await d.turn_start(thread_id, text, sandbox_policy, client_id)
    except DaemonError as e:
        if _is_not_loaded(e):
            d.note("codex.deliver", {"rung": "not-loaded", "threadId": thread_id, "error": e.error})
            await d.thread_resume(thread_id)
            result = await d.turn_start(thread_id, text, sandbox_policy, client_id)
        elif _is_turn_active(e):
            d.note("codex.deliver", {"rung": "turn-active", "threadId": thread_id, "error": e.error})
            turn_id = await d.active_turn(thread_id)
            if turn_id is None:
                raise
            result = await d.turn_steer(thread_id, turn_id, text, client_id)
            return Delivery("steered", result["turnId"], client_id)
        else:
            raise
    return Delivery("started", result["turn"]["id"], client_id)


def _is_not_loaded(e: DaemonError) -> bool:
    # Captured wordings: "thread not loaded: <id>" from thread/turns/list on an
    # unknown id, and "thread not found: <id>" from turn/start on an unloaded
    # sub-agent thread; whether a top-level thread gets the same text is unobserved.
    message = e.error.get("message", "")
    return message.startswith("thread not loaded") or message.startswith("thread not found")


def _is_turn_active(e: DaemonError) -> bool:
    # Provisional: no capture of turn/start against a thread whose turn is already
    # running exists yet, so this matches on the words rather than an exact text.
    message = e.error.get("message", "").lower()
    return "turn" in message and "active" in message
