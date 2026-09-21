"""The bridge: one process per user hosting Codex threads as Claude Code peers.

It holds the single connection to the Codex app-server daemon, answers the
CLI over the control socket, keeps the thread table in the state file, and
runs one small peer child per hosted thread so each appears in Claude Code's
registry with a live pid. A reconcile pass on connect and every few seconds
brings the daemon's loaded threads and the state file back into agreement.

Extension points for other modules: `ops` (control-socket op name to handler),
`notifications` (daemon notification method to handler), `child_events`
(peer child event name to handler), and `on_server_request` (the handler for
requests the daemon sends us, replaced by the approvals module).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import subprocess
import sys
import time
from collections import deque
from pathlib import Path

from antiphon import callers, ipc, peers
from antiphon.callers import Caller
from antiphon.claude import registry
from antiphon.codex import approvals as approvals_mod
from antiphon.codex import daemon as daemon_mod
from antiphon.codex.daemon import Daemon, DaemonError, DaemonUnavailable, deliver
from antiphon.codex.ws import TransportClosed
from antiphon.ipc import IpcError
from antiphon.rawlog import RawLog
from antiphon.state import State, SubAgent, ThreadState, ensure_home

log = logging.getLogger("antiphon.bridge")

# Daemon methods whose disappearance means the protocol moved under us.
PINNED_METHODS = ("thread/loaded/list", "turn/steer", "thread/resume")
METHOD_NOT_FOUND = -32601
# Ops that act on one thread, gated by who spawned it.
# `send` is absent: every caller may send, and its target may be a Claude session rather than a thread.
OWNED_OPS = frozenset({"interrupt", "stop", "name", "approve", "deny"})
WAIT_DEFAULT_TIMEOUT = 600.0
DAEMON_WAIT = 3.0
REGISTER_TIMEOUT = 10.0
DELIVER_TIMEOUT = 10.0
CHILD_EXIT_TIMEOUT = 3.0
CHILD_LINE_LIMIT = 16 * 1024 * 1024  # a final answer relayed through the child can be long
IDLE_DETAIL_CHARS = 200


class AlreadyRunning(Exception):
    """Another bridge answers on the control socket."""


class PeerChild:
    """One peer child process: the live pid, registry record and messaging socket of a
    hosted thread, driven over its stdin/stdout with one JSON line per command or event.

    `ready` and the `sent`/`send_failed` answers to `deliver` are resolved here in
    command order (the child handles commands one at a time); every other event goes
    to `on_event(thread_id, event)`, plus `{"ev": "exited"}` once the child is gone.
    """

    def __init__(self, thread_id: str, log_path: Path, config_dir: Path, on_event):
        self.thread_id = thread_id
        self.log_path = log_path
        self.config_dir = config_dir
        self.on_event = on_event
        self.proc: asyncio.subprocess.Process | None = None
        self._ready: asyncio.Future | None = None
        self._deliveries: deque[asyncio.Future] = deque()
        self._reader: asyncio.Task | None = None

    @property
    def pid(self) -> int:
        return self.proc.pid

    async def spawn(self) -> None:
        env = {**os.environ, "CLAUDE_CONFIG_DIR": str(self.config_dir)}
        with open(self.log_path, "ab") as log_file:
            self.proc = await asyncio.create_subprocess_exec(
                sys.executable, "-m", "antiphon.claude.peer",
                stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=log_file,
                env=env, limit=CHILD_LINE_LIMIT,
            )
        self._reader = asyncio.create_task(self._read(), name=f"antiphon-peer-{self.thread_id[:8]}")

    async def send(self, **cmd) -> None:
        self.proc.stdin.write(json.dumps(cmd).encode() + b"\n")
        await self.proc.stdin.drain()

    async def register(self, **fields) -> dict:
        self._ready = asyncio.get_running_loop().create_future()
        await self.send(cmd="register", **fields)
        return await asyncio.wait_for(self._ready, REGISTER_TIMEOUT)

    async def deliver(self, to_sock: str, text: str, from_name: str) -> dict:
        """Send a user frame to a peer socket; the child's answer, `{"ev": "sent", "msg_id"}`
        once the frame was written or `{"ev": "send_failed", "reason", ...}`."""
        outcome = asyncio.get_running_loop().create_future()
        self._deliveries.append(outcome)
        await self.send(cmd="deliver", to_sock=to_sock, text=text, from_name=from_name)
        return await asyncio.wait_for(outcome, DELIVER_TIMEOUT)

    async def close(self) -> None:
        if self.proc.returncode is None:
            try:
                await self.send(cmd="exited")
            except (BrokenPipeError, ConnectionResetError):
                pass  # already exiting; stdin EOF below is the same signal
            self.proc.stdin.close()
            try:
                await asyncio.wait_for(self.proc.wait(), CHILD_EXIT_TIMEOUT)
            except TimeoutError:
                log.warning("peer child %s did not exit; killing it", self.pid)
                self.proc.kill()
                await self.proc.wait()
        await self._reader

    async def _read(self) -> None:
        try:
            async for raw in self.proc.stdout:
                event = json.loads(raw)
                kind = event["ev"]
                if kind == "ready":
                    self._ready.set_result(event)
                elif (kind == "sent" or (kind == "send_failed" and "msg_id" in event)) and self._deliveries:
                    self._deliveries.popleft().set_result(event)
                    # Let the awaiting sender run before the next line is parsed: a receipt for
                    # this very message may already be buffered, and it must find the sender waiting.
                    await asyncio.sleep(0)
                else:
                    await self.on_event(self.thread_id, event)
        finally:
            for outcome in self._deliveries:
                if not outcome.done():
                    outcome.set_result({"ev": "send_failed", "reason": "peer child exited"})
            if self._ready is not None and not self._ready.done():
                self._ready.set_exception(RuntimeError("peer child exited before it was ready"))
            await self.proc.wait()
            await self.on_event(self.thread_id, {"ev": "exited", "rc": self.proc.returncode})


def _status_of(thread: dict) -> str:
    """Our status word for a thread object's `status` as the daemon reports it."""
    status = thread["status"]
    kind = status["type"]
    if kind == "active":
        return "approval" if "waitingOnApproval" in (status.get("activeFlags") or []) else "busy"
    if kind == "notLoaded":
        return "unloaded"
    return "idle"


def _source_label(source) -> str:
    # A top-level thread's source is a string; a sub-agent's is an object keyed by its kind.
    if isinstance(source, dict):
        return "/".join(source)
    return str(source)


def _outcome(turn: dict) -> tuple[str, str]:
    """A completed turn's status and outcome text: its final answer, or `<status>: <error>`."""
    status = turn["status"]
    if status == "completed":
        answers = [i["text"] for i in turn.get("items", []) if i.get("type") == "agentMessage" and i.get("phase") == "final_answer"]
        if not answers:
            answers = [i["text"] for i in turn.get("items", []) if i.get("type") == "agentMessage"]
        return status, "\n".join(answers)
    error = turn.get("error") or {}
    return status, f"{status}: {error.get('message', '')}"


def _git(argv: list[str], cwd: str, rawlog: RawLog) -> subprocess.CompletedProcess:
    rawlog.log("out", "git", {"argv": argv, "cwd": cwd})
    done = subprocess.run(["git", *argv], cwd=cwd, capture_output=True, text=True)
    rawlog.log("in", "git", {"rc": done.returncode, "stdout": done.stdout, "stderr": done.stderr})
    return done


class Bridge:
    reconcile_interval = 15.0
    reconnect_backoff = (1.0, 30.0)

    def __init__(self, home, *, sessions_dir=None, codex_home=None, ensure_running=daemon_mod.ensure_running,
                 process_table=None):
        self.home = Path(home)
        os.makedirs(self.home, 0o700, exist_ok=True)
        os.makedirs(self.home / "log", 0o700, exist_ok=True)
        self.sessions_dir = Path(sessions_dir) if sessions_dir else registry.sessions_dir()
        self.codex_home = str(codex_home or os.environ.get("CODEX_HOME") or Path.home() / ".codex")
        self._ensure_running = ensure_running
        self._process_table = process_table or callers.system_process_table()
        self.socket_path = self.home / "bridge.sock"
        self.state_path = self.home / "state.json"
        self.rawlog = RawLog(self.home / "log" / "raw.jsonl")
        self.state = State.load(self.state_path)
        for thread in self.state.threads.values():
            # Children die with the bridge (stdin EOF), so none of them survived a restart.
            thread.child_pid = None
        self.daemon: Daemon | None = None
        self.connected_once = False
        self.children: dict[str, PeerChild] = {}
        self.subscribed: dict[str, int] = {}  # thread id -> the epoch it was resumed on
        self.pin_failures: list[str] = []
        self.codex_failures: dict[str, str] = {}
        self._unknown_actions: set[str] = set()
        self._recovered_turns: set[str] = set()  # turn ids announced from the turn list, whose completion may still arrive
        self._turn_waiters: dict[str, list[asyncio.Future]] = {}
        self.last_reconcile: float | None = None
        self._reconcile_lock = asyncio.Lock()
        self._tasks: set[asyncio.Task] = set()
        self._server: asyncio.AbstractServer | None = None
        self._closing = False
        self.on_server_request = self.refuse_server_request
        self.sweeps = []  # async () -> None, run after every reconcile pass (approval reminders)
        self.ops = {
            "ping": self.op_ping,
            "start": self.op_start,
            "send": self.op_send,
            "interrupt": self.op_interrupt,
            "wait": self.op_wait,
            "status": self.op_status,
            "ls": self.op_ls,
            "stop": self.op_stop,
            "resume": self.op_resume,
            "name": self.op_name,
        }
        self.notifications = {
            "thread/started": self.on_thread_started,
            "thread/status/changed": self.on_thread_status_changed,
            "turn/started": self.on_turn_started,
            "turn/completed": self.on_turn_completed,
            "thread/closed": self.on_thread_closed,
            "thread/name/updated": self.on_thread_name_updated,
            "error": self.on_error,
        }
        self.child_events = {
            "inbound": self.on_child_inbound,
            "send_failed": self.on_child_send_failed,
            "subscribed": self.log_child_event,
            "unknown_frame": self.on_child_unknown_frame,
            "exited": self.on_child_exited,
        }
        self.approvals = approvals_mod.install(self)
        peers.install(self)

    # --- lifecycle ---------------------------------------------------------------

    async def bind(self) -> None:
        """Take the control socket; a live bridge there means this one must not run."""
        if self.socket_path.exists():
            try:
                await asyncio.to_thread(ipc.call, str(self.socket_path), "ping", {}, 2)
            except ipc.BridgeUnreachable:
                log.info("removing stale control socket %s", self.socket_path)
                self.socket_path.unlink()
            else:
                raise AlreadyRunning(f"a bridge already answers on {self.socket_path}")
        self._server = await ipc.serve(str(self.socket_path), self._on_ipc, extra=self._envelope_extra)

    async def start(self) -> None:
        await self.bind()
        self._spawn(self._connect_loop(), "antiphon-connect")
        self._spawn(self._reconcile_loop(), "antiphon-reconcile")

    async def run(self) -> None:
        await self.start()
        await asyncio.Event().wait()

    async def close(self) -> None:
        self._closing = True
        children, self.children = self.children, {}
        for child in children.values():
            await child.close()
        for task in list(self._tasks):
            task.cancel()
        for task in list(self._tasks):
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        if self.daemon is not None:
            await self.daemon.close()
            self.daemon = None
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self.socket_path.unlink(missing_ok=True)

    def _spawn(self, coro, name: str) -> asyncio.Task:
        task = asyncio.create_task(coro, name=name)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    def save(self) -> None:
        self.state.save(self.state_path)

    def _envelope_extra(self) -> dict:
        return {"degraded": self.state.degraded} if self.state.degraded else {}

    def _update_degraded(self) -> None:
        self.state.degraded = [*self.pin_failures, *self.codex_failures.values()]
        self.save()

    # --- control socket ------------------------------------------------------------

    async def _on_ipc(self, op: str, args: dict, caller_pid: int | None):
        caller = self.classify(caller_pid, args.get("claimed_thread"))
        return await self.dispatch(op, args, caller)

    def classify(self, caller_pid: int | None, claimed_thread: str | None) -> Caller:
        if caller_pid is None:
            return Caller(kind="human", claude_pid=None, claude_session_id=None, codex_thread=None)
        claude_pids = {r.pid: r.session_id for r in self.live_records() if "sessionId" in r.data}
        return callers.classify(caller_pid, claimed_thread, claude_pids=claude_pids,
                                known_threads=self.state.threads.keys(), table=self._process_table)

    async def dispatch(self, op: str, args: dict, caller: Caller):
        if op == "notify" and caller.kind == "claude":
            raise IpcError("usage", "a Claude Code session subscribes natively: use SendMessage with notify_when_idle")
        handler = self.ops.get(op)
        if handler is None:
            raise IpcError("unknown_op", f"unknown op {op!r}")
        try:
            self._check_ownership(op, args, caller)
            return await handler(args, caller)
        except KeyError as e:
            raise IpcError("usage", f"{op} needs argument {e.args[0]!r}") from e

    def _check_ownership(self, op: str, args: dict, caller: Caller) -> None:
        """The ownership rule: a caller drives, stops and approves what it spawned; a
        Claude session or a human may do so to any thread."""
        if op not in OWNED_OPS or (op == "name" and not args.get("target")):
            return
        if op in ("approve", "deny"):
            thread, _ = self.approvals.find(args["token"])
        else:
            thread = self.resolve(args["target"])
        # A Codex thread renaming itself is not acting on another caller's thread.
        spawner = None if op == "name" and thread.thread_id == caller.codex_thread else thread.spawner
        if not callers.permits(caller, op, spawner):
            raise IpcError("forbidden", callers.forbidden_message(caller, op, spawner))

    def live_records(self) -> list[registry.Record]:
        return registry.live_records(self.sessions_dir)

    # --- daemon connection ----------------------------------------------------------

    async def _wait_for_daemon(self) -> Daemon | None:
        """The live daemon connection, waiting briefly for the one a fresh bridge is still making."""
        deadline = time.monotonic() + DAEMON_WAIT
        while self.daemon is None and time.monotonic() < deadline:
            await asyncio.sleep(0.05)
        return self.daemon

    async def _require_daemon(self) -> Daemon:
        d = await self._wait_for_daemon()
        if d is None:
            raise IpcError("daemon_unreachable", "the Codex daemon is not connected; the bridge is reconnecting")
        return d

    async def _connect_loop(self) -> None:
        delay = self.reconnect_backoff[0]
        while not self._closing:
            try:
                path = await asyncio.to_thread(self._ensure_running, self.codex_home, self.rawlog)
                d = await Daemon.connect(path, self._on_notification, self._on_server_request, rawlog=self.rawlog)
            except (DaemonError, KeyError, TypeError) as e:
                # The daemon refused `initialize` or answered it in a shape the client cannot
                # read: it speaks a protocol we do not.
                self.codex_failures["initialize"] = "codex protocol: initialize unsupported"
                self._update_degraded()
                log.error("initialize failed: %r; retrying in %s s", e, delay)
            except (DaemonUnavailable, OSError, TransportClosed, TimeoutError) as e:
                # A restarting or absent daemon: keep trying, with the CLI reporting exit 5 meanwhile.
                log.warning("daemon unreachable: %r; retrying in %s s", e, delay)
            except Exception:
                # Whatever else went wrong, a bridge that stops reconnecting is worse than one
                # that logs the traceback and tries again.
                log.exception("connecting to the daemon failed; retrying in %s s", delay)
            else:
                delay = self.reconnect_backoff[0]
                self.codex_failures.pop("initialize", None)
                self._watch_pinned_methods(d)
                self.daemon = d
                self.connected_once = True
                self.subscribed.clear()
                self._update_degraded()
                log.info("connected to the Codex daemon (epoch %s, codex %s)", d.epoch, d.codex_version)
                self._spawn(self.reconcile(), "antiphon-reconcile-on-connect")
                await d.closed.wait()
                log.warning("daemon connection closed: %r", d.close_reason)
                self.daemon = None
                await d.close()
                continue
            await asyncio.sleep(delay)
            delay = min(delay * 2, self.reconnect_backoff[1])

    def _watch_pinned_methods(self, d: Daemon) -> None:
        """Turn a "method not found" on a method we rely on into a degraded reason."""
        original = d.request

        async def request(method, params, timeout=30):
            try:
                result = await original(method, params, timeout)
            except DaemonError as e:
                if method in PINNED_METHODS and e.error.get("code") == METHOD_NOT_FOUND:
                    self.codex_failures[method] = f"codex protocol: {method} unsupported"
                    self._update_degraded()
                raise
            if method in self.codex_failures:
                del self.codex_failures[method]
                self._update_degraded()
            return result

        d.request = request

    async def _on_notification(self, method: str, params) -> None:
        handler = self.notifications.get(method)
        if handler is not None:
            await handler(params)

    async def _on_server_request(self, request_id: int, method: str, params) -> None:
        await self.on_server_request(request_id, method, params)

    async def refuse_server_request(self, request_id: int, method: str, params) -> None:
        log.warning("refusing server request %s (id %s): %s", method, request_id, json.dumps(params)[:500])
        await self.daemon.respond_error(request_id, METHOD_NOT_FOUND, f"antiphon does not answer {method}")

    # --- notifications ----------------------------------------------------------------

    async def on_thread_started(self, params) -> None:
        thread = params["thread"]
        if thread["id"] in self.state.threads or self._sub_agent(thread["id"]) is not None:
            return
        self._spawn(self.reconcile(), "antiphon-reconcile-on-thread-started")

    async def on_thread_status_changed(self, params) -> None:
        status = _status_of(params)
        thread = self.state.threads.get(params["threadId"])
        sub = self._sub_agent(params["threadId"])
        if thread is not None:
            self._set_status(thread, status)
        elif sub is not None:
            sub.status = status
        else:
            return
        self.save()

    async def on_turn_started(self, params) -> None:
        thread = self.state.threads.get(params["threadId"])
        if thread is None:
            return
        thread.active_turn_id = params["turn"]["id"]
        self._set_status(thread, "busy")
        self.save()

    async def on_turn_completed(self, params) -> None:
        thread = self.state.threads.get(params["threadId"])
        if thread is None:
            return
        if params["turn"]["id"] in self._recovered_turns:
            # Already recorded and announced from the turn list after a reconnect.
            self._recovered_turns.discard(params["turn"]["id"])
            return
        self._record_turn_end(thread, params["turn"])

    def _record_turn_end(self, thread: ThreadState, turn: dict) -> None:
        """Record a finished turn's outcome, wake the waiters, and announce it."""
        status, final = _outcome(turn)
        thread.outcome = status
        thread.final = final
        thread.active_turn_id = None
        if status != "completed":
            thread.last_error = {"message": (turn.get("error") or {}).get("message", ""), "at": time.time()}
        self._set_status(thread, "idle")
        self.save()
        for waiter in self._turn_waiters.pop(thread.thread_id, []):
            if not waiter.done():
                waiter.set_result((status, final))
        self._spawn(self._announce_turn_end(thread, final), "antiphon-turn-end")

    async def _announce_turn_end(self, thread: ThreadState, final: str) -> None:
        """Report the outcome to the spawner, then tell the child's subscribers the thread is idle."""
        if thread.origin == "spawned" and thread.report and thread.spawner != "human":
            await self.deliver_to_spawner(thread, final)
        await self._child_idle(thread, final[:IDLE_DETAIL_CHARS])

    async def on_thread_closed(self, params) -> None:
        thread = self.state.threads.get(params["threadId"])
        if thread is None:
            return
        self.subscribed.pop(thread.thread_id, None)
        if thread.origin == "adopted":
            # A human's TUI thread going away is that session ending; ours merely
            # unloaded and comes back with thread/resume on the next use.
            await self._retire(thread)
        else:
            self._set_status(thread, "unloaded")
        self.save()

    async def on_thread_name_updated(self, params) -> None:
        thread = self.state.threads.get(params["threadId"])
        if thread is None or not params.get("threadName"):
            return
        thread.name = params["threadName"]
        await self._child_rename(thread)
        self.save()

    async def on_error(self, params) -> None:
        thread = self.state.threads.get(params.get("threadId"))
        if thread is None:
            return
        thread.last_error = {"message": params["error"].get("message", ""), "at": time.time(), "willRetry": params.get("willRetry")}
        self.save()

    def _set_status(self, thread: ThreadState, status: str) -> None:
        if thread.status == status:
            return
        thread.status = status
        if thread.thread_id in self.children:
            self._spawn(self._child_cmd(thread, cmd="status", status=status), "antiphon-child-status")

    def _sub_agent(self, thread_id: str) -> SubAgent | None:
        for thread in self.state.threads.values():
            if thread_id in thread.sub_agents:
                return thread.sub_agents[thread_id]
        return None

    # --- reconcile --------------------------------------------------------------------

    async def _reconcile_loop(self) -> None:
        while not self._closing:
            await asyncio.sleep(self.reconcile_interval)
            await self.reconcile()

    async def reconcile(self) -> None:
        async with self._reconcile_lock:
            self._check_pins()
            d = self.daemon
            if d is None:
                return
            try:
                await self._reconcile_with(d)
            except (TransportClosed, DaemonError, TimeoutError) as e:
                # The connection went away mid-pass or the daemon refused or sat on a call;
                # the next pass (or the reconnect) starts over from the daemon's truth.
                log.warning("reconcile interrupted: %r", e)
            except Exception:
                # A reply in a shape this bridge cannot read must not stop every later pass.
                log.exception("reconcile failed; the next pass starts over")
            self.last_reconcile = time.time()
            self.save()
        for sweep in self.sweeps:
            await sweep()

    async def _reconcile_with(self, d: Daemon) -> None:
        loaded = set(await d.loaded_list())
        for thread in list(self.state.threads.values()):
            if thread.origin == "adopted":
                await self._refresh_adopted(d, thread, loaded)
            elif self.subscribed.get(thread.thread_id) != d.epoch and thread.status != "unloaded":
                await self._subscribe(d, thread)
        known = set(self.state.threads) | {s for t in self.state.threads.values() for s in t.sub_agents}
        unknown = [tid for tid in loaded if tid not in known]
        if unknown:
            await self._adopt(d, unknown)
        for thread in list(self.state.threads.values()):
            if thread.child_pid is None:
                await self.ensure_peer(thread)

    async def _subscribe(self, d: Daemon, thread: ThreadState) -> None:
        was_busy = thread.status in ("busy", "approval")
        try:
            result = await d.thread_resume(thread.thread_id)
        except DaemonError as e:
            if e.error.get("message", "").startswith("no rollout found"):
                # A thread with no turn yet has no rollout file; it becomes resumable
                # after its first turn, so the next pass tries again.
                log.debug("thread %s has no rollout yet; not subscribed", thread.thread_id)
            else:
                log.warning("thread/resume %s failed: %r", thread.thread_id, e)
            return
        self.subscribed[thread.thread_id] = d.epoch
        self._set_status(thread, _status_of(result["thread"]))
        if was_busy:
            await self._recover_turn(d, thread)

    async def _recover_turn(self, d: Daemon, thread: ThreadState) -> None:
        """What became of the turn that was running when the bridge last saw this thread:
        it may have ended while the bridge or the daemon was away, unseen."""
        turns = (await d.request("thread/turns/list", {"threadId": thread.thread_id, "limit": 1, "sortDirection": "desc"}))["data"]
        if not turns:
            return
        turn = turns[0]
        if turn["status"] == "inProgress":
            thread.active_turn_id = turn["id"]
            return
        self._recovered_turns.add(turn["id"])
        self._record_turn_end(thread, turn)

    async def _refresh_adopted(self, d: Daemon, thread: ThreadState, loaded: set[str]) -> None:
        if thread.thread_id not in loaded:
            await self._retire(thread)
            return
        try:
            result = await d.thread_read(thread.thread_id)
        except DaemonError as e:
            log.warning("thread/read %s failed: %r", thread.thread_id, e)
            return
        self._set_status(thread, _status_of(result["thread"]))

    async def _adopt(self, d: Daemon, thread_ids: list[str]) -> None:
        """Host the daemon's threads we do not know; sub-agents go under their parent, never as peers."""
        threads = []
        for thread_id in thread_ids:
            try:
                threads.append((await d.thread_read(thread_id))["thread"])
            except DaemonError as e:
                log.warning("thread/read %s failed: %r", thread_id, e)
        for thread in sorted(threads, key=lambda t: t.get("parentThreadId") is not None):
            parent_id = thread.get("parentThreadId")
            if parent_id is None:
                name = registry.unique_name(thread.get("name") or f"codex-{os.path.basename(thread['cwd'])}", self.taken_names())
                self.state.threads[thread["id"]] = ThreadState(
                    thread_id=thread["id"], name=name, cwd=thread["cwd"], origin="adopted", spawner="human",
                    read_only=False, status=_status_of(thread),
                )
                log.info("adopted thread %s as %s (source %s)", thread["id"], name, _source_label(thread.get("source")))
            elif parent_id in self.state.threads:
                self.state.threads[parent_id].sub_agents[thread["id"]] = SubAgent(
                    thread_id=thread["id"], nickname=thread.get("agentNickname"), role=thread.get("agentRole"),
                    status=_status_of(thread),
                )
            else:
                log.info("ignoring sub-agent %s of unknown thread %s", thread["id"], parent_id)

    def _check_pins(self) -> None:
        """Compare every live Claude record with the shape this bridge was written against;
        any departure makes the bridge degraded until a pass finds them all clean."""
        failures = []
        for record in self.live_records():
            try:
                failures.extend(registry.pins_ok(record))
            except FileNotFoundError:
                continue  # the session exited between the listing and the process read
        if failures == self.pin_failures:
            return
        for failure in failures:
            log.error("Claude Code peer protocol pin failed: %s", failure)
        if not failures:
            log.info("Claude Code peer protocol pins clean again")
        self.pin_failures = failures
        self._update_degraded()

    async def _retire(self, thread: ThreadState) -> None:
        await self._child_exited(thread)
        self.state.threads.pop(thread.thread_id, None)
        self.subscribed.pop(thread.thread_id, None)

    # --- peer children ------------------------------------------------------------------

    async def ensure_peer(self, thread: ThreadState) -> None:
        """Register the thread in Claude Code's registry through a child of its own, when a
        live Claude session exists to copy the record shape from and no pin has failed."""
        if thread.thread_id in self.children:
            return
        self._check_pins()
        if self.pin_failures:
            log.info("not registering %s while degraded: %s", thread.name, "; ".join(self.pin_failures))
            return
        records = [r for r in self.live_records() if {"version", "pidDomain", "messagingSocketPath"} <= r.data.keys()]
        if not records:
            log.info("thread %s: waiting for a Claude Code session to copy the peer record shape from", thread.name)
            return
        record = records[0]
        child = PeerChild(thread.thread_id, self.home / "log" / f"peer-{thread.thread_id[:8]}.log",
                          self.sessions_dir.parent, self._on_child_event)
        await child.spawn()
        self.children[thread.thread_id] = child
        try:
            ready = await child.register(
                name=thread.name, cwd=thread.cwd, version=record.data["version"], pidDomain=record.data["pidDomain"],
                socket_dir=os.path.dirname(record.socket_path), status=thread.status,
            )
        except (TimeoutError, RuntimeError, ConnectionError) as e:
            # The thread stays hosted without a peer identity; the next reconcile tries again.
            log.error("peer child for %s did not register: %r (see %s)", thread.name, e, child.log_path)
            self.children.pop(thread.thread_id, None)
            await child.close()
            return
        thread.child_pid = ready["pid"]
        self.save()
        log.info("registered %s as a Claude Code peer (pid %s, socket %s)", thread.name, ready["pid"], ready["sock"])

    async def _child_cmd(self, thread: ThreadState, **cmd) -> None:
        child = self.children.get(thread.thread_id)
        if child is None:
            return
        try:
            await child.send(**cmd)
        except (BrokenPipeError, ConnectionResetError) as e:
            # The child is gone; its exit event forgets it and reconcile respawns it.
            log.warning("peer child of %s is not reading: %r", thread.name, e)

    async def _child_idle(self, thread: ThreadState, detail: str) -> None:
        await self._child_cmd(thread, cmd="idle", detail=detail)

    async def _child_rename(self, thread: ThreadState) -> None:
        await self._child_cmd(thread, cmd="rename", name=thread.name)

    async def _child_exited(self, thread: ThreadState) -> None:
        child = self.children.pop(thread.thread_id, None)
        if child is not None:
            thread.child_pid = None
            await child.close()

    async def _on_child_event(self, thread_id: str, event: dict) -> None:
        handler = self.child_events.get(event["ev"])
        if handler is None:
            log.warning("peer child of %s sent an event this bridge does not know: %s", thread_id, event)
            return
        await handler(thread_id, event)

    async def on_child_inbound(self, thread_id: str, event: dict) -> None:
        thread = self.state.threads.get(thread_id)
        if thread is None:
            return
        # Delivering may wait on the daemon; the child's reader must not.
        self._spawn(self._relay_inbound(thread, event), "antiphon-relay-inbound")

    async def _relay_inbound(self, thread: ThreadState, event: dict) -> None:
        text = f"[from {event['from_name']} via antiphon]\n{event['text']}"
        try:
            await self._deliver_into(thread, text)
        except (IpcError, DaemonError, TransportClosed, TimeoutError) as e:
            reason = e.error.get("message", str(e)) if isinstance(e, DaemonError) else str(e)
            log.warning("message from %s to %s not delivered: %s", event["from_name"], thread.name, reason)
            await self._child_cmd(thread, cmd="deliver_failed", msg_id=event["msg_id"], reason=reason)

    async def on_child_send_failed(self, thread_id: str, event: dict) -> None:
        log.warning("peer at %s is gone: %s", event.get("to_sock"), event.get("reason"))

    async def log_child_event(self, thread_id: str, event: dict) -> None:
        log.info("peer child of %s: %s", thread_id, event)

    async def on_child_unknown_frame(self, thread_id: str, event: dict) -> None:
        """A frame shape this bridge does not speak: logged with its bytes once per action,
        so a Claude Code change shows up in the log without flooding it."""
        raw = event.get("raw")
        try:
            action = str(json.loads(raw).get("action"))
        except (ValueError, TypeError, AttributeError):
            action = "<not a JSON object>"
        if action in self._unknown_actions:
            return
        self._unknown_actions.add(action)
        log.warning("unknown frame (action %s) on the peer socket of %s: %s", action, thread_id, raw)

    async def on_child_exited(self, thread_id: str, event: dict) -> None:
        # A child the bridge closed itself was forgotten before the exit; anything
        # still listed here died on its own.
        if self.children.pop(thread_id, None) is None:
            return
        thread = self.state.threads.get(thread_id)
        if thread is not None:
            log.warning("peer child of %s exited (rc %s); it is respawned on the next reconcile", thread.name, event.get("rc"))
            thread.child_pid = None
            self.save()

    async def deliver_to_spawner(self, thread: ThreadState, text: str) -> bool:
        """Get `text` to whoever spawned `thread`: a Claude session's socket (looked up by
        session id now, since the session's pid may have changed), a Codex thread as a turn,
        or, for a human, nowhere but the log. True when it was handed over."""
        spawner = thread.spawner
        if spawner == "human":
            log.info("%s reports to a human; nothing to deliver to: %.80s", thread.name, text)
            return False
        if spawner in self.state.threads:
            try:
                await self._deliver_into(self.state.threads[spawner], text)
            except (IpcError, DaemonError, TransportClosed, TimeoutError) as e:
                log.warning("could not deliver %s's report into its spawner thread: %r", thread.name, e)
                return False
            return True
        record = registry.resolve_session(spawner, self.sessions_dir)
        if record is None:
            log.warning("spawner %s of %s is gone (no live Claude record); dropping: %.80s", spawner, thread.name, text)
            return False
        child = self.children.get(thread.thread_id)
        if child is None:
            log.warning("%s has no peer child to deliver from; dropping: %.80s", thread.name, text)
            return False
        answer = await child.deliver(record.socket_path, text, thread.name)
        if answer["ev"] != "sent":
            log.warning("delivery from %s to its spawner failed: %s", thread.name, answer["reason"])
            return False
        return True

    async def ensure_loaded(self, thread: ThreadState) -> Daemon:
        """The daemon connection, with `thread` resumed first if it was unloaded."""
        d = await self._require_daemon()
        if thread.status == "unloaded":
            result = await d.thread_resume(thread.thread_id)
            self.subscribed[thread.thread_id] = d.epoch
            self._set_status(thread, _status_of(result["thread"]))
        return d

    async def _deliver_into(self, thread: ThreadState, text: str):
        """Steer or start a turn on a hosted thread, resuming it first if it was unloaded."""
        d = await self.ensure_loaded(thread)
        effort = None if thread.effort_sent else thread.effort
        delivery = await deliver(d, thread.thread_id, text, self._sandbox_policy(thread), effort)
        if delivery.kind == "started":
            thread.effort_sent = True
        thread.active_turn_id = delivery.turn_id
        self._set_status(thread, "busy")
        self.save()
        return delivery

    # --- names ---------------------------------------------------------------------------

    def taken_names(self) -> set[str]:
        names = {t.name for t in self.state.threads.values()}
        names.update(r.name for r in self.live_records() if "name" in r.data)
        return names

    def resolve(self, target: str) -> ThreadState:
        """The hosted thread a caller means: exact name, else a unique thread-id prefix."""
        by_name = [t for t in self.state.threads.values() if t.name == target]
        if len(by_name) == 1:
            return by_name[0]
        by_prefix = [t for t in self.state.threads.values() if t.thread_id.startswith(target)]
        if len(by_prefix) == 1:
            return by_prefix[0]
        if len(by_prefix) > 1:
            names = sorted(t.name for t in by_prefix)
            raise IpcError("ambiguous", f"{target!r} matches several threads: {', '.join(names)}", names)
        stopped = self._stopped_id(target)
        if stopped is not None:
            raise IpcError("stopped", f"{target!r} was stopped; bring it back with: antiphon resume {stopped}")
        if registry.by_name(target, self.sessions_dir) is not None:
            raise IpcError("not_a_thread", f"{target!r} is a Claude Code session, not a Codex thread")
        raise IpcError("unknown_target", f"no thread named {target!r}")

    def _stopped_id(self, target: str) -> str | None:
        """The thread id a stopped thread's former name, id or id prefix refers to."""
        if target in self.state.stopped:
            return self.state.stopped[target]
        return next((tid for tid in self.state.stopped.values() if tid.startswith(target)), None)

    # --- ops ---------------------------------------------------------------------------------

    async def op_ping(self, args: dict, caller: Caller) -> dict:
        if not self.connected_once:
            # A bridge the CLI just started is still making its first connection.
            await self._wait_for_daemon()
        self._check_pins()
        claude_versions = [r.data["version"] for r in self.live_records() if "version" in r.data]
        return {
            "pid": os.getpid(),
            "daemon": self.daemon is not None,
            "codex": self.daemon.codex_version if self.daemon else None,
            "claude": claude_versions[0] if claude_versions else None,
            "peers": sum(1 for t in self.state.threads.values() if t.child_pid is not None),
            "degraded": self.state.degraded,
        }

    async def op_start(self, args: dict, caller: Caller) -> dict:
        d = await self._require_daemon()
        cwd = args["cwd"]
        wanted = args.get("name") or f"codex-{os.path.basename(cwd.rstrip('/'))}"
        name = registry.unique_name(wanted, self.taken_names())
        worktree = None
        if args["worktree"]:
            worktree = await asyncio.to_thread(self._add_worktree, cwd, name)
            cwd = worktree
        # The daemon broadcasts thread/started before thread/start's own reply chain is
        # done; holding the reconcile lock keeps that pass from adopting our own thread.
        async with self._reconcile_lock:
            try:
                result = await d.thread_start(cwd, name, args["read_only"], args.get("model"), args["review_by_parent"])
            except DaemonError as e:
                raise IpcError("daemon", f"thread/start failed: {e.error.get('message')}", e.error) from e
            thread_id = result["thread"]["id"]
            thread = ThreadState(
                thread_id=thread_id, name=name, cwd=cwd, origin="spawned", spawner=caller.owner_id,
                read_only=args["read_only"], report=args["report"], effort=args.get("effort"), worktree=worktree,
            )
            self.state.threads[thread_id] = thread
            # thread/start subscribes the connection that made it.
            self.subscribed[thread_id] = d.epoch
            self.save()
            await self.ensure_peer(thread)
        return {"name": name, "thread_id": thread_id, "cwd": cwd}

    def _add_worktree(self, cwd: str, name: str) -> str:
        top = _git(["rev-parse", "--show-toplevel"], cwd, self.rawlog)
        if top.returncode != 0:
            raise IpcError("precondition", f"--worktree needs a git repository: {top.stderr.strip()}")
        repo = top.stdout.strip()
        path = os.path.join(os.path.dirname(repo), f"{os.path.basename(repo)}-worktrees", name)
        added = _git(["worktree", "add", path, "-b", f"codex/{name}"], repo, self.rawlog)
        if added.returncode != 0:
            raise IpcError("precondition", f"git worktree add failed: {added.stderr.strip()}")
        return path

    def _remove_worktree(self, path: str) -> str | None:
        """Remove a worktree the bridge created; the reason it stayed, if git kept it."""
        if not os.path.isdir(path):
            return None
        removed = _git(["worktree", "remove", path], path, self.rawlog)
        if removed.returncode == 0:
            return None
        return removed.stderr.strip()

    def _sandbox_policy(self, thread: ThreadState) -> dict | None:
        if thread.origin != "spawned":
            return None
        return {"type": "readOnly" if thread.read_only else "workspaceWrite", "networkAccess": True}

    async def op_send(self, args: dict, caller: Caller) -> dict:
        thread = self.resolve(args["target"])
        text = args["text"]
        try:
            delivery = await self._deliver_into(thread, text)
        except DaemonError as e:
            raise IpcError("delivery_rejected", f"{e.method}: {e.error.get('message')}", e.error) from e
        return {"kind": delivery.kind, "turn_id": delivery.turn_id, "thread_id": thread.thread_id, "name": thread.name}

    async def op_interrupt(self, args: dict, caller: Caller) -> dict:
        thread = self.resolve(args["target"])
        d = await self._require_daemon()
        turn_id = thread.active_turn_id
        if turn_id is None:
            return {"noop": "idle"}
        try:
            await d.turn_interrupt(thread.thread_id, turn_id)
        except DaemonError as e:
            # The turn may have ended between our last notification and this call.
            if await d.active_turn(thread.thread_id) is None:
                return {"noop": "idle"}
            raise IpcError("daemon", f"turn/interrupt failed: {e.error.get('message')}", e.error) from e
        return {"turn_id": turn_id}

    async def op_wait(self, args: dict, caller: Caller) -> dict:
        thread = self.resolve(args["target"])
        timeout = args.get("timeout") or WAIT_DEFAULT_TIMEOUT
        if thread.status in ("busy", "approval"):
            waiter = asyncio.get_running_loop().create_future()
            self._turn_waiters.setdefault(thread.thread_id, []).append(waiter)
            try:
                status, final = await asyncio.wait_for(waiter, timeout)
            except TimeoutError as e:
                raise IpcError("timeout", f"{thread.name} is still {thread.status} after {timeout} s") from e
        else:
            status, final = thread.outcome or thread.status, thread.final
        return {"status": status, "final": final, "thread_id": thread.thread_id}

    async def op_status(self, args: dict, caller: Caller) -> dict:
        target = args.get("target")
        if not target:
            return {
                "daemon": self.daemon is not None,
                "codex": self.daemon.codex_version if self.daemon else None,
                "epoch": self.daemon.epoch if self.daemon else None,
                "last_reconcile": self.last_reconcile,
                "threads": len(self.state.threads),
                "degraded": self.state.degraded,
            }
        t = self.resolve(target)
        return {
            "name": t.name, "thread_id": t.thread_id, "cwd": t.cwd, "origin": t.origin, "spawner": t.spawner,
            "status": t.status, "active_turn_id": t.active_turn_id, "pending": self.approvals.labels(t), "last_error": t.last_error,
            "final": t.final, "outcome": t.outcome, "read_only": t.read_only, "child_pid": t.child_pid,
            "worktree": t.worktree,
            "sub_agents": [{"thread_id": s.thread_id, "nickname": s.nickname, "role": s.role, "status": s.status} for s in t.sub_agents.values()],
        }

    async def op_ls(self, args: dict, caller: Caller) -> list[dict]:
        rows = []
        child_pids = {t.child_pid for t in self.state.threads.values()}
        for record in self.live_records():
            if record.pid in child_pids:
                continue
            rows.append({
                "name": record.data.get("name"), "kind": "claude", "status": record.data.get("status"),
                "cwd": record.data.get("cwd"), "self": caller.kind == "claude" and record.data.get("sessionId") == caller.claude_session_id,
            })
        for thread in self.state.threads.values():
            rows.append({"name": thread.name, "kind": "codex", "status": self.approvals.status_label(thread), "cwd": thread.cwd,
                         "self": caller.codex_thread == thread.thread_id, "thread_id": thread.thread_id})
            for sub in thread.sub_agents.values():
                rows.append({"name": sub.nickname or sub.thread_id[:8], "kind": "codex-agent", "status": sub.status,
                             "cwd": thread.cwd, "self": False, "thread_id": sub.thread_id, "parent": thread.name,
                             "role": sub.role})
        return rows

    async def op_stop(self, args: dict, caller: Caller) -> dict:
        thread = self.resolve(args["target"])
        d = self.daemon
        if d is not None:
            if thread.active_turn_id is not None:
                try:
                    await d.turn_interrupt(thread.thread_id, thread.active_turn_id)
                except DaemonError as e:
                    log.info("interrupt on stop of %s refused (turn probably over): %r", thread.name, e)
            try:
                await d.unsubscribe(thread.thread_id)
            except DaemonError as e:
                log.info("unsubscribe on stop of %s refused: %r", thread.name, e)
        await self._retire(thread)
        self.state.stopped[thread.name] = thread.thread_id
        reply = {"name": thread.name, "thread_id": thread.thread_id}
        if thread.worktree is not None:
            reason = await asyncio.to_thread(self._remove_worktree, thread.worktree)
            if reason is not None:
                reply["worktree_kept"] = thread.worktree
                reply["worktree_reason"] = reason
        self.save()
        return reply

    async def op_resume(self, args: dict, caller: Caller) -> dict:
        target = args["target"]
        d = await self._require_daemon()
        thread_id = self._stopped_id(target) or target
        async with self._reconcile_lock:
            if thread_id in self.state.threads:
                thread = self.state.threads[thread_id]
                return {"name": thread.name, "thread_id": thread_id}
            try:
                result = await d.thread_resume(thread_id)
            except DaemonError as e:
                raise IpcError("precondition", f"thread/resume {thread_id} failed: {e.error.get('message')}", e.error) from e
            info = result["thread"]
            former = next((n for n, tid in self.state.stopped.items() if tid == thread_id), None)
            name = registry.unique_name(former or info.get("name") or f"codex-{os.path.basename(info['cwd'])}", self.taken_names())
            thread = ThreadState(
                thread_id=thread_id, name=name, cwd=info["cwd"], origin="spawned", spawner=caller.owner_id,
                read_only=(result.get("sandbox") or {}).get("type") == "readOnly", status=_status_of(info),
            )
            self.state.threads[thread_id] = thread
            self.subscribed[thread_id] = d.epoch
            if former is not None:
                del self.state.stopped[former]
            self.save()
            await self.ensure_peer(thread)
        return {"name": name, "thread_id": thread_id}

    async def op_name(self, args: dict, caller: Caller) -> dict:
        thread = self.resolve(args["target"])
        d = await self._require_daemon()
        name = registry.unique_name(args["new"], self.taken_names() - {thread.name})
        try:
            await d.set_name(thread.thread_id, name)
        except DaemonError as e:
            raise IpcError("daemon", f"thread/name/set failed: {e.error.get('message')}", e.error) from e
        thread.name = name
        await self._child_rename(thread)
        self.save()
        return {"name": name, "thread_id": thread.thread_id}

def main() -> None:
    logging.basicConfig(level=logging.INFO, stream=sys.stderr, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    bridge = Bridge(ensure_home())
    try:
        asyncio.run(bridge.run())
    except AlreadyRunning as e:
        log.info("%s", e)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
