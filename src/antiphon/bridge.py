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
from pathlib import Path

from antiphon import callers, ipc
from antiphon.callers import Caller
from antiphon.claude import registry
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
WAIT_DEFAULT_TIMEOUT = 600.0
DAEMON_WAIT = 3.0
IDLE_DETAIL_CHARS = 200


class AlreadyRunning(Exception):
    """Another bridge answers on the control socket."""


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
        self.subscribed: dict[str, int] = {}  # thread id -> the epoch it was resumed on
        self.pin_failures: list[str] = []
        self.codex_failures: dict[str, str] = {}
        self._turn_waiters: dict[str, list[asyncio.Future]] = {}
        self.last_reconcile: float | None = None
        self._reconcile_lock = asyncio.Lock()
        self._tasks: set[asyncio.Task] = set()
        self._server: asyncio.AbstractServer | None = None
        self._closing = False
        self.on_server_request = self.refuse_server_request
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
            "item/autoApprovalReview/started": self.log_notification,
            "item/autoApprovalReview/completed": self.log_notification,
        }

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
        handler = self.ops.get(op)
        if handler is None:
            raise IpcError("unknown_op", f"unknown op {op!r}")
        try:
            return await handler(args, caller)
        except KeyError as e:
            raise IpcError("usage", f"{op} needs argument {e.args[0]!r}") from e

    def live_records(self) -> list[registry.Record]:
        return registry.live_records(self.sessions_dir)

    # --- daemon connection ----------------------------------------------------------

    async def _require_daemon(self) -> Daemon:
        """The live daemon connection, waiting briefly for the one a fresh bridge is still making."""
        deadline = time.monotonic() + DAEMON_WAIT
        while self.daemon is None and time.monotonic() < deadline:
            await asyncio.sleep(0.05)
        if self.daemon is None:
            raise IpcError("daemon_unreachable", "the Codex daemon is not connected; the bridge is reconnecting")
        return self.daemon

    async def _connect_loop(self) -> None:
        delay = self.reconnect_backoff[0]
        while not self._closing:
            try:
                path = await asyncio.to_thread(self._ensure_running, self.codex_home, self.rawlog)
                d = await Daemon.connect(path, self._on_notification, self._on_server_request, rawlog=self.rawlog)
            except DaemonError as e:
                # The daemon answered `initialize` with an error: it speaks a protocol we do not.
                self.codex_failures["initialize"] = "codex protocol: initialize unsupported"
                self._update_degraded()
                log.error("initialize refused: %r; retrying in %s s", e, delay)
            except (DaemonUnavailable, OSError, TransportClosed, TimeoutError) as e:
                # A restarting or absent daemon: keep trying, with the CLI reporting exit 5 meanwhile.
                log.warning("daemon unreachable: %r; retrying in %s s", e, delay)
            else:
                delay = self.reconnect_backoff[0]
                self.codex_failures.pop("initialize", None)
                self._watch_pinned_methods(d)
                self.daemon = d
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

    async def log_notification(self, params) -> None:
        log.info("notification: %s", json.dumps(params)[:1000])

    async def on_thread_started(self, params) -> None:
        thread = params["thread"]
        if thread["id"] in self.state.threads or self._sub_agent(thread["id"]) is not None:
            return
        self._spawn(self.reconcile(), "antiphon-reconcile-on-thread-started")

    async def on_thread_status_changed(self, params) -> None:
        status = _status_of(params)
        thread = self.state.threads.get(params["threadId"])
        if thread is not None:
            self._set_status(thread, status)
        else:
            sub = self._sub_agent(params["threadId"])
            if sub is not None:
                sub.status = status
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
        status, final = _outcome(params["turn"])
        thread.outcome = status
        thread.final = final
        thread.active_turn_id = None
        if status != "completed":
            thread.last_error = {"message": (params["turn"].get("error") or {}).get("message", ""), "at": time.time()}
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
        thread.status = status

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
            d = self.daemon
            if d is None:
                return
            try:
                await self._reconcile_with(d)
            except (TransportClosed, DaemonError) as e:
                # The connection went away mid-pass or the daemon refused a call; the
                # next pass (or the reconnect) starts over from the daemon's truth.
                log.warning("reconcile interrupted: %r", e)
            self.last_reconcile = time.time()
            self.save()

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

    async def _subscribe(self, d: Daemon, thread: ThreadState) -> None:
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

    async def _retire(self, thread: ThreadState) -> None:
        await self._child_exited(thread)
        self.state.threads.pop(thread.thread_id, None)
        self.subscribed.pop(thread.thread_id, None)

    # --- peer children (wired by the peer module) ------------------------------------------

    async def _child_idle(self, thread: ThreadState, detail: str) -> None:
        pass

    async def _child_rename(self, thread: ThreadState) -> None:
        pass

    async def _child_exited(self, thread: ThreadState) -> None:
        pass

    async def deliver_to_spawner(self, thread: ThreadState, text: str) -> bool:
        return False

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
        stopped = self.state.stopped.get(target) or next((tid for tid in self.state.stopped.values() if tid.startswith(target)), None)
        if stopped is not None:
            raise IpcError("stopped", f"{target!r} was stopped; bring it back with: antiphon resume {stopped}")
        if registry.by_name(target, self.sessions_dir) is not None:
            raise IpcError("not_a_thread", f"{target!r} is a Claude Code session, not a Codex thread")
        raise IpcError("unknown_target", f"no thread named {target!r}")

    # --- ops ---------------------------------------------------------------------------------

    async def op_ping(self, args: dict, caller: Caller) -> dict:
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
        try:
            result = await d.thread_start(cwd, name, args["read_only"], args.get("model"), args.get("effort"), args["review_by_parent"])
        except DaemonError as e:
            raise IpcError("daemon", f"thread/start failed: {e.error.get('message')}", e.error) from e
        thread_id = result["thread"]["id"]
        thread = ThreadState(
            thread_id=thread_id, name=name, cwd=cwd, origin="spawned", spawner=caller.owner_id,
            read_only=args["read_only"], report=args["report"], model=args.get("model"), effort=args.get("effort"),
            review_by_parent=args["review_by_parent"], worktree=worktree,
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
        d = await self._require_daemon()
        try:
            if thread.status == "unloaded":
                await d.thread_resume(thread.thread_id)
                self.subscribed[thread.thread_id] = d.epoch
            delivery = await deliver(d, thread.thread_id, text, self._sandbox_policy(thread))
        except DaemonError as e:
            raise IpcError("delivery_rejected", f"{e.method}: {e.error.get('message')}", e.error) from e
        thread.active_turn_id = delivery.turn_id
        self._set_status(thread, "busy")
        self.save()
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
            "status": t.status, "active_turn_id": t.active_turn_id, "pending": t.pending, "last_error": t.last_error,
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
            rows.append({"name": thread.name, "kind": "codex", "status": thread.status, "cwd": thread.cwd,
                         "self": caller.codex_thread == thread.thread_id, "thread_id": thread.thread_id})
            for sub in thread.sub_agents.values():
                rows.append({"name": sub.nickname or sub.thread_id[:8], "kind": "codex-agent", "status": sub.status,
                             "cwd": thread.cwd, "self": False, "thread_id": sub.thread_id, "parent": thread.name})
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
        thread_id = self.state.stopped.get(target) or next((tid for tid in self.state.stopped.values() if tid.startswith(target)), target)
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

    async def ensure_peer(self, thread: ThreadState) -> None:
        pass


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
