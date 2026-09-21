"""The Codex-side verbs: what a Codex thread does through its own peer identity.

A hosted Codex thread has a peer child of its own, and that child is how the
thread reaches the sessions it did not spawn: a `send` to a Claude session or to
another Codex thread leaves the child's socket as an ordinary cross-session
message labelled with the thread's name, and `notify` subscribes the child to
the target's idle notice, which comes back into the thread as a turn. `name`
without a target renames the caller's own thread. `install` registers all of
it into the bridge's op and child-event tables.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from antiphon import callers
from antiphon.callers import Caller
from antiphon.claude import registry
from antiphon.codex.daemon import DaemonError
from antiphon.codex.ws import TransportClosed
from antiphon.ipc import IpcError
from antiphon.state import ThreadState

if TYPE_CHECKING:
    from antiphon.bridge import Bridge, PeerChild

log = logging.getLogger("antiphon.peers")

# Claude Code sends no receipt for a message it took (observed), so a send that hears
# nothing for this long reports success. The negative receipt's shape (`status`,
# `drop_reason`) and these status words follow Claude Code's receipt handling as read
# at design time; no negative receipt from a real session has been captured yet.
RECEIPT_WAIT = 2.0
NEGATIVE_RECEIPTS = frozenset({"held", "dropped", "refused", "expired", "denied"})
NO_PEER_YET = "has no peer identity yet (a Claude Code session must be running for the bridge to register peers)"


def install(bridge: Bridge) -> None:
    side = CodexSide(bridge)
    bridge.ops["send"] = side.op_send
    bridge.ops["notify"] = side.op_notify
    bridge.ops["name"] = side.op_name
    bridge.child_events["status"] = side.on_child_status
    bridge.child_events["idle_notice"] = side.on_child_idle_notice


class CodexSide:
    def __init__(self, bridge: Bridge):
        self.bridge = bridge
        self.receipts: dict[str, asyncio.Future] = {}  # msg_id -> the status event a send is waiting for
        self.watched: dict[tuple[str, str], str] = {}  # (thread id, peer socket) -> the peer's name when notify ran

    # --- ops ---------------------------------------------------------------------------

    async def op_send(self, args: dict, caller: Caller) -> dict:
        target = args["target"]
        record = self._claude_session(target)
        if caller.kind == "claude" and record is not None:
            raise IpcError("usage", f"{target!r} is a Claude Code session: use SendMessage")
        if caller.kind != "codex":
            return await self.bridge.op_send(args, caller)
        if record is not None:
            return await self._labelled_send(self._own_thread(caller), record.name, record.socket_path, args["text"])
        thread = self.bridge.resolve(target)
        if thread.spawner == caller.owner_id:
            return await self.bridge.op_send(args, caller)
        own = self._own_thread(caller)
        to_sock = self._child_socket(thread)
        if to_sock is None:
            # The target has no peer identity to receive a frame on (no Claude Code session
            # to copy the record shape from), so the labelled text goes straight into its turn.
            return await self.bridge.op_send({**args, "text": f"[from {own.name} via antiphon]\n{args['text']}"}, caller)
        return await self._labelled_send(own, thread.name, to_sock, args["text"])

    async def op_notify(self, args: dict, caller: Caller) -> dict:
        if caller.kind == "claude":
            raise IpcError("usage", "a Claude Code session is told when a peer goes idle by SendMessage with notify_when_idle")
        if caller.kind != "codex":
            raise IpcError("usage", "notify runs inside a Codex thread; there is no session here to notify")
        own = self._own_thread(caller)
        name, to_sock = self._peer_socket(args["target"])
        child = self._child_of(own, f"cannot watch {name}")
        await child.send(cmd="subscribe", to_sock=to_sock)
        self.watched[(own.thread_id, to_sock)] = name
        return {"name": name}

    async def op_name(self, args: dict, caller: Caller) -> dict:
        target = args.get("target")
        if target is not None:
            thread = self.bridge.resolve(target)
        elif caller.kind == "codex":
            thread = self._own_thread(caller)
        else:
            raise IpcError("usage", "name needs a target: antiphon name <thread> <new>")
        spawner = None if thread.thread_id == caller.codex_thread else thread.spawner
        if not callers.permits(caller, "name", spawner):
            raise IpcError("forbidden", callers.forbidden_message(caller, "name", spawner))
        former = thread.name
        return {**await self.bridge.op_name({**args, "target": thread.thread_id}, caller), "former": former}

    # --- child events -------------------------------------------------------------------

    async def on_child_status(self, thread_id: str, event: dict) -> None:
        waiting = self.receipts.get(event.get("orig_msg_id"))
        if waiting is None or waiting.done():
            log.info("receipt for a message nobody is waiting on, from the peer child of %s: %s", thread_id, event)
            return
        waiting.set_result(event)

    async def on_child_idle_notice(self, thread_id: str, event: dict) -> None:
        thread = self.bridge.state.threads.get(thread_id)
        if thread is None:
            return
        from_sock = event["from_sock"]
        name = self.watched.pop((thread_id, from_sock), None) or self._peer_name(from_sock)
        detail = event.get("detail") or ""
        if event.get("state") == "idle":
            text = f"Peer {name} is idle: {detail}"
        else:
            text = f"Peer {name} has exited: {detail}"
        # Starting a turn waits on the daemon; the child's reader must not.
        self.bridge._spawn(self._relay_notice(thread, name, text), "antiphon-idle-notice")

    async def _relay_notice(self, thread: ThreadState, name: str, text: str) -> None:
        try:
            await self.bridge._deliver_into(thread, text)
        except (IpcError, DaemonError, TransportClosed, TimeoutError) as e:
            # The notice has no sender to bounce to; the log is where it lands.
            log.warning("idle notice from %s not delivered into %s: %r", name, thread.name, e)

    # --- helpers --------------------------------------------------------------------------

    async def _labelled_send(self, own: ThreadState, name: str, to_sock: str, text: str) -> dict:
        child = self._child_of(own, f"cannot message {name}")
        answer = await child.deliver(to_sock, text, own.name)
        if answer["ev"] != "sent":
            raise IpcError("delivery_rejected", f"{name}: {answer['reason']}", answer)
        msg_id = answer["msg_id"]
        receipt = asyncio.get_running_loop().create_future()
        self.receipts[msg_id] = receipt
        try:
            status = await asyncio.wait_for(receipt, RECEIPT_WAIT)
        except TimeoutError:
            return {"kind": "sent", "name": name, "msg_id": msg_id}
        finally:
            self.receipts.pop(msg_id, None)
        if status["status"] in NEGATIVE_RECEIPTS:
            raise IpcError("delivery_rejected", f"{name} {status['status']}: {status.get('detail') or 'no reason given'}", status)
        return {"kind": "sent", "name": name, "msg_id": msg_id}

    def _own_thread(self, caller: Caller) -> ThreadState:
        thread = self.bridge.state.threads.get(caller.codex_thread or "")
        if thread is None:
            raise IpcError("precondition", "this runs from a Codex thread antiphon does not host "
                           "(CODEX_THREAD_ID is unset, or names a thread the bridge does not know)")
        return thread

    def _child_of(self, thread: ThreadState, doing: str) -> PeerChild:
        child = self.bridge.children.get(thread.thread_id)
        if child is None:
            raise IpcError("precondition", f"{doing}: {thread.name} {NO_PEER_YET}")
        return child

    def _peer_socket(self, target: str) -> tuple[str, str]:
        """The name and socket of the peer a target names: a live Claude session, or a hosted thread's child."""
        record = self._claude_session(target)
        if record is not None:
            return record.name, record.socket_path
        thread = self.bridge.resolve(target)
        to_sock = self._child_socket(thread)
        if to_sock is None:
            raise IpcError("precondition", f"{thread.name} {NO_PEER_YET}")
        return thread.name, to_sock

    def _claude_session(self, name: str) -> registry.Record | None:
        """The live Claude Code session of that name; the peer children carry thread names and are not sessions."""
        record = registry.by_name(name, self.bridge.sessions_dir)
        if record is None or record.pid in {t.child_pid for t in self.bridge.state.threads.values()}:
            return None
        return record

    def _child_socket(self, thread: ThreadState) -> str | None:
        for record in self.bridge.live_records():
            if record.pid == thread.child_pid and "messagingSocketPath" in record.data:
                return record.socket_path
        return None

    def _peer_name(self, sock: str) -> str:
        for record in self.bridge.live_records():
            if record.data.get("messagingSocketPath") == sock and "name" in record.data:
                return record.name
        return sock
