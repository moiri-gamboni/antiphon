"""Sandbox escalations of hosted threads, answered by whoever spawned them.

A spawned thread's escalations are reviewed by Codex's automatic reviewer
inside the thread; the turn never blocks. Only its denials reach us, as
`item/autoApprovalReview/completed` notifications: each becomes a pending
record with a short token, and one message goes to the spawner with the
command and the two reply commands. `approve` records the override with the
daemon the way Codex's own TUI does and tells the thread to retry; `deny`
tells the thread why it stays denied.

A thread started with the spawner as reviewer instead gets blocking
`item/commandExecution/requestApproval` server requests; those are forwarded
the same way, answered with a decision on `approve`/`deny`, and the spawner
is reminded once when one has waited ten minutes. Waiting never cancels a
request: `deny` is the way out. A dropped connection does not lose one either:
the daemon re-sends a pending request on the new subscription, which re-keys
the record to the new id under the same token. A request is retired only once
it can no longer be answered: its asking turn ended, or its connection dropped
and its thread came back not waiting on approval.

A Claude Code hook run through `antiphon hook run` that answers `ask` is the
third kind: the shim's `hook_ask` op records it, sends the same message, and
blocks the tool call until `approve`/`deny` resolves it or the hook's own
timeout runs out, at which point the answer is deny and the record is dropped.
"""

from __future__ import annotations

import asyncio
import dataclasses
import hashlib
import json
import logging
import re
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

from antiphon.callers import Caller
from antiphon.codex.daemon import DaemonError
from antiphon.ipc import IpcError
from antiphon.state import Peer, SessionState, ThreadState

if TYPE_CHECKING:
    from antiphon.bridge import Bridge

log = logging.getLogger(__name__)

REQUEST_APPROVAL = "item/commandExecution/requestApproval"
REMIND_AFTER = 600.0
REPLY_COMMANDS = "Reply with: antiphon approve {token}   or   antiphon deny {token} -- <why>"


@dataclass
class Pending:
    token: str
    asker: str  # the hosted Codex thread, or the Claude Code session, whose action is held
    turn_id: str | None
    command: str
    cwd: str
    rationale: str | None
    risk_level: str | None
    review_completed_params: dict | None
    since: float
    resolved: bool
    kind: str  # "denied": the reviewer's denial, nothing blocks; "request": a blocking server request; "hook": a blocked hook
    request_id: int | str | None
    epoch: int | None
    # What the daemon offered on a blocking request. Nothing reads it since `deny` stopped
    # choosing between `decline` and `cancel`; it is kept only so a record round-trips
    # through the state file unchanged.
    available_decisions: list | None
    reminded: bool = False
    hook: str | None = None  # the Claude hook script that asked, for kind "hook"

    @property
    def label(self) -> str:
        return {"denied": "denied", "hook": "hook"}.get(self.kind, "approval")


def holding(peer: Peer, record: Pending) -> str:
    """What is holding the action, named for the spawner being asked: a Claude Code
    session antiphon started, or a Claude hook script running inside a Codex thread."""
    if isinstance(peer, SessionState):
        return f'the Claude Code session "{peer.name}"'
    return f'the Claude hook {record.hook} in "{peer.name}"'


def token_for(key: str) -> str:
    return hashlib.sha256(key.encode()).hexdigest()[:6]


def age(seconds: float) -> str:
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m"
    return f"{seconds // 3600}h{seconds % 3600 // 60:02d}m"


def _snake(name: str) -> str:
    # The notification spells names in camelCase (`unifiedExec`); the override method
    # wants the core event's snake_case (`unified_exec`).
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


def describe_action(action: dict) -> tuple[str, str]:
    """One line naming what the reviewer judged, and its working directory ("" when the
    action has none). Only the `command` shape is captured; the others follow the schema."""
    kind = action["type"]
    if kind == "command":
        summary = action["command"]
    elif kind == "execve":
        summary = " ".join(action["argv"])
    elif kind == "writeStdin":
        summary = f"stdin to process {action['processId']}: {action['stdin'].rstrip()}"
    elif kind == "applyPatch":
        summary = ", ".join(action["files"])
    elif kind == "networkAccess":
        summary = f"{action['protocol']} {action['target']}"
    elif kind == "mcpToolCall":
        summary = f"{action['server']}/{action['toolName']}"
    elif kind == "requestPermissions":
        summary = action.get("reason") or json.dumps(action["permissions"])
    else:
        summary = json.dumps(action)
    return summary, action.get("cwd") or ""


def guardian_event(completed: dict) -> dict:
    """The core event the override method takes, assembled from the completed-review
    notification as Codex's TUI assembles it; fields the TUI leaves empty are omitted.
    The `command` action is pinned by a capture; the other shapes get the same renaming."""
    review = completed["review"]
    action = {_snake(k): (_snake(v) if k in ("type", "source") else v) for k, v in completed["action"].items() if v is not None}
    event = {
        "id": completed["reviewId"],
        "turn_id": completed["turnId"],
        "started_at_ms": completed["startedAtMs"],
        "completed_at_ms": completed["completedAtMs"],
        "status": review["status"],
        "risk_level": review["riskLevel"],
        "user_authorization": review["userAuthorization"],
        "rationale": review["rationale"],
        "decision_source": completed["decisionSource"],
        "action": action,
    }
    return {k: v for k, v in event.items() if v is not None}


class Approvals:
    def __init__(self, bridge: Bridge):
        self.bridge = bridge
        self.clock = time.time
        self._hook_waiters: dict[str, asyncio.Future] = {}  # token -> the blocked hook_ask op

    # --- the records ------------------------------------------------------------------

    def pending(self, thread: Peer) -> list[Pending]:
        return [Pending(**p) for p in thread.pending]

    def _store(self, thread: Peer, records: list[Pending]) -> None:
        thread.pending = [dataclasses.asdict(p) for p in records]
        self.bridge.save()

    def _add(self, thread: Peer, record: Pending) -> None:
        self._store(thread, [*self.pending(thread), record])

    def _update(self, thread: Peer, record: Pending) -> None:
        self._store(thread, [record if p.token == record.token else p for p in self.pending(thread)])

    def find(self, token: str) -> tuple[Peer, Pending]:
        for thread in self.bridge.peers():
            for record in self.pending(thread):
                if record.token == token:
                    return thread, record
        raise IpcError("unknown_target", f"no pending approval with token {token!r}")

    def _unresolved(self, token: str) -> tuple[Peer, Pending]:
        thread, record = self.find(token)
        if record.resolved:
            raise IpcError("precondition", f"approval {token} was already resolved")
        return thread, record

    def _resolve(self, thread: Peer, record: Pending) -> None:
        record.resolved = True
        self._update(thread, record)

    def _drop(self, thread: Peer, record: Pending) -> None:
        self._store(thread, [p for p in self.pending(thread) if p.token != record.token])

    def labels(self, thread: Peer) -> list[str]:
        """`denied <token> <age>` / `approval <token> <age>` / `hook <token> <age>` for each
        unanswered escalation."""
        now = self.clock()
        return [f"{p.label} {p.token} {age(now - p.since)}" for p in self.pending(thread) if not p.resolved]

    def status_label(self, thread: ThreadState) -> str:
        """The thread's status word, or its oldest unanswered escalation."""
        labels = self.labels(thread)
        return labels[0] if labels else thread.status

    # --- the reviewer's denials ---------------------------------------------------------

    async def on_review_completed(self, params) -> None:
        thread = self.bridge.state.threads.get(params["threadId"])
        if params["review"]["status"] != "denied" or thread is None or thread.origin != "spawned":
            log.info("review %s on %s: %s", params["reviewId"], params["threadId"], params["review"]["status"])
            return
        command, cwd = describe_action(params["action"])
        record = Pending(
            token=token_for(params["reviewId"]), asker=thread.thread_id, turn_id=params["turnId"], command=command, cwd=cwd,
            rationale=params["review"]["rationale"], risk_level=params["review"]["riskLevel"],
            review_completed_params=params, since=self.clock(), resolved=False,
            kind="denied", request_id=None, epoch=None, available_decisions=None,
        )
        self._add(thread, record)
        log.warning("reviewer denied %r in %s (token %s)", record.command, thread.name, record.token)
        head = f'Codex\'s automatic reviewer denied an action in "{thread.name}" (token {record.token}): {record.rationale} (risk {record.risk_level})'
        self._notify(thread, record, head, "The thread has continued without it.")

    # --- the spawner as reviewer: blocking requests --------------------------------------

    async def on_server_request(self, request_id, method: str, params) -> None:
        # A few request kinds (token refresh, attestation) name no thread at all.
        thread = self.bridge.state.threads.get((params or {}).get("threadId"))
        if thread is not None and thread.origin == "adopted":
            # A human's TUI is the one to answer its own thread's requests.
            log.info("server request %s (id %s) on adopted thread %s left to its TUI", method, request_id, thread.name)
            return
        if method != REQUEST_APPROVAL or thread is None:
            # Questions to a user this thread does not have, and requests from threads we
            # do not host (Codex's own sub-agents among them), are refused rather than
            # left blocking.
            await self.bridge.refuse_server_request(request_id, method, params)
            return
        # The item id is unique for the life of the item, so it survives a daemon and
        # bridge restart; the epoch:id fallback is only for a request that carries none.
        token = token_for(params.get("itemId") or f"{self.bridge.daemon.epoch}:{request_id}")
        already = next((p for p in self.pending(thread) if p.token == token), None)
        if already is not None:
            # A pending request is re-sent whole on a new subscription after the connection
            # it arrived on dropped: same item, sometimes the same id. Re-keying the record
            # rather than opening a second one keeps `since`, so the reminder clock does not
            # restart and the spawner is not asked the same question twice. A record marked
            # resolved is re-opened: answering it only wrote to a socket, and the daemon
            # asking again is the proof that the answer never arrived. Two rows under one
            # token would be worse than either, since `find` returns only the first.
            already.request_id = request_id
            already.epoch = self.bridge.daemon.epoch
            already.resolved = False
            self._update(thread, already)
            log.info("approval request %s on %s was re-sent as id %s", token, thread.name, request_id)
            return
        record = Pending(
            token=token, asker=thread.thread_id,
            turn_id=params["turnId"], command=params["command"], cwd=params["cwd"],
            rationale=params["reason"], risk_level=None, review_completed_params=None,
            since=self.clock(), resolved=False, kind="request", request_id=request_id, epoch=self.bridge.daemon.epoch,
            available_decisions=params["availableDecisions"],
        )
        self._add(thread, record)
        log.warning("%s asks approval for %r (token %s)", thread.name, record.command, record.token)
        head = f'Codex asks to run an action in "{thread.name}" (token {record.token}): {record.rationale}'
        self._notify(thread, record, head, "The turn is blocked until you answer.")

    async def on_server_request_resolved(self, params) -> None:
        thread = self.bridge.state.threads.get(params["threadId"])
        if thread is None:
            return
        for record in self.pending(thread):
            if record.kind == "request" and record.request_id == params["requestId"] and record.epoch == self.bridge.daemon.epoch and not record.resolved:
                log.info("approval request %s on %s was answered elsewhere", record.token, thread.name)
                self._resolve(thread, record)

    # --- a Claude hook that answered ask: blocked until the spawner answers ----------------

    async def op_hook_ask(self, args: dict, caller: Caller) -> dict:
        """Block a tool call on the spawner's answer; `{"decision", "why", "token"}`.

        Both directions arrive here: a Claude Code hook script the shim runs inside a
        Codex thread, and the forward hook inside a Claude Code session antiphon started.
        The hook's timeout is the budget: unanswered by then, the answer is deny.
        """
        thread = self.bridge.peer(args["asker"])
        if thread is None:
            raise IpcError("unknown_target", f"{args['asker']} is not a thread or session antiphon hosts")
        record = Pending(
            token=token_for(f"hook:{args['asker']}:{time.time_ns()}"), asker=args["asker"], turn_id=args.get("turn_id"),
            command=args["command"], cwd=args["cwd"], rationale=args["reason"], risk_level=None, review_completed_params=None,
            since=self.clock(), resolved=False, kind="hook", request_id=None, epoch=None, available_decisions=None,
            hook=args["hook"],
        )
        self._add(thread, record)
        waiter = asyncio.get_running_loop().create_future()
        self._hook_waiters[record.token] = waiter
        log.warning("%s asks before %r (token %s)", holding(thread, record), record.command, record.token)
        if thread.spawner == "human":
            log.warning("%s has no spawner to ask; answer from a shell: antiphon approve %s", thread.name, record.token)
        head = f"Permission needed: {holding(thread, record)} asks before an action runs (token {record.token}): {record.rationale}"
        self._notify(thread, record, head, "The tool call is blocked until you answer.")
        try:
            decision, why = await asyncio.wait_for(waiter, args["timeout"])
        except TimeoutError:
            self._drop(thread, record)
            why = f"nobody answered `antiphon approve {record.token}` within {args['timeout']} s"
            log.warning("hook %s on %s: %s", record.token, thread.name, why)
            decision = "deny"
        finally:
            self._hook_waiters.pop(record.token, None)
        return {"decision": decision, "why": why, "token": record.token}

    def _settle_hook(self, thread: Peer, record: Pending, decision: str, why: str | None) -> None:
        waiter = self._hook_waiters.get(record.token)
        if waiter is None or waiter.done():
            self._drop(thread, record)
            raise IpcError("precondition", f"the hook that asked for {record.token} is no longer waiting")
        waiter.set_result((decision, why))
        self._resolve(thread, record)

    async def sweep(self) -> None:
        """After each reconcile pass: retire requests nothing will re-send, and remind
        the spawner once of a request or a blocked hook that has waited long enough."""
        d = self.bridge.daemon
        now = self.clock()
        for thread in self.bridge.peers():
            for record in self.pending(thread):
                if record.kind == "denied" or record.resolved:
                    continue
                # A request from a connection that dropped is re-sent on the new
                # subscription while its thread is still waiting on approval; the pass
                # before this sweep refreshed that status, so any other status means the
                # daemon has let the request go and no re-send is coming.
                if record.kind == "request" and d is not None and record.epoch != d.epoch and thread.status != "approval":
                    self._lost(thread, record)
                elif not record.reminded and now - record.since >= REMIND_AFTER:
                    record.reminded = True
                    self._update(thread, record)
                    waited = age(now - record.since)
                    if record.kind == "hook":
                        head = f"Still waiting: {holding(thread, record)} has blocked an action for {waited} (token {record.token}): {record.rationale}"
                        self._notify(thread, record, head, "The tool call stays blocked until you answer.")
                    else:
                        head = f'Still waiting: the action in "{thread.name}" (token {record.token}) has been blocked for {waited}: {record.rationale}'
                        self._notify(thread, record, head, "The turn stays blocked until you answer.")

    def _notify(self, thread: Peer, record: Pending, head: str, tail: str) -> None:
        cwd = f"  cwd: {record.cwd}\n" if record.cwd else ""
        text = f"{head}\n  command: {record.command}\n{cwd}{tail} {REPLY_COMMANDS.format(token=record.token)}"
        self.bridge._spawn(self.bridge.deliver_to_spawner(thread, text), "antiphon-approval-notice")

    # --- turn end ------------------------------------------------------------------------

    async def on_turn_completed(self, params) -> None:
        thread = self.bridge.state.threads.get(params["threadId"])
        if thread is None:
            return
        records = self.pending(thread)
        turn_id = params["turn"]["id"]
        # Drop resolved records, and any blocking request or hook whose asking turn just
        # ended: neither can outlive its turn, so keeping it would show a false "blocked" label.
        kept = [p for p in records if not p.resolved and not (p.kind in ("request", "hook") and p.turn_id == turn_id)]
        for record in records:
            waiter = self._hook_waiters.get(record.token) if record.kind == "hook" and record.turn_id == turn_id else None
            if waiter is not None and not waiter.done():
                waiter.set_result(("deny", "the turn ended before the hook was answered"))
        if len(kept) != len(records):
            self._store(thread, kept)

    # --- the ops ------------------------------------------------------------------------

    async def op_approve(self, args: dict, caller: Caller) -> dict:
        thread, record = self._unresolved(args["token"])
        if record.kind == "hook":
            self._settle_hook(thread, record, "allow", None)
            return self._reply(thread, record)
        if record.kind == "request":
            await self._answer(thread, record, {"decision": "accept"})
            return self._reply(thread, record)
        # The override is recorded and the thread is told to retry, but the guardian
        # re-reviews the retry: for an action it judges high-risk it denies again even
        # with the recorded re-approval (captured, guardian-retry.jsonl). So this path
        # is not a reliable way to run what the guardian refused; a thread whose
        # escalations must be approvable by the spawner should be started
        # --review-by-parent, where there is no guardian and the request is answered
        # directly. `approve` here does what it can and says exactly that to the spawner.
        d = await self.bridge.ensure_loaded(thread)
        try:
            await d.approve_guardian_denied(thread.thread_id, guardian_event(record.review_completed_params))
        except DaemonError as e:
            # The override's payload shape is pinned only for a captured `command` action;
            # when the daemon refuses it, the approval still reaches the thread as an instruction.
            log.warning("override refused for %s (%s); approving by message instead: %r", record.token, thread.name, e)
            retry = f"I approve running `{record.command}` in `{record.cwd}`: retry it now."
        else:
            retry = f"The action `{record.command}` that the reviewer denied is now approved: retry it now."
        await self._tell(thread, retry)
        self._resolve(thread, record)
        return self._reply(thread, record)

    async def op_deny(self, args: dict, caller: Caller) -> dict:
        thread, record = self._unresolved(args["token"])
        why = args["why"]
        if record.kind == "hook":
            # The hook's own deny output carries the reason to the thread; nothing to tell it.
            self._settle_hook(thread, record, "deny", why)
            return self._reply(thread, record)
        told = f"The action `{record.command}` stays denied: {why}"
        if record.kind == "request":
            # `decline` is never among the offered `availableDecisions` (accept,
            # acceptWithExecpolicyAmendment, cancel) but the daemon accepts it all the same
            # and resolves the request. Both refuse the command; `cancel` also interrupts
            # the turn, while `decline` lets it run on, which is what leaves a turn to tell
            # why the action stays denied.
            await self._answer(thread, record, {"decision": "decline"})
            await self._tell(thread, told)
        else:
            # No irreversible daemon call here, so resolve only once the thread has been told;
            # a failed delivery leaves the token live to retry.
            await self._tell(thread, told)
            self._resolve(thread, record)
        return self._reply(thread, record)

    async def _answer(self, thread: ThreadState, record: Pending, result: dict) -> None:
        d = await self.bridge._require_daemon()
        if record.epoch != d.epoch:
            # The id belongs to the connection that carried the request, which is gone.
            # The daemon re-sends the request on the new subscription, and that re-key
            # gives the same token an id to answer on; until then there is none.
            raise IpcError("precondition", f"approval {record.token} arrived on a Codex connection that dropped; "
                           "it is re-sent on the new one, so try again in a moment")
        if thread.active_turn_id != record.turn_id:
            # The asking turn is over (interrupted, say); its request id is dead, so answering
            # it would report a success that reaches nothing. Drop the stale record and refuse.
            self._store(thread, [p for p in self.pending(thread) if p.token != record.token])
            raise IpcError("precondition", f"the turn that asked for approval {record.token} has ended")
        await d.respond(record.request_id, result)
        self._resolve(thread, record)

    def _lost(self, thread: Peer, record: Pending) -> None:
        """A request whose connection dropped and whose thread has since stopped waiting on
        approval: the daemon let it go, so no re-send will give it an id to answer on. The
        record is dropped, not kept resolved, and the spawner is told the turn moved on
        without it. The thread's own turn is left alone: it is no longer blocked."""
        self._store(thread, [p for p in self.pending(thread) if p.token != record.token])
        text = (
            f'The approval request in "{thread.name}" (token {record.token}) for `{record.command}` was lost when the connection to Codex dropped, '
            "and the thread is no longer waiting on it. Send the thread a new instruction to try again."
        )
        log.warning("approval request %s lost with its connection; %s is no longer waiting on it", record.token, thread.name)
        self.bridge._spawn(self.bridge.deliver_to_spawner(thread, text), "antiphon-approval-notice")

    async def _tell(self, thread: ThreadState, text: str) -> None:
        try:
            await self.bridge._deliver_into(thread, text)
        except DaemonError as e:
            raise IpcError(
                "delivery_rejected",
                f"could not tell {thread.name} the outcome ({e.error.get('message')}); "
                f"say it yourself: antiphon send {thread.name} -- <instruction>",
                e.error,
            ) from e

    @staticmethod
    def _reply(thread: Peer, record: Pending) -> dict:
        return {"token": record.token, "kind": record.kind, "name": thread.name, "command": record.command}


def install(bridge: Bridge) -> Approvals:
    """Register the approval handlers and ops on a bridge; returns the handler object."""
    approvals = Approvals(bridge)
    for thread in bridge.peers():
        # A request's id lives on the connection it arrived on, which died with the previous
        # bridge process; the daemon re-sends it on the new subscription while the thread is
        # still waiting, and the sweep retires it when it is not. A blocked hook lived in an
        # op of that process: nobody waits on it any more, so it is forgotten. A record an
        # older bridge wrote in a shape this one cannot read goes the same way, rather than
        # stopping this bridge from starting at all.
        thread.pending = [record for record in thread.pending
                          if record["kind"] != "hook" and "asker" in record]
        for record in thread.pending:
            if record["kind"] == "request":
                record["epoch"] = None
    bridge.ops["approve"] = approvals.op_approve
    bridge.ops["deny"] = approvals.op_deny
    bridge.ops["hook_ask"] = approvals.op_hook_ask
    bridge.on_server_request = approvals.on_server_request
    bridge.notifications["item/autoApprovalReview/completed"] = approvals.on_review_completed
    bridge.notifications["serverRequest/resolved"] = approvals.on_server_request_resolved
    bridge.sweeps.append(approvals.sweep)
    turn_completed = bridge.notifications["turn/completed"]

    async def on_turn_completed(params):
        await turn_completed(params)
        await approvals.on_turn_completed(params)

    bridge.notifications["turn/completed"] = on_turn_completed
    return approvals
