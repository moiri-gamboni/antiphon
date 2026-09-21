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
request: `deny` is the way out. Only a request whose connection to the daemon
dropped is retired, since it can no longer be answered.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import logging
import re
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

from antiphon.callers import Caller
from antiphon.codex.daemon import Daemon, DaemonError
from antiphon.codex.ws import TransportClosed
from antiphon.ipc import IpcError
from antiphon.state import ThreadState

if TYPE_CHECKING:
    from antiphon.bridge import Bridge

log = logging.getLogger("antiphon.approvals")

REQUEST_APPROVAL = "item/commandExecution/requestApproval"
REMIND_AFTER = 600.0
REPLY_COMMANDS = "Reply with: antiphon approve {token}   or   antiphon deny {token} -- <why>"


@dataclass
class Pending:
    token: str
    thread_id: str
    turn_id: str | None
    command: str
    cwd: str
    rationale: str | None
    risk_level: str | None
    review_completed_params: dict | None
    since: float
    resolved: bool
    kind: str  # "denied": the reviewer's denial, nothing blocks; "request": a blocking server request
    request_id: int | str | None
    epoch: int | None
    available_decisions: list | None
    reminded: bool = False

    @property
    def label(self) -> str:
        return "denied" if self.kind == "denied" else "approval"


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

    # --- the records ------------------------------------------------------------------

    def pending(self, thread: ThreadState) -> list[Pending]:
        return [Pending(**p) for p in thread.pending]

    def _store(self, thread: ThreadState, records: list[Pending]) -> None:
        thread.pending = [dataclasses.asdict(p) for p in records]
        self.bridge.save()

    def _add(self, thread: ThreadState, record: Pending) -> None:
        self._store(thread, [*self.pending(thread), record])

    def _update(self, thread: ThreadState, record: Pending) -> None:
        self._store(thread, [record if p.token == record.token else p for p in self.pending(thread)])

    def find(self, token: str) -> tuple[ThreadState, Pending]:
        for thread in self.bridge.state.threads.values():
            for record in self.pending(thread):
                if record.token == token:
                    return thread, record
        raise IpcError("unknown_target", f"no pending approval with token {token!r}")

    def _unresolved(self, token: str) -> tuple[ThreadState, Pending]:
        thread, record = self.find(token)
        if record.resolved:
            raise IpcError("precondition", f"approval {token} was already resolved")
        return thread, record

    def _resolve(self, thread: ThreadState, record: Pending) -> None:
        record.resolved = True
        self._update(thread, record)

    def labels(self, thread: ThreadState) -> list[str]:
        """`denied <token> <age>` / `approval <token> <age>` for each unanswered escalation."""
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
            token=token_for(params["reviewId"]), thread_id=thread.thread_id, turn_id=params["turnId"], command=command, cwd=cwd,
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
        record = Pending(
            # The item id is unique for the life of the item, so it survives a daemon and
            # bridge restart; the epoch:id fallback is only for a request that carries none.
            token=token_for(params.get("itemId") or f"{self.bridge.daemon.epoch}:{request_id}"), thread_id=thread.thread_id,
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

    async def sweep(self) -> None:
        """After each reconcile pass: retire requests whose connection is gone, and remind
        the spawner once of a request that has waited long enough."""
        d = self.bridge.daemon
        now = self.clock()
        for thread in list(self.bridge.state.threads.values()):
            for record in self.pending(thread):
                if record.kind != "request" or record.resolved:
                    continue
                if d is not None and record.epoch != d.epoch:
                    await self._lost(thread, record, d)
                elif not record.reminded and now - record.since >= REMIND_AFTER:
                    record.reminded = True
                    self._update(thread, record)
                    head = f'Still waiting: the action in "{thread.name}" (token {record.token}) has been blocked for {age(now - record.since)}: {record.rationale}'
                    self._notify(thread, record, head, "The turn stays blocked until you answer.")

    def _notify(self, thread: ThreadState, record: Pending, head: str, tail: str) -> None:
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
        # Drop resolved records, and any blocking request whose asking turn just ended: a
        # request cannot outlive its turn, so keeping it would show a false "blocked" label.
        kept = [p for p in records if not p.resolved and not (p.kind == "request" and p.turn_id == turn_id)]
        if len(kept) != len(records):
            self._store(thread, kept)

    # --- the ops ------------------------------------------------------------------------

    async def op_approve(self, args: dict, caller: Caller) -> dict:
        thread, record = self._unresolved(args["token"])
        if record.kind == "request":
            await self._answer(thread, record, {"decision": "accept"})
            return self._reply(thread, record)
        d = await self.bridge.ensure_loaded(thread)
        try:
            await d.approve_guardian_denied(thread.thread_id, guardian_event(record.review_completed_params))
        except DaemonError as e:
            # The override's payload shape is pinned only for a captured `command` action;
            # when the daemon refuses it, the approval still reaches the thread as an
            # instruction the reviewer sees on the retry.
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
        told = f"The action `{record.command}` stays denied: {why}"
        if record.kind == "request":
            # Whether the daemon accepts `decline` is not captured; the captured requests
            # offer only accept, acceptWithExecpolicyAmendment and cancel.
            decision = "decline" if "decline" in record.available_decisions else "cancel"
            await self._answer(thread, record, {"decision": decision})
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
            await self._lost(thread, record, d)
            raise IpcError("precondition", f"approval {record.token} was lost with the Codex connection")
        if thread.active_turn_id != record.turn_id:
            # The asking turn is over (interrupted, say); its request id is dead, so answering
            # it would report a success that reaches nothing. Drop the stale record and refuse.
            self._store(thread, [p for p in self.pending(thread) if p.token != record.token])
            raise IpcError("precondition", f"the turn that asked for approval {record.token} has ended")
        await d.respond(record.request_id, result)
        self._resolve(thread, record)

    async def _lost(self, thread: ThreadState, record: Pending, d: Daemon) -> None:
        """A request from a connection that is gone cannot be answered on this one. What
        the daemon did with it is not captured, so a turn still waiting on it is ended
        rather than left blocked forever, and the spawner is told either way. The record is
        dropped, not kept resolved: nothing about a lost request can be answered later."""
        self._store(thread, [p for p in self.pending(thread) if p.token != record.token])
        interrupted = False
        if thread.active_turn_id == record.turn_id:
            try:
                await d.turn_interrupt(thread.thread_id, record.turn_id)
                interrupted = True
            except (DaemonError, TransportClosed, TimeoutError) as e:
                # Best-effort: the turn may be over, or the fresh connection may flap again;
                # either way the record is already resolved and the loop must not die here.
                log.info("interrupt after a lost approval on %s refused (turn probably over): %r", thread.name, e)
        outcome = "the turn was interrupted" if interrupted else "that turn is over"
        text = (
            f'The approval request in "{thread.name}" (token {record.token}) for `{record.command}` was lost when the connection to Codex dropped; '
            f"{outcome}. Send the thread a new instruction to try again."
        )
        log.warning("approval request %s on %s lost with its connection; %s", record.token, thread.name, outcome)
        self.bridge._spawn(self.bridge.deliver_to_spawner(thread, text), "antiphon-approval-notice")

    async def _tell(self, thread: ThreadState, text: str) -> None:
        try:
            await self.bridge._deliver_into(thread, text)
        except DaemonError as e:
            raise IpcError(
                "delivery_rejected",
                f"could not tell {thread.name} the outcome ({e.error.get('message')}); the token is still open — "
                f"retry, or say it yourself: antiphon send {thread.name} -- <instruction>",
                e.error,
            ) from e

    @staticmethod
    def _reply(thread: ThreadState, record: Pending) -> dict:
        return {"token": record.token, "kind": record.kind, "name": thread.name, "thread_id": thread.thread_id, "command": record.command}


def install(bridge: Bridge) -> Approvals:
    """Register the approval handlers and ops on a bridge; returns the handler object."""
    approvals = Approvals(bridge)
    for thread in bridge.state.threads.values():
        # A request lives on the connection it arrived on, which died with the previous
        # bridge process; the sweep retires these once the daemon is back.
        for record in thread.pending:
            if record["kind"] == "request":
                record["epoch"] = None
    bridge.ops["approve"] = approvals.op_approve
    bridge.ops["deny"] = approvals.op_deny
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
