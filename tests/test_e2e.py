"""End to end: a Claude Code session (fake) and a Codex thread (fake daemon) through the
real bridge and the real peer child, in a temporary home."""

import asyncio
import copy
import json
import os
import uuid
from pathlib import Path

import pytest

from antiphon import cli
from antiphon.bridge import Bridge
from antiphon.callers import Caller
from fake_claude import FakeClaude, captured_frames
from fake_daemon import FakeDaemon, load_fixture

THREAD_START = load_fixture("thread-start.jsonl")
APPROVAL = load_fixture("user-reviewer-request-approval.jsonl")
TURNS_LIST = load_fixture("turns-list.jsonl")
RESTART = load_fixture("daemon-restart.jsonl")
HOOK_ORDER = load_fixture("permission-hook-order.jsonl")

THREAD_ID = THREAD_START.result(2)["thread"]["id"]
TURN_STARTED = APPROVAL.result(3)
COMPLETED_TURN = TURNS_LIST.result(3)["data"][0]
COMPLETED_NOTICE = [n for n in HOOK_ORDER.notifications("turn/completed") if n["turn"]["status"] == "completed"][0]
FAILED_TURN = [n for n in HOOK_ORDER.notifications("turn/completed") if n["turn"]["status"] == "failed"][0]
CAPTURED_INBOUND = [f for f in captured_frames("peer-frames.jsonl", "frame") if f.get("type") == "user"][0]
CAPTURED_NOTIFY = [f for f in captured_frames("peer-frames.jsonl", "frame") if f.get("action") == "notify_when_idle"][0]
INBOUND_TEXT = "Spike zero round two: receipt and idle-notice test, please ignore."


def run(coro):
    return asyncio.run(coro)


async def until(condition, timeout: float = 5):
    async def poll():
        while not condition():
            await asyncio.sleep(0.01)

    await asyncio.wait_for(poll(), timeout)


def for_thread(params: dict, thread_id: str = THREAD_ID) -> dict:
    params = copy.deepcopy(params)
    params["threadId"] = thread_id
    return params


class Rig:
    """Bridge + fake daemon + one fake Claude session whose record is live (it is this process)."""

    def __init__(self, short_tmp: Path):
        self.tmp = short_tmp
        self.home = short_tmp / "antiphon"
        self.config_dir = short_tmp / "cc"
        self.sessions_dir = self.config_dir / "sessions"
        self.sock_dir = short_tmp / "socks"
        self.codex_home = short_tmp / "codex"
        self.daemon_sock = self.codex_home / "app-server-control" / "app-server-control.sock"
        self.daemon_sock.parent.mkdir(parents=True)
        self.fake = FakeDaemon(self.daemon_sock)
        self.fake.replies.update({
            "thread/start": {"result": THREAD_START.result(2)},
            "thread/name/set": {"result": {}},
            "thread/turns/list": {"result": {"data": [COMPLETED_TURN], "nextCursor": None, "backwardsCursor": None}},
            "turn/start": {"result": TURN_STARTED},
            "thread/loaded/list": {"result": {"data": [], "nextCursor": None}},
            "thread/resume": {"result": RESTART.result(6)},
            "thread/unsubscribe": {"result": {}},
        })
        self.claude = FakeClaude(self.sessions_dir, self.sock_dir, name="claude-main")
        self.caller = Caller(kind="claude", claude_pid=self.claude.pid, claude_session_id=self.claude.record["sessionId"], codex_thread=None)
        self.bridge = Bridge(self.home, sessions_dir=self.sessions_dir, codex_home=self.codex_home,
                             ensure_running=lambda codex_home, rawlog=None: str(self.daemon_sock))
        self.bridge.reconnect_backoff = (0.05, 0.2)
        self.bridge.reconcile_interval = 3600

    async def __aenter__(self):
        await self.fake.start()
        await self.bridge.start()
        await until(lambda: self.bridge.last_reconcile is not None)
        return self

    async def __aexit__(self, *exc):
        await self.bridge.close()
        await self.fake.stop()
        self.claude.close()

    async def start_thread(self, name="helper", **overrides) -> dict:
        args = dict(cwd=str(self.tmp / "work"), name=name, read_only=False, report=True, worktree=False, review_by_parent=False)
        args.update(overrides)
        result = await self.bridge.dispatch("start", args, self.caller)
        await until(lambda: self.bridge.state.threads[THREAD_ID].child_pid is not None)
        return result

    def child_sock(self) -> str:
        return str(self.sock_dir / f"{self.bridge.state.threads[THREAD_ID].child_pid}.sock")

    async def frames(self, count: int, timeout: float = 2.0) -> list[dict]:
        return await asyncio.to_thread(self.claude.wait_for_frames, count, timeout)

    def send_to_child(self, frame: dict) -> dict:
        return self.claude.replay(frame, self.child_sock())


def test_a_message_from_claude_starts_a_turn_with_the_prefixed_text_and_no_receipt(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            await rig.start_thread()
            rig.send_to_child(CAPTURED_INBOUND)
            turn_start = await asyncio.wait_for(rig.fake.wait_request("turn/start"), 5)
            frames = await rig.frames(1, timeout=2.0)
            return turn_start["params"], frames, rig.bridge.state.threads[THREAD_ID].status

    params, frames, status = run(body())
    assert params["input"] == [{"type": "text", "text": f"[from claude-main via antiphon]\n{INBOUND_TEXT}"}]
    assert params["sandboxPolicy"] == {"type": "workspaceWrite", "networkAccess": True}
    assert frames == []
    assert status == "busy"


def test_a_message_relayed_while_the_daemon_connection_is_dead_yields_one_dropped_receipt(short_tmp):
    from antiphon.codex.daemon import Daemon

    async def noop(*args):
        pass

    async def body():
        async with Rig(short_tmp) as rig:
            await rig.start_thread()
            # Stop the connect loop, then give the bridge a daemon whose connection has
            # dropped: the relay's first daemon call finds the transport gone, which must
            # reach the child as one dropped receipt, not vanish with the fire-and-forget task.
            rig.bridge._closing = True
            await rig.fake.drop()
            await until(lambda: rig.bridge.daemon is None)
            dead = await Daemon.connect(str(rig.daemon_sock), noop, noop)
            await rig.fake.drop()
            await asyncio.wait_for(dead.closed.wait(), 2)
            rig.bridge.daemon = dead
            rig.bridge.subscribed[THREAD_ID] = dead.epoch
            sent = rig.send_to_child(CAPTURED_INBOUND)
            frames = await rig.frames(1, timeout=5.0)
            more = await rig.frames(2, timeout=0.3)
            return sent, frames, more

    sent, frames, more = run(body())
    assert len(frames) == 1
    receipt = frames[0]
    assert receipt["action"] == "peer_message_status"
    assert receipt["status"] == "dropped"
    assert receipt["orig_msg_id"] == sent["msg_id"]
    assert more == frames


def test_a_rejected_turn_start_sends_exactly_one_dropped_receipt(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            rig.fake.replies["turn/start"] = {"error": {"code": -32600, "message": "direct app-server input is not allowed for multi-agent v2 sub-agents"}}
            await rig.start_thread()
            sent = rig.send_to_child(CAPTURED_INBOUND)
            frames = await rig.frames(1, timeout=5.0)
            more = await rig.frames(2, timeout=0.5)
            return sent, frames, more

    sent, frames, more = run(body())
    assert len(frames) == 1
    receipt = frames[0]
    assert receipt["action"] == "peer_message_status"
    assert receipt["status"] == "dropped"
    assert receipt["orig_msg_id"] == sent["msg_id"]
    assert "not allowed" in receipt["drop_reason"]
    assert more == frames


def test_turn_completed_reports_the_final_answer_to_the_spawner_then_the_idle_notice(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            await rig.start_thread()
            rig.send_to_child(CAPTURED_NOTIFY)
            await asyncio.wait_for(rig.fake.wait_request("thread/name/set"), 5)
            await rig.bridge.dispatch("send", {"target": "helper", "text": "go"}, rig.caller)
            await rig.fake.notify("turn/completed", for_thread(COMPLETED_NOTICE))
            frames = await rig.frames(2, timeout=5.0)
            return frames, rig.child_sock()

    frames, child_sock = run(body())
    assert [f.get("type") for f in frames] == ["user", "control"]
    report, notice = frames
    assert report["from"] == f"uds:{child_sock}"
    assert report["message"]["content"] == (
        f'<cross-session-message from="uds:{child_sock}" from-name="helper" from-mode="prompting">\nDONE\n</cross-session-message>'
    )
    assert notice["action"] == "peer_idle_notice"
    assert notice["state"] == "idle"
    assert notice["detail"] == "DONE"
    assert notice["orig_msg_id"] == CAPTURED_NOTIFY["msg_id"]


def test_a_final_answer_over_64_kib_reaches_the_spawner_whole(short_tmp):
    big = "A" * 100_000  # past the 64 KiB default line limit of the IPC and child pipes

    async def body():
        async with Rig(short_tmp) as rig:
            await rig.start_thread()
            await rig.bridge.dispatch("send", {"target": "helper", "text": "go"}, rig.caller)
            notice = for_thread(COMPLETED_NOTICE)
            for item in notice["turn"]["items"]:
                if item.get("type") == "agentMessage":
                    item["text"] = big
            await rig.fake.notify("turn/completed", notice)
            [report] = await rig.frames(1, timeout=5.0)
            return report

    report = run(body())
    assert report["type"] == "user"
    assert big in report["message"]["content"]


def test_a_failed_turn_reports_the_failure_and_no_report_skips_the_message(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            await rig.start_thread(report=False)
            rig.send_to_child(CAPTURED_NOTIFY)
            await asyncio.sleep(0.2)
            await rig.bridge.dispatch("send", {"target": "helper", "text": "go"}, rig.caller)
            await rig.fake.notify("turn/completed", for_thread(FAILED_TURN))
            return await rig.frames(1, timeout=5.0), await rig.frames(2, timeout=0.5)

    frames, more = run(body())
    [notice] = frames
    assert notice["action"] == "peer_idle_notice"
    assert notice["detail"].startswith("failed: You’ve hit your usage limit")
    assert more == frames


# --- approvals: the escalation reaches Claude, the CLI answers it ---------------------

DENIED = load_fixture("auto-review-denied.jsonl")
REVIEW_STARTED = DENIED.notifications("item/autoApprovalReview/started")[0]
REVIEW_DENIED = DENIED.notifications("item/autoApprovalReview/completed")[0]
OVERRIDE_EVENT = [m for m in load_fixture("guardian-override.jsonl").sent if m.get("method") == "thread/approveGuardianDeniedAction"][0]["params"]["event"]
REQUEST_PARAMS = APPROVAL.server_requests("item/commandExecution/requestApproval")[0]["params"]
DENIED_COMMAND = REVIEW_DENIED["action"]["command"]


async def cli_in_thread(rig: Rig, monkeypatch, *argv: str) -> int:
    """The real CLI against the rig's in-process bridge, from a thread so the bridge's loop keeps serving."""
    monkeypatch.setenv("ANTIPHON_HOME", str(rig.home))
    return await asyncio.to_thread(cli.main, list(argv))


def test_a_denied_review_reaches_claude_and_cli_approve_records_the_override_then_the_retry(short_tmp, monkeypatch):
    async def body():
        async with Rig(short_tmp) as rig:
            rig.fake.replies["thread/approveGuardianDeniedAction"] = {"result": {}}
            await rig.start_thread()
            await rig.fake.notify("item/autoApprovalReview/started", for_thread(REVIEW_STARTED))
            await rig.fake.notify("item/autoApprovalReview/completed", for_thread(REVIEW_DENIED))
            [frame] = await rig.frames(1, timeout=5.0)
            token = frame["message"]["content"].split("(token ", 1)[1][:6]
            code = await cli_in_thread(rig, monkeypatch, "approve", token)
            override = await asyncio.wait_for(rig.fake.wait_request("thread/approveGuardianDeniedAction"), 5)
            retry = await asyncio.wait_for(rig.fake.wait_request("turn/start"), 5)
            return frame["message"]["content"], token, code, override["params"], retry["params"]["input"][0]["text"]

    content, token, code, override, retry = run(body())
    assert DENIED_COMMAND in content
    assert f"antiphon approve {token}" in content and f"antiphon deny {token} -- <why>" in content
    assert code == 0
    assert override == {"threadId": THREAD_ID, "event": OVERRIDE_EVENT}
    assert json.dumps(override["event"]) == json.dumps(OVERRIDE_EVENT)
    assert "retry it now" in retry and DENIED_COMMAND in retry


def test_a_blocking_request_reaches_claude_and_cli_approve_answers_accept(short_tmp, monkeypatch):
    async def body():
        async with Rig(short_tmp) as rig:
            await rig.start_thread(review_by_parent=True)
            request_id = await rig.fake.server_request("item/commandExecution/requestApproval", for_thread(REQUEST_PARAMS))
            await until(lambda: rig.bridge.state.threads[THREAD_ID].pending)
            rig.bridge.state.threads[THREAD_ID].active_turn_id = REQUEST_PARAMS["turnId"]
            [frame] = await rig.frames(1, timeout=5.0)
            token = frame["message"]["content"].split("(token ", 1)[1][:6]
            code = await cli_in_thread(rig, monkeypatch, "approve", token)
            answer = await asyncio.wait_for(rig.fake.response(request_id), 5)
            return frame["message"]["content"], code, request_id, answer

    content, code, request_id, answer = run(body())
    assert REQUEST_PARAMS["command"] in content and "The turn is blocked until you answer" in content
    assert code == 0
    assert answer == {"jsonrpc": "2.0", "id": request_id, "result": {"decision": "accept"}}
