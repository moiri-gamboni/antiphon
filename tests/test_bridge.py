import asyncio
import copy
import json
import logging
import os
import socket
import subprocess
import uuid
from pathlib import Path

import pytest

from antiphon import ipc
from antiphon.bridge import AlreadyRunning, Bridge
from antiphon.callers import Caller
from fake_claude import FakeClaude, captured_frames, send_frame
from fake_daemon import FakeDaemon, load_fixture

THREAD_START = load_fixture("thread-start.jsonl")
APPROVAL = load_fixture("user-reviewer-request-approval.jsonl")
ADOPTION = load_fixture("adoption.jsonl")
SUB_AGENT = load_fixture("sub-agent.jsonl")
TURNS_LIST = load_fixture("turns-list.jsonl")
RESTART = load_fixture("daemon-restart.jsonl")
HOOK_ORDER = load_fixture("permission-hook-order.jsonl")

THREAD_ID = THREAD_START.result(2)["thread"]["id"]
TURN_STARTED = APPROVAL.result(3)
TURN_ID = TURN_STARTED["turn"]["id"]
COMPLETED_TURN = TURNS_LIST.result(3)["data"][0]
NO_ROLLOUT = ADOPTION.error(3)
ADOPTED_THREAD = ADOPTION.result(2)["thread"]
ADOPTED_ID = ADOPTED_THREAD["id"]
SUB_AGENT_THREAD = SUB_AGENT.result(2, occurrence=1)["thread"]
SUB_AGENT_ID = SUB_AGENT_THREAD["id"]
SUB_AGENT_PARENT = SUB_AGENT_THREAD["parentThreadId"]
FAILED_TURN = [n for n in HOOK_ORDER.notifications("turn/completed") if n["turn"]["status"] == "failed"][0]
COMPLETED_NOTICE = [n for n in HOOK_ORDER.notifications("turn/completed") if n["turn"]["status"] == "completed"][0]
ERROR_NOTICE = HOOK_ORDER.notifications("error")[0]
STEERED = {"turnId": TURN_ID}

HUMAN = Caller(kind="human", claude_pid=None, claude_session_id=None, codex_thread=None)
CLAUDE = Caller(kind="claude", claude_pid=4242, claude_session_id="8d0b3e2e-2f3f-4d33-9a5a-8f0b7d1a8e11", codex_thread=None)


def run(coro):
    return asyncio.run(coro)


async def until(condition, timeout: float = 3):
    async def poll():
        while not condition():
            await asyncio.sleep(0.01)

    await asyncio.wait_for(poll(), timeout)


def turns_list(*turns):
    return {"result": {"data": list(turns), "nextCursor": None, "backwardsCursor": None}}


def for_thread(params: dict, thread_id: str = THREAD_ID) -> dict:
    """A captured notification's params re-addressed to the thread under test."""
    params = copy.deepcopy(params)
    params["threadId"] = thread_id
    return params


def default_replies():
    return {
        "thread/start": {"result": THREAD_START.result(2)},
        "thread/name/set": {"result": {}},
        "thread/turns/list": turns_list(COMPLETED_TURN),
        "turn/start": {"result": TURN_STARTED},
        "turn/steer": {"result": STEERED},
        "turn/interrupt": {"result": {}},
        "thread/loaded/list": {"result": {"data": [], "nextCursor": None}},
        "thread/resume": {"result": RESTART.result(6)},
        "thread/unsubscribe": {"result": {}},
        "thread/read": {"result": ADOPTION.result(2)},
    }


class Rig:
    """The real bridge over the fake daemon in a temporary home, no Claude session."""

    def __init__(self, short_tmp: Path):
        self.tmp = short_tmp
        self.home = short_tmp / "antiphon"
        self.sessions_dir = short_tmp / "cc" / "sessions"
        self.codex_home = short_tmp / "codex"
        self.daemon_sock = self.codex_home / "app-server-control" / "app-server-control.sock"
        self.daemon_sock.parent.mkdir(parents=True)
        self.fake = FakeDaemon(self.daemon_sock)
        self.fake.replies.update(default_replies())
        self.bridge = self.make_bridge()

    def make_bridge(self) -> Bridge:
        bridge = Bridge(
            self.home,
            sessions_dir=self.sessions_dir,
            codex_home=self.codex_home,
            ensure_running=lambda codex_home, rawlog=None: str(self.daemon_sock),
        )
        bridge.reconnect_backoff = (0.05, 0.2)
        bridge.reconcile_interval = 3600
        return bridge

    async def __aenter__(self):
        await self.fake.start()
        await self.bridge.start()
        await until(lambda: self.bridge.last_reconcile is not None)
        return self

    async def __aexit__(self, *exc):
        await self.bridge.close()
        await self.fake.stop()

    async def start_thread(self, name="helper", caller=HUMAN, **overrides) -> dict:
        args = dict(cwd=str(self.tmp / "work"), name=name, read_only=False, report=True, worktree=False, review_by_parent=False)
        args.update(overrides)
        return await self.bridge.dispatch("start", args, caller)

    def methods(self) -> list[str]:
        return [r["method"] for r in self.fake.requests if r["method"] != "initialize"]


# --- start ----------------------------------------------------------------------


def test_start_creates_a_named_thread_records_the_spawner_and_answers_with_it(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            result = await rig.start_thread(name="helper", caller=CLAUDE, read_only=True, model="gpt-5.6-terra", effort="high")
            return result, rig.fake.received("thread/start")[0]["params"], rig.fake.received("thread/name/set")[0]["params"], rig.bridge.state.threads[THREAD_ID]

    result, start_params, name_params, thread = run(body())
    assert result == {"name": "helper", "thread_id": THREAD_ID, "cwd": str(short_tmp / "work")}
    assert start_params["cwd"] == str(short_tmp / "work")
    assert start_params["sandbox"] == "read-only"
    assert start_params["model"] == "gpt-5.6-terra"
    assert name_params == {"threadId": THREAD_ID, "name": "helper"}
    assert thread.spawner == CLAUDE.claude_session_id
    assert thread.origin == "spawned"
    assert thread.read_only is True
    assert thread.effort == "high"
    assert thread.child_pid is None
    assert json.loads((short_tmp / "antiphon" / "state.json").read_text())["threads"][THREAD_ID]["name"] == "helper"


def test_start_dedupes_the_name_against_hosted_threads(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            second = THREAD_START.result(2)
            second = json.loads(json.dumps(second).replace(THREAD_ID, "01a0c390-0000-7000-8000-000000000002"))
            rig.fake.replies["thread/start"] = [{"result": THREAD_START.result(2)}, {"result": second}]
            first = await rig.start_thread(name="helper")
            again = await rig.start_thread(name="helper")
            return first["name"], again["name"]

    assert run(body()) == ("helper", "helper-2")


def test_start_without_a_name_uses_the_directory_name(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            return (await rig.start_thread(name=None, cwd="/srv/projects/webapp"))["name"]

    assert run(body()) == "codex-webapp"


def test_start_with_worktree_checks_out_a_branch_beside_the_repo(short_tmp):
    repo = short_tmp / "myrepo"
    repo.mkdir()
    subprocess.run(["git", "init", "-q", "-b", "main"], cwd=repo, check=True)
    subprocess.run(["git", "-c", "user.email=a@b", "-c", "user.name=t", "commit", "--allow-empty", "-q", "-m", "root"], cwd=repo, check=True)
    (repo / "sub").mkdir()

    async def body():
        async with Rig(short_tmp) as rig:
            result = await rig.start_thread(name="helper", cwd=str(repo / "sub"), worktree=True)
            return result, rig.fake.received("thread/start")[0]["params"]["cwd"], rig.bridge.state.threads[THREAD_ID].worktree

    result, started_cwd, worktree = run(body())
    expected = str(short_tmp / "myrepo-worktrees" / "helper")
    assert result["cwd"] == expected == started_cwd == worktree
    branch = subprocess.run(["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=expected, capture_output=True, text=True).stdout.strip()
    assert branch == "codex/helper"


def test_a_failed_thread_start_removes_the_worktree_it_created(short_tmp):
    repo = short_tmp / "myrepo"
    repo.mkdir()
    subprocess.run(["git", "init", "-q", "-b", "main"], cwd=repo, check=True)
    subprocess.run(["git", "-c", "user.email=a@b", "-c", "user.name=t", "commit", "--allow-empty", "-q", "-m", "root"], cwd=repo, check=True)

    async def body():
        async with Rig(short_tmp) as rig:
            rig.fake.replies["thread/start"] = {"error": {"code": -32600, "message": "no such model"}}
            with pytest.raises(ipc.IpcError) as info:
                await rig.start_thread(name="helper", cwd=str(repo), worktree=True)
            return info.value

    error = run(body())
    assert error.kind == "precondition"
    expected = short_tmp / "myrepo-worktrees" / "helper"
    assert not expected.exists()
    branches = subprocess.run(["git", "branch", "--list", "codex/helper"], cwd=repo, capture_output=True, text=True).stdout
    assert branches.strip() == ""


def test_a_transport_failure_during_thread_start_removes_the_worktree(short_tmp):
    repo = short_tmp / "myrepo"
    repo.mkdir()
    subprocess.run(["git", "init", "-q", "-b", "main"], cwd=repo, check=True)
    subprocess.run(["git", "-c", "user.email=a@b", "-c", "user.name=t", "commit", "--allow-empty", "-q", "-m", "root"], cwd=repo, check=True)

    async def body():
        from antiphon.codex.ws import TransportClosed

        async with Rig(short_tmp) as rig:
            async def boom(*args, **kwargs):
                raise TransportClosed("connection lost")

            rig.bridge.daemon.thread_start = boom
            with pytest.raises(ipc.IpcError) as info:
                await rig.start_thread(name="helper", cwd=str(repo), worktree=True)
            return info.value

    error = run(body())
    assert error.kind == "daemon_unreachable"
    assert not (short_tmp / "myrepo-worktrees" / "helper").exists()
    branches = subprocess.run(["git", "branch", "--list", "codex/helper"], cwd=repo, capture_output=True, text=True).stdout
    assert branches.strip() == ""


def test_start_with_worktree_outside_a_repository_is_a_precondition_error(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            with pytest.raises(ipc.IpcError) as info:
                await rig.start_thread(name="helper", cwd=str(short_tmp), worktree=True)
            return info.value, rig.methods()

    error, methods = run(body())
    assert error.kind == "precondition"
    assert "not a git repository" in error.message
    assert "thread/start" not in methods


# --- send, wait, interrupt ------------------------------------------------------


def test_start_from_an_unrecognised_codex_caller_is_refused(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            with pytest.raises(ipc.IpcError) as info:
                await rig.start_thread(caller=CODEX_UNHOSTED)
            return info.value, rig.methods()

    error, methods = run(body())
    assert error.kind == "precondition"
    assert "CODEX_THREAD_ID" in error.message
    assert "thread/start" not in methods


def test_resume_from_an_unrecognised_codex_caller_is_refused(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            with pytest.raises(ipc.IpcError) as info:
                await rig.bridge.dispatch("resume", {"target": "01a0c399-780c-73f3-9209-e09112a796a0"}, CODEX_UNHOSTED)
            return info.value, rig.methods()

    error, methods = run(body())
    assert error.kind == "precondition"
    assert "CODEX_THREAD_ID" in error.message
    assert "thread/resume" not in methods


def test_send_on_an_idle_spawned_thread_starts_a_turn_with_the_network_enabled_sandbox(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            await rig.start_thread()
            result = await rig.bridge.dispatch("send", {"target": "helper", "text": "do it"}, HUMAN)
            return result, rig.fake.received("turn/start")[0]["params"], rig.bridge.state.threads[THREAD_ID]

    result, params, thread = run(body())
    assert result["kind"] == "started"
    assert result["turn_id"] == TURN_ID
    assert result["thread_id"] == THREAD_ID
    assert params["sandboxPolicy"] == {"type": "workspaceWrite", "networkAccess": True}
    assert params["input"] == [{"type": "text", "text": "do it"}]
    assert thread.status == "busy"
    assert thread.active_turn_id == TURN_ID


def test_send_on_a_read_only_thread_uses_the_read_only_sandbox(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            await rig.start_thread(read_only=True)
            await rig.bridge.dispatch("send", {"target": "helper", "text": "look"}, HUMAN)
            return rig.fake.received("turn/start")[0]["params"]["sandboxPolicy"]

    assert run(body()) == {"type": "readOnly", "networkAccess": True}


def test_send_on_an_active_thread_steers(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            rig.fake.replies["thread/turns/list"] = turns_list(TURN_STARTED["turn"])
            await rig.start_thread()
            return await rig.bridge.dispatch("send", {"target": "helper", "text": "also this"}, HUMAN)

    assert run(body())["kind"] == "steered"


def test_send_by_thread_id_prefix_and_ambiguity_lists_candidates(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            await rig.start_thread(name="one")
            other = copy.deepcopy(rig.bridge.state.threads[THREAD_ID])
            other.thread_id, other.name = "01a0c390-ffff-7000-8000-000000000002", "two"
            rig.bridge.state.threads[other.thread_id] = other
            by_prefix = await rig.bridge.dispatch("status", {"target": THREAD_ID[:12]}, HUMAN)
            with pytest.raises(ipc.IpcError) as info:
                await rig.bridge.dispatch("status", {"target": "01a0c390"}, HUMAN)
            with pytest.raises(ipc.IpcError) as missing:
                await rig.bridge.dispatch("status", {"target": "nobody"}, HUMAN)
            return by_prefix["name"], info.value, missing.value

    name, ambiguous, missing = run(body())
    assert name == "one"
    assert ambiguous.kind == "ambiguous"
    assert sorted(ambiguous.raw) == ["one", "two"]
    assert missing.kind == "unknown_target"


def test_a_refused_thread_start_is_a_precondition_not_a_daemon_error(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            rig.fake.replies["thread/start"] = {"error": {"code": -32600, "message": "no such model"}}
            with pytest.raises(ipc.IpcError) as info:
                await rig.start_thread()
            return info.value

    error = run(body())
    assert error.kind == "precondition"
    assert "no such model" in error.message


def test_a_transport_failure_during_an_op_is_daemon_unreachable(short_tmp):
    from antiphon.codex.daemon import Daemon

    async def noop(*args):
        pass

    async def body():
        async with Rig(short_tmp) as rig:
            await rig.start_thread()
            rig.bridge._closing = True
            await rig.fake.drop()
            await until(lambda: rig.bridge.daemon is None)
            dead = await Daemon.connect(str(rig.daemon_sock), noop, noop)
            await rig.fake.drop()
            await asyncio.wait_for(dead.closed.wait(), 2)
            rig.bridge.daemon = dead
            rig.bridge.subscribed[THREAD_ID] = dead.epoch
            with pytest.raises(ipc.IpcError) as info:
                await rig.bridge.dispatch("send", {"target": "helper", "text": "x"}, HUMAN)
            return info.value.kind

    assert run(body()) == "daemon_unreachable"


def test_a_rejected_turn_start_is_a_delivery_error_with_the_daemons_message(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            rig.fake.replies["turn/start"] = {"error": {"code": -32600, "message": "direct app-server input is not allowed for multi-agent v2 sub-agents"}}
            await rig.start_thread()
            with pytest.raises(ipc.IpcError) as info:
                await rig.bridge.dispatch("send", {"target": "helper", "text": "x"}, HUMAN)
            return info.value

    error = run(body())
    assert error.kind == "delivery_rejected"
    assert "not allowed" in error.message


def test_wait_returns_the_final_answer_when_the_turn_completes(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            await rig.start_thread()
            await rig.bridge.dispatch("send", {"target": "helper", "text": "go"}, HUMAN)
            waiting = asyncio.create_task(rig.bridge.dispatch("wait", {"target": "helper", "timeout": 5}, HUMAN))
            await asyncio.sleep(0.05)
            await rig.fake.notify("turn/completed", for_thread(COMPLETED_NOTICE))
            return await waiting, rig.bridge.state.threads[THREAD_ID]

    result, thread = run(body())
    assert result == {"status": "completed", "final": "DONE", "thread_id": THREAD_ID}
    assert thread.status == "idle"
    assert thread.active_turn_id is None
    assert thread.final == "DONE"


def test_wait_on_a_failed_turn_reports_the_failure_text(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            await rig.start_thread()
            await rig.bridge.dispatch("send", {"target": "helper", "text": "go"}, HUMAN)
            await rig.fake.notify("turn/completed", for_thread(FAILED_TURN))
            await until(lambda: rig.bridge.state.threads[THREAD_ID].status == "idle")
            return await rig.bridge.dispatch("wait", {"target": "helper", "timeout": 5}, HUMAN), rig.bridge.state.threads[THREAD_ID].last_error

    result, last_error = run(body())
    assert result["status"] == "failed"
    assert result["final"].startswith("failed: You’ve hit your usage limit")
    assert last_error["message"].startswith("You’ve hit your usage limit")


def test_stop_wakes_a_pending_wait(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            await rig.start_thread()
            await rig.bridge.dispatch("send", {"target": "helper", "text": "go"}, HUMAN)
            waiting = asyncio.create_task(rig.bridge.dispatch("wait", {"target": "helper", "timeout": 5}, HUMAN))
            await asyncio.sleep(0.05)
            await rig.bridge.dispatch("stop", {"target": "helper"}, HUMAN)
            return await asyncio.wait_for(waiting, 1)

    assert run(body())["status"] == "stopped"


def test_own_thread_closed_wakes_a_pending_wait_without_a_stale_answer(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            await rig.start_thread()
            await rig.bridge.dispatch("send", {"target": "helper", "text": "go"}, HUMAN)
            rig.bridge.state.threads[THREAD_ID].final = "an earlier turn's answer"  # must not be returned
            waiting = asyncio.create_task(rig.bridge.dispatch("wait", {"target": "helper", "timeout": 5}, HUMAN))
            await asyncio.sleep(0.05)
            await rig.fake.notify("thread/closed", {"threadId": THREAD_ID})
            return await asyncio.wait_for(waiting, 1)

    result = run(body())
    assert result["status"] == "unloaded"
    assert result["final"] is None


def test_wait_times_out_with_kind_timeout(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            await rig.start_thread()
            await rig.bridge.dispatch("send", {"target": "helper", "text": "go"}, HUMAN)
            with pytest.raises(ipc.IpcError) as info:
                await rig.bridge.dispatch("wait", {"target": "helper", "timeout": 0.05}, HUMAN)
            return info.value.kind

    assert run(body()) == "timeout"


def test_interrupt_when_the_turn_just_ended_is_a_noop(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            await rig.start_thread()
            await rig.bridge.dispatch("send", {"target": "helper", "text": "go"}, HUMAN)
            rig.fake.replies["turn/interrupt"] = {"error": {"code": -32600, "message": "turn not active"}}
            rig.fake.replies["thread/turns/list"] = turns_list(COMPLETED_TURN)
            return await rig.bridge.dispatch("interrupt", {"target": "helper"}, HUMAN)

    assert run(body()) == {"noop": "idle"}


def test_interrupt_ends_the_active_turn(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            await rig.start_thread()
            await rig.bridge.dispatch("send", {"target": "helper", "text": "go"}, HUMAN)
            result = await rig.bridge.dispatch("interrupt", {"target": "helper"}, HUMAN)
            return result, rig.fake.received("turn/interrupt")[0]["params"]

    result, params = run(body())
    assert result == {"turn_id": TURN_ID}
    assert params == {"threadId": THREAD_ID, "turnId": TURN_ID}


def test_interrupt_on_an_idle_thread_is_a_noop_without_calling_the_daemon(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            await rig.start_thread()
            result = await rig.bridge.dispatch("interrupt", {"target": "helper"}, HUMAN)
            return result, rig.fake.received("turn/interrupt")

    result, calls = run(body())
    assert result == {"noop": "idle"}
    assert calls == []


# --- notifications -------------------------------------------------------------


def test_status_changes_and_errors_update_the_thread(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            await rig.start_thread()
            thread = rig.bridge.state.threads[THREAD_ID]
            await rig.fake.notify("thread/status/changed", {"threadId": THREAD_ID, "status": {"type": "active", "activeFlags": ["waitingOnApproval"]}})
            await until(lambda: thread.status == "approval")
            await rig.fake.notify("error", for_thread(ERROR_NOTICE))
            await until(lambda: thread.last_error is not None)
            await rig.fake.notify("thread/name/updated", {"threadId": THREAD_ID, "threadName": "renamed-by-codex"})
            await until(lambda: thread.name == "renamed-by-codex")
            await rig.fake.notify("thread/status/changed", {"threadId": THREAD_ID, "status": {"type": "idle"}})
            await until(lambda: thread.status == "idle")
            return thread.last_error

    last_error = run(body())
    assert last_error["message"].startswith("You’ve hit your usage limit")
    assert last_error["willRetry"] is False
    assert isinstance(last_error["at"], float)


def test_a_server_request_is_refused_with_method_not_found(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            request = APPROVAL.server_requests("item/commandExecution/requestApproval")[0]
            request_id = await rig.fake.server_request(request["method"], request["params"])
            return await asyncio.wait_for(rig.fake.response(request_id), 2)

    reply = run(body())
    assert reply["error"]["code"] == -32601
    assert "result" not in reply


# --- reconnect and reconcile ----------------------------------------------------


def test_daemon_reconnect_resubscribes_every_hosted_thread_on_a_new_epoch(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            await rig.start_thread()
            first_epoch = rig.bridge.daemon.epoch
            await rig.fake.drop()
            await until(lambda: rig.bridge.daemon is not None and rig.bridge.daemon.epoch > first_epoch)
            await until(lambda: len(rig.fake.received("thread/resume")) == 1)
            ping = await rig.bridge.dispatch("ping", {}, HUMAN)
            return rig.fake.received("thread/resume")[0]["params"], ping, len(rig.fake.connections)

    resume_params, ping, connections = run(body())
    assert resume_params == {"threadId": THREAD_ID}
    assert ping["daemon"] is True
    assert connections == 2


def test_a_raising_sweep_does_not_kill_the_reconcile_loop(short_tmp):
    async def body():
        rig = Rig(short_tmp)
        rig.bridge.reconcile_interval = 0.02
        calls = []

        async def bad_sweep():
            calls.append(True)
            raise RuntimeError("sweep boom")

        rig.bridge.sweeps.append(bad_sweep)
        task = asyncio.create_task(rig.bridge._reconcile_loop())
        try:
            await until(lambda: len(calls) >= 3, timeout=3)
        finally:
            rig.bridge._closing = True
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        return len(calls)

    assert run(body()) >= 3


def test_a_raising_post_connect_step_does_not_kill_the_connect_loop(short_tmp):
    async def body():
        rig = Rig(short_tmp)
        calls = []
        real = rig.bridge._update_degraded

        def flaky():
            calls.append(True)
            if len(calls) == 1:
                raise RuntimeError("degraded write boom")
            return real()

        rig.bridge._update_degraded = flaky
        await rig.fake.start()
        await rig.bridge.start()
        try:
            await until(lambda: rig.bridge.daemon is not None and len(calls) >= 2, timeout=5)
            return rig.bridge.connected_once, rig.bridge.daemon is not None
        finally:
            await rig.bridge.close()
            await rig.fake.stop()

    connected_once, has_daemon = run(body())
    assert connected_once and has_daemon


def test_ping_right_after_start_waits_for_the_first_daemon_connection(short_tmp):
    async def body():
        rig = Rig(short_tmp)
        await rig.bridge.start()
        try:
            pinging = asyncio.create_task(rig.bridge.dispatch("ping", {}, HUMAN))
            await asyncio.sleep(0.3)
            await rig.fake.start()
            return await asyncio.wait_for(pinging, 5)
        finally:
            await rig.bridge.close()
            await rig.fake.stop()

    assert run(body())["daemon"] is True


def test_an_initialize_reply_of_an_unknown_shape_degrades_and_keeps_retrying(short_tmp):
    async def body():
        rig = Rig(short_tmp)
        rig.fake.replies["initialize"] = {"result": {"codexHome": "~/.codex"}}
        await rig.fake.start()
        await rig.bridge.start()
        try:
            await until(lambda: rig.bridge.state.degraded == ["codex protocol: initialize unsupported"])
            rig.fake.replies["initialize"] = {"result": THREAD_START.result(1)}
            await until(lambda: rig.bridge.daemon is not None)
            return rig.bridge.state.degraded
        finally:
            await rig.bridge.close()
            await rig.fake.stop()

    assert run(body()) == []


def test_a_thread_read_of_an_unknown_shape_fails_one_pass_and_the_next_pass_still_runs(short_tmp, caplog):
    async def body():
        async with Rig(short_tmp) as rig:
            rig.fake.replies["thread/loaded/list"] = {"result": {"data": [ADOPTED_ID], "nextCursor": None}}
            rig.fake.replies["thread/read"] = {"result": {"thread": {"id": ADOPTED_ID, "status": {"type": "idle"}}}}
            with caplog.at_level(logging.ERROR, logger="antiphon.bridge"):
                await rig.bridge.reconcile()
            failed = list(rig.bridge.state.threads)
            rig.fake.replies["thread/read"] = {"result": ADOPTION.result(2)}
            await rig.bridge.reconcile()
            return failed, list(rig.bridge.state.threads)

    failed, recovered = run(body())
    assert failed == []
    assert recovered == [ADOPTED_ID]
    assert "reconcile failed" in caplog.text and "KeyError" in caplog.text


def test_reconnect_survives_a_connection_dropped_during_initialize(short_tmp):
    """The restart capture shows the socket accepting one connection and dropping it before
    the new daemon answers `initialize`; the bridge must treat that as one more retry."""

    async def body():
        async with Rig(short_tmp) as rig:
            await rig.start_thread()
            first_epoch = rig.bridge.daemon.epoch
            rig.fake.replies["initialize"] = None
            await rig.fake.drop()
            await until(lambda: len(rig.fake.received("initialize")) == 2)
            rig.fake.conn.abort()
            rig.fake.replies["initialize"] = {"result": THREAD_START.result(1)}
            await until(lambda: rig.bridge.daemon is not None and rig.bridge.daemon.epoch > first_epoch)
            await until(lambda: len(rig.fake.received("thread/resume")) == 1)
            return len(rig.fake.received("initialize")), rig.bridge.daemon.epoch - first_epoch

    initializes, epochs = run(body())
    assert initializes == 3
    assert epochs == 2


def test_a_turn_that_ended_while_the_bridge_was_away_is_recovered_on_resubscribe(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            await rig.start_thread()
            await rig.bridge.dispatch("send", {"target": "helper", "text": "go"}, HUMAN)
        # A fresh bridge over the same home: the state file says busy, the daemon says idle.
        rig.fake = FakeDaemon(rig.daemon_sock)
        rig.fake.replies.update(default_replies())
        rig.bridge = rig.make_bridge()
        async with rig:
            result = await rig.bridge.dispatch("wait", {"target": "helper", "timeout": 5}, HUMAN)
            interrupt = await rig.bridge.dispatch("interrupt", {"target": "helper"}, HUMAN)
            return result, interrupt, rig.methods()

    result, interrupt, methods = run(body())
    assert result == {"status": "completed", "final": "BLOCKED", "thread_id": THREAD_ID}
    assert interrupt == {"noop": "idle"}
    assert methods[:3] == ["thread/loaded/list", "thread/resume", "thread/turns/list"]


def test_a_wait_in_flight_across_a_daemon_restart_gets_the_recovered_outcome(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            await rig.start_thread()
            await rig.bridge.dispatch("send", {"target": "helper", "text": "go"}, HUMAN)
            waiting = asyncio.create_task(rig.bridge.dispatch("wait", {"target": "helper", "timeout": 5}, HUMAN))
            await asyncio.sleep(0.05)
            await rig.fake.drop()
            return await asyncio.wait_for(waiting, 5)

    assert run(body()) == {"status": "completed", "final": "BLOCKED", "thread_id": THREAD_ID}


def test_a_recovered_turn_is_announced_once_even_if_its_completion_arrives_afterwards(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            claude = FakeClaude(rig.sessions_dir, short_tmp / "socks")
            spawner = Caller(kind="claude", claude_pid=claude.pid, claude_session_id=claude.record["sessionId"], codex_thread=None)
            try:
                await rig.start_thread(caller=spawner)
                await rig.bridge.dispatch("send", {"target": "helper", "text": "go"}, spawner)
                await rig.fake.drop()
                await until(lambda: rig.bridge.state.threads[THREAD_ID].outcome == "completed")
                late = for_thread(COMPLETED_NOTICE)
                late["turn"]["id"] = COMPLETED_TURN["id"]
                await rig.fake.notify("turn/completed", late)
                frames = await asyncio.to_thread(claude.wait_for_frames, 2, 1.0)
                return frames
            finally:
                claude.close()

    frames = run(body())
    assert [f["message"]["content"].split("\n")[1] for f in frames] == ["BLOCKED"]


def test_the_thread_started_broadcast_for_our_own_new_thread_does_not_adopt_it(short_tmp):
    async def slow_name_set(params):
        await asyncio.sleep(0.4)
        return {"result": {}}

    async def body():
        async with Rig(short_tmp) as rig:
            claude = FakeClaude(rig.sessions_dir, short_tmp / "socks")
            try:
                rig.fake.replies["thread/name/set"] = slow_name_set
                rig.fake.replies["thread/loaded/list"] = {"result": {"data": [THREAD_ID], "nextCursor": None}}
                rig.fake.replies["thread/read"] = {"result": {"thread": THREAD_START.result(2)["thread"]}}
                starting = asyncio.create_task(rig.start_thread(name="helper", caller=CLAUDE))
                await asyncio.sleep(0.1)
                await rig.fake.notify("thread/started", THREAD_START.notifications("thread/started")[0])
                await starting
                await rig.bridge.reconcile()
                thread = rig.bridge.state.threads[THREAD_ID]
                names = sorted(r.name for r in rig.bridge.live_records())
                return thread, names, list(rig.bridge.children)
            finally:
                claude.close()

    thread, names, children = run(body())
    assert (thread.origin, thread.spawner, thread.name) == ("spawned", CLAUDE.claude_session_id, "helper")
    assert thread.child_pid is not None
    assert names == ["claude-main", "helper"]
    assert children == [THREAD_ID]


def test_ping_reports_the_daemon_unreachable_while_it_is_down(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            await rig.fake.stop()
            await until(lambda: rig.bridge.daemon is None)
            down = await rig.bridge.dispatch("ping", {}, HUMAN)
            with pytest.raises(ipc.IpcError) as info:
                await rig.start_thread()
            rig.fake = FakeDaemon(rig.daemon_sock)
            rig.fake.replies.update(default_replies())
            await rig.fake.start()
            await until(lambda: rig.bridge.daemon is not None)
            up = await rig.bridge.dispatch("ping", {}, HUMAN)
            return down, info.value.kind, up

    down, kind, up = run(body())
    assert down["daemon"] is False
    assert kind == "daemon_unreachable"
    assert up["daemon"] is True
    assert up["codex"] == "0.155.1"


def test_reconcile_runs_the_sweeps_even_while_the_daemon_is_down(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            await rig.fake.stop()
            await until(lambda: rig.bridge.daemon is None)
            ran = []

            async def sweep():
                ran.append(True)

            rig.bridge.sweeps.append(sweep)
            await rig.bridge.reconcile()
            return ran

    assert run(body()) == [True]


def test_reconcile_adopts_a_loaded_thread_absent_from_state_without_subscribing(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            rig.fake.replies["thread/loaded/list"] = {"result": {"data": [ADOPTED_ID], "nextCursor": None}}
            await rig.bridge.reconcile()
            thread = rig.bridge.state.threads[ADOPTED_ID]
            return thread, rig.methods()

    thread, methods = run(body())
    assert (thread.origin, thread.spawner, thread.name, thread.cwd, thread.status) == ("adopted", "human", "codex-antiphon-capture", "~/antiphon-capture", "idle")
    assert "thread/resume" not in methods
    assert methods.count("thread/read") == 1


def test_reconcile_keeps_a_sub_agent_under_its_parent_and_never_resumes_it(short_tmp):
    parent = copy.deepcopy(ADOPTION.result(2))
    parent["thread"]["id"] = SUB_AGENT_PARENT
    parent["thread"]["name"] = "orchestrator"

    def read(params):
        if params["threadId"] == SUB_AGENT_ID:
            return {"result": SUB_AGENT.result(2, occurrence=1)}
        return {"result": parent}

    async def body():
        async with Rig(short_tmp) as rig:
            rig.fake.replies["thread/read"] = read
            rig.fake.replies["thread/loaded/list"] = {"result": {"data": [SUB_AGENT_ID, SUB_AGENT_PARENT], "nextCursor": None}}
            await rig.bridge.reconcile()
            await rig.bridge.reconcile()
            await rig.fake.notify("thread/status/changed", {"threadId": SUB_AGENT_ID, "status": {"type": "active", "activeFlags": []}})
            await until(lambda: rig.bridge.state.threads[SUB_AGENT_PARENT].sub_agents[SUB_AGENT_ID].status == "busy")
            rows = await rig.bridge.dispatch("ls", {}, HUMAN)
            return rig.bridge.state, [r["method"] + ":" + r["params"]["threadId"] for r in rig.fake.requests if r["method"] == "thread/resume"], rows

    state, resumes, rows = run(body())
    assert list(state.threads) == [SUB_AGENT_PARENT]
    sub = state.threads[SUB_AGENT_PARENT].sub_agents[SUB_AGENT_ID]
    assert (sub.nickname, sub.role) == ("Bernoulli", None)
    assert resumes == []
    assert [(r["name"], r["kind"], r.get("parent")) for r in rows] == [("orchestrator", "codex", None), ("Bernoulli", "codex-agent", "orchestrator")]


def test_reconcile_leaves_a_thread_with_no_rollout_unsubscribed_and_retries_next_pass(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            await rig.start_thread()
            rig.bridge.subscribed.clear()
            rig.fake.replies["thread/resume"] = [{"error": NO_ROLLOUT}, {"result": RESTART.result(6)}]
            await rig.bridge.reconcile()
            first = dict(rig.bridge.subscribed)
            await rig.bridge.reconcile()
            return first, dict(rig.bridge.subscribed), len(rig.fake.received("thread/resume"))

    first, second, resumes = run(body())
    assert first == {}
    assert list(second) == [THREAD_ID]
    assert resumes == 2


def test_own_thread_closed_is_unloaded_and_the_next_send_resumes_first(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            await rig.start_thread()
            await rig.fake.notify("thread/closed", {"threadId": THREAD_ID})
            await until(lambda: rig.bridge.state.threads[THREAD_ID].status == "unloaded")
            await rig.bridge.reconcile()
            resumes_before = len(rig.fake.received("thread/resume"))
            await rig.bridge.dispatch("send", {"target": "helper", "text": "again"}, HUMAN)
            return resumes_before, rig.methods()

    resumes_before, methods = run(body())
    assert resumes_before == 0
    assert methods[-3:] == ["thread/resume", "thread/turns/list", "turn/start"]


def test_adopted_thread_closed_is_removed(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            rig.fake.replies["thread/loaded/list"] = {"result": {"data": [ADOPTED_ID], "nextCursor": None}}
            await rig.bridge.reconcile()
            await rig.fake.notify("thread/closed", {"threadId": ADOPTED_ID})
            await until(lambda: ADOPTED_ID not in rig.bridge.state.threads)
            return json.loads((rig.home / "state.json").read_text())["threads"]

    assert run(body()) == {}


def test_thread_started_broadcast_adopts_the_thread(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            rig.fake.replies["thread/loaded/list"] = {"result": {"data": [ADOPTED_ID], "nextCursor": None}}
            await rig.fake.notify("thread/started", {"thread": ADOPTED_THREAD})
            await until(lambda: ADOPTED_ID in rig.bridge.state.threads)
            return rig.bridge.state.threads[ADOPTED_ID].origin

    assert run(body()) == "adopted"


# --- stop, resume, name, status, ls ---------------------------------------------


def test_stop_interrupts_unsubscribes_and_retires_the_thread(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            await rig.start_thread()
            await rig.bridge.dispatch("send", {"target": "helper", "text": "go"}, HUMAN)
            result = await rig.bridge.dispatch("stop", {"target": "helper"}, HUMAN)
            with pytest.raises(ipc.IpcError) as info:
                await rig.bridge.dispatch("send", {"target": "helper", "text": "more"}, HUMAN)
            return result, rig.methods(), rig.bridge.state, info.value

    result, methods, state, error = run(body())
    assert result == {"name": "helper", "thread_id": THREAD_ID}
    assert methods[-2:] == ["turn/interrupt", "thread/unsubscribe"]
    assert state.threads == {}
    assert state.stopped == {"helper": THREAD_ID}
    assert error.kind == "stopped"
    assert f"antiphon resume {THREAD_ID}" in error.message


def test_a_stopped_thread_is_not_readopted_while_the_daemon_still_lists_it(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            await rig.start_thread(name="helper", caller=CLAUDE)
            await rig.bridge.dispatch("stop", {"target": "helper"}, CLAUDE)
            # The daemon keeps the thread loaded after unsubscribe; a reconcile pass must
            # not adopt a thread the user stopped.
            rig.fake.replies["thread/loaded/list"] = {"result": {"data": [THREAD_ID], "nextCursor": None}}
            rig.fake.replies["thread/read"] = {"result": {"thread": {"id": THREAD_ID, "cwd": str(rig.tmp / "work"), "name": "helper", "status": {"type": "idle"}}}}
            await rig.bridge.reconcile()
            with pytest.raises(ipc.IpcError) as info:
                await rig.bridge.dispatch("send", {"target": "helper", "text": "again"}, CLAUDE)
            return list(rig.bridge.state.threads), rig.bridge.state.stopped, info.value

    threads, stopped, error = run(body())
    assert threads == []
    assert stopped == {"helper": THREAD_ID}
    assert error.kind == "stopped"
    assert f"antiphon resume {THREAD_ID}" in error.message


def test_stop_removes_a_clean_worktree_and_keeps_a_dirty_one(short_tmp):
    repo = short_tmp / "myrepo"
    repo.mkdir()
    subprocess.run(["git", "init", "-q", "-b", "main"], cwd=repo, check=True)
    subprocess.run(["git", "-c", "user.email=a@b", "-c", "user.name=t", "commit", "--allow-empty", "-q", "-m", "root"], cwd=repo, check=True)
    second = json.loads(json.dumps(THREAD_START.result(2)).replace(THREAD_ID, "01a0c390-0000-7000-8000-000000000002"))

    async def body():
        async with Rig(short_tmp) as rig:
            rig.fake.replies["thread/start"] = [{"result": THREAD_START.result(2)}, {"result": second}]
            clean = await rig.start_thread(name="clean", cwd=str(repo), worktree=True)
            dirty = await rig.start_thread(name="dirty", cwd=str(repo), worktree=True)
            (Path(dirty["cwd"]) / "x.txt").write_text("dirty\n")
            stopped_clean = await rig.bridge.dispatch("stop", {"target": "clean"}, HUMAN)
            stopped_dirty = await rig.bridge.dispatch("stop", {"target": "dirty"}, HUMAN)
            return clean["cwd"], dirty["cwd"], stopped_clean, stopped_dirty

    clean_path, dirty_path, stopped_clean, stopped_dirty = run(body())
    assert not os.path.exists(clean_path)
    assert "worktree_kept" not in stopped_clean
    assert os.path.exists(dirty_path)
    assert stopped_dirty["worktree_kept"] == dirty_path
    assert "modified or untracked" in stopped_dirty["worktree_reason"]


def test_resume_rehosts_a_stopped_thread_under_the_caller(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            await rig.start_thread()
            await rig.bridge.dispatch("stop", {"target": "helper"}, HUMAN)
            resumed = json.loads(json.dumps(RESTART.result(6)).replace("01a0c399-780c-73f3-9209-e09112a796a0", THREAD_ID))
            rig.fake.replies["thread/resume"] = {"result": resumed}
            by_name = await rig.bridge.dispatch("resume", {"target": "helper"}, CLAUDE)
            thread = rig.bridge.state.threads[THREAD_ID]
            return by_name, thread, rig.bridge.state.stopped, rig.fake.received("thread/resume")[-1]["params"]

    result, thread, stopped, params = run(body())
    assert result == {"name": "helper", "thread_id": THREAD_ID}
    assert params == {"threadId": THREAD_ID}
    assert (thread.spawner, thread.origin, thread.status, thread.read_only) == (CLAUDE.claude_session_id, "spawned", "idle", False)
    assert stopped == {}


def test_resume_of_a_never_hosted_thread_by_id_names_it_after_its_directory(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            result = await rig.bridge.dispatch("resume", {"target": "01a0c399-780c-73f3-9209-e09112a796a0"}, HUMAN)
            return result, rig.bridge.state.threads["01a0c399-780c-73f3-9209-e09112a796a0"].cwd

    result, cwd = run(body())
    assert result == {"name": "codex-codex-steer-test", "thread_id": "01a0c399-780c-73f3-9209-e09112a796a0"}
    assert cwd == "/tmp/codex-steer-test"


def test_name_renames_the_thread_on_the_daemon_and_in_state(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            await rig.start_thread(name="helper")
            result = await rig.bridge.dispatch("name", {"target": "helper", "new": "planner"}, HUMAN)
            status = await rig.bridge.dispatch("status", {"target": "planner"}, HUMAN)
            return result, rig.fake.received("thread/name/set")[-1]["params"], status["name"]

    result, params, name = run(body())
    assert result == {"name": "planner", "thread_id": THREAD_ID, "former": "helper"}
    assert params == {"threadId": THREAD_ID, "name": "planner"}
    assert name == "planner"


def test_status_of_a_thread_and_of_the_bridge(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            await rig.start_thread(caller=CLAUDE)
            thread = await rig.bridge.dispatch("status", {"target": "helper"}, HUMAN)
            summary = await rig.bridge.dispatch("status", {}, HUMAN)
            return thread, summary

    thread, summary = run(body())
    assert thread["name"] == "helper"
    assert thread["thread_id"] == THREAD_ID
    assert thread["origin"] == "spawned"
    assert thread["status"] == "idle"
    assert thread["spawner"] == CLAUDE.claude_session_id
    assert thread["active_turn_id"] is None
    assert thread["pending"] == []
    assert thread["last_error"] is None
    assert summary["daemon"] is True
    assert summary["codex"] == "0.155.1"
    assert summary["threads"] == 1
    assert summary["degraded"] == []


def test_ls_lists_threads_and_marks_a_codex_callers_own_row(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            await rig.start_thread(name="helper")
            me = Caller(kind="codex", claude_pid=None, claude_session_id=None, codex_thread=THREAD_ID)
            return await rig.bridge.dispatch("ls", {}, me), await rig.bridge.dispatch("ls", {}, HUMAN)

    mine, human = run(body())
    assert mine == [{"name": "helper", "kind": "codex", "status": "idle", "cwd": str(short_tmp / "work"), "self": True, "thread_id": THREAD_ID}]
    assert human[0]["self"] is False


def test_a_reshaped_daemon_reply_is_an_internal_error_not_a_missing_argument(short_tmp, caplog):
    async def body():
        async with Rig(short_tmp) as rig:
            rig.fake.replies["thread/start"] = {"result": {"thread": {}}}  # the id the bridge reads is gone
            with caplog.at_level(logging.ERROR, logger="antiphon.ipc"):
                with pytest.raises(ipc.IpcError) as info:
                    await asyncio.to_thread(
                        ipc.call, str(rig.home / "bridge.sock"), "start",
                        dict(cwd="/w", name="h", read_only=False, report=True, worktree=False, review_by_parent=False),
                    )
            return info.value, caplog.text

    error, log_text = run(body())
    assert error.kind == "internal"
    assert "KeyError" in error.message
    assert "KeyError" in log_text  # the traceback reached the bridge log


def test_unknown_op_and_missing_argument_are_reported_not_crashed(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            await rig.start_thread()
            with pytest.raises(ipc.IpcError) as unknown:
                await rig.bridge.dispatch("teleport", {}, HUMAN)
            with pytest.raises(ipc.IpcError) as missing:
                await rig.bridge.dispatch("send", {"target": "helper"}, HUMAN)
            return unknown.value.kind, missing.value.kind

    assert run(body()) == ("unknown_op", "usage")


# --- the bind lock ---------------------------------------------------------------


def test_bind_unlinks_a_stale_socket_and_a_second_bridge_yields_to_a_live_one(short_tmp):
    async def body():
        rig = Rig(short_tmp)
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as stale:
            stale.bind(str(rig.home / "bridge.sock"))
        await rig.fake.start()
        try:
            await rig.bridge.start()
            answered = await asyncio.to_thread(ipc.call, str(rig.home / "bridge.sock"), "ping", {})
            second = rig.make_bridge()
            with pytest.raises(AlreadyRunning):
                await second.bind()
            return answered
        finally:
            await rig.bridge.close()
            await rig.fake.stop()

    assert "daemon" in run(body())


def test_bind_yields_to_a_live_bridge_still_making_its_first_daemon_connection(short_tmp, monkeypatch):
    from antiphon import bridge as bridge_mod

    monkeypatch.setattr(bridge_mod, "BIND_PROBE_TIMEOUT", 0.2)

    async def body():
        rig = Rig(short_tmp)
        # Bridge A binds and serves, but the daemon is not up yet, so its ping waits
        # for the first connection (DAEMON_WAIT) — longer than B's bind probe.
        await rig.bridge.start()
        try:
            second = rig.make_bridge()
            with pytest.raises(AlreadyRunning):
                await second.bind()
            still_serving = str(rig.home / "bridge.sock") == str(rig.bridge.socket_path) and rig.bridge.socket_path.exists()
            await rig.fake.start()
            answered = await asyncio.to_thread(ipc.call, str(rig.home / "bridge.sock"), "ping", {})
            return still_serving, answered
        finally:
            await rig.bridge.close()
            await rig.fake.stop()

    still_serving, answered = run(body())
    assert still_serving
    assert answered["daemon"] is True


def test_a_bridge_restart_forgets_dead_children_but_keeps_the_threads(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            await rig.start_thread()
            rig.bridge.state.threads[THREAD_ID].child_pid = 99999
            rig.bridge.save()
        fresh = rig.make_bridge()
        return fresh.state.threads[THREAD_ID].child_pid, fresh.state.threads[THREAD_ID].name

    assert run(body()) == (None, "helper")


def test_ipc_requests_reach_the_ops_with_the_caller_classified(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            result = await asyncio.to_thread(ipc.call, str(rig.home / "bridge.sock"), "start", dict(cwd="/w", name="h", read_only=False, report=True, worktree=False, review_by_parent=False))
            return result, rig.bridge.state.threads[THREAD_ID].spawner

    result, spawner = run(body())
    assert result["name"] == "h"
    assert spawner == "human"


# --- peer children ---------------------------------------------------------------


def test_start_without_a_live_claude_session_leaves_the_thread_unregistered_and_says_so(short_tmp, caplog):
    async def body():
        async with Rig(short_tmp) as rig:
            with caplog.at_level(logging.INFO, logger="antiphon.bridge"):
                await rig.start_thread()
            return rig.bridge.state.threads[THREAD_ID].child_pid

    assert run(body()) is None
    assert "waiting for a Claude Code session to copy the peer record shape from" in caplog.text


def test_a_claude_session_appearing_makes_the_next_reconcile_register_the_peer(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            await rig.start_thread()
            claude = FakeClaude(rig.sessions_dir, short_tmp / "socks")
            try:
                await rig.bridge.reconcile()
                thread = rig.bridge.state.threads[THREAD_ID]
                record_path = rig.sessions_dir / f"{thread.child_pid}.json"
                record = json.loads(record_path.read_text())
                await rig.bridge.dispatch("name", {"target": "helper", "new": "planner"}, HUMAN)
                await until(lambda: json.loads(record_path.read_text())["name"] == "planner")
                await rig.fake.notify("thread/status/changed", {"threadId": THREAD_ID, "status": {"type": "active", "activeFlags": []}})
                await until(lambda: json.loads(record_path.read_text())["status"] == "busy")
                child_pid = thread.child_pid
                await rig.bridge.dispatch("stop", {"target": "planner"}, HUMAN)
                await until(lambda: not record_path.exists())
                return child_pid, record, os.path.exists(record["messagingSocketPath"]), rig.bridge.children
            finally:
                claude.close()

    child_pid, record, socket_left, children = run(body())
    assert record["name"] == "helper"
    assert record["pid"] == child_pid
    assert record["cwd"] == str(short_tmp / "work")
    assert record["version"] == "2.1.278"
    assert record["messagingSocketPath"] == str(short_tmp / "socks" / f"{child_pid}.sock")
    assert socket_left is False
    assert children == {}


def test_deliver_to_spawner_reaches_a_claude_spawner_by_session_id_and_logs_a_gone_one(short_tmp, caplog):
    async def body():
        async with Rig(short_tmp) as rig:
            claude = FakeClaude(rig.sessions_dir, short_tmp / "socks")
            spawner = Caller(kind="claude", claude_pid=claude.pid, claude_session_id=claude.record["sessionId"], codex_thread=None)
            try:
                await rig.start_thread(caller=spawner)
                thread = rig.bridge.state.threads[THREAD_ID]
                delivered = await rig.bridge.deliver_to_spawner(thread, "the answer is 42")
                [frame] = await asyncio.to_thread(claude.wait_for_frames, 1, 2.0)
                claude.record_path.unlink()
                with caplog.at_level(logging.WARNING, logger="antiphon.bridge"):
                    gone = await rig.bridge.deliver_to_spawner(thread, "again")
                return delivered, frame, gone
            finally:
                claude.close()

    delivered, frame, gone = run(body())
    assert delivered is True
    assert frame["type"] == "user"
    assert "the answer is 42" in frame["message"]["content"]
    assert 'from-name="helper"' in frame["message"]["content"]
    assert gone is False
    assert "spawner" in caplog.text and "gone" in caplog.text


def test_deliver_to_spawner_into_a_codex_spawner_is_a_turn_on_that_thread(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            await rig.start_thread(name="parent")
            second = json.loads(json.dumps(THREAD_START.result(2)).replace(THREAD_ID, "01a0c390-0000-7000-8000-000000000002"))
            rig.fake.replies["thread/start"] = {"result": second}
            spawner = Caller(kind="codex", claude_pid=None, claude_session_id=None, codex_thread=THREAD_ID)
            await rig.start_thread(name="child", caller=spawner)
            child = rig.bridge.state.threads["01a0c390-0000-7000-8000-000000000002"]
            delivered = await rig.bridge.deliver_to_spawner(child, "child says hi")
            return delivered, rig.fake.received("turn/start")[-1]["params"]

    delivered, params = run(body())
    assert delivered is True
    assert params["threadId"] == THREAD_ID
    assert params["input"] == [{"type": "text", "text": "child says hi"}]


def test_a_child_that_dies_is_forgotten_and_respawned_on_the_next_reconcile(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            claude = FakeClaude(rig.sessions_dir, short_tmp / "socks")
            try:
                await rig.start_thread()
                thread = rig.bridge.state.threads[THREAD_ID]
                first = thread.child_pid
                os.kill(first, 9)
                await until(lambda: thread.child_pid is None)
                await rig.bridge.reconcile()
                return first, thread.child_pid
            finally:
                claude.close()

    first, second = run(body())
    assert first is not None and second is not None and first != second


# --- degradation pins ---------------------------------------------------------------


def broken_record(sessions_dir: Path, name: str, **changes) -> Path:
    """A second live record (this process again) with one departure from the expected shape."""
    base = json.loads(next(sessions_dir.glob("*.json")).read_text())
    record = {**base, "name": name, **{k: v for k, v in changes.items() if v is not None}}
    for key, value in changes.items():
        if value is None:
            record.pop(key, None)
    path = sessions_dir / f"{name}.json"
    path.write_text(json.dumps(record))
    return path


def test_a_record_missing_a_required_field_degrades_the_bridge_until_a_clean_pass(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            claude = FakeClaude(rig.sessions_dir, short_tmp / "socks")
            try:
                await rig.start_thread(name="first")
                first_child = rig.bridge.state.threads[THREAD_ID].child_pid
                bad = broken_record(rig.sessions_dir, "claude-broken", pidDomain=None)
                await rig.bridge.reconcile()
                degraded = list(rig.bridge.state.degraded)
                ping = await rig.bridge.dispatch("ping", {}, HUMAN)
                second = json.loads(json.dumps(THREAD_START.result(2)).replace(THREAD_ID, "01a0c390-0000-7000-8000-000000000002"))
                rig.fake.replies["thread/start"] = {"result": second}
                await rig.start_thread(name="second")
                second_child = rig.bridge.state.threads["01a0c390-0000-7000-8000-000000000002"].child_pid
                first_alive = rig.bridge.state.threads[THREAD_ID].child_pid == first_child and THREAD_ID in rig.bridge.children
                bad.unlink()
                await rig.bridge.reconcile()
                return degraded, ping, second_child, first_alive, rig.bridge.state.degraded, rig.bridge.state.threads["01a0c390-0000-7000-8000-000000000002"].child_pid
            finally:
                claude.close()

    degraded, ping, second_child, first_alive, cleared, registered_later = run(body())
    assert len(degraded) == 1
    assert "pidDomain" in degraded[0] and "claude-broken.json" in degraded[0]
    assert ping["degraded"] == degraded
    assert second_child is None
    assert first_alive is True
    assert cleared == []
    assert registered_later is not None


def test_a_wrong_protocol_number_is_a_degraded_reason(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            claude = FakeClaude(rig.sessions_dir, short_tmp / "socks")
            try:
                broken_record(rig.sessions_dir, "claude-v2", peerProtocol=2)
                await rig.bridge.reconcile()
                return rig.bridge.state.degraded
            finally:
                claude.close()

    [reason] = run(body())
    assert "peerProtocol" in reason and "2" in reason


def test_a_record_whose_process_vanishes_during_the_pass_is_skipped_not_a_failure(short_tmp, monkeypatch):
    from antiphon import bridge as bridge_mod

    calls = []
    real = bridge_mod.registry.pins_ok

    def flaky(record, proc_root="/proc"):
        calls.append(record.path.name)
        if record.path.name == "claude-vanishing.json":
            raise FileNotFoundError(f"/proc/{record.pid}/stat")
        return real(record, proc_root)

    monkeypatch.setattr(bridge_mod.registry, "pins_ok", flaky)

    async def body():
        async with Rig(short_tmp) as rig:
            claude = FakeClaude(rig.sessions_dir, short_tmp / "socks")
            try:
                broken_record(rig.sessions_dir, "claude-vanishing")
                await rig.bridge.reconcile()
                return rig.bridge.state.degraded
            finally:
                claude.close()

    assert run(body()) == []
    assert "claude-vanishing.json" in calls


def test_method_not_found_on_a_pinned_method_degrades_with_the_method_name(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            rig.fake.replies["thread/turns/list"] = turns_list(TURN_STARTED["turn"])
            rig.fake.replies["turn/steer"] = {"error": {"code": -32601, "message": "Method not found"}}
            rig.fake.replies["turn/start"] = {"result": TURN_STARTED}
            await rig.start_thread()
            await rig.bridge.dispatch("send", {"target": "helper", "text": "x"}, HUMAN)
            degraded = list(rig.bridge.state.degraded)
            rig.fake.replies["turn/steer"] = {"result": STEERED}
            await rig.bridge.dispatch("send", {"target": "helper", "text": "y"}, HUMAN)
            return degraded, rig.bridge.state.degraded

    degraded, after = run(body())
    assert degraded == ["codex protocol: turn/steer unsupported"]
    assert after == []


def test_an_unknown_frame_is_logged_once_per_action(short_tmp, caplog):
    probe = [f for f in captured_frames("peer-frames.jsonl", "frame") if f.get("action") == "sandbox_probe"][0]

    async def body():
        async with Rig(short_tmp) as rig:
            claude = FakeClaude(rig.sessions_dir, short_tmp / "socks")
            try:
                await rig.start_thread()
                sock = str(short_tmp / "socks" / f"{rig.bridge.state.threads[THREAD_ID].child_pid}.sock")
                with caplog.at_level(logging.WARNING, logger="antiphon.bridge"):
                    for _ in range(2):
                        await asyncio.to_thread(send_frame, sock, probe)
                    await asyncio.to_thread(send_frame, sock, {**probe, "action": "other_probe"})
                    await asyncio.sleep(0.5)
            finally:
                claude.close()

    run(body())
    warnings = [r.getMessage() for r in caplog.records if "unknown frame" in r.getMessage()]
    assert len([w for w in warnings if "sandbox_probe" in w]) == 1
    assert len([w for w in warnings if "other_probe" in w]) == 1


# --- rename, Codex-side send and notify, the effort dial ---------------------------

CODEX_ME = Caller(kind="codex", claude_pid=None, claude_session_id=None, codex_thread=THREAD_ID)
CODEX_UNHOSTED = Caller(kind="codex", claude_pid=None, claude_session_id=None, codex_thread=None)
OTHER_ID = "01a0c390-0000-7000-8000-000000000002"
CAPTURED_IDLE_NOTICE = captured_frames("peer-frames-idle-notice.jsonl", "sent")[0]


def second_thread_reply() -> dict:
    return {"result": json.loads(json.dumps(THREAD_START.result(2)).replace(THREAD_ID, OTHER_ID))}


def claude_caller(claude: FakeClaude) -> Caller:
    return Caller(kind="claude", claude_pid=claude.pid, claude_session_id=claude.record["sessionId"], codex_thread=None)


def child_sock(rig: Rig, thread_id: str) -> str:
    return str(rig.tmp / "socks" / f"{rig.bridge.state.threads[thread_id].child_pid}.sock")


def test_effort_is_sent_on_the_first_turn_even_after_a_daemon_reconnect(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            await rig.start_thread(effort="high")
            first_epoch = rig.bridge.daemon.epoch
            await rig.fake.drop()
            await until(lambda: rig.bridge.daemon is not None and rig.bridge.daemon.epoch > first_epoch)
            await until(lambda: len(rig.fake.received("thread/resume")) == 1)
            await rig.bridge.dispatch("send", {"target": "helper", "text": "first"}, HUMAN)
            await rig.fake.notify("turn/completed", for_thread(COMPLETED_NOTICE))
            await until(lambda: rig.bridge.state.threads[THREAD_ID].status == "idle")
            await rig.bridge.dispatch("send", {"target": "helper", "text": "second"}, HUMAN)
            return [r["params"] for r in rig.fake.received("turn/start")]

    first, second = run(body())
    assert first["effort"] == "high"
    assert "effort" not in second


def test_rename_dedupes_against_a_live_claude_record(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            claude = FakeClaude(rig.sessions_dir, short_tmp / "socks", name="planner")
            try:
                await rig.start_thread(name="helper")
                result = await rig.bridge.dispatch("name", {"target": "helper", "new": "planner"}, HUMAN)
                return result, rig.fake.received("thread/name/set")[-1]["params"]
            finally:
                claude.close()

    result, params = run(body())
    assert result["name"] == "planner-2"
    assert params == {"threadId": THREAD_ID, "name": "planner-2"}


def test_a_codex_caller_without_a_target_renames_its_own_thread(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            await rig.start_thread(name="helper")
            result = await rig.bridge.dispatch("name", {"target": None, "new": "planner"}, CODEX_ME)
            with pytest.raises(ipc.IpcError) as unhosted:
                await rig.bridge.dispatch("name", {"target": None, "new": "x"}, CODEX_UNHOSTED)
            return result, rig.bridge.state.threads[THREAD_ID].name, unhosted.value.kind

    result, name, kind = run(body())
    assert result == {"name": "planner", "thread_id": THREAD_ID, "former": "helper"}
    assert name == "planner"
    assert kind == "precondition"


def test_a_codex_caller_renames_the_threads_it_spawned_but_not_others(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            await rig.start_thread(name="helper")
            rig.fake.replies["thread/start"] = second_thread_reply()
            await rig.start_thread(name="other", caller=HUMAN)
            with pytest.raises(ipc.IpcError) as forbidden:
                await rig.bridge.dispatch("name", {"target": "other", "new": "mine"}, CODEX_ME)
            rig.bridge.state.threads[OTHER_ID].spawner = THREAD_ID
            allowed = await rig.bridge.dispatch("name", {"target": "other", "new": "mine"}, CODEX_ME)
            return forbidden.value, allowed

    error, allowed = run(body())
    assert error.kind == "forbidden"
    assert "codex caller" in error.message
    assert allowed == {"name": "mine", "thread_id": OTHER_ID, "former": "other"}


def test_a_codex_caller_sends_to_a_claude_session_as_a_labelled_message(short_tmp, monkeypatch):
    from antiphon import peers

    monkeypatch.setattr(peers, "RECEIPT_WAIT", 0.3)

    async def body():
        async with Rig(short_tmp) as rig:
            claude = FakeClaude(rig.sessions_dir, short_tmp / "socks")
            try:
                await rig.start_thread(name="helper")
                result = await rig.bridge.dispatch("send", {"target": "claude-main", "text": "hello there"}, CODEX_ME)
                [frame] = await asyncio.to_thread(claude.wait_for_frames, 1, 2.0)
                return result, frame, rig.fake.received("turn/start")
            finally:
                claude.close()

    result, frame, turns = run(body())
    assert result["kind"] == "sent"
    assert result["msg_id"] == frame["msg_id"]
    assert frame["type"] == "user"
    assert frame["message"]["content"].startswith('<cross-session-message from="uds:')
    assert 'from-name="helper"' in frame["message"]["content"]
    assert "\nhello there\n" in frame["message"]["content"]
    assert turns == []


def test_a_deliverys_awaiter_runs_before_the_childs_next_event_is_dispatched(short_tmp):
    """A receipt that arrives in the same read as the child's `sent` must find the send
    already waiting for it, so the reader yields to the awaiter before parsing on."""
    from antiphon.bridge import PeerChild

    class StubProc:
        pid = 1
        returncode = None

        def __init__(self):
            self.stdout = asyncio.StreamReader()
            self.stdin = type("Stdin", (), {"write": lambda self, data: None, "drain": staticmethod(_noop)})()

        async def wait(self):
            return 0

    async def _noop():
        pass

    async def body():
        events = []

        async def on_event(thread_id, event):
            events.append(event)

        child = PeerChild(THREAD_ID, short_tmp / "peer.log", short_tmp / "cc", on_event)
        child.proc = StubProc()
        child._reader = asyncio.create_task(child._read())

        async def sender():
            # What a labelled send does: the events seen at the moment deliver() returns.
            answer = await child.deliver("/nowhere.sock", "hi", "helper")
            return answer, list(events)

        sending = asyncio.create_task(sender())
        await asyncio.sleep(0.01)
        child.proc.stdout.feed_data(b'{"ev": "sent", "msg_id": "m1"}\n{"ev": "status", "orig_msg_id": "m1", "status": "held", "detail": "busy"}\n')
        answer, seen_when_resumed = await sending
        child.proc.stdout.feed_eof()
        await child._reader
        return answer, seen_when_resumed, events

    answer, seen_when_resumed, events = run(body())
    assert answer == {"ev": "sent", "msg_id": "m1"}
    assert seen_when_resumed == []
    assert [e["ev"] for e in events] == ["status", "exited"]


class StubProc:
    """A peer child process stand-in: a controllable stdout, a stdin that swallows
    writes, and a returncode the reader's kill/wait can move off None."""

    pid = 4321

    def __init__(self):
        self.returncode = None
        self.stdout = asyncio.StreamReader()

        async def _drain():
            pass

        self.stdin = type("Stdin", (), {"write": lambda self, data: None, "drain": staticmethod(_drain)})()
        self.killed = False

    def kill(self):
        self.killed = True
        self.returncode = -9

    async def wait(self):
        if self.returncode is None:
            self.returncode = 0
        return self.returncode


def test_a_delivery_that_times_out_is_reported_and_the_reader_keeps_relaying(short_tmp, monkeypatch):
    from antiphon import bridge as bridge_mod
    from antiphon.bridge import PeerChild

    monkeypatch.setattr(bridge_mod, "DELIVER_TIMEOUT", 0.1)

    async def body():
        events = []

        async def on_event(thread_id, event):
            events.append(event)

        child = PeerChild(THREAD_ID, short_tmp / "peer.log", short_tmp / "cc", on_event)
        child.proc = StubProc()
        child._reader = asyncio.create_task(child._read())
        answer = await child.deliver("/nowhere.sock", "hi", "helper")  # no answer within the timeout
        # The late `sent` for the timed-out delivery, then a real inbound frame.
        child.proc.stdout.feed_data(
            b'{"ev": "sent", "msg_id": "m1"}\n'
            b'{"ev": "inbound", "msg_id": "m2", "from_sock": "s", "from_name": "x", "text": "y"}\n'
        )
        await until(lambda: any(e["ev"] == "inbound" for e in events))
        reader_alive = not child._reader.done()
        child.proc.stdout.feed_eof()
        await child._reader
        return answer, [e["ev"] for e in events], reader_alive

    answer, event_kinds, reader_alive = run(body())
    assert answer["ev"] == "send_failed"
    assert reader_alive
    assert event_kinds == ["inbound", "exited"]


def test_a_reader_error_while_the_child_lives_kills_it_and_emits_exited(short_tmp):
    from antiphon.bridge import PeerChild

    async def body():
        events = []

        async def on_event(thread_id, event):
            events.append(event)

        child = PeerChild(THREAD_ID, short_tmp / "peer.log", short_tmp / "cc", on_event)
        child.proc = StubProc()
        child._reader = asyncio.create_task(child._read())
        child.proc.stdout.feed_data(b"not json\n")
        await child._reader
        return child.proc.killed, events

    killed, events = run(body())
    assert killed is True
    assert [e["ev"] for e in events] == ["exited"]


def test_a_send_the_child_cannot_write_is_a_delivery_error(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            claude = FakeClaude(rig.sessions_dir, short_tmp / "socks")
            try:
                await rig.start_thread(name="helper")
                claude.listener.close()
                with pytest.raises(ipc.IpcError) as info:
                    await rig.bridge.dispatch("send", {"target": "claude-main", "text": "hello"}, CODEX_ME)
                return info.value
            finally:
                claude.record_path.unlink()

    error = run(body())
    assert error.kind == "delivery_rejected"
    assert "claude-main" in error.message and "FileNotFoundError" in error.message


def test_a_negative_receipt_within_the_wait_is_a_delivery_error(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            claude = FakeClaude(rig.sessions_dir, short_tmp / "socks")
            try:
                await rig.start_thread(name="helper")
                sending = asyncio.create_task(rig.bridge.dispatch("send", {"target": "claude-main", "text": "hello"}, CODEX_ME))
                [frame] = await asyncio.to_thread(claude.wait_for_frames, 1, 2.0)
                # No negative receipt from a real Claude session is captured; this is the
                # shape the peer child parses, built the way the peer tests build it.
                held = {
                    "type": "control",
                    "action": "peer_message_status",
                    "orig_msg_id": frame["msg_id"],
                    "status": "held",
                    "drop_reason": "session is busy with a permission prompt",
                    "from": f"uds:{claude.sock_path}",
                    "msgV": 1,
                    "msg_id": str(uuid.uuid4()),
                }
                await asyncio.to_thread(send_frame, child_sock(rig, THREAD_ID), held)
                with pytest.raises(ipc.IpcError) as info:
                    await asyncio.wait_for(sending, 5)
                return info.value
            finally:
                claude.close()

    error = run(body())
    assert error.kind == "delivery_rejected"
    assert "held" in error.message and "permission prompt" in error.message


def test_a_codex_caller_sends_to_another_codex_thread_through_its_child(short_tmp, monkeypatch):
    from antiphon import peers

    monkeypatch.setattr(peers, "RECEIPT_WAIT", 0.3)

    async def body():
        async with Rig(short_tmp) as rig:
            claude = FakeClaude(rig.sessions_dir, short_tmp / "socks")
            try:
                await rig.start_thread(name="helper")
                rig.fake.replies["thread/start"] = second_thread_reply()
                await rig.start_thread(name="other")
                result = await rig.bridge.dispatch("send", {"target": "other", "text": "ping"}, CODEX_ME)
                turn_start = await asyncio.wait_for(rig.fake.wait_request("turn/start"), 5)
                return result, turn_start["params"], claude.frames
            finally:
                claude.close()

    result, params, claude_frames = run(body())
    assert result["kind"] == "sent"
    assert params["threadId"] == OTHER_ID
    assert params["input"] == [{"type": "text", "text": "[from helper via antiphon]\nping"}]
    assert claude_frames == []


def test_a_codex_caller_sends_directly_to_a_thread_it_spawned(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            await rig.start_thread(name="helper")
            rig.fake.replies["thread/start"] = second_thread_reply()
            await rig.start_thread(name="child", caller=CODEX_ME)
            result = await rig.bridge.dispatch("send", {"target": "child", "text": "go"}, CODEX_ME)
            return result, rig.fake.received("turn/start")[0]["params"]

    result, params = run(body())
    assert result["kind"] == "started"
    assert params["threadId"] == OTHER_ID
    assert params["input"] == [{"type": "text", "text": "go"}]


def test_a_codex_caller_outside_a_hosted_thread_cannot_send_a_labelled_message(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            claude = FakeClaude(rig.sessions_dir, short_tmp / "socks")
            try:
                await rig.start_thread(name="helper")
                with pytest.raises(ipc.IpcError) as info:
                    await rig.bridge.dispatch("send", {"target": "claude-main", "text": "x"}, CODEX_UNHOSTED)
                return info.value
            finally:
                claude.close()

    error = run(body())
    assert error.kind == "precondition"
    assert "CODEX_THREAD_ID" in error.message


def test_a_claude_caller_sending_to_a_claude_session_is_told_to_use_send_message(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            claude = FakeClaude(rig.sessions_dir, short_tmp / "socks")
            try:
                with pytest.raises(ipc.IpcError) as info:
                    await rig.bridge.dispatch("send", {"target": "claude-main", "text": "x"}, claude_caller(claude))
                return info.value
            finally:
                claude.close()

    error = run(body())
    assert error.kind == "usage"
    assert "SendMessage" in error.message


def test_notify_subscribes_to_the_peer_and_relays_its_idle_notice_as_a_turn(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            claude = FakeClaude(rig.sessions_dir, short_tmp / "socks")
            try:
                await rig.start_thread(name="helper")
                result = await rig.bridge.dispatch("notify", {"target": "claude-main"}, CODEX_ME)
                [subscribe] = await asyncio.to_thread(claude.wait_for_frames, 1, 2.0)
                notice = dict(CAPTURED_IDLE_NOTICE, orig_msg_id=subscribe["msg_id"])
                notice["from"] = f"uds:{claude.sock_path}"
                await asyncio.to_thread(send_frame, child_sock(rig, THREAD_ID), notice)
                turn_start = await asyncio.wait_for(rig.fake.wait_request("turn/start"), 5)
                return result, subscribe, turn_start["params"]
            finally:
                claude.close()

    result, subscribe, params = run(body())
    assert result == {"name": "claude-main"}
    assert subscribe["action"] == "notify_when_idle"
    assert params["threadId"] == THREAD_ID
    assert params["input"] == [{"type": "text", "text": f"Peer claude-main is idle: {CAPTURED_IDLE_NOTICE['detail']}"}]


def test_ls_rows_carry_a_sub_agents_nickname_and_role(short_tmp):
    parent = copy.deepcopy(ADOPTION.result(2))
    parent["thread"]["id"] = SUB_AGENT_PARENT
    parent["thread"]["name"] = "orchestrator"
    sub = copy.deepcopy(SUB_AGENT.result(2, occurrence=1))
    sub["thread"]["agentRole"] = "reviewer"

    def read(params):
        return {"result": sub if params["threadId"] == SUB_AGENT_ID else parent}

    async def body():
        async with Rig(short_tmp) as rig:
            rig.fake.replies["thread/read"] = read
            rig.fake.replies["thread/loaded/list"] = {"result": {"data": [SUB_AGENT_ID, SUB_AGENT_PARENT], "nextCursor": None}}
            await rig.bridge.reconcile()
            return await rig.bridge.dispatch("ls", {}, HUMAN)

    [_, row] = run(body())
    assert (row["name"], row["role"], row["parent"]) == ("Bernoulli", "reviewer", "orchestrator")
