"""Claude Code sessions a Codex thread starts: the launch, the ownership, the messages
both ways, and the permission requests such a session routes back to its spawner.

Nothing here starts a real Claude Code session or takes a real Codex turn: the launcher
is injected, the Codex side is the fake daemon, and the stand-in writes the registry
record a background session writes for itself, replayed from `claude-bg-session.jsonl`
and backed by a real process so the record is live and passes the registry pins.
"""

import asyncio
import dataclasses
import json
import os
import subprocess
import time
import uuid
from pathlib import Path

import pytest

from antiphon.bridge import Bridge
from antiphon.callers import Caller
from antiphon.claude import launch, registry
from antiphon.ipc import IpcError
from antiphon.state import ThreadState
from fake_claude import FakeClaude, Listener, captured_events, send_frame
from fake_daemon import FakeDaemon, load_fixture

REPO = Path(__file__).parent.parent
FORWARD_HOOK = REPO / "hooks" / "claude-permission-forward.sh"
BG_RECORD = captured_events("claude-bg-session.jsonl")[0]["data"]

THREAD_START = load_fixture("thread-start.jsonl")
APPROVAL = load_fixture("user-reviewer-request-approval.jsonl")
SPAWNER_THREAD = THREAD_START.result(2)["thread"]["id"]
TURN_STARTED = APPROVAL.result(3)


def run(coro):
    return asyncio.run(coro)


async def until(condition, timeout: float = 5):
    async def poll():
        while not condition():
            await asyncio.sleep(0.01)

    await asyncio.wait_for(poll(), timeout)


class StandInSession:
    """The registry record and messaging socket a background Claude Code session writes
    for itself, backed by a real process so several can be live at once."""

    def __init__(self, sessions_dir: Path, sock_dir: Path, name: str, session_id: str,
                 *, name_source: str = "peer"):
        self.proc = subprocess.Popen(["sleep", "300"])
        self.pid = self.proc.pid
        self.sock_path = str(sock_dir / f"{self.pid}.sock")
        self.listener = Listener(self.sock_path)
        now = int(time.time() * 1000)
        self.record = dict(
            BG_RECORD, pid=self.pid, sessionId=session_id, cwd=os.getcwd(), startedAt=now,
            procStart=registry.proc_start(self.pid), messagingSocketPath=self.sock_path,
            name=name, nameSource=name_source, nameSince=now, jobId=session_id[:8],
            updatedAt=now, statusUpdatedAt=now,
        )
        self.record_path = sessions_dir / f"{self.pid}.json"
        self.record_path.write_text(json.dumps(self.record))

    def set_status(self, status: str) -> None:
        self.record.update(status=status, statusUpdatedAt=int(time.time() * 1000))
        self.record_path.write_text(json.dumps(self.record))

    def rename(self, name: str, name_source: str) -> None:
        self.record.update(name=name, nameSource=name_source, updatedAt=int(time.time() * 1000))
        self.record_path.write_text(json.dumps(self.record))

    def close(self) -> None:
        self.listener.close()
        self.record_path.unlink(missing_ok=True)
        self.proc.terminate()
        self.proc.wait()


class FakeLauncher:
    """Stands in for `claude --bg`: records what it was asked to start, and registers a
    stand-in session for it."""

    def __init__(self, sessions_dir: Path, sock_dir: Path, *, register: bool = True, fails: str | None = None):
        self.sessions_dir = sessions_dir
        self.sock_dir = sock_dir
        self.register = register
        self.fails = fails
        self.specs: list[launch.Spec] = []
        self.sessions: list[StandInSession] = []

    async def __call__(self, spec: launch.Spec, rawlog) -> launch.Launched:
        self.specs.append(spec)
        rawlog.log("out", "claude", {"argv": launch.claude_argv(spec), "cwd": spec.cwd})
        if self.fails:
            raise launch.LaunchFailed(self.fails)
        # `claude --bg` assigns the session its own id and takes the job id from that;
        # only the name it was given comes back (observed live, claude-bg-session.jsonl).
        assigned = str(uuid.uuid4())
        if self.register:
            self.sessions.append(StandInSession(self.sessions_dir, self.sock_dir, spec.name, assigned))
        return launch.Launched(argv=launch.claude_argv(spec), stdout=f"backgrounded · {assigned[:8]} · {spec.name}\n")

    def close(self) -> None:
        for session in self.sessions:
            session.close()


class Rig:
    """A bridge with one hosted Codex thread as the starting caller, a fake Codex daemon,
    and a Claude Code session already running so the bridge can register peers at all."""

    def __init__(self, short_tmp: Path, **launcher_options):
        self.tmp = short_tmp
        self.home = short_tmp / "antiphon"
        self.sessions_dir = short_tmp / "cc" / "sessions"
        self.sock_dir = short_tmp / "socks"
        self.sessions_dir.mkdir(parents=True)
        self.sock_dir.mkdir(parents=True)
        self.codex_home = short_tmp / "codex"
        self.daemon_sock = self.codex_home / "app-server-control" / "app-server-control.sock"
        self.daemon_sock.parent.mkdir(parents=True)
        self.fake = FakeDaemon(self.daemon_sock)
        self.fake.replies.update({
            "turn/start": {"result": TURN_STARTED},
            "thread/loaded/list": {"result": {"data": [], "nextCursor": None}},
            "thread/resume": {"result": THREAD_START.result(2)},
            "thread/turns/list": {"result": {"data": [], "nextCursor": None, "backwardsCursor": None}},
        })
        self.launcher = FakeLauncher(self.sessions_dir, self.sock_dir, **launcher_options)
        self.stopped: list[str] = []
        self.human = FakeClaude(self.sessions_dir, self.sock_dir, name="claude-main")
        self.bridge = Bridge(self.home, sessions_dir=self.sessions_dir, codex_home=self.codex_home,
                             ensure_running=lambda codex_home, rawlog=None: str(self.daemon_sock),
                             launch_claude=self.launcher,
                             stop_claude=lambda job_id, rawlog: self.stopped.append(job_id))
        self.bridge.reconcile_interval = 3600
        self.bridge.reconnect_backoff = (0.05, 0.2)
        self.thread = ThreadState(thread_id=SPAWNER_THREAD, name="codex-spawner", cwd=str(short_tmp),
                                  origin="spawned", spawner="human", read_only=False)
        self.bridge.state.threads[SPAWNER_THREAD] = self.thread
        self.caller = Caller(kind="codex", claude_pid=None, claude_session_id=None, codex_thread=SPAWNER_THREAD)

    async def __aenter__(self):
        await self.fake.start()
        await self.bridge.start()
        await until(lambda: self.bridge.last_reconcile is not None)
        await until(lambda: self.thread.child_pid is not None)
        return self

    async def __aexit__(self, *exc):
        await self.bridge.close()
        await self.fake.stop()
        self.launcher.close()
        self.human.close()

    @property
    def child_sock(self) -> str:
        return str(self.sock_dir / f"{self.thread.child_pid}.sock")

    async def start_claude(self, **overrides) -> dict:
        args = {"cwd": str(self.tmp), "name": "helper"}
        args.update(overrides)
        return await self.bridge.dispatch("start_claude", args, self.caller)


# --- the command ------------------------------------------------------------------------


def spec(**overrides) -> launch.Spec:
    fields = dict(name="helper", cwd="/work", model=None, hook=None, gate=None)
    fields.update(overrides)
    return launch.Spec(**fields)


def test_the_command_starts_a_background_session_under_the_given_name():
    argv = launch.claude_argv(spec(model="sonnet", prompt="read the diff"))
    assert argv[0] == "claude"
    assert "--bg" in argv
    # No --session-id: a background session assigns its own and ignores one it is given,
    # so asking for an id would only invite matching the record on something it ignores.
    assert "--session-id" not in argv
    assert argv[argv.index("--name") + 1] == "helper"
    assert argv[argv.index("--model") + 1] == "sonnet"
    assert argv[-1] == "read the diff"
    assert "--settings" not in argv


def test_the_forward_hook_is_installed_into_the_started_session_alone():
    argv = launch.claude_argv(spec(hook="/repo/hooks/forward.sh", gate="Bash"))
    settings = json.loads(argv[argv.index("--settings") + 1])
    [group] = settings["hooks"]["PreToolUse"]
    assert group["matcher"] == "Bash"
    [handler] = group["hooks"]
    assert handler["type"] == "command"
    assert handler["timeout"] == launch.HOOK_TIMEOUT
    # The hook is told how long it has, so its answer is printed before Claude Code gives up.
    assert handler["command"] == f"/repo/hooks/forward.sh {launch.HOOK_TIMEOUT - launch.ASK_MARGIN}"


def test_without_a_gate_the_hook_applies_to_every_tool():
    argv = launch.claude_argv(spec(hook="/repo/hooks/forward.sh", gate=None))
    [group] = json.loads(argv[argv.index("--settings") + 1])["hooks"]["PreToolUse"]
    assert "matcher" not in group


def test_instructions_are_appended_to_the_sessions_system_prompt():
    argv = launch.claude_argv(spec(instructions="You trace execution paths.", prompt="read the diff"))
    assert argv[argv.index("--append-system-prompt") + 1] == "You trace execution paths."
    assert argv[-2:] == ["--", "read the diff"]
    assert "--append-system-prompt" not in launch.claude_argv(spec())


def test_the_shipped_forward_hook_is_found_and_runnable():
    found = launch.forward_hook()
    assert found is not None and os.access(found, os.X_OK)


# --- the launch -------------------------------------------------------------------------


def test_start_claude_records_the_spawner_and_reports_the_registered_session(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            result = await rig.start_claude()
            record = registry.resolve_session(result["session_id"], rig.sessions_dir)
            return result, rig.bridge.state.sessions[result["session_id"]], rig.launcher.specs[0], record

    result, session, started, record = run(body())
    assert result["name"] == "helper"
    assert session.spawner == SPAWNER_THREAD
    assert session.cwd == str(short_tmp)
    assert session.job_id == result["session_id"][:8]
    # The id comes back from the record the session wrote, not from anything asked for.
    assert started.name == result["name"]
    assert record is not None and record.name == "helper"
    assert result["pid"] == record.pid


def test_start_claude_hands_the_instructions_to_the_launch(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            await rig.start_claude(instructions="You trace execution paths.")
            return rig.launcher.specs[0]

    assert run(body()).instructions == "You trace execution paths."


def test_start_claude_from_a_claude_session_is_a_usage_error(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            caller = Caller(kind="claude", claude_pid=os.getpid(), claude_session_id="s-1", codex_thread=None)
            with pytest.raises(IpcError) as raised:
                await rig.bridge.dispatch("start_claude", {"cwd": str(short_tmp)}, caller)
            return raised.value

    error = run(body())
    assert error.kind == "usage"
    assert "Agent tool" in error.message


def test_a_session_that_never_registers_is_reported_with_the_command_and_its_output(short_tmp):
    async def body():
        async with Rig(short_tmp, register=False) as rig:
            rig.bridge.register_timeout = 0.2
            with pytest.raises(IpcError) as raised:
                await rig.start_claude()
            return raised.value, dict(rig.bridge.state.sessions)

    error, sessions = run(body())
    assert error.kind == "precondition"
    assert "did not register" in error.message
    assert "claude --bg" in error.message and "backgrounded" in error.message
    assert sessions == {}


def test_a_command_that_refuses_to_start_is_reported_as_a_precondition(short_tmp):
    async def body():
        async with Rig(short_tmp, fails="claude --bg exited 1: not logged in") as rig:
            with pytest.raises(IpcError) as raised:
                await rig.start_claude()
            return raised.value, dict(rig.bridge.state.sessions)

    error, sessions = run(body())
    assert error.kind == "precondition"
    assert "not logged in" in error.message
    assert sessions == {}


def test_the_requested_name_is_made_unique_against_the_peers_already_listed(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            taken = StandInSession(rig.sessions_dir, rig.sock_dir, "helper", "another-session")
            try:
                result = await rig.start_claude(name="helper")
            finally:
                taken.close()
            return result, rig.launcher.specs[0]

    result, started = run(body())
    assert result["name"] == "helper-2"
    assert started.name == "helper-2"


def test_the_name_the_session_derives_before_its_own_is_not_taken_for_it(short_tmp):
    """A session's first record carries a name derived from its directory; the name it was
    given lands a moment later (observed). Reading the first record would misname the peer."""

    class LateNamer(FakeLauncher):
        async def __call__(self, started: launch.Spec, rawlog) -> launch.Launched:
            self.specs.append(started)
            session = StandInSession(self.sessions_dir, self.sock_dir, "tmp-45", str(uuid.uuid4()),
                                     name_source="derived")
            self.sessions.append(session)
            asyncio.get_running_loop().call_later(0.2, session.rename, started.name, "peer")
            return launch.Launched(argv=launch.claude_argv(started), stdout="backgrounded\n")

    async def body():
        async with Rig(short_tmp) as rig:
            rig.bridge._launch_claude = LateNamer(rig.sessions_dir, rig.sock_dir)
            rig.launcher = rig.bridge._launch_claude
            return await rig.start_claude(name="helper")

    assert run(body())["name"] == "helper"


# --- messages both ways -------------------------------------------------------------------


def message_text(frame: dict) -> str:
    content = frame["message"]["content"]
    return content.split(">\n", 1)[1].rsplit("\n</cross-session-message>", 1)[0]


def test_a_send_from_the_spawner_arrives_as_a_cross_session_message(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            await rig.start_claude()
            session = rig.launcher.sessions[0]
            result = await rig.bridge.dispatch("send", {"target": "helper", "text": "look at the diff"}, rig.caller)
            frames = await asyncio.to_thread(session.listener.wait_for_frames, 1, 2.0)
            return result, frames

    result, frames = run(body())
    assert result["kind"] == "sent" and result["name"] == "helper"
    [frame] = frames
    assert frame["type"] == "user"
    assert message_text(frame) == "look at the diff"
    assert 'from-name="codex-spawner"' in frame["message"]["content"]


def test_the_sessions_reply_reaches_the_spawning_thread_as_a_turn(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            await rig.start_claude()
            session = rig.launcher.sessions[0]
            send_frame(rig.child_sock, {
                "msgV": 1, "msg_id": "9d0b1a6c-0000-4000-8000-000000000001", "type": "user",
                "message": {"role": "user", "content":
                            f'<cross-session-message from="uds:{session.sock_path}" from-name="helper" '
                            f'from-mode="prompting">\nthe diff looks fine\n</cross-session-message>'},
                "priority": "next", "from": f"uds:{session.sock_path}",
            })
            return await asyncio.wait_for(rig.fake.wait_request("turn/start"), 5)

    turn = run(body())
    assert turn["params"]["input"] == [{"type": "text", "text": "[from helper via antiphon]\nthe diff looks fine"}]


# --- the session verbs ---------------------------------------------------------------------


def test_ls_shows_a_started_session_with_the_thread_that_started_it(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            await rig.start_claude()
            return await rig.bridge.dispatch("ls", {}, rig.caller)

    rows = {row["name"]: row for row in run(body())}
    assert rows["helper"]["kind"] == "claude"
    assert rows["helper"]["spawner"] == SPAWNER_THREAD
    assert rows["claude-main"]["spawner"] is None


def test_status_of_a_started_session_names_its_spawner_and_its_job(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            started = await rig.start_claude()
            return started, await rig.bridge.dispatch("status", {"target": "helper"}, rig.caller)

    started, status = run(body())
    assert status["kind"] == "claude"
    assert status["session_id"] == started["session_id"]
    assert status["spawner"] == SPAWNER_THREAD
    assert status["job_id"] == started["job_id"]
    assert status["pending"] == []


def test_stop_ends_the_session_through_claude_and_forgets_it(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            started = await rig.start_claude()
            result = await rig.bridge.dispatch("stop", {"target": "helper"}, rig.caller)
            return started, result, rig.stopped, dict(rig.bridge.state.sessions)

    started, result, stopped, sessions = run(body())
    assert result["name"] == "helper"
    assert stopped == [started["job_id"]]
    assert sessions == {}


def test_a_session_started_by_another_thread_may_not_be_stopped(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            await rig.start_claude()
            other = Caller(kind="codex", claude_pid=None, claude_session_id=None, codex_thread="someone-else")
            with pytest.raises(IpcError) as refused:
                await rig.bridge.dispatch("stop", {"target": "helper"}, other)
            return refused.value, rig.stopped

    error, stopped = run(body())
    assert error.kind == "forbidden"
    assert stopped == []


def test_interrupt_says_a_claude_session_has_no_such_surface(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            await rig.start_claude()
            with pytest.raises(IpcError) as refused:
                await rig.bridge.dispatch("interrupt", {"target": "helper"}, rig.caller)
            return refused.value

    error = run(body())
    assert error.kind == "usage"
    assert "no way to interrupt" in error.message


def test_wait_on_a_session_returns_when_its_idle_notice_arrives(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            await rig.start_claude()
            session = rig.launcher.sessions[0]
            waiting = asyncio.create_task(rig.bridge.dispatch("wait", {"target": "helper", "timeout": 5}, rig.caller))
            subscription = (await asyncio.to_thread(session.listener.wait_for_frames, 1, 2.0))[0]
            send_frame(rig.child_sock, {
                "type": "control", "action": "peer_idle_notice", "orig_msg_id": subscription["msg_id"],
                "state": "idle", "finished_at": 0, "detail": "the diff is fine",
                "from": f"uds:{session.sock_path}", "from_mode": "prompting",
                "msgV": 1, "msg_id": "9d0b1a6c-0000-4000-8000-000000000002",
            })
            return subscription, await asyncio.wait_for(waiting, 5)

    subscription, result = run(body())
    assert subscription["action"] == "notify_when_idle"
    assert result == {"status": "idle", "final": "the diff is fine", "name": "helper"}


def test_a_wait_that_times_out_leaves_its_subscription_to_arrive_as_a_notice(short_tmp):
    """A subscription cannot be withdrawn, so the one a timed-out wait made must be handed
    to `notify` rather than left to arrive as a turn from nobody."""

    async def body():
        async with Rig(short_tmp) as rig:
            await rig.start_claude()
            session = rig.launcher.sessions[0]
            with pytest.raises(IpcError) as timed_out:
                await rig.bridge.dispatch("wait", {"target": "helper", "timeout": 0.2}, rig.caller)
            subscription = (await asyncio.to_thread(session.listener.wait_for_frames, 1, 2.0))[0]
            send_frame(rig.child_sock, {
                "type": "control", "action": "peer_idle_notice", "orig_msg_id": subscription["msg_id"],
                "state": "idle", "finished_at": 0, "detail": "done after all",
                "from": f"uds:{session.sock_path}", "from_mode": "prompting",
                "msgV": 1, "msg_id": "9d0b1a6c-0000-4000-8000-000000000004",
            })
            turn = await asyncio.wait_for(rig.fake.wait_request("turn/start"), 5)
            return timed_out.value, turn["params"]["input"][0]["text"]

    error, relayed = run(body())
    assert error.kind == "timeout"
    assert "antiphon notify" in error.message
    assert relayed == "Peer helper is idle: done after all"


# --- the forward hook ------------------------------------------------------------------------


def hook_input(session_id: str, command: str = "rm -rf /tmp/scratch") -> str:
    # The PreToolUse input shape from the Claude Code hooks reference.
    return json.dumps({
        "session_id": session_id, "transcript_path": "/dev/null", "cwd": "/work",
        "permission_mode": "default", "hook_event_name": "PreToolUse", "tool_name": "Bash",
        "tool_input": {"command": command, "description": "clean up"}, "tool_use_id": "toolu_01",
    })


def run_forward_hook(home: Path, payload: str, budget: str = "5") -> subprocess.CompletedProcess:
    return subprocess.run([str(FORWARD_HOOK), budget], input=payload, capture_output=True, text=True,
                          env={**os.environ, "ANTIPHON_HOME": str(home)})


def decision_of(done: subprocess.CompletedProcess) -> dict:
    assert done.returncode == 0, done.stderr
    return json.loads(done.stdout)["hookSpecificOutput"]


def test_the_tools_a_session_answers_its_spawner_with_are_not_held(short_tmp):
    """Holding every tool holds the session's own reply: answering a spawner costs a peer
    listing, a tool lookup and the send itself, so a gate over all of them makes the
    session unable to say anything without three approvals (observed live)."""
    async def body():
        async with Rig(short_tmp) as rig:
            started = await rig.start_claude()
            answers = {}
            for tool in ("SendMessage", "ListAgents", "ToolSearch"):
                payload = json.loads(hook_input(started["session_id"]))
                payload["tool_name"] = tool
                payload["tool_input"] = {"to": "codex-spawner", "message": "done"}
                done = await asyncio.to_thread(run_forward_hook, rig.home, json.dumps(payload), "5")
                answers[tool] = done
            return answers, rig.bridge.state.sessions[started["session_id"]].pending

    answers, pending = run(body())
    for tool, done in answers.items():
        # No decision at all: the session's normal permission rules apply, as if no hook ran.
        assert done.returncode == 0, (tool, done.stderr)
        assert done.stdout.strip() == "", (tool, done.stdout)
    assert pending == []  # and nothing was put to the spawner


def test_the_hook_forwards_the_call_to_the_spawner_and_returns_its_approval(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            started = await rig.start_claude()
            hooking = asyncio.create_task(
                asyncio.to_thread(run_forward_hook, rig.home, hook_input(started["session_id"])))
            turn = await asyncio.wait_for(rig.fake.wait_request("turn/start"), 5)
            text = turn["params"]["input"][0]["text"]
            token = text.split("(token ", 1)[1][:6]
            await rig.bridge.dispatch("approve", {"token": token}, rig.caller)
            return text, token, await asyncio.wait_for(hooking, 10)

    text, token, done = run(body())
    assert text.startswith(f'Permission needed: the Claude Code session "helper" asks before an action runs (token {token})')
    assert "command: rm -rf /tmp/scratch" in text
    assert f"antiphon approve {token}" in text
    assert decision_of(done) == {"hookEventName": "PreToolUse", "permissionDecision": "allow"}


def test_the_hook_returns_the_spawners_denial_with_its_reason(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            started = await rig.start_claude()
            hooking = asyncio.create_task(
                asyncio.to_thread(run_forward_hook, rig.home, hook_input(started["session_id"])))
            await until(lambda: rig.bridge.state.sessions[started["session_id"]].pending)
            token = rig.bridge.state.sessions[started["session_id"]].pending[0]["token"]
            await rig.bridge.dispatch("deny", {"token": token, "why": "that path is not yours"}, rig.caller)
            return await asyncio.wait_for(hooking, 10)

    out = decision_of(run(body()))
    assert out["permissionDecision"] == "deny"
    assert out["permissionDecisionReason"] == "that path is not yours"


def test_an_unanswered_call_is_denied_when_the_budget_runs_out(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            started = await rig.start_claude()
            done = await asyncio.wait_for(
                asyncio.to_thread(run_forward_hook, rig.home, hook_input(started["session_id"]), "1"), 15)
            return done, rig.bridge.state.sessions[started["session_id"]].pending

    done, pending = run(body())
    out = decision_of(done)
    assert out["permissionDecision"] == "deny"
    assert "antiphon approve" in out["permissionDecisionReason"]
    assert pending == []


def test_with_no_bridge_to_ask_the_call_is_denied(short_tmp):
    out = decision_of(run_forward_hook(short_tmp / "nowhere", hook_input("any-session")))
    assert out["permissionDecision"] == "deny"
    assert "could not put this to" in out["permissionDecisionReason"]


def test_a_call_from_a_session_antiphon_did_not_start_is_denied(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            return await asyncio.to_thread(run_forward_hook, rig.home, hook_input("a-session-nobody-started"))

    out = decision_of(run(body()))
    assert out["permissionDecision"] == "deny"
    assert "antiphon hosts" in out["permissionDecisionReason"]


def test_a_tool_input_too_large_for_an_environment_variable_still_reaches_the_bridge(short_tmp):
    """A Write of a whole file is an ordinary tool input, and the one most worth gating."""
    payload = json.loads(hook_input("x"))
    payload["tool_name"] = "Write"
    payload["tool_input"] = {"file_path": "/work/big.txt", "content": "A" * 400_000}

    async def body():
        async with Rig(short_tmp) as rig:
            started = await rig.start_claude()
            payload["session_id"] = started["session_id"]
            hooking = asyncio.create_task(
                asyncio.to_thread(run_forward_hook, rig.home, json.dumps(payload)))
            await until(lambda: rig.bridge.state.sessions[started["session_id"]].pending)
            record = rig.bridge.state.sessions[started["session_id"]].pending[0]
            await rig.bridge.dispatch("deny", {"token": record["token"], "why": "too big"}, rig.caller)
            return record, await asyncio.wait_for(hooking, 15)

    record, done = run(body())
    assert record["command"].startswith('{"file_path"')
    assert decision_of(done)["permissionDecisionReason"] == "too big"


def test_the_hook_denies_a_document_it_cannot_read(short_tmp):
    """Claude Code lets a call through when a hook exits non-zero with no JSON, so an input
    this hook cannot read has to come out as a deny rather than a crash."""
    for payload in ("", "not json", '{"tool_name": "Bash"}'):
        out = decision_of(run_forward_hook(short_tmp / "nowhere", payload))
        assert out["permissionDecision"] == "deny"
        assert "could not put this to" in out["permissionDecisionReason"]


def test_the_hook_runs_as_the_shell_command_the_settings_carry(short_tmp):
    """Claude Code runs a hook's `command` string through a shell, not as an argv list."""
    settings = json.loads(launch.hook_settings(launch.forward_hook(), "Bash"))
    [handler] = settings["hooks"]["PreToolUse"][0]["hooks"]
    done = subprocess.run(["sh", "-c", handler["command"]], input=hook_input("nobody"),
                          capture_output=True, text=True,
                          env={**os.environ, "ANTIPHON_HOME": str(short_tmp / "nowhere")})
    assert decision_of(done)["permissionDecision"] == "deny"


def test_a_state_file_an_older_bridge_wrote_still_loads(short_tmp):
    """The pending record's owner field was renamed; a bridge that cannot read an old record
    forgets it rather than refusing to start at all."""
    home = short_tmp / "antiphon"
    (home / "log").mkdir(parents=True)
    thread = dataclasses.asdict(ThreadState(thread_id="t1", name="helper", cwd="/work", origin="spawned",
                                            spawner="s", read_only=False))
    thread["pending"] = [{
        "token": "aaaaaa", "thread_id": "t1", "turn_id": None, "command": "x", "cwd": "/work",
        "rationale": "r", "risk_level": None, "review_completed_params": None, "since": 0.0,
        "resolved": False, "kind": "request", "request_id": 3, "epoch": 1,
        "available_decisions": ["accept"], "reminded": False, "hook": None,
    }]
    (home / "state.json").write_text(json.dumps({"v": 1, "threads": {"t1": thread}, "stopped": {}, "degraded": []}))

    bridge = Bridge(home, sessions_dir=short_tmp / "sessions", codex_home=short_tmp / "codex",
                    ensure_running=lambda codex_home, rawlog=None: str(short_tmp / "nowhere.sock"))
    assert list(bridge.state.threads) == ["t1"]
    assert bridge.state.sessions == {}
    assert bridge.state.threads["t1"].pending == []


# --- what the review found -------------------------------------------------------------


def test_wait_does_not_call_a_session_idle_before_it_has_reacted_to_the_prompt(short_tmp):
    """A session's record still says idle for a moment after it is given work. Reporting
    that as a finished turn would hand the caller an answer that never happened."""

    async def body():
        async with Rig(short_tmp) as rig:
            await rig.start_claude(prompt="do the thing")
            session = rig.launcher.sessions[0]
            waiting = asyncio.create_task(rig.bridge.dispatch("wait", {"target": "helper", "timeout": 5}, rig.caller))
            subscription = (await asyncio.to_thread(session.listener.wait_for_frames, 1, 2.0))[0]
            send_frame(rig.child_sock, {
                "type": "control", "action": "peer_idle_notice", "orig_msg_id": subscription["msg_id"],
                "state": "idle", "finished_at": 0, "detail": "the thing is done",
                "from": f"uds:{session.sock_path}", "from_mode": "prompting",
                "msgV": 1, "msg_id": "9d0b1a6c-0000-4000-8000-000000000003",
            })
            return await asyncio.wait_for(waiting, 5)

    assert run(body()) == {"status": "idle", "final": "the thing is done", "name": "helper"}


def test_wait_from_a_terminal_names_what_a_terminal_can_do(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            await rig.start_claude()
            human = Caller(kind="human", claude_pid=None, claude_session_id=None, codex_thread=None)
            with pytest.raises(IpcError) as refused:
                await rig.bridge.dispatch("wait", {"target": "helper", "timeout": 5}, human)
            return refused.value

    error = run(body())
    assert error.kind == "usage"
    assert "antiphon status" in error.message


def test_reconcile_forgets_a_session_that_is_no_longer_running(short_tmp):
    """Nothing else removes one: a session stopped by hand or lost to a reboot would keep
    its name reserved and keep being reported as live."""

    async def body():
        async with Rig(short_tmp) as rig:
            started = await rig.start_claude()
            rig.launcher.sessions[0].close()
            rig.launcher.sessions.clear()
            await rig.bridge.reconcile()
            return started, dict(rig.bridge.state.sessions)

    started, sessions = run(body())
    assert started["name"] == "helper"
    assert sessions == {}


def test_a_new_session_may_take_the_name_of_one_that_has_gone(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            await rig.start_claude()
            rig.launcher.sessions[0].close()
            rig.launcher.sessions.clear()
            await rig.bridge.reconcile()
            again = await rig.start_claude()
            status = await rig.bridge.dispatch("status", {"target": "helper"}, rig.caller)
            return again, status

    again, status = run(body())
    assert again["name"] == "helper"
    assert status["session_id"] == again["session_id"]


def test_a_prompt_that_begins_with_a_dash_is_not_read_as_a_flag():
    argv = launch.claude_argv(spec(prompt="--no-report is not a flag here"))
    assert argv[-2:] == ["--", "--no-report is not a flag here"]


def test_the_spawner_is_reminded_about_a_session_that_has_waited_long_enough(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            now = [1_000_000.0]
            rig.bridge.approvals.clock = lambda: now[0]
            started = await rig.start_claude()
            hooking = asyncio.create_task(
                asyncio.to_thread(run_forward_hook, rig.home, hook_input(started["session_id"]), "30"))
            first = await asyncio.wait_for(rig.fake.wait_request("turn/start"), 5)
            now[0] += 600.0
            await rig.bridge.reconcile()
            await rig.bridge.reconcile()
            token = rig.bridge.state.sessions[started["session_id"]].pending[0]["token"]
            await rig.bridge.dispatch("deny", {"token": token, "why": "no"}, rig.caller)
            await asyncio.wait_for(hooking, 15)
            reminders = [r["params"]["input"][0]["text"] for r in rig.fake.received("turn/start")
                         if r["params"]["input"][0]["text"].startswith("Still waiting")]
            return first, token, reminders

    first, token, reminders = run(body())
    assert first["params"]["input"][0]["text"].startswith("Permission needed:")
    assert len(reminders) == 1
    assert reminders[0].startswith(f'Still waiting: the Claude Code session "helper" has blocked an action for 10m (token {token})')
