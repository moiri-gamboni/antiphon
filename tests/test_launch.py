"""Claude Code sessions a Codex thread starts: the launch, the ownership, the messages
both ways, and the permission requests such a session routes back to its spawner.

Nothing here starts a real Claude Code session or takes a real Codex turn: the launcher
is injected, the Codex side is the fake daemon, and the stand-in writes the registry
record a background session writes for itself, replayed from `claude-bg-session.jsonl`
and backed by a real process so the record is live and passes the registry pins.
"""

import asyncio
import json
import os
import subprocess
import time
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

    async def __call__(self, spec: launch.Spec) -> launch.Launched:
        self.specs.append(spec)
        if self.fails:
            raise launch.LaunchFailed(self.fails)
        if self.register:
            self.sessions.append(StandInSession(self.sessions_dir, self.sock_dir, spec.name, spec.session_id))
        return launch.Launched(argv=launch.claude_argv(spec), stdout=f"backgrounded · {spec.session_id[:8]} · {spec.name}\n")

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
                             launch_claude=self.launcher, stop_claude=self.stopped.append)
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
    fields = dict(session_id="1ec2f3d4-0000-4000-8000-000000000001", name="helper", cwd="/work",
                  model=None, hook=None, gate=None)
    fields.update(overrides)
    return launch.Spec(**fields)


def test_the_command_starts_a_background_session_under_the_given_id_and_name():
    argv = launch.claude_argv(spec(model="sonnet", prompt="read the diff"))
    assert argv[0] == "claude"
    assert "--bg" in argv
    assert argv[argv.index("--session-id") + 1] == "1ec2f3d4-0000-4000-8000-000000000001"
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
    assert started.session_id == result["session_id"]
    assert record is not None and record.name == "helper"
    assert result["pid"] == record.pid


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
        async def __call__(self, started: launch.Spec) -> launch.Launched:
            self.specs.append(started)
            session = StandInSession(self.sessions_dir, self.sock_dir, "tmp-45", started.session_id,
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
            session.set_status("busy")
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


def test_wait_on_a_session_that_is_already_idle_returns_at_once(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            await rig.start_claude()
            return await rig.bridge.dispatch("wait", {"target": "helper", "timeout": 30}, rig.caller)

    assert run(body()) == {"status": "idle", "final": None, "name": "helper"}


def test_wait_on_a_session_that_stays_busy_times_out(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            await rig.start_claude()
            rig.launcher.sessions[0].set_status("busy")
            with pytest.raises(IpcError) as timed_out:
                await rig.bridge.dispatch("wait", {"target": "helper", "timeout": 0.2}, rig.caller)
            return timed_out.value

    error = run(body())
    assert error.kind == "timeout"


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
