"""The CLI over the real bridge (started lazily as a subprocess) over the fake daemon."""

import copy
import json
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

from antiphon import cli, ipc
from fake_claude import FakeClaude
from fake_daemon import FakeDaemonThread, load_fixture

THREAD_START = load_fixture("thread-start.jsonl")
APPROVAL = load_fixture("user-reviewer-request-approval.jsonl")
TURNS_LIST = load_fixture("turns-list.jsonl")
RESTART = load_fixture("daemon-restart.jsonl")
HOOK_ORDER = load_fixture("permission-hook-order.jsonl")

THREAD_ID = THREAD_START.result(2)["thread"]["id"]
TURN_STARTED = APPROVAL.result(3)
TURN_ID = TURN_STARTED["turn"]["id"]
COMPLETED_TURN = TURNS_LIST.result(3)["data"][0]
FAILED_TURN = [n for n in HOOK_ORDER.notifications("turn/completed") if n["turn"]["status"] == "failed"][0]
COMPLETED_NOTICE = [n for n in HOOK_ORDER.notifications("turn/completed") if n["turn"]["status"] == "completed"][0]


def for_thread(params: dict, thread_id: str = THREAD_ID) -> dict:
    params = copy.deepcopy(params)
    params["threadId"] = thread_id
    return params


def default_replies():
    return {
        "thread/start": {"result": THREAD_START.result(2)},
        "thread/name/set": {"result": {}},
        "thread/turns/list": {"result": {"data": [COMPLETED_TURN], "nextCursor": None, "backwardsCursor": None}},
        "turn/start": {"result": TURN_STARTED},
        "turn/steer": {"result": {"turnId": TURN_ID}},
        "turn/interrupt": {"result": {}},
        "thread/loaded/list": {"result": {"data": [], "nextCursor": None}},
        "thread/resume": {"result": RESTART.result(6)},
        "thread/unsubscribe": {"result": {}},
    }


class CliRig:
    """A temporary home with the fake daemon listening where the bridge expects it, a
    stub `codex` on PATH that fails (so nothing ever starts the real daemon), and the
    environment the CLI and the bridge it spawns read."""

    def __init__(self, short_tmp: Path, monkeypatch):
        self.tmp = short_tmp
        self.home = short_tmp / "antiphon"
        self.config_dir = short_tmp / "cc"
        self.codex_home = short_tmp / "codex"
        self.daemon_sock = self.codex_home / "app-server-control" / "app-server-control.sock"
        self.daemon_sock.parent.mkdir(parents=True)
        bin_dir = short_tmp / "bin"
        bin_dir.mkdir()
        stub = bin_dir / "codex"
        stub.write_text("#!/bin/sh\necho 'stub codex: refusing to start a daemon' >&2\nexit 1\n")
        stub.chmod(0o755)
        monkeypatch.setenv("ANTIPHON_HOME", str(self.home))
        monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(self.config_dir))
        monkeypatch.setenv("CODEX_HOME", str(self.codex_home))
        monkeypatch.setenv("PATH", f"{bin_dir}:{os.environ['PATH']}")
        monkeypatch.delenv("CODEX_THREAD_ID", raising=False)
        self.daemon = FakeDaemonThread(self.daemon_sock)
        self.daemon.replies.update(default_replies())
        self.daemon.start()

    def run(self, *argv: str, capsys=None) -> tuple[int, str, str]:
        """The CLI in this process; stdout and stderr captured by pytest."""
        code = cli.main(list(argv))
        out, err = capsys.readouterr() if capsys else ("", "")
        return code, out, err

    def run_subprocess(self, *argv: str, env: dict | None = None) -> subprocess.CompletedProcess:
        return subprocess.run([sys.executable, "-m", "antiphon", *argv], capture_output=True, text=True, env=dict(os.environ, **(env or {})))

    def bridge_pid(self) -> int:
        return ipc.call(str(self.home / "bridge.sock"), "ping", {})["pid"]

    def close(self) -> None:
        try:
            pid = self.bridge_pid()
        except ipc.BridgeUnreachable:
            pid = None
        if pid is not None:
            os.kill(pid, signal.SIGKILL)
        # A bridge started from this process is its child: reap it, or it lingers as a
        # zombie that still answers `kill -0`. One started by a CLI subprocess belongs to init.
        for proc in cli._spawned:
            proc.wait(timeout=5)
        cli._spawned.clear()
        if pid is not None:
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                try:
                    os.kill(pid, 0)
                except ProcessLookupError:
                    break
                time.sleep(0.02)
        self.daemon.stop()


@pytest.fixture
def rig(short_tmp, monkeypatch):
    r = CliRig(short_tmp, monkeypatch)
    yield r
    r.close()


# --- lazy start -----------------------------------------------------------------


def test_lazy_start_spawns_the_bridge_and_reports_when_it_never_answers(rig, monkeypatch, capsys):
    spawned = []
    monkeypatch.setattr(cli, "spawn_bridge", lambda home: spawned.append(home))
    monkeypatch.setattr(cli, "START_TIMEOUT", 0.3)
    (rig.home / "log").mkdir(parents=True)
    (rig.home / "log" / "bridge.out").write_text("".join(f"line {n}\n" for n in range(1, 26)))

    code, out, err = rig.run("ping", capsys=capsys)
    assert code == 1
    assert spawned == [rig.home]
    assert "line 6\n" in err and "line 25\n" in err and "line 5\n" not in err
    assert "did not answer" in err


def test_lazy_start_brings_up_a_real_bridge_and_ping_reports_it(rig, capsys):
    code, out, err = rig.run("ping", capsys=capsys)
    assert code == 0
    assert out == "bridge ok · codex 0.155.1 · claude none · peers 0\n"
    assert err == ""
    assert (rig.home / "bridge.sock").exists()
    assert os.stat(rig.home).st_mode & 0o777 == 0o700


def test_ping_exits_5_while_the_daemon_is_down_and_recovers_after_a_reconnect(rig, capsys):
    assert rig.run("ping", capsys=capsys)[0] == 0
    rig.daemon.stop()
    time.sleep(0.3)
    code, out, err = rig.run("ping", capsys=capsys)
    assert code == 5
    assert "unreachable" in out
    rig.daemon = FakeDaemonThread(rig.daemon_sock)
    rig.daemon.replies.update(default_replies())
    rig.daemon.start()
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline and rig.run("ping", capsys=capsys)[0] != 0:
        time.sleep(0.2)
    assert rig.run("ping", capsys=capsys)[0] == 0
    assert rig.daemon.received("initialize")


def test_bridge_reconnects_after_the_daemon_drops_the_connection(rig, capsys):
    assert rig.run("start", "-n", "helper", "-C", str(rig.tmp), capsys=capsys)[0] == 0
    rig.daemon.drop()
    resume = rig.daemon.wait_request("thread/resume", timeout=10)
    assert resume["params"] == {"threadId": THREAD_ID}
    assert rig.run("ping", capsys=capsys)[0] == 0
    assert len([c for c in rig.daemon.fake.connections if c.upgrade_request]) == 2


# --- start, send, wait ------------------------------------------------------------


def test_start_with_a_prompt_creates_the_thread_then_sends(rig, capsys):
    code, out, err = rig.run("start", "-n", "helper", "-C", str(rig.tmp), "--", "reply with one word", capsys=capsys)
    assert code == 0
    assert out == f"started helper ({THREAD_ID}) in {rig.tmp}\nstarted turn {TURN_ID} on helper\n"
    methods = [r["method"] for r in rig.daemon.requests if r["method"] != "initialize"]
    assert methods.index("thread/start") < methods.index("turn/start")
    assert rig.daemon.received("turn/start")[0]["params"]["input"] == [{"type": "text", "text": "reply with one word"}]


def test_start_defaults_the_directory_to_the_callers_cwd(rig, capsys, monkeypatch):
    monkeypatch.chdir(rig.tmp)
    assert rig.run("start", "-n", "helper", capsys=capsys)[0] == 0
    assert rig.daemon.received("thread/start")[0]["params"]["cwd"] == str(rig.tmp)


def test_send_wait_prints_the_final_answer(rig, capsys):
    assert rig.run("start", "-n", "helper", "-C", str(rig.tmp), capsys=capsys)[0] == 0
    result = {}

    def wait():
        result["exit"] = rig.run("send", "helper", "--wait", "--", "go")

    t = threading.Thread(target=wait)
    t.start()
    rig.daemon.wait_request("turn/start")
    time.sleep(0.2)
    rig.daemon.notify("turn/completed", for_thread(COMPLETED_NOTICE))
    t.join(timeout=10)
    out, err = capsys.readouterr()
    assert result["exit"][0] == 0
    assert out.endswith("DONE\n")


def test_a_failed_turn_exits_6_with_the_failure_text(rig, capsys):
    assert rig.run("start", "-n", "helper", "-C", str(rig.tmp), capsys=capsys)[0] == 0
    assert rig.run("send", "helper", "--", "go", capsys=capsys)[0] == 0
    rig.daemon.notify("turn/completed", for_thread(FAILED_TURN))
    time.sleep(0.3)
    code, out, err = rig.run("wait", "helper", capsys=capsys)
    assert code == 6
    assert out.startswith("failed: You’ve hit your usage limit")


def test_wait_survives_a_bridge_restart(rig, capsys):
    assert rig.run("start", "-n", "helper", "-C", str(rig.tmp), capsys=capsys)[0] == 0
    assert rig.run("send", "helper", "--", "go", capsys=capsys)[0] == 0
    # The restarted bridge learns the thread's status from thread/resume: the turn is still running.
    mid_turn = copy.deepcopy(RESTART.result(6))
    mid_turn["thread"]["status"] = {"type": "active", "activeFlags": []}
    rig.daemon.replies["thread/resume"] = {"result": mid_turn}
    rig.daemon.replies["thread/turns/list"] = {"result": {"data": [TURN_STARTED["turn"]], "nextCursor": None, "backwardsCursor": None}}
    waiting = subprocess.Popen([sys.executable, "-m", "antiphon", "wait", "helper"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=dict(os.environ))
    time.sleep(0.5)
    os.kill(rig.bridge_pid(), signal.SIGKILL)
    rig.daemon.wait_connections(2, timeout=15)
    rig.daemon.wait_request("thread/resume", timeout=10)
    time.sleep(0.3)
    rig.daemon.notify("turn/completed", for_thread(COMPLETED_NOTICE))
    out, err = waiting.communicate(timeout=15)
    assert waiting.returncode == 0, err
    assert out == "DONE\n"


def test_wait_gives_up_after_a_second_loss_naming_the_rerun_command(rig, capsys, monkeypatch):
    assert rig.run("start", "-n", "helper", "-C", str(rig.tmp), capsys=capsys)[0] == 0
    assert rig.run("send", "helper", "--", "go", capsys=capsys)[0] == 0
    monkeypatch.setattr(cli, "WAIT_RETRY_BACKOFF", 0.1)
    calls = []

    def dying_call(path, op, args, timeout=30):
        calls.append(op)
        if op == "wait":
            raise ipc.BridgeUnreachable("the bridge closed the connection without replying")
        return original(path, op, args, timeout)

    original = ipc.call_raw
    monkeypatch.setattr(ipc, "call_raw", dying_call)
    code, out, err = rig.run("wait", "helper", capsys=capsys)
    assert code == 1
    assert f"antiphon wait {THREAD_ID}" in err
    assert calls.count("wait") == 2


def test_exit_codes_follow_the_error_kind(rig, capsys):
    assert rig.run("start", "-n", "helper", "-C", str(rig.tmp), capsys=capsys)[0] == 0
    assert rig.run("send", "nobody", "--", "x", capsys=capsys)[0] == 2
    rig.daemon.replies["turn/start"] = {"error": {"code": -32600, "message": "direct app-server input is not allowed for multi-agent v2 sub-agents"}}
    code, out, err = rig.run("send", "helper", "--", "x", capsys=capsys)
    assert code == 3
    assert "not allowed" in err
    rig.daemon.replies["turn/start"] = {"result": TURN_STARTED}
    assert rig.run("send", "helper", "--", "x", capsys=capsys)[0] == 0
    assert rig.run("wait", "helper", "--timeout", "0.2", capsys=capsys)[0] == 4


DENIED = load_fixture("auto-review-denied.jsonl")
REVIEW_STARTED = DENIED.notifications("item/autoApprovalReview/started")[0]
REVIEW_DENIED = DENIED.notifications("item/autoApprovalReview/completed")[0]
DENIED_TOKEN = "e16d64"  # sha256 of the captured reviewId, first 6 hex chars


def test_approve_and_deny_verbs_answer_a_denial_and_an_unknown_token_exits_2(rig, capsys):
    rig.daemon.replies["thread/approveGuardianDeniedAction"] = {"result": {}}
    assert rig.run("start", "-n", "helper", "-C", str(rig.tmp), capsys=capsys)[0] == 0
    rig.daemon.notify("item/autoApprovalReview/started", for_thread(REVIEW_STARTED))
    rig.daemon.notify("item/autoApprovalReview/completed", for_thread(REVIEW_DENIED))
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and f"denied {DENIED_TOKEN}" not in rig.run("status", "helper", capsys=capsys)[1]:
        time.sleep(0.05)
    code, out, err = rig.run("ls", capsys=capsys)
    assert code == 0 and f"denied {DENIED_TOKEN} " in out
    code, out, err = rig.run("deny", "nope00", "--", "no", capsys=capsys)
    assert code == 2 and "nope00" in err
    code, out, err = rig.run("approve", DENIED_TOKEN, capsys=capsys)
    assert (code, out) == (0, f"approved {DENIED_TOKEN} on helper: {REVIEW_DENIED['action']['command']}\n")
    assert rig.daemon.wait_request("thread/approveGuardianDeniedAction")["params"]["threadId"] == THREAD_ID
    assert "retry it now" in rig.daemon.wait_request("turn/start")["params"]["input"][0]["text"]
    code, out, err = rig.run("deny", DENIED_TOKEN, "--", "already", "done", capsys=capsys)
    assert code == 2 and "already resolved" in err


def test_interrupt_reports_a_noop_when_the_thread_is_idle(rig, capsys):
    assert rig.run("start", "-n", "helper", "-C", str(rig.tmp), capsys=capsys)[0] == 0
    code, out, err = rig.run("interrupt", "helper", capsys=capsys)
    assert code == 0
    assert out == "nothing to interrupt: helper is idle\n"


# --- ls, status, stop, resume, name --------------------------------------------------


def test_ls_prints_an_aligned_table_with_a_star_on_the_callers_row(rig):
    claude = FakeClaude(rig.config_dir / "sessions", rig.tmp / "socks", name="claude-main")
    try:
        assert rig.run_subprocess("start", "-n", "helper", "-C", str(rig.tmp)).returncode == 0
        done = rig.run_subprocess("ls")
    finally:
        claude.close()
    assert done.returncode == 0, done.stderr
    lines = done.stdout.splitlines()
    assert lines[0].split() == ["NAME", "KIND", "STATUS", "CWD"]
    rows = {line.split()[1] if line.startswith("*") else line.split()[0]: line for line in lines[1:]}
    assert rows["claude-main"].startswith("* claude-main")
    assert not rows["helper"].startswith("*")
    assert rows["helper"].split() == ["helper", "codex", "idle", str(rig.tmp)]
    name_col = [line.index("claude") for line in lines[1:] if "claude-main" in line][0]
    assert all(line[name_col - 1] == " " for line in lines)


def test_status_stop_resume_and_name_round_trip(rig, capsys):
    assert rig.run("start", "-n", "helper", "-C", str(rig.tmp), capsys=capsys)[0] == 0
    code, out, err = rig.run("status", "helper", capsys=capsys)
    assert code == 0
    assert f"thread_id: {THREAD_ID}" in out and "status: idle" in out
    code, out, err = rig.run("name", "helper", "planner", capsys=capsys)
    assert (code, out) == (0, "renamed helper to planner\n")
    code, out, err = rig.run("stop", "planner", capsys=capsys)
    assert (code, out) == (0, "stopped planner\n")
    code, out, err = rig.run("send", "planner", "--", "x", capsys=capsys)
    assert code == 2 and f"antiphon resume {THREAD_ID}" in err
    resumed = json.loads(json.dumps(RESTART.result(6)).replace("01a0c399-780c-73f3-9209-e09112a796a0", THREAD_ID))
    rig.daemon.replies["thread/resume"] = {"result": resumed}
    code, out, err = rig.run("resume", "planner", capsys=capsys)
    assert (code, out) == (0, f"resumed planner ({THREAD_ID})\n")
    code, out, err = rig.run("status", capsys=capsys)
    assert code == 0 and "threads: 1" in out


def test_every_op_carries_the_callers_claimed_thread(rig, monkeypatch, capsys):
    monkeypatch.setenv("CODEX_THREAD_ID", "01a0c390-e298-7b53-87d2-3333c99c6ac4")
    seen = []
    original = ipc.call_raw

    def recording(path, op, args, timeout=30):
        seen.append((op, args.get("claimed_thread")))
        return original(path, op, args, timeout)

    monkeypatch.setattr(ipc, "call_raw", recording)
    assert rig.run("ls", capsys=capsys)[0] == 0
    assert seen[-1] == ("ls", "01a0c390-e298-7b53-87d2-3333c99c6ac4")


def test_a_codex_caller_is_refused_by_the_cli_with_exit_2(rig):
    assert rig.run_subprocess("start", "-n", "helper", "-C", str(rig.tmp)).returncode == 0
    # The bridge classifies callers by the command name of their ancestors: a shell
    # copied to a file named `codex` makes the CLI it runs a Codex caller.
    codex = rig.tmp / "codexbin" / "codex"
    codex.parent.mkdir()
    codex.write_bytes(Path(os.path.realpath("/bin/sh")).read_bytes())
    codex.chmod(0o755)
    done = subprocess.run([str(codex), "-c", f"{sys.executable} -m antiphon stop helper"], capture_output=True, text=True)
    assert done.returncode == 2, done.stderr
    assert "codex caller" in done.stderr and "may not stop" in done.stderr
    assert rig.run_subprocess("stop", "helper").returncode == 0


def test_ping_shows_both_versions_and_the_peer_count(rig, capsys):
    claude = FakeClaude(rig.config_dir / "sessions", rig.tmp / "socks", name="claude-main")
    try:
        assert rig.run("start", "-n", "helper", "-C", str(rig.tmp), capsys=capsys)[0] == 0
        code, out, err = rig.run("ping", capsys=capsys)
    finally:
        claude.close()
    assert code == 0
    assert out == "bridge ok · codex 0.155.1 · claude 2.1.278 · peers 1\n"


def test_every_verb_prints_the_degraded_banner_and_ping_exits_2(rig, capsys):
    claude = FakeClaude(rig.config_dir / "sessions", rig.tmp / "socks", name="claude-main")
    try:
        assert rig.run("ping", capsys=capsys)[0] == 0
        broken = dict(claude.record, name="claude-old", peerProtocol=0)
        (rig.config_dir / "sessions" / "claude-old.json").write_text(json.dumps(broken))
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and rig.run("ping", capsys=capsys)[0] == 0:
            time.sleep(0.2)
        code, out, err = rig.run("ping", capsys=capsys)
        ls_code, ls_out, ls_err = rig.run("ls", capsys=capsys)
    finally:
        claude.close()
    assert code == 2
    assert out.startswith("bridge DEGRADED\n") and "peerProtocol" in out
    assert err.startswith("antiphon: DEGRADED — ") and "claude-old.json" in err
    assert ls_code == 0
    assert ls_err.startswith("antiphon: DEGRADED — ")


def test_the_bridge_subcommand_runs_in_the_foreground_and_yields_to_a_running_bridge(rig, capsys):
    assert rig.run("ping", capsys=capsys)[0] == 0
    done = rig.run_subprocess("bridge")
    assert done.returncode == 0
    assert "already answers" in done.stderr
