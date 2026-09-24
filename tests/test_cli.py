"""The CLI over the real bridge (started lazily as a subprocess) over the fake daemon."""

import asyncio
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
from antiphon.bridge import Bridge
from antiphon.codex.daemon import HEADLESS_INSTRUCTIONS
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
    # The daemon itself stays up and keeps the thread loaded, so the resume only rejoins it.
    rig.daemon.replies["thread/loaded/list"] = {"result": {"data": [THREAD_ID], "nextCursor": None}}
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


AGENT_FILE = """---
name: code-explorer
description: traces a feature through the code
tools: Read, Grep
---

You trace execution paths.
---
Report file:line references.
"""


def test_read_instructions_drops_a_leading_frontmatter_block_and_nothing_else(tmp_path):
    agent = tmp_path / "agent.md"
    agent.write_text(AGENT_FILE)
    assert cli.read_instructions(str(agent)) == "You trace execution paths.\n---\nReport file:line references."


def test_read_instructions_keeps_a_file_without_frontmatter_whole(tmp_path):
    plain = tmp_path / "plain.md"
    plain.write_text("# Role\n\nYou review diffs.\n---\nBe terse.\n")
    unclosed = tmp_path / "unclosed.md"
    unclosed.write_text("---\nname: x\nYou review diffs.\n")
    assert cli.read_instructions(str(plain)) == "# Role\n\nYou review diffs.\n---\nBe terse."
    assert cli.read_instructions(str(unclosed)) == "---\nname: x\nYou review diffs."


def test_start_with_instructions_sends_the_files_body_after_antiphons_own(rig, capsys):
    agent = rig.tmp / "agent.md"
    agent.write_text(AGENT_FILE)
    assert rig.run("start", "-n", "helper", "-C", str(rig.tmp), "--instructions", str(agent), capsys=capsys)[0] == 0
    sent = rig.daemon.received("thread/start")[0]["params"]["developerInstructions"]
    assert sent == HEADLESS_INSTRUCTIONS + "\n\nYou trace execution paths.\n---\nReport file:line references."


def test_start_refuses_a_missing_or_empty_instructions_file_before_starting_anything(rig, capsys):
    missing = rig.tmp / "nowhere.md"
    code, out, err = rig.run("start", "-n", "helper", "-C", str(rig.tmp), "--instructions", str(missing), capsys=capsys)
    assert code == 2
    assert f"cannot read {missing}" in err
    empty = rig.tmp / "empty.md"
    empty.write_text("---\nname: x\n---\n\n")
    code, out, err = rig.run("start", "-n", "helper", "-C", str(rig.tmp), "--instructions", str(empty), capsys=capsys)
    assert code == 2
    assert "is empty" in err
    assert rig.daemon.received("thread/start") == []


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


def test_start_refuses_a_wait_placed_after_the_separator(rig, capsys):
    code, out, err = rig.run("start", "-n", "x", "-C", str(rig.tmp), "--", "the brief", "--wait", capsys=capsys)
    assert code == 2
    assert "--wait" in err and "antiphon start --wait --" in err
    assert rig.daemon.received("thread/start") == []


def test_send_refuses_a_wait_placed_after_the_separator(rig, capsys):
    assert rig.run("start", "-n", "helper", "-C", str(rig.tmp), capsys=capsys)[0] == 0
    code, out, err = rig.run("send", "helper", "--", "go now", "--wait", capsys=capsys)
    assert code == 2
    assert "--wait" in err


def test_send_refuses_a_timeout_placed_after_the_separator(rig, capsys):
    assert rig.run("start", "-n", "helper", "-C", str(rig.tmp), capsys=capsys)[0] == 0
    # --timeout carries a value, so the number is the last token, not --timeout itself.
    code, out, err = rig.run("send", "helper", "--", "go now", "--timeout", "30", capsys=capsys)
    assert code == 2
    assert "--timeout" in err
    assert rig.daemon.received("turn/start") == []


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


def test_wait_reports_a_stopped_or_unloaded_thread_as_a_non_success(tmp_path):
    class FakeClient:
        home = tmp_path

        def call(self, op, args=None, timeout=30):
            if op == "status":
                return {"kind": "codex", "thread_id": "t1"}
            return {"status": "stopped", "final": None, "thread_id": "t1"}

    assert cli._wait(FakeClient(), "helper", None) == 6


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


DENIED = load_fixture("guardian-retry-high.jsonl")
REVIEW_STARTED = DENIED.notifications("item/autoApprovalReview/started")[0]
REVIEW_DENIED = DENIED.notifications("item/autoApprovalReview/completed")[0]
DENIED_TOKEN = "717a21"  # sha256 of the captured reviewId, first 6 hex chars


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
    assert "I authorize you to retry this command" in rig.daemon.wait_request("turn/start")["params"]["input"][0]["text"]
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
    assert lines[0] == "you are claude-main"
    assert lines[1].split() == ["NAME", "KIND", "STATUS", "CWD"]
    # Ordered rows, not a dict keyed by name: a hosted thread's own peer-child record
    # slipping in as a second "helper" row (kind claude) must make this fail, not be
    # collapsed away.
    data_rows = lines[2:]
    assert len(data_rows) == 2
    names = [line.split()[1] if line.startswith("*") else line.split()[0] for line in data_rows]
    assert names == ["claude-main", "helper"]
    assert data_rows[0].startswith("* claude-main")
    assert not data_rows[1].startswith("*")
    assert data_rows[1].split() == ["helper", "codex", "idle", str(rig.tmp)]
    name_col = [line.index("claude") for line in data_rows if "claude-main" in line][0]
    assert all(line[name_col - 1] == " " for line in lines[1:])


def test_ls_has_no_header_for_a_human_and_indents_a_sub_agent_with_its_role(rig, capsys):
    sub_agent = load_fixture("sub-agent.jsonl")
    sub = copy.deepcopy(sub_agent.result(2, occurrence=1))
    sub["thread"]["agentRole"] = "reviewer"
    sub_id, parent_id = sub["thread"]["id"], sub["thread"]["parentThreadId"]
    parent = copy.deepcopy(load_fixture("adoption.jsonl").result(2))
    parent["thread"]["id"] = parent_id
    parent["thread"]["name"] = "orchestrator"
    rig.daemon.replies["thread/read"] = lambda params: {"result": sub if params["threadId"] == sub_id else parent}
    rig.daemon.replies["thread/loaded/list"] = {"result": {"data": [sub_id, parent_id], "nextCursor": None}}
    claude = FakeClaude(rig.config_dir / "sessions", rig.tmp / "socks", name="claude-main")
    try:
        assert rig.run("ping", capsys=capsys)[0] == 0
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and rig.run("ping", capsys=capsys)[1] != "bridge ok · codex 0.155.1 · claude 2.1.278 · peers 1\n":
            time.sleep(0.1)
        code, out, err = rig.run("ls", capsys=capsys)
        registered = sorted(json.loads(p.read_text())["name"] for p in (rig.config_dir / "sessions").glob("*.json"))
    finally:
        claude.close()
    assert code == 0
    lines = out.splitlines()
    assert lines[0].split() == ["NAME", "KIND", "STATUS", "CWD"]
    assert [line.split()[0] for line in lines[2:]] == ["orchestrator", "Bernoulli"]
    assert lines[3].startswith("    Bernoulli (reviewer)")
    assert lines[3].split()[2] == "codex-agent"
    assert registered == ["claude-main", "orchestrator"]


# --- attach --------------------------------------------------------------------------


def tmux_run(calls: list, returncode: int = 0, stdout: str = "main:@3.%7\n", stderr: str = ""):
    def fake(argv, **kwargs):
        calls.append(argv)
        return subprocess.CompletedProcess(argv, returncode=returncode, stdout=stdout, stderr=stderr)

    return fake


def raw_tmux_lines(rig) -> list[dict]:
    return [json.loads(line) for line in (rig.home / "log" / "raw.jsonl").read_text().splitlines() if '"tmux"' in line]


def test_attach_inside_tmux_opens_a_window_on_the_thread_and_prints_the_pane(rig, capsys, monkeypatch):
    assert rig.run("start", "-n", "helper", "-C", str(rig.tmp), capsys=capsys)[0] == 0
    monkeypatch.setenv("TMUX", "/tmp/tmux-1000/default,1,0")
    calls = []
    monkeypatch.setattr(subprocess, "run", tmux_run(calls))
    code, out, err = rig.run("attach", "helper", capsys=capsys)
    assert (code, out, err) == (0, "main:@3.%7\n", "")
    # The window opens in the thread's own directory: a window inheriting the caller's
    # directory makes Codex ask which one to resume in, and ask to trust the wrong one.
    assert calls == [["tmux", "new-window", "-P", "-F", "#{session_name}:#{window_id}.#{pane_id}",
                      "-c", str(rig.tmp), "-n", "helper", f"codex resume {THREAD_ID}"]]
    logged = raw_tmux_lines(rig)
    assert [(line["dir"], line["boundary"]) for line in logged] == [("out", "tmux"), ("in", "tmux")]
    assert logged[0]["data"] == calls[0]
    assert logged[1]["data"] == {"rc": 0, "stdout": "main:@3.%7\n", "stderr": ""}


def test_attach_outside_tmux_prints_the_resume_command(rig, capsys, monkeypatch):
    assert rig.run("start", "-n", "helper", "-C", str(rig.tmp), capsys=capsys)[0] == 0
    monkeypatch.delenv("TMUX", raising=False)
    monkeypatch.setattr(subprocess, "run", tmux_run([], returncode=1, stderr="must not run"))
    code, out, err = rig.run("attach", "helper", capsys=capsys)
    assert (code, out, err) == (0, f"codex resume {THREAD_ID}\n", "")


def test_attach_reports_a_tmux_failure_with_exit_2(rig, capsys, monkeypatch):
    assert rig.run("start", "-n", "helper", "-C", str(rig.tmp), capsys=capsys)[0] == 0
    monkeypatch.setenv("TMUX", "/tmp/tmux-1000/default,1,0")
    monkeypatch.setattr(subprocess, "run", tmux_run([], returncode=1, stdout="", stderr="no current session\n"))
    code, out, err = rig.run("attach", "helper", capsys=capsys)
    assert code == 2
    assert out == ""
    assert "no current session" in err


def test_start_visible_starts_the_thread_then_attaches(rig, capsys, monkeypatch):
    monkeypatch.setenv("TMUX", "/tmp/tmux-1000/default,1,0")
    started_before_tmux = []

    def fake(argv, **kwargs):
        started_before_tmux.append(len(rig.daemon.received("thread/start")))
        return subprocess.CompletedProcess(argv, returncode=0, stdout="main:@4.%9\n", stderr="")

    monkeypatch.setattr(subprocess, "run", fake)
    code, out, err = rig.run("start", "-n", "helper", "-C", str(rig.tmp), "--visible", capsys=capsys)
    assert code == 0
    assert out == f"started helper ({THREAD_ID}) in {rig.tmp}\nmain:@4.%9\n"
    assert started_before_tmux == [1]


# --- the Codex-side verbs ---------------------------------------------------------------


class CodexAncestry:
    """A process table in which every caller is the child of one Codex process."""

    CODEX_PID = 424242

    def parent(self, pid: int) -> int | None:
        return 1 if pid == self.CODEX_PID else self.CODEX_PID

    def comm(self, pid: int) -> str | None:
        return "codex" if pid == self.CODEX_PID else None


class BridgeThread:
    """The real bridge in this process, on its own loop in a thread, with the caller
    classification injected: a CLI call made from the test process is then a Codex caller."""

    def __init__(self, rig: CliRig):
        self.bridge = Bridge(
            rig.home, sessions_dir=rig.config_dir / "sessions", codex_home=rig.codex_home,
            ensure_running=lambda codex_home, rawlog=None: str(rig.daemon_sock), process_table=CodexAncestry(),
        )
        self.bridge.reconnect_backoff = (0.05, 0.2)
        self.bridge.reconcile_interval = 3600
        self.loop = asyncio.new_event_loop()
        self.stop: asyncio.Event | None = None
        self.up = threading.Event()
        self.thread = threading.Thread(target=self.loop.run_until_complete, args=(self._main(),), daemon=True, name="bridge")
        self.thread.start()
        assert self.up.wait(5)

    async def _main(self) -> None:
        self.stop = asyncio.Event()
        await self.bridge.start()
        while self.bridge.last_reconcile is None:
            await asyncio.sleep(0.01)
        self.up.set()
        await self.stop.wait()
        await self.bridge.close()

    def close(self) -> None:
        self.loop.call_soon_threadsafe(self.stop.set)
        self.thread.join(timeout=10)
        self.loop.close()


@pytest.fixture
def codex_rig(rig, monkeypatch):
    """The CLI rig with a Claude session present, the thread `helper` already hosted, and
    every CLI call classified as coming from it — a Codex thread acting through antiphon."""
    from antiphon.callers import Caller

    claude = FakeClaude(rig.config_dir / "sessions", rig.tmp / "socks", name="claude-main")
    bridge = BridgeThread(rig)
    human = Caller(kind="human", claude_pid=None, claude_session_id=None, codex_thread=None)
    args = dict(cwd=str(rig.tmp), name="helper", read_only=False, report=True, worktree=False, review_by_parent=False)
    # Host the caller's own thread first (a Codex thread is spawned by someone else before it
    # can act); only then is the Codex caller recognised rather than owning nothing.
    asyncio.run_coroutine_threadsafe(bridge.bridge.dispatch("start", args, human), bridge.loop).result(5)
    monkeypatch.setenv("CODEX_THREAD_ID", THREAD_ID)
    rig.claude = claude
    try:
        yield rig
    finally:
        bridge.close()
        claude.close()


def test_a_codex_caller_sees_its_own_name_in_the_ls_header(codex_rig, capsys):
    rig = codex_rig
    code, out, err = rig.run("ls", capsys=capsys)
    assert code == 0
    assert out.splitlines()[0] == "you are helper"
    assert [line.split()[1] for line in out.splitlines()[2:] if line.startswith("*")] == ["helper"]


def test_a_codex_caller_renames_itself_without_naming_a_target(codex_rig, capsys):
    rig = codex_rig
    code, out, err = rig.run("name", "planner", capsys=capsys)
    assert (code, out) == (0, "renamed helper to planner\n")
    assert rig.daemon.received("thread/name/set")[-1]["params"] == {"threadId": THREAD_ID, "name": "planner"}


def test_a_codex_caller_sends_to_a_claude_session_and_is_told_sent(codex_rig, capsys, monkeypatch):
    from antiphon import peers

    monkeypatch.setattr(peers, "RECEIPT_WAIT", 0.3)
    rig = codex_rig
    code, out, err = rig.run("send", "claude-main", "--", "hello from codex", capsys=capsys)
    assert (code, out) == (0, "sent\n")
    [frame] = rig.claude.wait_for_frames(1)
    assert 'from-name="helper"' in frame["message"]["content"]
    assert "hello from codex" in frame["message"]["content"]


def test_a_codex_caller_subscribes_to_a_peers_idle_notice(codex_rig, capsys):
    rig = codex_rig
    code, out, err = rig.run("notify", "claude-main", capsys=capsys)
    assert code == 0
    assert "claude-main" in out
    [frame] = rig.claude.wait_for_frames(1)
    assert frame["action"] == "notify_when_idle"


def test_a_claude_caller_running_notify_exits_2_naming_notify_when_idle(rig, capsys):
    claude = FakeClaude(rig.config_dir / "sessions", rig.tmp / "socks", name="claude-main")
    try:
        assert rig.run_subprocess("start", "-n", "helper", "-C", str(rig.tmp)).returncode == 0
        done = rig.run_subprocess("notify", "helper")
    finally:
        claude.close()
    assert done.returncode == 2
    assert "notify_when_idle" in done.stderr


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


# --- start --claude: a Claude Code session of the caller's own ---------------------------


class RecordingClient:
    """A client that answers `status` and `start_claude` from a script and records the calls."""

    def __init__(self, tmp_path, **replies):
        self.home = tmp_path
        self.calls: list[tuple[str, dict]] = []
        self.replies = replies

    def call(self, op, args=None, timeout=30):
        self.calls.append((op, args or {}))
        return self.replies[op]


STARTED_SESSION = {"name": "helper", "session_id": "1ec2f3d4-0000-4000-8000-000000000001",
                   "cwd": "/work", "pid": 4242, "job_id": "1ec2f3d4", "hook": "/repo/hooks/forward.sh"}


def start_claude_args(**overrides):
    parsed = cli.build_parser().parse_args(["start", "--claude", "-C", "/work", "-n", "helper"])
    for key, value in overrides.items():
        setattr(parsed, key, value)
    return parsed


def test_start_claude_passes_the_directory_name_model_gate_and_prompt(tmp_path, capsys):
    client = RecordingClient(tmp_path, start_claude=STARTED_SESSION)
    args = start_claude_args(model="sonnet", gate="Bash", prompt=["read", "the", "diff"])
    assert cli.verb_start(args, client) == 0
    [(op, sent)] = client.calls
    assert op == "start_claude"
    assert sent["cwd"] == "/work" and sent["name"] == "helper"
    assert sent["model"] == "sonnet" and sent["gate"] == "Bash"
    assert sent["prompt"] == "read the diff"
    assert "helper" in capsys.readouterr().out


def test_start_claude_refuses_the_flags_that_only_fit_a_codex_thread(tmp_path, capsys):
    client = RecordingClient(tmp_path, start_claude=STARTED_SESSION)
    assert cli.verb_start(start_claude_args(read_only=True), client) == 2
    assert client.calls == []
    assert "--read-only" in capsys.readouterr().err


def test_start_claude_sends_the_instruction_files_body_alone(tmp_path, capsys):
    agent = tmp_path / "agent.md"
    agent.write_text(AGENT_FILE)
    client = RecordingClient(tmp_path, start_claude=STARTED_SESSION)
    assert cli.verb_start(start_claude_args(instructions=str(agent)), client) == 0
    [(op, sent)] = client.calls
    # A Claude Code session keeps its own system prompt; antiphon's headless text is for Codex threads.
    assert sent["instructions"] == "You trace execution paths.\n---\nReport file:line references."


def test_start_claude_refuses_an_unreadable_instructions_file_before_starting_anything(tmp_path, capsys):
    client = RecordingClient(tmp_path, start_claude=STARTED_SESSION)
    assert cli.verb_start(start_claude_args(instructions=str(tmp_path / "nowhere.md")), client) == 2
    assert client.calls == []
    assert "cannot read" in capsys.readouterr().err


def test_start_claude_says_so_when_no_forward_hook_was_installed(tmp_path, capsys):
    client = RecordingClient(tmp_path, start_claude={**STARTED_SESSION, "hook": None})
    assert cli.verb_start(start_claude_args(), client) == 0
    assert "decides its own permissions" in capsys.readouterr().err


def test_attach_on_a_session_prints_the_command_that_opens_it(tmp_path, capsys, monkeypatch):
    monkeypatch.delenv("TMUX", raising=False)
    client = RecordingClient(tmp_path, status={"kind": "claude", "name": "helper", "job_id": "1ec2f3d4", "cwd": "/work"})
    args = cli.build_parser().parse_args(["attach", "helper"])
    assert cli.verb_attach(args, client) == 0
    assert capsys.readouterr().out.strip() == "claude attach 1ec2f3d4"


def test_attach_on_a_session_with_no_job_recorded_says_where_to_look(tmp_path, capsys):
    client = RecordingClient(tmp_path, status={"kind": "claude", "name": "helper", "job_id": None, "cwd": "/work"})
    args = cli.build_parser().parse_args(["attach", "helper"])
    assert cli.verb_attach(args, client) == 2
    assert "claude agents" in capsys.readouterr().err


def test_wait_on_a_session_waits_on_its_session_id(tmp_path, capsys):
    client = RecordingClient(
        tmp_path,
        status={"kind": "claude", "session_id": "1ec2f3d4-0000-4000-8000-000000000001"},
        wait={"status": "idle", "final": "done", "name": "helper"},
    )
    assert cli._wait(client, "helper", 5) == 0
    assert client.calls[1] == ("wait", {"target": "1ec2f3d4-0000-4000-8000-000000000001", "timeout": 5})
    assert capsys.readouterr().out == "done\n"


def test_help_lists_every_exit_code(capsys):
    with pytest.raises(SystemExit) as info:
        cli.main(["--help"])
    out = capsys.readouterr().out
    assert info.value.code == 0
    listed = out.split("exit codes:", 1)[1]
    assert [int(line.split()[0]) for line in listed.splitlines() if line.strip()] == [0, 1, 2, 3, 4, 5, 6]
