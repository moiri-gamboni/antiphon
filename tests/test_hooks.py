"""The approval prompt hook against the state the bridge writes, and the installer's
edit of the user's Claude Code settings."""

import dataclasses
import json
import os
import subprocess
from pathlib import Path

from antiphon.codex.approvals import Pending, describe_action
from antiphon.state import State, ThreadState
from fake_daemon import load_fixture

REPO = Path(__file__).parent.parent
HOOK = REPO / "hooks" / "approve-ask.sh"
INSTALL = REPO / "install.sh"
DENIED = load_fixture("auto-review-denied.jsonl").notifications("item/autoApprovalReview/completed")[0]


def pending_from_capture() -> Pending:
    command, cwd = describe_action(DENIED["action"])
    return Pending(
        token="e16d64", asker="t1",
        turn_id=DENIED["turnId"], command=command, cwd=cwd, rationale=DENIED["review"]["rationale"],
        risk_level=DENIED["review"]["riskLevel"], review_completed_params=DENIED,
        since=0.0, resolved=False, kind="denied", request_id=None, epoch=None, available_decisions=None,
    )


def write_state(home: Path, records: list[dict]) -> None:
    (home / "log").mkdir(parents=True)
    thread = ThreadState(thread_id="t1", name="helper", cwd="/work", origin="spawned", spawner="s", read_only=False, pending=records)
    State(threads={"t1": thread}).save(home / "state.json")


def run_hook(home: Path, command: str) -> dict | None:
    # The PreToolUse input shape from the Claude Code hooks reference, command substituted.
    payload = {"session_id": "abc123", "cwd": "/work", "hook_event_name": "PreToolUse", "tool_name": "Bash",
               "tool_input": {"command": command, "description": "x"}, "tool_use_id": "toolu_01"}
    done = subprocess.run([str(HOOK)], input=json.dumps(payload), capture_output=True, text=True,
                          env={**os.environ, "ANTIPHON_HOME": str(home)})
    assert done.returncode == 0, done.stderr
    return json.loads(done.stdout) if done.stdout.strip() else None


def test_approve_and_deny_ask_with_the_records_command_cwd_and_reason(tmp_path):
    write_state(tmp_path, [dataclasses.asdict(pending_from_capture())])
    for command in ("antiphon approve e16d64", "cd /work && antiphon deny e16d64 -- too risky"):
        out = run_hook(tmp_path, command)["hookSpecificOutput"]
        assert (out["hookEventName"], out["permissionDecision"]) == ("PreToolUse", "ask")
        reason = out["permissionDecisionReason"]
        assert 'on Codex thread "helper"' in reason
        assert f"command: {DENIED['action']['command']}" in reason
        assert f"cwd: {DENIED['action']['cwd']}" in reason
        assert f"reason: {DENIED['review']['rationale']}" in reason


def test_other_commands_pass_without_a_decision(tmp_path):
    write_state(tmp_path, [dataclasses.asdict(pending_from_capture())])
    assert run_hook(tmp_path, "antiphon status helper") is None
    assert run_hook(tmp_path, "ls -la") is None


def test_an_unreadable_record_still_asks(tmp_path):
    record = dataclasses.asdict(pending_from_capture())
    del record["rationale"]
    write_state(tmp_path, [record])
    reason = run_hook(tmp_path, "antiphon approve e16d64")["hookSpecificOutput"]["permissionDecisionReason"]
    assert "e16d64" in reason and "not readable" in reason
    assert "not readable" in run_hook(tmp_path / "nowhere", "antiphon approve e16d64")["hookSpecificOutput"]["permissionDecisionReason"]


def test_install_adds_the_hook_beside_other_hooks_and_uninstall_removes_only_it(tmp_path):
    home = tmp_path / "home"
    config_dir = home / "cc"
    config_dir.mkdir(parents=True)
    other = {"matcher": "Bash", "hooks": [{"type": "command", "command": "/elsewhere/guard.sh"}]}
    (config_dir / "settings.json").write_text(json.dumps({"model": "keep", "hooks": {"PreToolUse": [other]}}))
    stubs = tmp_path / "bin"
    stubs.mkdir()
    (stubs / "pkill").write_text("#!/bin/sh\nexit 1\n")  # uninstall must not reach a real bridge
    (stubs / "pkill").chmod(0o755)
    env = {**os.environ, "HOME": str(home), "CLAUDE_CONFIG_DIR": str(config_dir), "PATH": f"{stubs}:{os.environ['PATH']}"}

    for _ in range(2):
        assert subprocess.run([str(INSTALL), "install", "--no-service", "--human-approvals"], env=env, capture_output=True, text=True).returncode == 0
    settings = json.loads((config_dir / "settings.json").read_text())
    assert settings["model"] == "keep"
    assert settings["hooks"]["PreToolUse"] == [other, {"matcher": "Bash", "hooks": [{"type": "command", "command": str(HOOK), "timeout": 5}]}]
    assert (config_dir / "skills" / "antiphon").resolve() == REPO / "skills" / "claude"
    assert (home / ".agents" / "skills" / "antiphon").resolve() == REPO / "skills" / "codex"

    assert subprocess.run([str(INSTALL), "uninstall"], env=env, capture_output=True, text=True).returncode == 0
    assert json.loads((config_dir / "settings.json").read_text()) == {"model": "keep", "hooks": {"PreToolUse": [other]}}
    assert not (config_dir / "skills" / "antiphon").exists()
    assert not (home / ".agents" / "skills" / "antiphon").exists()
