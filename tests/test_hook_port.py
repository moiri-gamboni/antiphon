"""Claude Code hook scripts run as Codex hooks: the shim's field mapping and decision
mapping against the generated Codex schemas, the block-and-forward of an `ask` through
the bridge, and the installer's edit of `hooks.json` plus the trust write."""

import asyncio
import copy
import json
import os
import shlex
import stat
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

from antiphon import cli, ipc
from antiphon.codex import hooks
from antiphon.codex.approvals import REMIND_AFTER
from antiphon.ipc import IpcError
from antiphon.state import ThreadState
from fake_claude import FakeClaude
from fake_daemon import FIXTURES, load_fixture
from test_cli import CliRig
from test_e2e import THREAD_ID, Rig, for_thread, run, until

SCHEMAS = FIXTURES / "codex-hook-schemas"
REPO = Path(__file__).parent.parent
APPROVE_ASK = REPO / "hooks" / "approve-ask.sh"

# The one real Codex hook input on record, and the hooks/list entry shape.
CAPTURED_INPUT = [json.loads(line)["permission_hook_input"] for line in (FIXTURES / "permission-hook-order.jsonl").read_text().splitlines() if line.startswith('{"permission_hook_input"')][0]
HOOKS_LIST = load_fixture("hooks-list.jsonl")
CAPTURED_ENTRY = HOOKS_LIST.result(2, occurrence=1)["data"][0]["hooks"][0]
COMPLETED_NOTICE = [n for n in load_fixture("permission-hook-order.jsonl").notifications("turn/completed") if n["turn"]["status"] == "completed"][0]


def schema(name: str) -> dict:
    return json.loads((SCHEMAS / f"{name}.schema.json").read_text())


def codex_input(event: str, **overrides) -> dict:
    """A stdin document for `event` with every required field of its generated schema,
    the values taken from the captured PermissionRequest input where the field exists."""
    required = schema(f"{hooks.SCHEMA_NAMES[event]}.command.input")["required"]
    extra = {"tool_use_id": "call-abc123", "tool_response": {"output": "ok", "metadata": {"exit_code": 0}}}
    doc = {}
    for field in required:
        if field == "hook_event_name":
            doc[field] = event
        elif field in CAPTURED_INPUT:
            doc[field] = copy.deepcopy(CAPTURED_INPUT[field])
        else:
            doc[field] = extra[field]
    doc.update(overrides)
    return doc


def check_schema(value, node: dict, root: dict, where: str = "$") -> None:
    """A small draft-07 checker for the parts these schemas use: properties with
    additionalProperties false, required, type, enum, const, $ref, allOf."""
    if "$ref" in node:
        check_schema(value, root["definitions"][node["$ref"].rsplit("/", 1)[1]], root, where)
        return
    for sub in node.get("allOf", []):
        check_schema(value, sub, root, where)
    if "const" in node:
        assert value == node["const"], f"{where}: {value!r} != {node['const']!r}"
    if "enum" in node:
        assert value in node["enum"], f"{where}: {value!r} not in {node['enum']}"
    kinds = node.get("type")
    if kinds is not None:
        kinds = [kinds] if isinstance(kinds, str) else kinds
        python = {"object": dict, "string": str, "boolean": bool, "null": type(None), "array": list}
        assert any(isinstance(value, python[k]) for k in kinds), f"{where}: {value!r} is not {kinds}"
    if isinstance(value, dict) and "properties" in node:
        for key in node.get("required", []):
            assert key in value, f"{where}: missing {key}"
        for key, sub in value.items():
            if key in node["properties"]:
                check_schema(sub, node["properties"][key], root, f"{where}.{key}")
            else:
                assert node.get("additionalProperties", True) is not False, f"{where}: unexpected key {key}"


def valid_output(event: str, text: str) -> dict:
    doc = json.loads(text)
    root = schema(f"{hooks.SCHEMA_NAMES[event]}.command.output")
    check_schema(doc, root, root)
    return doc


def script(tmp_path: Path, body: str, name: str = "hook.sh") -> str:
    path = tmp_path / name
    path.write_text("#!/bin/sh\n" + body + "\n")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)
    return str(path)


def no_ask(codex_input, reason, budget):
    raise AssertionError("the hook did not ask; the bridge must not be called")


def shim(tmp_path: Path, body: str, event: str = "PermissionRequest", ask=no_ask, doc: dict | None = None) -> str:
    home = tmp_path / "home"
    return hooks.run_shim(script(tmp_path, body), json.dumps(doc or codex_input(event)), home=home, ask=ask)


# --- the input mapping ---------------------------------------------------------------


def test_the_captured_permission_request_input_becomes_a_claude_pretooluse_input():
    mapped = hooks.claude_input(CAPTURED_INPUT)
    assert mapped["hook_event_name"] == "PreToolUse"
    for field in ("session_id", "transcript_path", "cwd", "permission_mode", "tool_name", "tool_input"):
        assert mapped[field] == CAPTURED_INPUT[field]
    assert "tool_use_id" not in mapped  # Codex gives a PermissionRequest hook no tool use id


def test_pre_and_post_tool_use_inputs_keep_their_event_and_tool_use_fields():
    pre = hooks.claude_input(codex_input("PreToolUse"))
    assert pre["hook_event_name"] == "PreToolUse"
    assert pre["tool_use_id"] == "call-abc123"
    post = hooks.claude_input(codex_input("PostToolUse"))
    assert post["hook_event_name"] == "PostToolUse"
    assert post["tool_use_id"] == "call-abc123"
    assert post["tool_response"] == {"output": "ok", "metadata": {"exit_code": 0}}


def test_the_script_receives_the_mapped_input_on_stdin(tmp_path):
    seen = tmp_path / "seen.json"
    out = shim(tmp_path, f"cat > {seen}", doc=CAPTURED_INPUT)
    assert out == ""
    assert json.loads(seen.read_text()) == hooks.claude_input(CAPTURED_INPUT)


# --- the output mapping ----------------------------------------------------------------

ALLOW = "echo '{\"hookSpecificOutput\": {\"hookEventName\": \"PreToolUse\", \"permissionDecision\": \"allow\"}}'"
DENY = "echo '{\"hookSpecificOutput\": {\"hookEventName\": \"PreToolUse\", \"permissionDecision\": \"deny\", \"permissionDecisionReason\": \"no uploads\"}}'"
ASK = "echo '{\"hookSpecificOutput\": {\"hookEventName\": \"PreToolUse\", \"permissionDecision\": \"ask\", \"permissionDecisionReason\": \"touches the home directory\"}}'"


def test_allow_on_a_permission_request_skips_the_reviewer(tmp_path):
    doc = valid_output("PermissionRequest", shim(tmp_path, ALLOW))
    assert doc["hookSpecificOutput"]["decision"] == {"behavior": "allow"}


def test_allow_on_a_codex_pretooluse_prints_no_decision(tmp_path):
    # Codex rejects `permissionDecision: allow` without an input rewrite as unsupported and
    # fails the hook; silence is the decision that lets the call proceed.
    assert shim(tmp_path, ALLOW, event="PreToolUse") == ""


def test_deny_carries_the_reason_in_each_events_shape(tmp_path):
    doc = valid_output("PermissionRequest", shim(tmp_path, DENY))
    assert doc["hookSpecificOutput"]["decision"] == {"behavior": "deny", "message": "no uploads"}
    doc = valid_output("PreToolUse", shim(tmp_path, DENY, event="PreToolUse"))
    assert doc["hookSpecificOutput"]["permissionDecision"] == "deny"
    assert doc["hookSpecificOutput"]["permissionDecisionReason"] == "no uploads"


def test_the_deprecated_block_decision_and_exit_2_both_deny(tmp_path):
    doc = valid_output("PermissionRequest", shim(tmp_path, "echo '{\"decision\": \"block\", \"reason\": \"old style\"}'"))
    assert doc["hookSpecificOutput"]["decision"] == {"behavior": "deny", "message": "old style"}
    doc = valid_output("PreToolUse", shim(tmp_path, "echo 'blocked by policy' >&2; exit 2", event="PreToolUse"))
    assert doc["hookSpecificOutput"]["permissionDecisionReason"] == "blocked by policy"


def test_a_deny_without_a_reason_still_names_the_script(tmp_path):
    # Codex treats a deny with an empty reason as invalid output and lets the call through.
    doc = valid_output("PermissionRequest", shim(tmp_path, "exit 2"))
    assert doc["hookSpecificOutput"]["decision"]["behavior"] == "deny"
    assert "hook.sh" in doc["hookSpecificOutput"]["decision"]["message"]


def test_no_decision_forms_print_nothing(tmp_path):
    assert shim(tmp_path, "exit 0") == ""
    assert shim(tmp_path, "echo hook ran fine") == ""
    assert shim(tmp_path, "echo '{\"systemMessage\": \"note\"}'", event="PreToolUse") == json.dumps({"systemMessage": "note"})
    # A non-blocking error in Claude's contract (exit 1) is not a decision either.
    assert shim(tmp_path, "echo oops >&2; exit 1") == ""


def test_output_that_looks_like_json_but_is_not_denies_with_the_raw_text(tmp_path):
    doc = valid_output("PermissionRequest", shim(tmp_path, "printf '{\"decision\":'"))
    assert doc["hookSpecificOutput"]["decision"]["behavior"] == "deny"
    assert '{"decision":' in doc["hookSpecificOutput"]["decision"]["message"]


def test_a_missing_script_denies(tmp_path):
    out = hooks.run_shim(str(tmp_path / "nowhere.sh"), json.dumps(codex_input("PermissionRequest")), home=tmp_path / "home", ask=no_ask)
    doc = valid_output("PermissionRequest", out)
    assert doc["hookSpecificOutput"]["decision"]["behavior"] == "deny"
    assert "nowhere.sh" in doc["hookSpecificOutput"]["decision"]["message"]


def test_post_tool_use_block_and_additional_context_pass_through(tmp_path):
    doc = valid_output("PostToolUse", shim(tmp_path, "echo '{\"decision\": \"block\", \"reason\": \"output leaked a key\"}'", event="PostToolUse"))
    assert doc == {"decision": "block", "reason": "output leaked a key"}
    body = "echo '{\"hookSpecificOutput\": {\"hookEventName\": \"PostToolUse\", \"additionalContext\": \"remember this\"}}'"
    doc = valid_output("PostToolUse", shim(tmp_path, body, event="PostToolUse"))
    assert doc["hookSpecificOutput"]["additionalContext"] == "remember this"


def test_additional_context_and_input_rewrites_reach_a_codex_pretooluse(tmp_path):
    body = "echo '{\"hookSpecificOutput\": {\"hookEventName\": \"PreToolUse\", \"permissionDecision\": \"allow\", \"updatedInput\": {\"command\": \"echo safe\"}, \"additionalContext\": \"rewritten\"}}'"
    doc = valid_output("PreToolUse", shim(tmp_path, body, event="PreToolUse"))
    assert doc["hookSpecificOutput"]["permissionDecision"] == "allow"
    assert doc["hookSpecificOutput"]["updatedInput"] == {"command": "echo safe"}
    assert doc["hookSpecificOutput"]["additionalContext"] == "rewritten"


def test_an_input_rewrite_on_a_permission_request_denies_instead_of_dropping_the_rewrite(tmp_path):
    body = "echo '{\"hookSpecificOutput\": {\"hookEventName\": \"PreToolUse\", \"permissionDecision\": \"allow\", \"updatedInput\": {\"command\": \"echo safe\"}}}'"
    doc = valid_output("PermissionRequest", shim(tmp_path, body))
    assert doc["hookSpecificOutput"]["decision"]["behavior"] == "deny"
    assert "rewr" in doc["hookSpecificOutput"]["decision"]["message"]


def test_ask_forwards_the_reason_and_the_answer_becomes_the_decision(tmp_path):
    asked = []

    def allow(codex_input, reason, budget):
        asked.append((codex_input["session_id"], reason, budget))
        return "allow", None

    doc = valid_output("PermissionRequest", shim(tmp_path, ASK, ask=allow))
    assert doc["hookSpecificOutput"]["decision"] == {"behavior": "allow"}
    assert asked[0][:2] == (CAPTURED_INPUT["session_id"], "touches the home directory")
    assert 0 < asked[0][2] <= hooks.DEFAULT_TIMEOUT

    doc = valid_output("PreToolUse", shim(tmp_path, ASK, event="PreToolUse", ask=lambda i, r, b: ("deny", "not on my watch")))
    assert doc["hookSpecificOutput"]["permissionDecision"] == "deny"
    assert doc["hookSpecificOutput"]["permissionDecisionReason"] == "not on my watch"


def test_a_claude_sized_timeout_still_runs_the_script_and_only_an_ask_is_short_of_budget(tmp_path):
    home = tmp_path / "home"
    assert hooks.run_shim(script(tmp_path, "exit 0"), json.dumps(codex_input("PermissionRequest")), home=home, ask=no_ask, timeout=5) == ""
    out = hooks.run_shim(script(tmp_path, ASK), json.dumps(codex_input("PermissionRequest")), home=home, ask=no_ask, timeout=5)
    doc = valid_output("PermissionRequest", out)
    assert doc["hookSpecificOutput"]["decision"]["behavior"] == "deny"
    assert "5" in doc["hookSpecificOutput"]["decision"]["message"] and "touches the home directory" in doc["hookSpecificOutput"]["decision"]["message"]


def test_continue_false_and_an_ask_after_the_tool_ran_both_deny(tmp_path):
    doc = valid_output("PreToolUse", shim(tmp_path, "echo '{\"continue\": false, \"stopReason\": \"stop everything\"}'", event="PreToolUse"))
    assert doc["hookSpecificOutput"]["permissionDecisionReason"] == "stop everything"
    doc = valid_output("PostToolUse", shim(tmp_path, ASK, event="PostToolUse"))
    assert doc["decision"] == "block" and "touches the home directory" in doc["reason"]


def test_ask_denies_when_the_bridge_cannot_answer(tmp_path):
    def unreachable(codex_input, reason, budget):
        raise ipc.BridgeUnreachable("no bridge")

    doc = valid_output("PermissionRequest", shim(tmp_path, ASK, ask=unreachable))
    assert doc["hookSpecificOutput"]["decision"]["behavior"] == "deny"
    assert "no bridge" in doc["hookSpecificOutput"]["decision"]["message"]

    def unknown(codex_input, reason, budget):
        raise IpcError("unknown_target", "not a thread antiphon hosts")

    doc = valid_output("PermissionRequest", shim(tmp_path, ASK, ask=unknown))
    assert "not a thread antiphon hosts" in doc["hookSpecificOutput"]["decision"]["message"]


def test_the_repos_approve_ask_hook_runs_unchanged_through_the_shim(tmp_path):
    from test_hooks import pending_from_capture, write_state
    import dataclasses

    home = tmp_path / "home"
    write_state(home, [dataclasses.asdict(pending_from_capture())])
    asked = []

    def record(codex_input, reason, budget):
        asked.append(reason)
        return "deny", "the user said no"

    env = dict(os.environ, ANTIPHON_HOME=str(home))
    doc = codex_input("PermissionRequest", tool_input={"command": "antiphon approve e16d64"})
    out = hooks.run_shim(str(APPROVE_ASK), json.dumps(doc), home=home, ask=record, env=env)
    assert valid_output("PermissionRequest", out)["hookSpecificOutput"]["decision"]["behavior"] == "deny"
    assert 'on Codex thread "helper"' in asked[0]
    doc = codex_input("PermissionRequest", tool_input={"command": "ls -la"})
    assert hooks.run_shim(str(APPROVE_ASK), json.dumps(doc), home=home, ask=no_ask, env=env) == ""


def test_the_shim_logs_the_raw_input_and_output_of_both_sides(tmp_path):
    shim(tmp_path, DENY)
    lines = [json.loads(line) for line in (tmp_path / "home" / "log" / "hooks.jsonl").read_text().splitlines()]
    assert [(line["dir"], line["boundary"]) for line in lines] == [("in", "codex-hook"), ("out", "claude-hook"), ("in", "claude-hook"), ("out", "codex-hook")]
    assert json.loads(lines[0]["data"]) == codex_input("PermissionRequest")
    assert lines[2]["data"]["rc"] == 0 and "no uploads" in lines[2]["data"]["stdout"]
    assert lines[3]["data"]["hookSpecificOutput"]["decision"]["message"] == "no uploads"


# --- the bridge op: block and forward --------------------------------------------------


def hook_ask_args(**overrides) -> dict:
    args = {
        "thread_id": THREAD_ID, "turn_id": CAPTURED_INPUT["turn_id"], "tool_name": "Bash",
        "command": CAPTURED_INPUT["tool_input"]["command"], "cwd": CAPTURED_INPUT["cwd"],
        "reason": "touches the home directory", "hook": "guard.sh", "timeout": 5.0,
    }
    args.update(overrides)
    return args


def message_texts(frames: list[dict]) -> list[str]:
    texts = []
    for frame in frames:
        if frame.get("type") == "user":
            content = frame["message"]["content"]
            texts.append(content.split(">\n", 1)[1].rsplit("\n</cross-session-message>", 1)[0])
    return texts


def test_a_hook_ask_records_a_pending_hook_and_messages_the_spawner_once(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            await rig.start_thread()
            asking = asyncio.create_task(rig.bridge.dispatch("hook_ask", hook_ask_args(), rig.caller))
            await until(lambda: rig.bridge.state.threads[THREAD_ID].pending)
            [record] = rig.bridge.state.threads[THREAD_ID].pending
            frames = await rig.frames(1, timeout=5.0)
            more = await rig.frames(2, timeout=0.3)
            rows = await rig.bridge.dispatch("ls", {}, rig.caller)
            status = await rig.bridge.dispatch("status", {"target": "helper"}, rig.caller)
            result = await rig.bridge.dispatch("approve", {"token": record["token"]}, rig.caller)
            answer = await asyncio.wait_for(asking, 5)
            return record, message_texts(frames), more == frames, rows, status, result, answer

    record, texts, nothing_more, rows, status, result, answer = run(body())
    token = record["token"]
    assert record["kind"] == "hook" and record["hook"] == "guard.sh"
    assert texts == [
        f'The Claude hook guard.sh asks before an action runs in "helper" (token {token}): touches the home directory\n'
        f"  command: {CAPTURED_INPUT['tool_input']['command']}\n"
        f"  cwd: {CAPTURED_INPUT['cwd']}\n"
        f"The tool call is blocked until you answer. Reply with: antiphon approve {token}   or   antiphon deny {token} -- <why>"
    ]
    assert nothing_more
    assert [r["status"] for r in rows if r["kind"] == "codex"] == [f"hook {token} 0s"]
    assert status["pending"] == [f"hook {token} 0s"]
    assert result["kind"] == "hook" and result["token"] == token
    assert answer == {"decision": "allow", "why": None, "token": token}


def test_deny_with_a_reason_unblocks_the_hook_with_that_reason(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            await rig.start_thread()
            asking = asyncio.create_task(rig.bridge.dispatch("hook_ask", hook_ask_args(), rig.caller))
            await until(lambda: rig.bridge.state.threads[THREAD_ID].pending)
            token = rig.bridge.state.threads[THREAD_ID].pending[0]["token"]
            await rig.bridge.dispatch("deny", {"token": token, "why": "that file is private"}, rig.caller)
            answer = await asyncio.wait_for(asking, 5)
            with pytest.raises(IpcError) as again:
                await rig.bridge.dispatch("approve", {"token": token}, rig.caller)
            return answer, again.value.kind, rig.fake.received("turn/start")

    answer, kind, turns = run(body())
    assert answer["decision"] == "deny" and answer["why"] == "that file is private"
    assert kind == "precondition"
    assert turns == []  # nothing is told to the thread: the hook's own output carries the reason


def test_an_unanswered_hook_ask_denies_at_its_timeout_and_the_record_is_retired(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            await rig.start_thread()
            answer = await rig.bridge.dispatch("hook_ask", hook_ask_args(timeout=0.3), rig.caller)
            return answer, rig.bridge.state.threads[THREAD_ID].pending

    answer, pending = run(body())
    assert answer["decision"] == "deny"
    assert "0.3" in answer["why"] and answer["token"] in answer["why"]
    assert pending == []


def test_a_human_owned_thread_waits_for_a_shell_answer_and_the_deny_names_the_token(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            rig.bridge.state.threads["t-human"] = ThreadState(
                thread_id="t-human", name="tui", cwd="/work", origin="adopted", spawner="human", read_only=False,
            )
            asking = asyncio.create_task(rig.bridge.dispatch("hook_ask", hook_ask_args(thread_id="t-human", timeout=0.5), rig.caller))
            await until(lambda: rig.bridge.state.threads["t-human"].pending)
            token = rig.bridge.state.threads["t-human"].pending[0]["token"]
            rows = await rig.bridge.dispatch("ls", {}, rig.caller)
            frames = await rig.frames(1, timeout=0.3)
            answer = await asyncio.wait_for(asking, 5)
            return token, rows, frames, answer

    token, rows, frames, answer = run(body())
    assert [r["status"] for r in rows if r["name"] == "tui"] == [f"hook {token} 0s"]
    assert frames == []
    assert answer["decision"] == "deny" and token in answer["why"]


def test_a_hook_ask_for_a_thread_antiphon_does_not_host_is_refused(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            with pytest.raises(IpcError) as refused:
                await rig.bridge.dispatch("hook_ask", hook_ask_args(thread_id="01a0c390-0000-7000-8000-000000000009"), rig.caller)
            return refused.value

    error = run(body())
    assert error.kind == "unknown_target" and "antiphon hosts" in error.message


def test_the_spawner_is_reminded_once_after_ten_minutes(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            now = [1_000_000.0]
            rig.bridge.approvals.clock = lambda: now[0]
            await rig.start_thread()
            asking = asyncio.create_task(rig.bridge.dispatch("hook_ask", hook_ask_args(timeout=30), rig.caller))
            await rig.frames(1, timeout=5.0)
            now[0] += REMIND_AFTER
            await rig.bridge.reconcile()
            await rig.bridge.reconcile()
            frames = await rig.frames(2, timeout=5.0)
            more = await rig.frames(3, timeout=0.3)
            token = rig.bridge.state.threads[THREAD_ID].pending[0]["token"]
            await rig.bridge.dispatch("deny", {"token": token, "why": "no"}, rig.caller)
            await asking
            return message_texts(frames), more == frames, token

    texts, nothing_more, token = run(body())
    assert texts[1].startswith(f'Still waiting: the Claude hook guard.sh in "helper" (token {token}) has blocked an action for 10m')
    assert nothing_more


def test_a_turn_ending_while_the_hook_waits_denies_and_drops_the_record(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            await rig.start_thread()
            asking = asyncio.create_task(rig.bridge.dispatch("hook_ask", hook_ask_args(timeout=30, turn_id=COMPLETED_NOTICE["turn"]["id"]), rig.caller))
            await until(lambda: rig.bridge.state.threads[THREAD_ID].pending)
            await rig.fake.notify("turn/completed", for_thread(COMPLETED_NOTICE))
            answer = await asyncio.wait_for(asking, 5)
            return answer, rig.bridge.state.threads[THREAD_ID].pending

    answer, pending = run(body())
    assert answer["decision"] == "deny" and "turn" in answer["why"]
    assert pending == []


def test_a_bridge_restart_forgets_hook_records_nobody_waits_on(short_tmp):
    async def body():
        async with Rig(short_tmp) as rig:
            await rig.start_thread()
            asking = asyncio.create_task(rig.bridge.dispatch("hook_ask", hook_ask_args(timeout=30), rig.caller))
            await until(lambda: rig.bridge.state.threads[THREAD_ID].pending)
            rig.bridge.save()
            asking.cancel()
        from antiphon.bridge import Bridge

        again = Bridge(rig.home, sessions_dir=rig.sessions_dir, codex_home=rig.codex_home, ensure_running=lambda *a, **k: str(rig.daemon_sock))
        return again.state.threads[THREAD_ID].pending

    assert run(body()) == []


# --- the installer ------------------------------------------------------------------------


# The event label inside a hook's config-state key (`codex-rs/hooks/src/lib.rs`, `hook_event_key_label`).
EVENT_KEY_LABELS = {"PermissionRequest": "permission_request", "PreToolUse": "pre_tool_use", "PostToolUse": "post_tool_use"}


def fake_hook_hash(entry_key: str, command: str) -> str:
    import hashlib

    return "sha256:" + hashlib.sha256(f"{entry_key}|{command}".encode()).hexdigest()


class FakeHooksCatalog:
    """`hooks/list` and `config/batchWrite` as the daemon answers them, over the hooks.json
    in a temporary Codex home: entries in the captured shape, keyed and hashed the way the
    daemon keys and hashes them, trusted once a batch write named their hash."""

    def __init__(self, codex_home: Path):
        self.codex_home = codex_home
        self.trusted: dict[str, str] = {}
        self.writes: list[dict] = []
        self.listed = 0

    def hooks_list(self, params) -> dict:
        self.listed += 1
        path = self.codex_home / "hooks.json"
        entries = []
        events = json.loads(path.read_text())["hooks"] if path.exists() else {}
        order = 0
        for event, groups in events.items():
            for group_index, group in enumerate(groups):
                for handler_index, handler in enumerate(group["hooks"]):
                    key = f"{path}:{EVENT_KEY_LABELS[event]}:{group_index}:{handler_index}"
                    current = fake_hook_hash(key, handler["command"])
                    entry = dict(
                        CAPTURED_ENTRY, key=key, eventName=event[0].lower() + event[1:], command=handler["command"],
                        matcher=group.get("matcher"), timeoutSec=handler.get("timeout", 600),
                        statusMessage=handler.get("statusMessage"), sourcePath=str(path), source="user",
                        displayOrder=order, currentHash=current,
                        trustStatus="trusted" if self.trusted.get(key) == current else ("modified" if key in self.trusted else "untrusted"),
                    )
                    entries.append(entry)
                    order += 1
        return {"result": {"data": [{"cwd": cwd, "hooks": entries, "warnings": [], "errors": []} for cwd in params["cwds"]]}}

    def config_batch_write(self, params) -> dict:
        self.writes.append(params)
        for edit in params["edits"]:
            for key, state in edit["value"].items():
                self.trusted[key] = state["trusted_hash"]
        return {"result": {"status": "ok", "version": "1", "filePath": str(self.codex_home / "config.toml"), "overriddenMetadata": None}}


@pytest.fixture
def install_rig(short_tmp, monkeypatch):
    rig = CliRig(short_tmp, monkeypatch)
    rig.catalog = FakeHooksCatalog(rig.codex_home)
    rig.daemon.replies["hooks/list"] = rig.catalog.hooks_list
    rig.daemon.replies["config/batchWrite"] = rig.catalog.config_batch_write
    yield rig
    rig.close()


def test_install_writes_the_entry_beside_other_hooks_and_trusts_it(install_rig, tmp_path, capsys):
    rig = install_rig
    guard = script(tmp_path, "exit 0", name="guard.sh")
    other = {"matcher": "Bash", "hooks": [{"type": "command", "command": "/elsewhere/guard.sh"}]}
    (rig.codex_home / "hooks.json").write_text(json.dumps({"description": "mine", "hooks": {"PreToolUse": [other]}}))

    for _ in range(2):
        code, out, err = rig.run("hook", "install", guard, "--matcher", "Bash", capsys=capsys)
        assert code == 0, err
    data = json.loads((rig.codex_home / "hooks.json").read_text())
    assert data["description"] == "mine"
    assert data["hooks"]["PreToolUse"][0] == other
    [ours] = data["hooks"]["PreToolUse"][1:]
    [entry] = ours["hooks"]
    assert ours["matcher"] == "Bash"
    assert entry["type"] == "command" and entry["timeout"] == 600
    assert shlex.split(entry["command"])[-4:] == ["hook", "run", guard, "--timeout"] or shlex.split(entry["command"])[-5:-1] == ["hook", "run", guard, "--timeout"]
    key = f"{rig.codex_home / 'hooks.json'}:pre_tool_use:1:0"
    assert rig.catalog.writes == [{
        "edits": [{"keyPath": "hooks.state", "value": {key: {"trusted_hash": fake_hook_hash(key, entry["command"])}}, "mergeStrategy": "upsert"}],
        "reloadUserConfig": True,
    }]
    assert "trusted" in out


def test_install_on_another_event_with_a_timeout_and_uninstall_removes_only_ours(install_rig, tmp_path, capsys):
    rig = install_rig
    guard = script(tmp_path, "exit 0", name="guard.sh")
    other = {"matcher": "Bash", "hooks": [{"type": "command", "command": "/elsewhere/guard.sh"}]}
    (rig.codex_home / "hooks.json").write_text(json.dumps({"hooks": {"PermissionRequest": [other]}}))
    assert rig.run("hook", "install", guard, "--event", "PermissionRequest", "--timeout", "30", capsys=capsys)[0] == 0
    data = json.loads((rig.codex_home / "hooks.json").read_text())
    [entry] = data["hooks"]["PermissionRequest"][1]["hooks"]
    assert entry["timeout"] == 30 and shlex.split(entry["command"])[-1] == "30"
    assert "matcher" not in data["hooks"]["PermissionRequest"][1]

    code, out, err = rig.run("hook", "list", capsys=capsys)
    assert code == 0
    assert "PermissionRequest" in out and guard in out and "trusted" in out

    code, out, err = rig.run("hook", "uninstall", guard, capsys=capsys)
    assert code == 0
    assert json.loads((rig.codex_home / "hooks.json").read_text()) == {"hooks": {"PermissionRequest": [other]}}


def test_install_fails_when_codex_does_not_list_the_hook(install_rig, tmp_path, capsys):
    rig = install_rig
    rig.daemon.replies["hooks/list"] = lambda params: {"result": {"data": [{"cwd": params["cwds"][0], "hooks": [], "warnings": ["hooks are off"], "errors": []}]}}
    guard = script(tmp_path, "exit 0", name="guard.sh")
    code, out, err = rig.run("hook", "install", guard, capsys=capsys)
    assert code == 2
    assert "hooks are off" in err
    assert rig.catalog.writes == []


# --- end to end through the CLI --------------------------------------------------------------


def test_unreadable_stdin_exits_2_with_the_reason_on_stderr(tmp_path, monkeypatch):
    # Without an event no JSON answer parses; exit 2 plus stderr denies on every event.
    monkeypatch.setenv("ANTIPHON_HOME", str(tmp_path / "home"))
    done = subprocess.run([sys.executable, "-m", "antiphon", "hook", "run", script(tmp_path, "exit 0")], input="not json", capture_output=True, text=True, env=dict(os.environ))
    assert done.returncode == 2
    assert "not a Codex hook input" in done.stderr and done.stdout == ""


def test_a_codex_hook_input_on_stdin_blocks_until_approve_then_prints_allow(install_rig, tmp_path, monkeypatch):
    rig = install_rig
    claude = FakeClaude(rig.config_dir / "sessions", rig.tmp / "socks", name="claude-main")
    try:
        assert rig.run_subprocess("start", "-n", "helper", "-C", str(rig.tmp)).returncode == 0
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and ipc.call(str(rig.home / "bridge.sock"), "ping", {})["peers"] < 1:
            time.sleep(0.1)
        guard = script(tmp_path, ASK, name="guard.sh")
        doc = codex_input("PermissionRequest", session_id=THREAD_ID)
        shim = {}

        def run_shim():
            shim["done"] = subprocess.run(
                [sys.executable, "-m", "antiphon", "hook", "run", guard], input=json.dumps(doc), capture_output=True,
                text=True, env=dict(os.environ), timeout=20,
            )

        shim_thread = threading.Thread(target=run_shim)
        shim_thread.start()
        [frame] = claude.wait_for_frames(1, timeout=10)
        token = frame["message"]["content"].split("(token ", 1)[1][:6]
        done = rig.run_subprocess("approve", token)
        assert done.returncode == 0, done.stderr
        shim_thread.join(timeout=20)
    finally:
        claude.close()
    assert shim["done"].returncode == 0, shim["done"].stderr
    assert valid_output("PermissionRequest", shim["done"].stdout)["hookSpecificOutput"]["decision"] == {"behavior": "allow"}
    assert f"approved {token} on helper" in done.stdout
