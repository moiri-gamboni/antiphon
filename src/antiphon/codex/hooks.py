"""Claude Code hook scripts run unchanged as Codex hooks.

Codex's hook events mirror Claude Code's: a command hook reads one JSON document
on stdin and answers with JSON on stdout or a blocking reason on stderr with exit
code 2. The two contracts differ in a few field names and in what each side
accepts, so `antiphon hook run <script>` sits between them: it maps the Codex
input to the Claude shape, runs the script, and maps the script's answer to what
Codex parses for that event. Contracts, read for Codex 0.155.1:

- Codex: the generated schemas in `tests/fixtures/codex-hook-schemas/` (from
  `codex-rs/hooks/schema/generated/`), the event handlers in
  `codex-rs/hooks/src/events/` and the output parser in
  `codex-rs/hooks/src/engine/output_parser.rs`; https://learn.chatgpt.com/docs/hooks.
- Claude Code: https://code.claude.com/docs/en/hooks (PreToolUse and PostToolUse
  input fields, the exit-code contract, `hookSpecificOutput`).

Input mapping (Codex field -> Claude field): `session_id` (the Codex thread id,
which the captured input confirms) -> `session_id`; `transcript_path` (nullable
in Codex) -> `transcript_path`; `cwd`, `permission_mode`, `tool_name`,
`tool_input`, `tool_use_id`, `tool_response`, `agent_id`, `agent_type` keep their
names; `hook_event_name` becomes `PreToolUse` for both `PermissionRequest` and
`PreToolUse`, and stays `PostToolUse`. Codex's `model` and `turn_id` have no
Claude counterpart and are passed through untouched. Claude fields with no
Codex source and therefore absent: `prompt_id`, `scratchpad_dir`, `effort`, and
`tool_use_id` on a PermissionRequest.

Output mapping (Claude decision -> Codex output): `allow` -> a PermissionRequest
allow, or silence on a Codex PreToolUse (Codex rejects `permissionDecision:
allow` without an input rewrite and lets the call through as a failed hook, so
silence is the faithful answer); `deny`, the deprecated `decision: block`, exit
code 2 with stderr, and `continue: false` -> a deny carrying the reason (a
default reason is supplied when the script gave none, since Codex treats a deny
without a reason as invalid output and proceeds); `ask` -> the bridge forwards
the request to the thread's spawner and the answer becomes allow or deny; no
decision -> no decision. Output that looks like JSON but is not, and input
rewrites on a PermissionRequest (Codex cannot apply them there), deny with the
raw text as the message: fail closed, never silently open.

Codex's default timeout for a command hook is 600 seconds (`codex-rs/hooks/src/
engine/discovery.rs`, `normalize_command_hook`); the same number is written to
`hooks.json` and is the budget within which a forwarded `ask` must be answered.
"""

from __future__ import annotations

import json
import logging
import os
import shlex
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from antiphon.codex.daemon import DaemonError
from antiphon.ipc import BridgeUnreachable, IpcError
from antiphon.rawlog import RawLog

if TYPE_CHECKING:
    from antiphon.bridge import Bridge

log = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 600
# Kept back from the hook's timeout so the shim prints its answer before Codex kills it:
# one second after the script, and enough to start a bridge and reach it before an ask.
SCRIPT_MARGIN = 1.0
IPC_MARGIN = 5.0
CLAUDE_EVENT = {"PermissionRequest": "PreToolUse", "PreToolUse": "PreToolUse", "PostToolUse": "PostToolUse"}
SCHEMA_NAMES = {"PermissionRequest": "permission-request", "PreToolUse": "pre-tool-use", "PostToolUse": "post-tool-use"}


class HookInputError(Exception):
    """The document on stdin is not a Codex hook input for a supported event."""


@dataclass
class Decision:
    kind: str  # "allow" | "deny" | "ask" | "none"
    reason: str | None = None
    updated_input: object = None
    additional_context: str | None = None
    system_message: str | None = None


def claude_input(codex_input: dict) -> dict:
    mapped = dict(codex_input)
    mapped["hook_event_name"] = CLAUDE_EVENT[codex_input["hook_event_name"]]
    return mapped


def interpret(rc: int, stdout: str, stderr: str, script: str) -> Decision:
    """The Claude hook contract applied to what the script did."""
    text = stdout.strip()
    doc = None
    if text.startswith("{"):
        try:
            doc = json.loads(text)
        except ValueError:
            doc = None
        if not isinstance(doc, dict):
            return Decision("deny", f"{os.path.basename(script)} printed output that is not a JSON object: {stdout.strip()}")
    doc = doc or {}
    specific = doc.get("hookSpecificOutput") or {}
    reason = specific.get("permissionDecisionReason") or doc.get("reason")
    decision = Decision(
        "none", reason, updated_input=specific.get("updatedInput"),
        additional_context=specific.get("additionalContext"), system_message=doc.get("systemMessage"),
    )
    permission = specific.get("permissionDecision")
    legacy = doc.get("decision")
    if rc == 2:
        decision.kind = "deny"
        decision.reason = reason or stderr.strip()
    elif permission in ("allow", "deny", "ask"):
        decision.kind = permission
    elif legacy in ("block", "deny"):
        decision.kind = "deny"
    elif legacy in ("approve", "allow"):
        decision.kind = "allow"
    elif doc.get("continue") is False:
        decision.kind = "deny"
        decision.reason = doc.get("stopReason") or reason
    if decision.kind == "deny" and not (decision.reason or "").strip():
        decision.reason = f"denied by the Claude hook {os.path.basename(script)}"
    return decision


def codex_output(event: str, decision: Decision) -> dict | None:
    """What Codex parses for `event`, or None when there is nothing to say."""
    out: dict = {}
    if decision.system_message:
        out["systemMessage"] = decision.system_message
    if event == "PermissionRequest":
        if decision.kind == "allow" and decision.updated_input is not None:
            decision = Decision("deny", "the hook rewrote the tool input, which a Codex PermissionRequest hook cannot apply")
        if decision.kind == "allow":
            out["hookSpecificOutput"] = {"hookEventName": event, "decision": {"behavior": "allow"}}
        elif decision.kind == "deny":
            out["hookSpecificOutput"] = {"hookEventName": event, "decision": {"behavior": "deny", "message": decision.reason}}
        return out or None
    specific: dict = {}
    if decision.additional_context:
        specific["additionalContext"] = decision.additional_context
    if event == "PreToolUse":
        if decision.kind == "deny":
            specific.update(permissionDecision="deny", permissionDecisionReason=decision.reason)
        elif decision.kind == "allow" and decision.updated_input is not None:
            specific.update(permissionDecision="allow", updatedInput=decision.updated_input)
    elif decision.kind == "deny":
        out.update(decision="block", reason=decision.reason)
    if specific:
        out["hookSpecificOutput"] = {"hookEventName": event, **specific}
    return out or None


def summarize_tool_input(tool_input) -> str:
    """One line naming the action for a message: the shell command, else the arguments."""
    if isinstance(tool_input, dict) and isinstance(tool_input.get("command"), str):
        return tool_input["command"]
    return json.dumps(tool_input)


def run_shim(script: str, stdin_text: str, *, home, ask, event: str | None = None,
             timeout: float = DEFAULT_TIMEOUT, env: dict | None = None) -> str:
    """Run `script` as the Claude hook for the Codex input `stdin_text`; the text to print
    for Codex ("" for no decision). `ask(codex_input, reason, budget)` answers an `ask`
    with `("allow" | "deny", why)` within `budget` seconds. Raises `HookInputError`
    when stdin is not a hook input, since without the event no JSON answer parses."""
    started = time.monotonic()
    rawlog = RawLog(Path(home) / "log" / "hooks.jsonl")
    rawlog.log("in", "codex-hook", stdin_text)
    try:
        codex_in = json.loads(stdin_text)
        event = event or codex_in["hook_event_name"]
        if event not in CLAUDE_EVENT:
            raise HookInputError(f"unsupported Codex hook event {event!r}")
        mapped = claude_input(codex_in)
    except (ValueError, KeyError, TypeError) as e:
        raise HookInputError(f"stdin is not a Codex hook input: {e!r}") from e
    rawlog.log("out", "claude-hook", {"script": script, "stdin": mapped})
    budget = timeout - (time.monotonic() - started) - SCRIPT_MARGIN
    try:
        done = subprocess.run([script], input=json.dumps(mapped), capture_output=True, text=True, timeout=budget, env=env)
    except OSError as e:
        # A script Codex cannot run would make Codex fail the hook and proceed; a deny
        # keeps a broken guard closed.
        decision = Decision("deny", f"antiphon could not run {script}: {e}")
        rawlog.log("in", "claude-hook", {"error": repr(e)})
    except subprocess.TimeoutExpired as e:
        decision = Decision("deny", f"{os.path.basename(script)} did not answer within {budget:.0f} s")
        rawlog.log("in", "claude-hook", {"error": repr(e)})
    else:
        rawlog.log("in", "claude-hook", {"rc": done.returncode, "stdout": done.stdout, "stderr": done.stderr})
        decision = interpret(done.returncode, done.stdout, done.stderr, script)
    if decision.kind == "ask":
        decision = _forward_ask(codex_in, decision, ask, timeout - (time.monotonic() - started) - IPC_MARGIN, event, script, timeout)
    output = codex_output(event, decision)
    rawlog.log("out", "codex-hook", output)
    return json.dumps(output) if output is not None else ""


def _forward_ask(codex_in: dict, decision: Decision, ask, budget: float, event: str, script: str, timeout: float) -> Decision:
    reason = decision.reason or f"the Claude hook {os.path.basename(script)} asked"
    if event == "PostToolUse":
        # The tool already ran; there is nothing to ask permission for.
        return Decision("deny", f"{os.path.basename(script)} answered ask after the tool ran: {reason}")
    if budget < 1:
        return Decision("deny", f"{reason} (the hook's {timeout:g} s timeout leaves no time to forward the request; install it with a longer --timeout)")
    try:
        verdict, why = ask(codex_in, reason, budget)
    except (IpcError, BridgeUnreachable) as e:
        # The bridge could not put the question to anyone; the call stays blocked with the
        # reason, as it would under Claude Code with nobody at the prompt.
        message = getattr(e, "message", None) or str(e)
        return Decision("deny", f"{reason} (antiphon could not forward the request: {message})")
    if verdict == "allow":
        return Decision("allow", system_message=decision.system_message)
    return Decision("deny", why or "denied by the session that spawned this thread", system_message=decision.system_message)


# --- hooks.json ---------------------------------------------------------------------------


def codex_home() -> Path:
    return Path(os.environ.get("CODEX_HOME") or Path.home() / ".codex")


def hooks_file(home: Path | None = None) -> Path:
    return (home or codex_home()) / "hooks.json"


def load_hooks(path: Path) -> dict:
    if not path.exists():
        return {"hooks": {}}
    data = json.loads(path.read_text())
    data.setdefault("hooks", {})
    return data


def save_hooks(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2) + "\n")


def antiphon_command() -> str:
    found = shutil.which("antiphon")
    if found:
        return shlex.quote(found)
    return f"{shlex.quote(sys.executable)} -m antiphon"


def shim_command(script: str, timeout: int) -> str:
    return f"{antiphon_command()} hook run {shlex.quote(script)} --timeout {timeout}"


def script_of(command: str) -> str | None:
    """The Claude script a hooks.json command runs through the shim, or None for any other command."""
    try:
        tokens = shlex.split(command)
    except ValueError:
        return None
    for i in range(len(tokens) - 2):
        if tokens[i : i + 2] == ["hook", "run"]:
            return tokens[i + 2]
    return None


def installed(data: dict) -> list[dict]:
    """The shim entries in a hooks.json document: `{event, matcher, script, timeout, command}`."""
    found = []
    for event, groups in data["hooks"].items():
        for group in groups:
            for handler in group.get("hooks", []):
                script = script_of(handler.get("command", ""))
                if script is not None:
                    found.append({"event": event, "matcher": group.get("matcher"), "script": script,
                                  "timeout": handler.get("timeout", DEFAULT_TIMEOUT), "command": handler["command"]})
    return found


def remove_script(data: dict, script: str) -> bool:
    """Drop every shim entry for `script`, and the groups and events left empty; True if any."""
    removed = False
    for event in list(data["hooks"]):
        groups = data["hooks"][event]
        for group in groups:
            kept = [h for h in group.get("hooks", []) if script_of(h.get("command", "")) != script]
            removed = removed or len(kept) != len(group.get("hooks", []))
            group["hooks"] = kept
        groups[:] = [g for g in groups if g.get("hooks")]
        if not groups:
            del data["hooks"][event]
    return removed


def add_script(data: dict, script: str, event: str, matcher: str | None, timeout: int) -> dict:
    """Append one matcher group running `script` through the shim; the handler entry added."""
    handler = {
        "type": "command", "command": shim_command(script, timeout), "timeout": timeout,
        "statusMessage": f"antiphon: Claude hook {os.path.basename(script)} (an ask blocks until antiphon approve|deny <token>)",
    }
    group = {"hooks": [handler]}
    if matcher:
        group = {"matcher": matcher, **group}
    data["hooks"].setdefault(event, []).append(group)
    return handler


# --- the bridge ops: the daemon's view of the hooks, and the trust write ----------------------


async def op_hook_list(bridge: Bridge, args: dict, caller) -> list[dict]:
    """`hooks/list` for one directory: the entries with their keys, hashes and trust status."""
    d = await bridge._require_daemon()
    params = {"cwds": [args["cwd"]]} if args.get("cwd") else {}
    try:
        return (await d.request("hooks/list", params))["data"]
    except DaemonError as e:
        raise IpcError("precondition", f"hooks/list failed: {e.error.get('message')}", e.error) from e


async def op_hook_trust(bridge: Bridge, args: dict, caller) -> dict:
    """Record a hook's current hash as trusted, through the daemon's config write (the call
    Codex's own TUI makes: `hooks.state.<key>.trusted_hash` upserted into the user
    config.toml and reloaded). Shape from `codex-rs/app-server-protocol/src/protocol/v2/
    config.rs`; no capture of this call exists yet."""
    d = await bridge._require_daemon()
    params = {
        "edits": [{"keyPath": "hooks.state", "value": {args["key"]: {"trusted_hash": args["hash"]}}, "mergeStrategy": "upsert"}],
        "reloadUserConfig": True,
    }
    try:
        return await d.request("config/batchWrite", params)
    except DaemonError as e:
        raise IpcError("precondition", f"config/batchWrite failed: {e.error.get('message')}", e.error) from e


def install(bridge: Bridge) -> None:
    async def hook_list(args, caller):
        return await op_hook_list(bridge, args, caller)

    async def hook_trust(args, caller):
        return await op_hook_trust(bridge, args, caller)

    bridge.ops["hook_list"] = hook_list
    bridge.ops["hook_trust"] = hook_trust
