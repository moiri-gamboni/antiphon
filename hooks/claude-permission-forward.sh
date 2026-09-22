#!/usr/bin/env bash
# Claude Code PreToolUse hook: the session's permission decisions are made by whoever
# started it. Installed by `antiphon start --claude` into that session alone, through
# --settings, so no other session on the machine runs it.
#
#   claude-permission-forward.sh [seconds]
#
# Claude Code has no permission-request event, so this runs before each call of the tools
# it is installed for (all of them, or the --gate matcher). It hands the call to the
# bridge, which puts it to the session's spawner as a message with a token and blocks
# until `antiphon approve <token>` or `antiphon deny <token> -- <why>` answers, or the
# argument's seconds run out (default 595, just under Claude Code's 600 s hook timeout).
# The answer becomes this hook's decision: allow lets the call through, deny blocks it
# with the reason.
#
# Anything that goes wrong here — no bridge, an unknown session, a malformed input, a
# request nobody answers — denies the call and says why. A guard that fails open is not a
# guard, and Claude Code lets a tool call through when a hook exits non-zero without JSON.
# The hook input stays on stdin, where a whole file's contents fit; an environment
# variable does not hold one.
set -uo pipefail

exec python3 <(cat <<'PY'
import json
import socket
import sys

socket_path, budget = sys.argv[1], float(sys.argv[2])


def ask(request: dict) -> dict:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
        s.settimeout(budget + 5)
        s.connect(socket_path)
        s.sendall(json.dumps(request).encode() + b"\n")
        data = b""
        while not data.endswith(b"\n"):
            chunk = s.recv(65536)
            if not chunk:
                raise ConnectionError("the bridge closed the connection without answering")
            data += chunk
    return json.loads(data)


def decide() -> tuple[str, str | None]:
    hook_input = json.loads(sys.stdin.read())
    tool_input = hook_input.get("tool_input") or {}
    command = tool_input.get("command")
    reply = ask({"v": 1, "op": "hook_ask", "args": {
        "asker": hook_input["session_id"],
        "tool_name": hook_input["tool_name"],
        "command": command if isinstance(command, str) else json.dumps(tool_input),
        "cwd": hook_input.get("cwd", ""),
        "reason": f"{hook_input['tool_name']} needs a decision",
        "hook": "claude-permission-forward.sh",
        "timeout": budget,
    }})
    if reply["ok"]:
        return reply["result"]["decision"], reply["result"]["why"]
    return "deny", reply["error"]["message"]


try:
    decision, why = decide()
except Exception as e:
    decision, why = "deny", f"antiphon could not put this to the session's spawner: {e!r}"

out = {"hookEventName": "PreToolUse", "permissionDecision": decision}
if decision == "deny" or why:
    out["permissionDecisionReason"] = why or "denied by the session that started this one"
print(json.dumps({"hookSpecificOutput": out}))
PY
) "${ANTIPHON_HOME:-$HOME/.antiphon}/bridge.sock" "${1:-595}"
