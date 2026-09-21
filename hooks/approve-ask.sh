#!/usr/bin/env bash
# Claude Code PreToolUse hook (matcher: Bash): a real permission prompt before an
# `antiphon approve <token>` or `antiphon deny <token>` reaches Codex.
#
# An escalation arrives in a Claude session as a cross-session message, which Claude
# Code never treats as the user's consent; without this hook the session decides on its
# own. With it, the prompt shows the command, its directory and Codex's stated reason,
# read from the pending record in the bridge's state file. Any other command passes
# untouched (exit 0, no output); an approval whose record cannot be read still prompts. Installed by `install.sh --human-approvals`; the
# settings.json entry it adds:
#
#   {"hooks": {"PreToolUse": [{"matcher": "Bash", "hooks": [
#       {"type": "command", "command": "<repo>/hooks/approve-ask.sh", "timeout": 5}]}]}}
set -uo pipefail

input=$(cat || true)
home=${ANTIPHON_HOME:-$HOME/.antiphon}
if [ -d "$home/log" ]; then
    printf '%s\n' "in $input" >> "$home/log/approve-ask.log"
fi
output=$(ANTIPHON_HOOK_INPUT="$input" python3 - "$home/state.json" <<'PY'
import json, os, re, sys

command = json.loads(os.environ["ANTIPHON_HOOK_INPUT"]).get("tool_input", {}).get("command", "")
match = re.search(r"(?:^|[;&|]\s*|\s)antiphon\s+(approve|deny)\s+([0-9a-f]{6})\b", command)
if match is None:
    sys.exit(0)
verb, token = match.groups()


def describe() -> list[str]:
    for thread in json.load(open(sys.argv[1]))["threads"].values():
        for record in thread["pending"]:
            if record["token"] == token and not record["resolved"]:
                lines = [f'answers an escalation on Codex thread "{thread["name"]}"', f"command: {record['command']}"]
                if record["cwd"]:
                    lines.append(f"cwd: {record['cwd']}")
                return [*lines, f"reason: {record['rationale']}"]
    raise KeyError(token)


# Once the command is an approval, the prompt always appears: a record this hook cannot
# read (no state file, a token the bridge does not know, a field renamed) must not let
# the approval through silently.
try:
    lines = describe()
except (OSError, ValueError, KeyError, TypeError):
    lines = [f"pending record for {token} not readable in {sys.argv[1]}"]
print(json.dumps({"hookSpecificOutput": {
    "hookEventName": "PreToolUse",
    "permissionDecision": "ask",
    "permissionDecisionReason": "\n".join([f"antiphon {verb} {token} {lines[0]}", *lines[1:]]),
}}))
PY
)
if [ -d "$home/log" ]; then
    printf '%s\n' "out $output" >> "$home/log/approve-ask.log"
fi
[ -n "$output" ] && printf '%s\n' "$output"
exit 0
