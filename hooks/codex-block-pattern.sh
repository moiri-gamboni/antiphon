#!/usr/bin/env bash
# Codex PermissionRequest hook: deny any escalated command matching a pattern, before
# Codex's automatic reviewer sees it. The Codex counterpart of a Claude Code PreToolUse
# guard. Hooks run first and once: a deny here is final, an allow skips the reviewer,
# and no output lets the reviewer decide as usual.
#
# Register it in ~/.codex/hooks.json (the pattern is the first argument; the default
# below applies when none is given):
#
#   {"hooks": {"PermissionRequest": [{"matcher": "Bash", "hooks": [
#       {"type": "command", "command": "<repo>/hooks/codex-block-pattern.sh 'rm -rf|git push --force'", "timeout": 5}]}]}}
#
# A user hook runs only once trusted (`/hooks` in the Codex TUI, which records the
# hook's hash under [hooks.state] in config.toml) or with --dangerously-bypass-hook-trust.
set -uo pipefail

pattern=${1:-'rm -rf|git push --force'}
input=$(cat || true)
home=${ANTIPHON_HOME:-$HOME/.antiphon}
if [ -d "$home/log" ]; then
    printf '%s\n' "in $input" >> "$home/log/codex-hooks.log"
fi
output=$(ANTIPHON_HOOK_INPUT="$input" python3 - "$pattern" <<'PY'
import json, os, re, sys

command = json.loads(os.environ["ANTIPHON_HOOK_INPUT"]).get("tool_input", {}).get("command", "")
match = re.search(sys.argv[1], command)
if match is None:
    sys.exit(0)
print(json.dumps({"hookSpecificOutput": {
    "hookEventName": "PermissionRequest",
    "decision": {"behavior": "deny", "message": f"blocked by the antiphon guard hook: the command matches {match.group(0)!r}"},
}}))
PY
)
if [ -d "$home/log" ]; then
    printf '%s\n' "out $output" >> "$home/log/codex-hooks.log"
fi
[ -n "$output" ] && printf '%s\n' "$output"
exit 0
