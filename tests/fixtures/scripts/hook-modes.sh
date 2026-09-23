#!/bin/bash
# Does a PermissionRequest hook's own decision short-circuit the automatic reviewer?
# One escalation per mode: the hook answers allow, then deny.
set -uo pipefail
CAP="$(dirname "$0")/../capture_daemon.py"
CWD=$HOME/antiphon-capture
PROBE=$HOME/antiphon-hook-probe.txt
OUTDIR=${1:-.}

json() { python3 -c 'import json,sys;print(json.dumps(sys.argv[1]))' "$1"; }
PROMPT="Using a shell command, create the file $PROBE containing the single word hooked. Then reply with the single word DONE."
START="{\"cwd\":\"$CWD\",\"approvalPolicy\":\"on-request\",\"approvalsReviewer\":\"auto_review\",\"sandbox\":\"workspace-write\",\"ephemeral\":false}"
TURN="{\"threadId\":\"\$THREAD\",\"sandboxPolicy\":{\"type\":\"workspaceWrite\",\"networkAccess\":false},\"input\":[{\"type\":\"text\",\"text\":$(json "$PROMPT")}]}"

for mode in allow deny; do
  echo "=== mode $mode"
  printf '%s\n' "$mode" > "$CWD/hook-mode"
  rm -f "$PROBE"
  python3 "$CAP" --listen 5 thread/start "$START" turn/start "$TURN" @await turn/completed 300 \
    > "$OUTDIR/hook-$mode.jsonl"
  echo "  exit=$? lines=$(wc -l < "$OUTDIR/hook-$mode.jsonl")"
  if [ -e "$PROBE" ]; then echo "  the command ran: $(cat "$PROBE")"; else echo "  the command did not run"; fi
done

printf 'log\n' > "$CWD/hook-mode"
rm -f "$PROBE"
echo "=== hook mode restored to log, probe removed"
