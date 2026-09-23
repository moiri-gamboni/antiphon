#!/bin/bash
# turn/start on a busy thread returns the running turn instead of erroring.
# Does the text reach the model like a steer, or is it dropped? The second
# call carries a word the first turn had no reason to say.
# $THREAD in single quotes is a capture_daemon.py placeholder.
set -uo pipefail
CAP="$(dirname "$0")/../capture_daemon.py"
CWD=$HOME/antiphon-capture
OUT=$1

json() { python3 -c 'import json,sys;print(json.dumps(sys.argv[1]))' "$1"; }
LONG="Run this shell command and wait for it: sleep 45. Then reply with the single word SLEPT."
SECOND="Additional instruction: end your reply with the word BANANA."
START="{\"cwd\":\"$CWD\",\"approvalPolicy\":\"on-request\",\"approvalsReviewer\":\"auto_review\",\"sandbox\":\"workspace-write\",\"ephemeral\":false}"
turn() { printf '{"threadId":"$THREAD","sandboxPolicy":{"type":"workspaceWrite","networkAccess":false},"input":[{"type":"text","text":%s}]}' "$(json "$1")"; }

python3 "$CAP" --listen 5 \
  thread/start "$START" \
  turn/start "$(turn "$LONG")" \
  @sleep 12 \
  turn/start "$(turn "$SECOND")" \
  @await turn/completed 240 \
  > "$OUT"
echo "exit=$? lines=$(wc -l < "$OUT")"
echo "--- did the model see the second instruction?"
python3 -c "
import json,sys
for line in open('$OUT'):
    line=line.strip()
    if not line.startswith('{'): continue
    d=json.loads(line); d=d.get('sent',d); p=d.get('params') or {}
    it=p.get('item') or {}
    if d.get('method')=='item/completed' and it.get('type') in ('userMessage','agentMessage'):
        print(it['type'], ':', json.dumps(it)[:300])
"
