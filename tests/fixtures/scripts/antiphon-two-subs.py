#!/usr/bin/env python3
"""Two connections subscribed to one user-reviewer thread. Connection A raises an
escalation and answers it; connection B only watches. Does B, the silent second
subscriber (the role the bridge plays on an adopted thread), get told the request
was resolved so it can clear its own pending record?

Every frame each connection sees is printed, tagged A/ or B/, to stdout.
"""
import json
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from capture_daemon import DEFAULT_SOCKET, Connection  # noqa: E402

CWD = os.path.expanduser("~/antiphon-capture")
PROBE = os.path.expanduser("~/antiphon-twosub-probe.txt")


class Conn:
    def __init__(self, tag):
        self.tag = tag
        self.c = Connection(DEFAULT_SOCKET)
        self.n = 0
        self.thread = None

    def send(self, msg):
        print(json.dumps({"from": self.tag, "sent": msg}), flush=True)
        self.c.send_text(json.dumps(msg))

    def req(self, method, params):
        self.n += 1
        rid = self.n
        self.send({"jsonrpc": "2.0", "id": rid, "method": method, "params": params})
        return rid

    def notify(self, method, params):
        self.send({"jsonrpc": "2.0", "method": method, "params": params})

    def drain(self, seconds, want=None, on_request=None):
        """Read for `seconds`; return the message whose method == want, or None.
        on_request(msg) is called for any */requestApproval server request."""
        end = time.time() + seconds
        found = None
        while time.time() < end:
            try:
                line = self.c.recv_text(0.4)
            except EOFError as e:
                print(json.dumps({"from": self.tag, "note": f"closed: {e}"}), flush=True)
                return found
            if line is None:
                continue
            print(json.dumps({"from": self.tag, "recv": json.loads(line)}), flush=True)
            msg = json.loads(line)
            if isinstance(msg.get("result"), dict) and isinstance(msg["result"].get("thread"), dict):
                self.thread = msg["result"]["thread"]["id"]
            m = msg.get("method")
            if m and "id" in msg and m.endswith("/requestApproval") and on_request:
                on_request(self, msg)
            if m == want:
                found = msg
        return found

    def init(self):
        self.req("initialize", {"clientInfo": {"name": "capture_daemon", "version": "0.1"},
                                "capabilities": {"experimentalApi": True}})
        self.drain(2)
        self.notify("initialized", {})


a = Conn("A")
a.init()
a.req("thread/start", {"cwd": CWD, "approvalPolicy": "on-request", "approvalsReviewer": "user",
                       "sandbox": "workspace-write", "ephemeral": False})
a.drain(5, want="thread/start")
thread = a.thread
print(json.dumps({"note": f"thread {thread}"}), flush=True)

# A benign first turn so a rollout exists and B can resume/subscribe.
a.req("turn/start", {"threadId": thread, "sandboxPolicy": {"type": "workspaceWrite", "networkAccess": False},
                     "input": [{"type": "text", "text": "Reply with the single word READY. Do not run any commands."}]})
a.drain(120, want="turn/completed")

# B subscribes to the same thread.
b = Conn("B")
b.init()
b.req("thread/resume", {"threadId": thread})
b.drain(5, want="thread/resume")

# A raises an escalation: a write outside the workspace.
if os.path.exists(PROBE):
    os.remove(PROBE)
a.req("turn/start", {"threadId": thread, "sandboxPolicy": {"type": "workspaceWrite", "networkAccess": False},
                     "input": [{"type": "text", "text": f"Using a shell command, create the file {PROBE} containing the single word two. Then reply with the single word DONE."}]})

answered = {"done": False}


def answer(conn, msg):
    if answered["done"]:
        return
    answered["done"] = True
    print(json.dumps({"note": f"A answering request id {msg['id']} with accept"}), flush=True)
    conn.send({"jsonrpc": "2.0", "id": msg["id"], "result": {"decision": "accept"}})


# Pump both until A's request arrives and is answered, watching B throughout.
end = time.time() + 200
while time.time() < end and not answered["done"]:
    a.drain(0.6, on_request=answer)
    b.drain(0.6)
# After A answered, keep watching B for serverRequest/resolved and the turn's end.
print(json.dumps({"note": "answered; watching B for the resolution"}), flush=True)
deadline = time.time() + 120
while time.time() < deadline:
    a.drain(0.6)
    got = b.drain(0.6, want="serverRequest/resolved")
    if got is not None:
        print(json.dumps({"note": "B saw serverRequest/resolved"}), flush=True)
        break

print(json.dumps({"note": f"probe exists: {os.path.exists(PROBE)}"}), flush=True)
if os.path.exists(PROBE):
    os.remove(PROBE)
a.c.close()
b.c.close()
