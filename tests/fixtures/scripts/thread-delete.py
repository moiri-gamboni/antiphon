#!/usr/bin/env python3
"""Who hears that a thread was deleted, and what a later resume of it gets back.

A starts a thread and runs a benign turn so a rollout exists, then unsubscribes from
it: the position the bridge is in after `antiphon stop`. B only initializes and
subscribes to nothing. C, a third client standing in for a human's terminal, deletes
the thread, then tries to resume and read it.

Every frame each connection sees is printed, tagged A/B/C, to stdout.
"""
import json
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from capture_daemon import DEFAULT_SOCKET, Connection  # noqa: E402

CWD = os.path.expanduser("~/antiphon-capture")


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
        self.send({"jsonrpc": "2.0", "id": self.n, "method": method, "params": params})

    def notify(self, method, params):
        self.send({"jsonrpc": "2.0", "method": method, "params": params})

    def drain(self, seconds, want=None):
        """Read for `seconds`, or until a message whose method is `want`, or a reply to
        the last request when `want` is "reply"; return that message or None."""
        end = time.time() + seconds
        while time.time() < end:
            try:
                line = self.c.recv_text(0.4)
            except EOFError as e:
                print(json.dumps({"from": self.tag, "note": f"closed: {e}"}), flush=True)
                return None
            if line is None:
                continue
            msg = json.loads(line)
            print(json.dumps({"from": self.tag, "recv": msg}), flush=True)
            if isinstance(msg.get("result"), dict) and isinstance(msg["result"].get("thread"), dict):
                self.thread = msg["result"]["thread"]["id"]
            if msg.get("method") == want or (want == "reply" and msg.get("id") == self.n and "method" not in msg):
                return msg
        return None

    def init(self):
        self.req("initialize", {"clientInfo": {"name": "capture_daemon", "version": "0.1"},
                                "capabilities": {"experimentalApi": True}})
        self.drain(5, want="reply")
        self.notify("initialized", {})


a = Conn("A")
a.init()
a.req("thread/start", {"cwd": CWD, "approvalPolicy": "on-request", "sandbox": "read-only", "ephemeral": False})
a.drain(10, want="reply")
thread = a.thread
print(json.dumps({"note": f"thread {thread}"}), flush=True)
a.req("turn/start", {"threadId": thread,
                     "input": [{"type": "text", "text": "Reply with the single word READY. Do not run any commands."}]})
a.drain(120, want="turn/completed")
a.req("thread/unsubscribe", {"threadId": thread})
a.drain(5, want="reply")

b = Conn("B")
b.init()

c = Conn("C")
c.init()
c.req("thread/delete", {"threadId": thread})
c.drain(10, want="reply")

print(json.dumps({"note": "deleted; watching A and B"}), flush=True)
a.drain(3)
b.drain(3)
c.drain(1)

c.req("thread/resume", {"threadId": thread})
c.drain(10, want="reply")
c.req("thread/read", {"threadId": thread})
c.drain(10, want="reply")
for conn in (a, b, c):
    conn.c.close()
