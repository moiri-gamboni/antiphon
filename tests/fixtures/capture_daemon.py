#!/usr/bin/env python3
"""Dependency-free WebSocket JSON-RPC client for the Codex app-server daemon.

This is the tool every capture in this directory was made with. It speaks the
daemon's control socket exactly the way the Codex CLI does (HTTP upgrade on
`GET /rpc`, masked text frames, `initialize` then `initialized`) and prints
every frame in both directions as one JSON line on stdout:

    {...}                     a message received from the daemon, verbatim
    {"sent": {...}}           a message this client sent
    {"note": "...", "t": N}   a marker for something that happened on our side

usage: capture_daemon.py [--listen SECS] [--respond DECISION] [--socket PATH]
                         CALL [CALL ...]

A CALL is `METHOD [PARAMS_JSON]` or one of the pseudo-calls:

    @await METHOD [SECS]   keep reading until a message with that method arrives
    @sleep SECS            keep reading for SECS seconds
    @close                 close the connection without answering anything
    @reconnect [SECS]      wait for the daemon to close the connection (SECS,
                           default 120), then connect and initialize again

Placeholders inside PARAMS_JSON are replaced before sending: `$THREAD` (the
last thread id seen in a result or notification), `$TURN` (the last turn id),
`$GUARDIAN_EVENT` (the
core-shaped `GuardianAssessmentEvent` assembled from the last
`item/autoApprovalReview/completed` notification the way the Codex TUI does
it, for `thread/approveGuardianDeniedAction`), and `$DENIED_COMMAND` (that
review's `action.command`, for use inside a JSON string).

`--respond DECISION` answers every `*/requestApproval` server request with
`{"decision": DECISION}` as soon as it arrives; without it they stay pending.
"""
import base64
import json
import os
import socket
import struct
import sys
import time

DEFAULT_SOCKET = os.path.expanduser("~/.codex/app-server-control/app-server-control.sock")


def note(text, **extra):
    print(json.dumps({"note": text, "t": time.time(), **extra}), flush=True)


class Connection:
    def __init__(self, path):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.connect(path)
        key = base64.b64encode(os.urandom(16)).decode()
        request = (
            "GET /rpc HTTP/1.1\r\nHost: localhost\r\nConnection: Upgrade\r\n"
            "Upgrade: websocket\r\nSec-WebSocket-Version: 13\r\n"
            f"Sec-WebSocket-Key: {key}\r\n\r\n"
        )
        self.sock.sendall(request.encode())
        buf = b""
        while b"\r\n\r\n" not in buf:
            data = self.sock.recv(4096)
            if not data:
                raise EOFError(f"closed during handshake: {buf!r}")
            buf += data
        head, _, self.buf = buf.partition(b"\r\n\r\n")
        print("<<handshake>>", head.split(b"\r\n")[0].decode(), file=sys.stderr)

    def send_text(self, text):
        payload = text.encode()
        mask = os.urandom(4)
        n = len(payload)
        header = bytes([0x81])
        if n < 126:
            header += bytes([0x80 | n])
        elif n < 65536:
            header += bytes([0x80 | 126]) + struct.pack(">H", n)
        else:
            header += bytes([0x80 | 127]) + struct.pack(">Q", n)
        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        self.sock.sendall(header + mask + masked)

    def _need(self, n):
        while len(self.buf) < n:
            data = self.sock.recv(65536)
            if not data:
                raise EOFError("socket closed")
            self.buf += data

    def recv_text(self, timeout):
        """One text frame, or None when nothing arrived within timeout."""
        self.sock.settimeout(timeout)
        try:
            self._need(2)
        except socket.timeout:
            return None
        opcode = self.buf[0] & 0x0F
        n = self.buf[1] & 0x7F
        offset = 2
        if n == 126:
            self._need(4)
            n = struct.unpack(">H", self.buf[2:4])[0]
            offset = 4
        elif n == 127:
            self._need(10)
            n = struct.unpack(">Q", self.buf[2:10])[0]
            offset = 10
        self._need(offset + n)
        payload = self.buf[offset:offset + n]
        self.buf = self.buf[offset + n:]
        if opcode == 0x8:
            raise EOFError("close frame")
        if opcode == 0x9:
            self.sock.sendall(bytes([0x8A, 0x80 | len(payload)]) + b"\0\0\0\0" + payload)
            return self.recv_text(timeout)
        return payload.decode(errors="replace")

    def close(self):
        self.sock.close()


class Client:
    def __init__(self, socket_path, respond):
        self.socket_path = socket_path
        self.respond = respond
        self.conn = None
        self.next_id = 0
        self.thread_id = None
        self.turn_id = None
        self.last_by_method = {}

    def connect(self):
        self.conn = Connection(self.socket_path)
        self.request("initialize", {
            "clientInfo": {"name": "capture_daemon", "version": "0.1"},
            "capabilities": {"experimentalApi": True},
        })
        self.notify("initialized", {})

    def send(self, message):
        print(json.dumps({"sent": message}), flush=True)
        self.conn.send_text(json.dumps(message))

    def notify(self, method, params):
        self.send({"jsonrpc": "2.0", "method": method, "params": params})

    def request(self, method, params, timeout=60):
        self.next_id += 1
        rid = self.next_id
        self.send({"jsonrpc": "2.0", "id": rid, "method": method, "params": params})
        end = time.time() + timeout
        while time.time() < end:
            msg = self.pump(0.5)
            if msg is not None and msg.get("id") == rid and "method" not in msg:
                return msg
        note(f"timeout waiting for the response to {method} (id {rid})")
        return None

    def pump(self, timeout):
        """Read one frame, print it, track ids, answer approvals. Raises EOFError."""
        line = self.conn.recv_text(timeout)
        if line is None:
            return None
        print(line, flush=True)
        msg = json.loads(line)
        self.track(msg)
        return msg

    def track(self, msg):
        params = msg.get("params") or {}
        result = msg.get("result") or {}
        method = msg.get("method")
        if method:
            self.last_by_method[method] = msg
        # Only messages carrying a whole thread object (thread/start and
        # thread/resume results, the thread/started broadcast) retarget
        # $THREAD; bare threadId notifications also arrive for other threads.
        for holder in (params, result):
            if isinstance(holder.get("thread"), dict) and "id" in holder["thread"]:
                self.thread_id = holder["thread"]["id"]
            if isinstance(holder.get("turn"), dict) and "id" in holder["turn"] \
                    and holder.get("threadId", self.thread_id) == self.thread_id:
                self.turn_id = holder["turn"]["id"]
        if method and "id" in msg:
            if self.respond and method.endswith("/requestApproval"):
                self.send({"jsonrpc": "2.0", "id": msg["id"],
                           "result": {"decision": self.respond}})

    def guardian_event(self):
        """The TUI's `GuardianAssessmentEvent` built from the last completed review."""
        p = self.last_by_method["item/autoApprovalReview/completed"]["params"]
        review = p["review"]
        action = dict(p["action"])
        if action.get("source") == "unifiedExec":
            action["source"] = "unified_exec"
        event = {
            "id": p["reviewId"],
            "turn_id": p["turnId"],
            "started_at_ms": p["startedAtMs"],
            "completed_at_ms": p.get("completedAtMs"),
            "status": review["status"],
            "risk_level": review.get("riskLevel"),
            "user_authorization": review.get("userAuthorization"),
            "rationale": review.get("rationale"),
            "decision_source": p.get("decisionSource"),
            "action": action,
        }
        return {k: v for k, v in event.items() if v is not None}

    def fill(self, params_json):
        text = params_json
        if "$GUARDIAN_EVENT" in text:
            text = text.replace('"$GUARDIAN_EVENT"', json.dumps(self.guardian_event()))
        if "$DENIED_COMMAND" in text:
            command = self.last_by_method["item/autoApprovalReview/completed"]["params"]["action"]["command"]
            text = text.replace("$DENIED_COMMAND", json.dumps(command)[1:-1])
        text = text.replace("$THREAD", self.thread_id or "$THREAD")
        text = text.replace("$TURN", self.turn_id or "$TURN")
        return json.loads(text)

    def listen(self, seconds, until=None):
        end = time.time() + seconds
        while time.time() < end:
            try:
                msg = self.pump(0.5)
            except EOFError as e:
                note(f"connection closed by the daemon: {e}")
                return "closed"
            if msg is not None and until is not None and msg.get("method") == until:
                return "seen"
        return "timeout"


def parse_args(argv):
    options = {"listen": 0.0, "respond": None, "socket": DEFAULT_SOCKET}
    while argv and argv[0].startswith("--"):
        flag = argv.pop(0)[2:]
        value = argv.pop(0)
        options[flag] = float(value) if flag == "listen" else value
    calls = []
    while argv:
        name = argv.pop(0)
        if name.startswith("@"):
            args = []
            if name == "@await":
                args.append(argv.pop(0))
            if argv and argv[0].replace(".", "", 1).isdigit():
                args.append(argv.pop(0))
            calls.append((name, args))
        else:
            params = "{}"
            if argv and argv[0][:1] in "{[":
                params = argv.pop(0)
            calls.append((name, params))
    return options, calls


def main():
    options, calls = parse_args(sys.argv[1:])
    client = Client(options["socket"], options["respond"])
    client.connect()
    for name, arg in calls:
        if name == "@await":
            method = arg[0]
            seconds = float(arg[1]) if len(arg) > 1 else 120.0
            note(f"waiting up to {seconds:g}s for {method}")
            note(f"{method}: {client.listen(seconds, until=method)}")
        elif name == "@sleep":
            note(f"listening for {float(arg[0]):g}s")
            client.listen(float(arg[0]))
        elif name == "@close":
            note("closing the connection without answering")
            client.conn.close()
            client.conn = None
        elif name == "@reconnect":
            seconds = float(arg[0]) if arg else 120.0
            if client.conn is not None and client.listen(seconds) != "closed":
                note("closing our side")
                client.conn.close()
            deadline = time.time() + 60
            while True:
                try:
                    client.connect()
                    break
                except (OSError, EOFError) as e:
                    # The socket path can accept a connection and drop it while
                    # the daemon is still coming up; keep trying until the deadline.
                    if time.time() > deadline:
                        raise
                    note(f"reconnect failed, retrying: {e!r}")
                    time.sleep(1)
            note("reconnected")
        else:
            client.request(name, client.fill(arg))
    if options["listen"]:
        client.listen(options["listen"])


if __name__ == "__main__":
    main()
