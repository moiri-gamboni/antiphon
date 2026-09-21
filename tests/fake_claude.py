"""A fake Claude Code session for tests: a registry record for the test process plus a
listener on a Unix socket that collects every frame sent to it, and a sender that replays
frames the way Claude Code sends them (probe connections first, one frame per connection).
"""

from __future__ import annotations

import json
import os
import socket
import threading
import time
from pathlib import Path

from antiphon.claude import registry

FIXTURES = Path(__file__).parent / "fixtures"

# Socket path of the Claude session in the peer captures; replayed frames have it rewritten
# to the fake session's own socket.
CAPTURED_CLAUDE_SOCK = "/run/user/1000/cc-socks/1002.sock"
CAPTURED_STUB_SOCK = "/run/user/1000/cc-socks/1001.sock"


def captured_events(name: str) -> list[dict]:
    return [json.loads(line) for line in (FIXTURES / name).read_text().splitlines()]


def captured_frames(name: str, kind: str) -> list[dict]:
    """The frames of one kind in a peer capture: ``frame`` (received by the stub) or
    ``sent`` (sent by the stub, unwrapped from its ``to``/``frame`` envelope)."""
    frames = []
    for event in captured_events(name):
        if event["kind"] != kind:
            continue
        frames.append(event["data"]["frame"] if kind == "sent" else event["data"])
    return frames


def send_frame(sock_path: str, frame: dict, probes: int = 2) -> None:
    """Send one frame the way Claude Code does: connect-and-close probes, then one
    connection carrying one newline-terminated JSON line."""
    for _ in range(probes):
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
            s.connect(sock_path)
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
        s.connect(sock_path)
        s.sendall(json.dumps(frame).encode() + b"\n")


class Listener:
    """Collects the raw lines and parsed frames arriving on a Unix socket."""

    def __init__(self, sock_path: str):
        self.sock_path = sock_path
        self.lines: list[bytes] = []
        self.frames: list[dict] = []
        self._server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._server.bind(sock_path)
        self._server.listen(8)
        self._thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._thread.start()

    def _accept_loop(self) -> None:
        while True:
            try:
                conn, _ = self._server.accept()
            except OSError:
                return
            with conn:
                buf = b""
                while chunk := conn.recv(65536):
                    buf += chunk
                for line in buf.split(b"\n"):
                    if line:
                        self.lines.append(line)
                        self.frames.append(json.loads(line))

    def wait_for_frames(self, count: int, timeout: float = 2.0) -> list[dict]:
        deadline = time.monotonic() + timeout
        while len(self.frames) < count and time.monotonic() < deadline:
            time.sleep(0.01)
        return list(self.frames)

    def close(self) -> None:
        self._server.close()
        os.unlink(self.sock_path)


class FakeClaude:
    """A registry record for the test process, live by construction, and its socket."""

    def __init__(self, sessions_dir: Path, sock_dir: Path, name: str = "claude-main"):
        sessions_dir.mkdir(parents=True, exist_ok=True)
        sock_dir.mkdir(parents=True, exist_ok=True)
        self.pid = os.getpid()
        self.sock_path = str(sock_dir / f"{self.pid}.sock")
        self.listener = Listener(self.sock_path)
        now = int(time.time() * 1000)
        record = captured_events("peer-frames.jsonl")[0]["data"]
        self.record = dict(
            record,
            pid=self.pid,
            cwd=os.getcwd(),
            startedAt=now,
            procStart=registry.proc_start(self.pid),
            messagingSocketPath=self.sock_path,
            name=name,
            nameSince=now,
            updatedAt=now,
            statusUpdatedAt=now,
        )
        self.record_path = sessions_dir / f"{self.pid}.json"
        self.record_path.write_text(json.dumps(self.record))

    @property
    def frames(self) -> list[dict]:
        return self.listener.frames

    @property
    def lines(self) -> list[bytes]:
        return self.listener.lines

    def wait_for_frames(self, count: int, timeout: float = 2.0) -> list[dict]:
        return self.listener.wait_for_frames(count, timeout)

    def replay(self, frame: dict, to_sock: str) -> dict:
        """Send a captured frame to a peer with the captured Claude socket path rewritten
        to this fake session's own, so replies come back here."""
        text = json.dumps(frame).replace(CAPTURED_CLAUDE_SOCK, self.sock_path)
        rewritten = json.loads(text)
        send_frame(to_sock, rewritten)
        return rewritten

    def close(self) -> None:
        self.listener.close()
        self.record_path.unlink(missing_ok=True)
