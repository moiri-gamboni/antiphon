import json
import os
import queue
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

import pytest

from antiphon.claude import registry
from fake_claude import (
    CAPTURED_CLAUDE_SOCK,
    CAPTURED_STUB_SOCK,
    FakeClaude,
    Listener,
    captured_events,
    captured_frames,
    send_frame,
)

CAPTURED_RECORD = captured_events("peer-frames.jsonl")[0]["data"]
CAPTURED_INBOUND = [f for f in captured_frames("peer-frames.jsonl", "frame") if f.get("type") == "user"][0]
CAPTURED_NOTIFY = [f for f in captured_frames("peer-frames.jsonl", "frame") if f.get("action") == "notify_when_idle"][0]
CAPTURED_PROBE = [f for f in captured_frames("peer-frames.jsonl", "frame") if f.get("action") == "sandbox_probe"][0]
CAPTURED_SENT_USER = [f for f in captured_frames("peer-frames.jsonl", "sent") if f.get("type") == "user"][0]
CAPTURED_SENT_IDLE = captured_frames("peer-frames-idle-notice.jsonl", "sent")[0]


class Child:
    """The peer child as the bridge runs it: a subprocess driven over stdin/stdout."""

    def __init__(self, tmp_path: Path):
        self.sessions_dir = tmp_path / "cc" / "sessions"
        self.sock_dir = tmp_path / "socks"
        self.sock_dir.mkdir(parents=True, exist_ok=True)
        env = dict(os.environ, CLAUDE_CONFIG_DIR=str(tmp_path / "cc"), HOME=str(tmp_path))
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "antiphon.claude.peer"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            env=env,
            text=True,
            bufsize=1,
        )
        self.events: queue.Queue[dict] = queue.Queue()
        self.pump = threading.Thread(target=self._pump, daemon=True)
        self.pump.start()

    def _pump(self) -> None:
        for line in self.proc.stdout:
            self.events.put(json.loads(line))

    def send(self, **cmd) -> None:
        self.proc.stdin.write(json.dumps(cmd) + "\n")
        self.proc.stdin.flush()

    def event(self, timeout: float = 2.0) -> dict:
        return self.events.get(timeout=timeout)

    def register(self, name: str = "codex-one", **overrides) -> dict:
        cmd = dict(
            name=name,
            cwd="/tmp",
            version=CAPTURED_RECORD["version"],
            pidDomain=CAPTURED_RECORD["pidDomain"],
            socket_dir=str(self.sock_dir),
            status="idle",
        )
        cmd.update(overrides)
        self.send(cmd="register", **cmd)
        ready = self.event()
        assert ready["ev"] == "ready"
        self.pid = ready["pid"]
        self.sock = ready["sock"]
        self.record_path = self.sessions_dir / f"{self.pid}.json"
        return ready

    def record(self) -> dict:
        return json.loads(self.record_path.read_text())

    def close(self) -> None:
        if self.proc.poll() is None:
            self.proc.kill()
        self.proc.wait()
        # The pump thread leaves the read loop at EOF; only then is stdout safe to close.
        self.pump.join(timeout=2.0)
        self.proc.stdin.close()
        self.proc.stdout.close()


@pytest.fixture
def child(tmp_path):
    c = Child(tmp_path)
    yield c
    c.close()


@pytest.fixture
def claude(child):
    fake = FakeClaude(child.sessions_dir, child.sock_dir)
    yield fake
    fake.close()


def hand_computed_proc_start(pid: int) -> str:
    if sys.platform.startswith("linux"):
        stat = Path(f"/proc/{pid}/stat").read_text()
        return stat.rsplit(")", 1)[1].split()[19]
    out = subprocess.run(
        ["ps", "-o", "lstart=", "-p", str(pid)],
        env=dict(os.environ, LC_ALL="C", TZ="UTC"),
        capture_output=True,
    ).stdout
    return out.decode().strip()


def wait_until(predicate, timeout: float = 1.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return predicate()


def test_register_writes_a_record_shaped_like_a_claude_session(child):
    ready = child.register(name="codex-one", cwd="/work")
    assert ready["pid"] == child.proc.pid
    assert ready["sock"] == str(child.sock_dir / f"{child.proc.pid}.sock")

    record = child.record()
    assert list(record) == list(CAPTURED_RECORD)
    for key, captured_value in CAPTURED_RECORD.items():
        assert type(record[key]) is type(captured_value), key
    assert record["pid"] == child.proc.pid
    assert record["cwd"] == "/work"
    assert record["name"] == "codex-one"
    assert record["status"] == "idle"
    assert record["peerProtocol"] == 1
    assert record["peerFeatures"] == ["notify_idle"]
    assert record["kind"] == "interactive"
    assert record["entrypoint"] == "cli"
    assert record["nameSource"] == "user"
    assert record["version"] == CAPTURED_RECORD["version"]
    assert record["pidDomain"] == CAPTURED_RECORD["pidDomain"]
    assert record["messagingSocketPath"] == ready["sock"]
    assert record["procStart"] == hand_computed_proc_start(child.proc.pid)
    assert uuid.UUID(record["sessionId"]).version == 4
    assert abs(record["startedAt"] - time.time() * 1000) < 5000
    assert os.stat(ready["sock"]).st_mode & 0o777 == 0o600
    assert registry.pins_ok(registry.Record(child.record_path, record)) == []


def test_status_updates_the_record_timestamps(child):
    child.register()
    before = child.record()
    time.sleep(0.002)
    child.send(cmd="status", status="busy")
    assert wait_until(lambda: child.record()["status"] == "busy")
    after = child.record()
    assert after["statusUpdatedAt"] > before["statusUpdatedAt"]
    assert after["updatedAt"] > before["updatedAt"]
    assert after["nameSince"] == before["nameSince"]


def test_rename_updates_the_record(child):
    child.register(name="codex-one")
    before = child.record()
    time.sleep(0.002)
    child.send(cmd="rename", name="planner")
    assert wait_until(lambda: child.record()["name"] == "planner")
    after = child.record()
    assert after["nameSince"] > before["nameSince"]
    assert after["statusUpdatedAt"] == before["statusUpdatedAt"]


def test_inbound_message_is_unwrapped_with_its_sender(child, claude):
    child.register()
    claude.replay(CAPTURED_INBOUND, child.sock)
    event = child.event()
    assert event == {
        "ev": "inbound",
        "msg_id": CAPTURED_INBOUND["msg_id"],
        "from_sock": claude.sock_path,
        "from_name": "claude-main",
        "text": "Spike zero round two: receipt and idle-notice test, please ignore.",
    }


def test_delivered_message_gets_no_receipt(child, claude):
    child.register()
    claude.replay(CAPTURED_INBOUND, child.sock)
    assert child.event()["ev"] == "inbound"
    assert claude.wait_for_frames(1, timeout=0.5) == []


def test_deliver_failed_sends_one_dropped_receipt_to_the_sender(child, claude):
    child.register()
    claude.replay(CAPTURED_INBOUND, child.sock)
    msg_id = child.event()["msg_id"]
    child.send(cmd="deliver_failed", msg_id=msg_id, reason="turn/start rejected")
    frames = claude.wait_for_frames(1)
    assert len(frames) == 1
    receipt = frames[0]
    assert uuid.UUID(receipt.pop("msg_id")).version == 4
    assert receipt == {
        "type": "control",
        "action": "peer_message_status",
        "orig_msg_id": msg_id,
        "status": "dropped",
        "drop_reason": "turn/start rejected",
        "from": f"uds:{child.sock}",
        "msgV": 1,
    }
    assert claude.wait_for_frames(2, timeout=0.3) == frames


def test_deliver_sends_the_captured_user_frame_shape(child, claude):
    child.register(name="codex-stub")
    text = CAPTURED_SENT_USER["message"]["content"].split("\n")[1]
    child.send(cmd="deliver", to_sock=claude.sock_path, text=text, from_name="codex-stub")
    sent = child.event()
    assert sent["ev"] == "sent"
    assert uuid.UUID(sent["msg_id"]).version == 4
    assert len(claude.wait_for_frames(1)) == 1
    raw = claude.lines[0].decode()
    frame = json.loads(raw)
    assert frame["msg_id"] == sent["msg_id"]
    assert frame["from"] == f"uds:{child.sock}"
    assert frame["message"]["content"] == (
        f'<cross-session-message from="uds:{child.sock}" from-name="codex-stub" from-mode="prompting">\n'
        f"{text}\n</cross-session-message>"
    )
    normalised = raw.replace(sent["msg_id"], CAPTURED_SENT_USER["msg_id"]).replace(child.sock, CAPTURED_STUB_SOCK)
    assert normalised == json.dumps(CAPTURED_SENT_USER)


def test_deliver_to_a_dead_socket_is_reported(child):
    child.register()
    child.send(cmd="deliver", to_sock=str(child.sock_dir / "gone.sock"), text="x", from_name="codex-one")
    event = child.event()
    assert event["ev"] == "send_failed"
    assert event["to_sock"] == str(child.sock_dir / "gone.sock")
    assert "FileNotFoundError" in event["reason"]
    assert uuid.UUID(event["msg_id"]).version == 4
    assert child.proc.poll() is None


def test_subscribe_sends_the_captured_notify_when_idle_shape(child, claude):
    child.register()
    child.send(cmd="subscribe", to_sock=claude.sock_path)
    assert len(claude.wait_for_frames(1)) == 1
    raw = claude.lines[0].decode()
    frame = json.loads(raw)
    normalised = raw.replace(frame["msg_id"], CAPTURED_NOTIFY["msg_id"]).replace(child.sock, CAPTURED_CLAUDE_SOCK)
    assert normalised == json.dumps(CAPTURED_NOTIFY)


def test_idle_sends_one_notice_per_subscriber_then_forgets_them(child, claude):
    child.register()
    other = Listener(str(child.sock_dir / "1002.sock"))
    try:
        claude.replay(CAPTURED_NOTIFY, child.sock)
        assert child.event() == {"ev": "subscribed", "from_sock": claude.sock_path}
        other_notify = dict(CAPTURED_NOTIFY)
        other_notify["from"] = f"uds:{other.sock_path}"
        other_notify["msg_id"] = str(uuid.uuid4())
        send_frame(child.sock, other_notify)
        assert child.event() == {"ev": "subscribed", "from_sock": other.sock_path}

        child.send(cmd="idle", detail="Done: the answer is 42")
        [notice] = claude.wait_for_frames(1)
        [other_notice] = other.wait_for_frames(1)
        assert notice["orig_msg_id"] == CAPTURED_NOTIFY["msg_id"]
        assert other_notice["orig_msg_id"] == other_notify["msg_id"]
        for n in (notice, other_notice):
            assert n["state"] == "idle"
            assert n["detail"] == "Done: the answer is 42"
            assert n["from"] == f"uds:{child.sock}"
            assert abs(n["finished_at"] - time.time() * 1000) < 5000

        raw = claude.lines[0].decode()
        normalised = (
            raw.replace(notice["msg_id"], CAPTURED_SENT_IDLE["msg_id"])
            .replace(CAPTURED_NOTIFY["msg_id"], CAPTURED_SENT_IDLE["orig_msg_id"])
            .replace(str(notice["finished_at"]), str(CAPTURED_SENT_IDLE["finished_at"]))
            .replace("Done: the answer is 42", CAPTURED_SENT_IDLE["detail"])
            .replace(child.sock, CAPTURED_STUB_SOCK)
        )
        assert normalised == json.dumps(CAPTURED_SENT_IDLE)

        child.send(cmd="idle", detail="second turn")
        assert claude.wait_for_frames(2, timeout=0.3) == [notice]
        assert other.wait_for_frames(2, timeout=0.1) == [other_notice]
    finally:
        other.close()


def test_inbound_control_frames_are_reported_by_kind(child, claude):
    child.register()
    status = {
        "type": "control",
        "action": "peer_message_status",
        "orig_msg_id": "7755f008-4744-46c0-a8ee-9c8f1892f5ec",
        "status": "dropped",
        "drop_reason": "session is shutting down",
        "from": f"uds:{claude.sock_path}",
        "msgV": 1,
        "msg_id": str(uuid.uuid4()),
    }
    send_frame(child.sock, status)
    assert child.event() == {
        "ev": "status",
        "orig_msg_id": "7755f008-4744-46c0-a8ee-9c8f1892f5ec",
        "status": "dropped",
        "detail": "session is shutting down",
    }

    claude.replay(CAPTURED_SENT_IDLE, child.sock)
    assert child.event() == {
        "ev": "idle_notice",
        "from_sock": CAPTURED_STUB_SOCK,
        "state": "idle",
        "detail": CAPTURED_SENT_IDLE["detail"],
    }

    send_frame(child.sock, CAPTURED_PROBE)
    assert child.event() == {"ev": "unknown_frame", "raw": json.dumps(CAPTURED_PROBE)}


def test_stdin_eof_notifies_subscribers_and_removes_record_and_socket(child, claude):
    child.register()
    claude.replay(CAPTURED_NOTIFY, child.sock)
    assert child.event()["ev"] == "subscribed"

    child.proc.stdin.close()
    [notice] = claude.wait_for_frames(1, timeout=1.0)
    assert notice["action"] == "peer_idle_notice"
    assert notice["state"] == "exited"
    assert notice["orig_msg_id"] == CAPTURED_NOTIFY["msg_id"]
    assert wait_until(lambda: not child.record_path.exists() and not os.path.exists(child.sock), timeout=1.0)
    assert child.proc.wait(timeout=1.0) == 0


def test_exited_command_behaves_like_stdin_eof(child, claude):
    child.register()
    claude.replay(CAPTURED_NOTIFY, child.sock)
    assert child.event()["ev"] == "subscribed"

    child.send(cmd="exited")
    [notice] = claude.wait_for_frames(1, timeout=1.0)
    assert notice["state"] == "exited"
    assert wait_until(lambda: not child.record_path.exists() and not os.path.exists(child.sock), timeout=1.0)
    assert child.proc.wait(timeout=1.0) == 0


def test_messages_longer_than_the_default_pipe_buffer_pass_both_ways(child, claude):
    child.register(name="codex-stub")
    long_text = "x" * 200_000
    child.send(cmd="deliver", to_sock=claude.sock_path, text=long_text, from_name="codex-stub")
    assert child.event()["ev"] == "sent"
    [frame] = claude.wait_for_frames(1)
    assert frame["message"]["content"].split("\n")[1] == long_text

    inbound = dict(CAPTURED_INBOUND)
    inbound["message"] = {"role": "user", "content": CAPTURED_INBOUND["message"]["content"].replace("Spike zero", long_text)}
    claude.replay(inbound, child.sock)
    event = child.event()
    assert event["ev"] == "inbound"
    assert event["text"].startswith(long_text)


def test_exit_reaches_every_subscriber_when_the_bridge_and_one_subscriber_are_gone(tmp_path):
    sessions_dir = tmp_path / "cc" / "sessions"
    sock_dir = tmp_path / "socks"
    sock_dir.mkdir()
    claude = FakeClaude(sessions_dir, sock_dir)
    env = dict(os.environ, CLAUDE_CONFIG_DIR=str(tmp_path / "cc"), HOME=str(tmp_path))
    proc = subprocess.Popen(
        [sys.executable, "-m", "antiphon.claude.peer"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, env=env, text=True
    )
    try:
        register = dict(cmd="register", name="codex-one", cwd="/tmp", version="2.1.278", pidDomain="d", socket_dir=str(sock_dir), status="idle")
        proc.stdin.write(json.dumps(register) + "\n")
        proc.stdin.flush()
        ready = json.loads(proc.stdout.readline())
        dead = Listener(str(sock_dir / "1002.sock"))
        for sock in (dead.sock_path, claude.sock_path):
            notify = dict(CAPTURED_NOTIFY, msg_id=str(uuid.uuid4()))
            notify["from"] = f"uds:{sock}"
            send_frame(ready["sock"], notify)
            assert json.loads(proc.stdout.readline())["ev"] == "subscribed"
        dead.close()

        proc.stdout.close()
        proc.stdin.close()
        [notice] = claude.wait_for_frames(1, timeout=1.0)
        assert notice["state"] == "exited"
        record_path = sessions_dir / f"{ready['pid']}.json"
        assert wait_until(lambda: not record_path.exists() and not os.path.exists(ready["sock"]), timeout=1.0)
        assert proc.wait(timeout=1.0) == 0
    finally:
        if proc.poll() is None:
            proc.kill()
        proc.wait()
        claude.close()


def test_a_bad_command_still_notifies_and_cleans_up(child, claude):
    child.register()
    claude.replay(CAPTURED_NOTIFY, child.sock)
    assert child.event()["ev"] == "subscribed"

    child.send(cmd="deliver_failed", msg_id="never-seen", reason="x")
    [notice] = claude.wait_for_frames(1, timeout=1.0)
    assert notice["state"] == "exited"
    assert wait_until(lambda: not child.record_path.exists() and not os.path.exists(child.sock), timeout=1.0)
    assert child.proc.wait(timeout=1.0) != 0
