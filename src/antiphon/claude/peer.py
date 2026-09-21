"""One Claude Code peer for one Codex thread.

Claude Code lists a peer only while the pid in its registry record is alive, so each Codex
thread the bridge exposes gets a process of its own: this one. The bridge drives it over
stdin with one JSON command per line and reads one JSON event per line from stdout.

Commands (bridge to child):
  register {name, cwd, version, pidDomain, socket_dir, status}
      bind <socket_dir>/<pid>.sock, write the registry record, reply ready {pid, sock}
  status {status}                    update the record's status
  rename {name}                      update the record's name
  deliver {to_sock, text, from_name} send a user frame, reply sent {msg_id}
  subscribe {to_sock}                send notify_when_idle to a peer
  idle {detail}                      one peer_idle_notice per held subscription, then forget them
  deliver_failed {msg_id, reason}    a dropped peer_message_status to that message's sender
  exited                             exited notices to every subscriber, unlink record and socket, exit

Stdin closing is treated exactly like ``exited``, so a bridge that dies still tells the
subscribers.

Events (child to bridge):
  ready {pid, sock}                        sent {msg_id}
  inbound {msg_id, from_sock, from_name, text}
  subscribed {from_sock}                   status {orig_msg_id, status, detail}
  idle_notice {from_sock, state, detail}   unknown_frame {raw}
  send_failed {to_sock, reason[, msg_id]}  a frame could not be written to a peer socket

Every frame on the messaging socket is a single JSON line per connection. Claude Code
opens and closes a connection or two without writing before it sends a frame; those empty
connections are normal.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import signal
import sys
import time
import uuid

from antiphon.claude import registry

log = logging.getLogger(__name__)

# asyncio's default line limit is 64 KiB; a Codex final answer or a Claude message can be
# longer, and a frame over the limit would be dropped (socket side) or fatal (stdin side).
FRAME_LIMIT = 16 * 1024 * 1024

# A peer that accepted the connection but stopped reading would block a large write forever
# and the command loop with it; bound the whole connect+write+drain.
SEND_TIMEOUT = 5.0

WRAPPER = re.compile(
    r'\A<cross-session-message from="(?P<from>[^"]*)" from-name="(?P<name>[^"]*)"[^>]*>\n'
    r"(?P<text>.*)\n</cross-session-message>\Z",
    re.DOTALL,
)


def now_ms() -> int:
    return int(time.time() * 1000)


def write_atomic(path: str, data: dict) -> None:
    tmp = f"{path}.{os.getpid()}.tmp"
    with open(tmp, "w") as f:
        json.dump(data, f)
    os.replace(tmp, path)


class Peer:
    def __init__(self) -> None:
        self.record: dict = {}
        self.record_path = ""
        self.sock_path = ""
        self.server: asyncio.AbstractServer | None = None
        self.subscriptions: dict[str, str] = {}  # subscriber socket path -> its notify msg_id
        self.senders: dict[str, str] = {}  # inbound msg_id -> sender socket path

    @property
    def address(self) -> str:
        return f"uds:{self.sock_path}"

    # -- bridge side -------------------------------------------------------------------

    def emit(self, **event: object) -> None:
        line = json.dumps(event)
        log.debug("to bridge: %s", line)
        try:
            sys.stdout.write(line + "\n")
            sys.stdout.flush()
        except BrokenPipeError:
            # The bridge is gone; the event has no reader, but the shutdown that follows
            # still owes the subscribers their notices, so this is not fatal. Pointing
            # stdout at /dev/null keeps the interpreter's final flush from failing too.
            log.warning("bridge pipe closed, event not delivered: %s", line)
            os.dup2(os.open(os.devnull, os.O_WRONLY), sys.stdout.fileno())

    async def handle_command(self, cmd: dict) -> bool:
        """Apply one bridge command; True means the bridge asked this peer to exit."""
        match cmd["cmd"]:
            case "register":
                await self.register(cmd)
            case "status":
                self.update_record(status=cmd["status"], statusUpdatedAt=now_ms())
            case "rename":
                self.update_record(name=cmd["name"], nameSince=now_ms())
            case "deliver":
                msg_id = str(uuid.uuid4())
                content = (
                    f'<cross-session-message from="{self.address}" from-name="{cmd["from_name"]}" '
                    f'from-mode="prompting">\n{cmd["text"]}\n</cross-session-message>'
                )
                frame = {
                    "msgV": 1,
                    "msg_id": msg_id,
                    "type": "user",
                    "message": {"role": "user", "content": content},
                    "priority": "next",
                    "from": self.address,
                }
                if await self.send(cmd["to_sock"], frame, msg_id=msg_id):
                    self.emit(ev="sent", msg_id=msg_id)
            case "subscribe":
                await self.send(
                    cmd["to_sock"],
                    {
                        "type": "control",
                        "action": "notify_when_idle",
                        "from": self.address,
                        "from_mode": "prompting",
                        "msgV": 1,
                        "msg_id": str(uuid.uuid4()),
                    },
                )
            case "idle":
                await self.notify_subscribers("idle", cmd["detail"])
            case "deliver_failed":
                to_sock = self.senders.pop(cmd["msg_id"])
                await self.send(
                    to_sock,
                    {
                        "type": "control",
                        "action": "peer_message_status",
                        "orig_msg_id": cmd["msg_id"],
                        "status": "dropped",
                        "drop_reason": cmd["reason"],
                        "from": self.address,
                        "msgV": 1,
                        "msg_id": str(uuid.uuid4()),
                    },
                )
            case "exited":
                return True
            case other:
                raise ValueError(f"unknown command {other!r}")
        return False

    async def register(self, cmd: dict) -> None:
        pid = os.getpid()
        self.sock_path = os.path.join(cmd["socket_dir"], f"{pid}.sock")
        if os.path.exists(self.sock_path):
            os.unlink(self.sock_path)
        self.server = await asyncio.start_unix_server(self.handle_connection, path=self.sock_path, limit=FRAME_LIMIT)
        os.chmod(self.sock_path, 0o600)
        now = now_ms()
        self.record = {
            "pid": pid,
            "sessionId": str(uuid.uuid4()),
            "cwd": cmd["cwd"],
            "startedAt": now,
            "procStart": registry.proc_start(pid),
            "version": cmd["version"],
            "peerProtocol": registry.PEER_PROTOCOL,
            "peerFeatures": ["notify_idle"],
            "kind": "interactive",
            "entrypoint": "cli",
            "pidDomain": cmd["pidDomain"],
            "messagingSocketPath": self.sock_path,
            "name": cmd["name"],
            "nameSource": "user",
            "nameSince": now,
            "status": cmd["status"],
            "updatedAt": now,
            "statusUpdatedAt": now,
        }
        sessions_dir = registry.sessions_dir()
        os.makedirs(sessions_dir, exist_ok=True)
        self.record_path = str(sessions_dir / f"{pid}.json")
        write_atomic(self.record_path, self.record)
        self.emit(ev="ready", pid=pid, sock=self.sock_path)

    def update_record(self, **fields: object) -> None:
        self.record.update(fields, updatedAt=now_ms())
        write_atomic(self.record_path, self.record)

    async def notify_subscribers(self, state: str, detail: str) -> None:
        subscriptions, self.subscriptions = self.subscriptions, {}
        for to_sock, orig_msg_id in subscriptions.items():
            await self.send(
                to_sock,
                {
                    "type": "control",
                    "action": "peer_idle_notice",
                    "orig_msg_id": orig_msg_id,
                    "state": state,
                    "finished_at": now_ms(),
                    "detail": detail,
                    "from": self.address,
                    "from_mode": "prompting",
                    "msgV": 1,
                    "msg_id": str(uuid.uuid4()),
                },
            )

    async def shutdown(self) -> None:
        try:
            await self.notify_subscribers("exited", "session exited")
        finally:
            if self.server is not None:
                self.server.close()
            for path in (self.record_path, self.sock_path):
                try:
                    os.unlink(path)
                except FileNotFoundError:
                    pass

    # -- Claude side ------------------------------------------------------------------

    async def send(self, to_sock: str, frame: dict, **failure_fields: object) -> bool:
        line = json.dumps(frame)
        log.debug("to %s: %s", to_sock, line)
        writer = None
        try:
            async with asyncio.timeout(SEND_TIMEOUT):
                _, writer = await asyncio.open_unix_connection(to_sock)
                writer.write(line.encode() + b"\n")
                await writer.drain()
            writer.close()
        except (OSError, TimeoutError) as e:
            # The peer's socket is gone or refusing (its session exited while its record
            # lingered), or it accepted but stopped reading: the bridge is told and this peer
            # stays up for its other peers. Abort so the buffered bytes do not keep it alive.
            if writer is not None:
                writer.transport.abort()
            log.error("could not send to %s: %r", to_sock, e)
            self.emit(ev="send_failed", to_sock=to_sock, reason=repr(e), **failure_fields)
            return False
        return True

    async def handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            async for raw in reader:
                line = raw.decode(errors="replace").rstrip("\n")
                if not line:
                    continue
                log.debug("from socket: %s", line)
                self.handle_frame(line)
        finally:
            writer.close()

    def handle_frame(self, line: str) -> None:
        try:
            frame = json.loads(line)
        except ValueError:
            frame = None
        if not isinstance(frame, dict):
            self.emit(ev="unknown_frame", raw=line)
            return
        from_sock = str(frame.get("from", "")).removeprefix("uds:")
        kind = (frame.get("type"), frame.get("action"))
        if kind == ("user", None) and isinstance(frame.get("message"), dict):
            content = str(frame["message"].get("content", ""))
            wrapper = WRAPPER.match(content)
            if wrapper is None:
                from_name, text = None, content
            else:
                from_name, text = wrapper["name"], wrapper["text"]
            self.senders[frame["msg_id"]] = from_sock
            self.emit(ev="inbound", msg_id=frame["msg_id"], from_sock=from_sock, from_name=from_name, text=text)
        elif kind == ("control", "notify_when_idle"):
            self.subscriptions[from_sock] = frame["msg_id"]
            self.emit(ev="subscribed", from_sock=from_sock)
        elif kind == ("control", "peer_message_status"):
            self.emit(
                ev="status",
                orig_msg_id=frame.get("orig_msg_id"),
                status=frame.get("status"),
                detail=frame.get("drop_reason"),
            )
        elif kind == ("control", "peer_idle_notice"):
            self.emit(ev="idle_notice", from_sock=from_sock, state=frame.get("state"), detail=frame.get("detail"))
        else:
            self.emit(ev="unknown_frame", raw=line)


async def run() -> int:
    peer = Peer()
    stdin = asyncio.StreamReader(limit=FRAME_LIMIT)
    loop = asyncio.get_running_loop()
    await loop.connect_read_pipe(lambda: asyncio.StreamReaderProtocol(stdin), sys.stdin)

    def _stop(name: str) -> None:
        log.info("signal %s, shutting down", name)
        stdin.feed_eof()

    # A systemd/launchd stop signals the whole process group; treat it like the bridge
    # closing our stdin, so shutdown() still tells subscribers and removes our files.
    for sig in (signal.SIGTERM, signal.SIGHUP):
        loop.add_signal_handler(sig, _stop, sig.name)
    code = 0
    try:
        async for raw in stdin:
            line = raw.decode().strip()
            if not line:
                continue
            log.debug("from bridge: %s", line)
            if await peer.handle_command(json.loads(line)):
                break
    except Exception:
        # A malformed command is a bridge bug, but the subscribers and the registry must
        # not be left believing this peer is alive: shut down properly, then fail loudly.
        log.exception("command failed, shutting down")
        code = 1
    finally:
        await peer.shutdown()
    return code


def main() -> None:
    logging.basicConfig(level=logging.DEBUG, stream=sys.stderr, format="%(asctime)s %(name)s %(message)s")
    sys.exit(asyncio.run(run()))


if __name__ == "__main__":
    main()
