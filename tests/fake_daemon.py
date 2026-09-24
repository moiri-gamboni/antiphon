"""In-process stand-in for the Codex app-server daemon.

`FakeDaemon` listens on a Unix socket, speaks the same WebSocket upgrade and
framing as the real control socket, answers `initialize` with the captured
result, and serves every other request from `replies` (per method: a reply
dict, a list of reply dicts consumed in order, or a callable taking the
request params). It records what it received and can, on demand, emit a
notification, send a server request, or drop the connection.

    fake = FakeDaemon(tmp_path / "daemon.sock")
    fake.replies["thread/start"] = {"result": fixture.result(2)}
    fake.replies["turn/start"] = lambda params: {"error": {"code": -32600, "message": "..."}}
    await fake.start()
    ...
    await fake.notify("turn/completed", params)
    request_id = await fake.server_request("item/commandExecution/requestApproval", params)
    answer = await fake.response(request_id)
    await fake.drop()          # EOF on the client side, the socket keeps listening
    await fake.stop()

A reply dict is `{"result": ...}` or `{"error": {...}}`, sent as the daemon
sends it (`{"id": N, "result": ...}` / `{"id": N, "error": ...}`); `None`
leaves the request unanswered. A list is served in order, its last entry
repeating, without being consumed (a fixture's lists can be shared). A method
with no reply configured is answered with a `-32601` error so the client under
test fails loudly instead of hanging. `server_request` takes a `request_id=` to
replay a captured one. `fake.requests` holds every request
message received, `fake.notifications` every client notification
(`initialized` included), `fake.connections` every accepted connection
(`conn.frames` has the raw frames, pongs included; `conn.stop_reading()` +
`conn.abort()` end it with a reset instead of an EOF).

`load_fixture(name)` parses a capture from `tests/fixtures/`: `.result(id)` /
`.error(id)` return a daemon line by request id (`occurrence=` picks a later
one when a capture spans two connections), `.notifications(method)` returns
the params of every notification with that method, and
`.server_requests(method)` every server request with that method.
"""
import asyncio
import base64
import hashlib
import json
import threading
from pathlib import Path

from antiphon.codex import ws

FIXTURES = Path(__file__).parent / "fixtures"
WS_ACCEPT_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


class Fixture:
    def __init__(self, lines: list[dict]):
        self.messages = [line for line in lines if "sent" not in line and "note" not in line]
        self.sent = [line["sent"] for line in lines if "sent" in line]

    def replies_to(self, request_id: int) -> list[dict]:
        """Every daemon reply with that id; more than one when a capture spans two connections."""
        found = [m for m in self.messages if m.get("id") == request_id and "method" not in m]
        if not found:
            raise KeyError(f"no reply with id {request_id} in the fixture")
        return found

    def result(self, request_id: int, occurrence: int = 0):
        return self.replies_to(request_id)[occurrence]["result"]

    def error(self, request_id: int, occurrence: int = 0) -> dict:
        return self.replies_to(request_id)[occurrence]["error"]

    def notifications(self, method: str) -> list[dict]:
        return [m["params"] for m in self.messages if m.get("method") == method and "id" not in m]

    def server_requests(self, method: str) -> list[dict]:
        return [m for m in self.messages if m.get("method") == method and "id" in m]


def load_fixture(name: str) -> Fixture:
    lines = []
    for raw in (FIXTURES / name).read_text().splitlines():
        if raw.startswith("{"):
            line = json.loads(raw)
            # A capture over several connections tags each frame with the one that saw it.
            lines.append(line["recv"] if "recv" in line else line)
    return Fixture(lines)


INITIALIZE_RESULT = load_fixture("thread-start.jsonl").result(1)


class Connection:
    """One accepted WebSocket connection."""

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.reader = reader
        self.writer = writer
        self.upgrade_request: bytes = b""
        self.frames: list[tuple[int, bytes]] = []
        self.closed = asyncio.Event()

    async def handshake(self) -> None:
        self.upgrade_request = await self.reader.readuntil(b"\r\n\r\n")
        key = None
        for line in self.upgrade_request.decode().split("\r\n"):
            if line.lower().startswith("sec-websocket-key:"):
                key = line.split(":", 1)[1].strip()
        accept = base64.b64encode(hashlib.sha1((key + WS_ACCEPT_GUID).encode()).digest()).decode()
        self.writer.write(
            b"HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n"
            + f"Sec-WebSocket-Accept: {accept}\r\n\r\n".encode()
        )
        await self.writer.drain()

    async def send_frame(self, opcode: int, payload: bytes) -> None:
        self.writer.write(ws.encode_frame(opcode, payload, None))
        await self.writer.drain()

    async def send_json(self, message: dict) -> None:
        await self.send_frame(ws.OP_TEXT, json.dumps(message).encode())

    async def drop(self) -> None:
        self.writer.close()
        await self.writer.wait_closed()

    def stop_reading(self) -> None:
        """Leave whatever the client sends next unread in the kernel buffer."""
        self.writer.transport.pause_reading()

    def abort(self) -> None:
        """Close at once; with unread data pending the client sees a reset, not an EOF."""
        self.writer.transport.abort()


class FakeDaemon:
    def __init__(self, socket_path):
        self.socket_path = str(socket_path)
        self.replies: dict[str, object] = {"initialize": {"result": INITIALIZE_RESULT}}
        self.requests: list[dict] = []
        self.notifications: list[dict] = []
        self.responses: dict[int, asyncio.Future] = {}
        self.connections: list[Connection] = []
        self._next_request_id = 0
        self._server = None
        self._served: dict[str, int] = {}
        self._waiters: list[tuple[list, str, asyncio.Future]] = []
        self._late: set[asyncio.Task] = set()

    @property
    def conn(self) -> Connection:
        return self.connections[-1]

    async def start(self) -> None:
        self._server = await asyncio.start_unix_server(self._serve, path=self.socket_path)

    async def stop(self) -> None:
        for conn in self.connections:
            if not conn.closed.is_set():
                await conn.drop()
        self._server.close()
        await self._server.wait_closed()

    async def drop(self) -> None:
        await self.conn.drop()

    async def notify(self, method: str, params) -> None:
        await self.conn.send_json({"method": method, "params": params})

    async def server_request(self, method: str, params, request_id: int | None = None) -> int:
        """Send a server request; `request_id` replays a captured one (the daemon reuses ids
        across connections, so a re-sent request can arrive under the id it had before)."""
        if request_id is None:
            self._next_request_id += 1
            request_id = self._next_request_id
        self.responses[request_id] = asyncio.get_running_loop().create_future()
        await self.conn.send_json({"method": method, "id": request_id, "params": params})
        return request_id

    async def response(self, request_id: int) -> dict:
        return await self.responses[request_id]

    async def wait_request(self, method: str) -> dict:
        """The first request with that method, waiting for it if none arrived yet."""
        return await self._wait(self.requests, method)

    async def wait_notification(self, method: str) -> dict:
        return await self._wait(self.notifications, method)

    async def _wait(self, seen: list[dict], method: str) -> dict:
        for message in seen:
            if message["method"] == method:
                return message
        future = asyncio.get_running_loop().create_future()
        self._waiters.append((seen, method, future))
        return await future

    def _wake_waiters(self, seen: list[dict], message: dict) -> None:
        for waiter in list(self._waiters):
            waiting_on, method, future = waiter
            if waiting_on is seen and method == message["method"] and not future.done():
                future.set_result(message)
                self._waiters.remove(waiter)

    def received(self, method: str) -> list[dict]:
        return [r for r in self.requests if r["method"] == method]

    async def _serve(self, reader, writer):
        conn = Connection(reader, writer)
        self.connections.append(conn)
        try:
            await conn.handshake()
            while True:
                opcode, payload = await ws.read_frame(reader)
                conn.frames.append((opcode, payload))
                if opcode == ws.OP_TEXT:
                    await self._handle(conn, json.loads(payload))
                elif opcode == ws.OP_CLOSE:
                    break
        except (asyncio.IncompleteReadError, ConnectionResetError, BrokenPipeError):
            pass
        finally:
            conn.closed.set()
            writer.close()

    async def _handle(self, conn: Connection, message: dict) -> None:
        if "method" not in message:
            self.responses[message["id"]].set_result(message)
            return
        if "id" not in message:
            self.notifications.append(message)
            self._wake_waiters(self.notifications, message)
            return
        self.requests.append(message)
        self._wake_waiters(self.requests, message)
        reply = self._reply_for(message["method"], message.get("params"))
        if asyncio.iscoroutine(reply):
            # A callable may return a coroutine to answer later, without holding up the
            # requests that arrive meanwhile (a slow daemon call racing another).
            task = asyncio.create_task(self._answer_later(conn, message["id"], reply))
            self._late.add(task)
            task.add_done_callback(self._late.discard)
        elif reply is not None:
            await conn.send_json({"id": message["id"], **reply})

    async def _answer_later(self, conn: "Connection", request_id: int, pending) -> None:
        reply = await pending
        if not conn.closed.is_set():
            await conn.send_json({"id": request_id, **reply})

    def _reply_for(self, method: str, params) -> dict | None:
        spec = self.replies.get(method)
        if spec is None:
            return {"error": {"code": -32601, "message": f"fake daemon: no reply configured for {method}"}}
        if isinstance(spec, list):
            # Counted rather than popped, so a fixture's reply lists survive reuse across tests.
            served = self._served.get(method, 0)
            self._served[method] = served + 1
            return spec[min(served, len(spec) - 1)]
        if callable(spec):
            return spec(params)
        return spec


class FakeDaemonThread:
    """A `FakeDaemon` served from its own event loop in a background thread, for tests
    whose subject is a separate process (the bridge started lazily by the CLI).

    `replies`/`requests`/`received` are the underlying fake's; `notify`, `drop`,
    `wait_request`, `wait_connections` and `stop` run on the daemon's loop and block
    the caller until done.
    """

    def __init__(self, socket_path):
        self.fake = FakeDaemon(socket_path)
        self.replies = self.fake.replies
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self.loop.run_forever, daemon=True, name="fake-daemon")

    def start(self) -> None:
        self.thread.start()
        self._run(self.fake.start())

    def stop(self) -> None:
        self._run(self.fake.stop())
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(timeout=5)
        self.loop.close()

    def _run(self, coro, timeout: float = 10):
        return asyncio.run_coroutine_threadsafe(coro, self.loop).result(timeout)

    @property
    def requests(self) -> list[dict]:
        return self.fake.requests

    def received(self, method: str) -> list[dict]:
        return self.fake.received(method)

    def wait_request(self, method: str, timeout: float = 5) -> dict:
        return self._run(asyncio.wait_for(self.fake.wait_request(method), timeout), timeout + 1)

    def wait_connections(self, count: int, timeout: float = 5) -> None:
        """Block until `count` connections have completed their upgrade."""

        async def poll():
            while len(self.fake.connections) < count or not self.fake.connections[count - 1].upgrade_request:
                await asyncio.sleep(0.01)

        self._run(asyncio.wait_for(poll(), timeout), timeout + 1)

    def notify(self, method: str, params) -> None:
        self._run(self.fake.notify(method, params))

    def drop(self) -> None:
        self._run(self.fake.drop())
