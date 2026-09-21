import asyncio

import pytest

from antiphon.codex import ws


def run(coro):
    return asyncio.run(coro)


async def decode(wire: bytes):
    reader = asyncio.StreamReader()
    reader.feed_data(wire)
    return await ws.read_frame(reader)


# --- pure framing -----------------------------------------------------------


def test_upgrade_request_bytes_match_the_capture_tool_for_a_given_key():
    # The bytes the committed capture tool (tests/fixtures/capture_daemon.py)
    # sends for this key; every fixture in tests/fixtures was recorded through it.
    key = "dGhlIHNhbXBsZSBub25jZQ=="
    assert ws.upgrade_request(key) == (
        b"GET /rpc HTTP/1.1\r\nHost: localhost\r\nConnection: Upgrade\r\n"
        b"Upgrade: websocket\r\nSec-WebSocket-Version: 13\r\n"
        b"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n"
    )


@pytest.mark.parametrize(
    "size, header",
    [
        (10, bytes.fromhex("81 8a")),
        (200, bytes.fromhex("81 fe 00c8")),
        (70_000, bytes.fromhex("81 ff 0000000000011170")),
    ],
)
def test_text_frames_round_trip_masked_at_each_length_class(size, header):
    payload = bytes(i % 251 for i in range(size))
    mask = bytes.fromhex("a1b2c3d4")

    wire = ws.encode_frame(ws.OP_TEXT, payload, mask)

    assert wire[: len(header)] == header
    assert wire[len(header) : len(header) + 4] == mask
    assert wire[len(header) + 4 :] != payload
    assert wire[len(header) + 4] == payload[0] ^ 0xA1

    opcode, decoded = run(decode(wire))
    assert opcode == ws.OP_TEXT
    assert decoded == payload


def test_unmasked_frames_decode_too():
    assert run(decode(ws.encode_frame(ws.OP_TEXT, b'{"id":1}', None))) == (ws.OP_TEXT, b'{"id":1}')


# --- over a Unix socket ------------------------------------------------------


async def with_fake(tmp_path, body):
    from fake_daemon import FakeDaemon

    fake = FakeDaemon(tmp_path / "daemon.sock")
    await fake.start()
    try:
        return await body(fake)
    finally:
        await fake.stop()


def test_connect_upgrades_and_exchanges_masked_text_frames(tmp_path):
    async def body(fake):
        sock = await ws.UnixWebSocket.connect(fake.socket_path)
        await sock.send_text('{"method":"initialized","params":{}}')
        await fake.wait_notification("initialized")
        await fake.conn.send_frame(ws.OP_TEXT, b'{"id":1,"result":{}}')
        text = await sock.recv_text()
        await sock.close()
        return fake.conn.upgrade_request, fake.conn.frames, text

    upgrade, frames, text = run(with_fake(tmp_path, body))
    assert upgrade.startswith(b"GET /rpc HTTP/1.1\r\nHost: localhost\r\n")
    assert frames[0] == (ws.OP_TEXT, b'{"method":"initialized","params":{}}')
    assert text == '{"id":1,"result":{}}'


def test_ping_is_answered_with_a_pong_and_yields_no_text(tmp_path):
    async def body(fake):
        sock = await ws.UnixWebSocket.connect(fake.socket_path)
        await fake.conn.send_frame(ws.OP_PING, b"keepalive")
        await fake.conn.send_frame(ws.OP_TEXT, b'{"method":"turn/completed"}')
        text = await sock.recv_text()
        await sock.close()
        return text, fake.conn.frames

    text, frames = run(with_fake(tmp_path, body))
    assert text == '{"method":"turn/completed"}'
    assert (ws.OP_PONG, b"keepalive") in frames


def test_close_frame_raises_transport_closed_with_the_reason(tmp_path):
    async def body(fake):
        sock = await ws.UnixWebSocket.connect(fake.socket_path)
        await fake.conn.send_frame(ws.OP_CLOSE, b"\x03\xe9going away")
        with pytest.raises(ws.TransportClosed) as info:
            await sock.recv_text()
        return info.value

    error = run(with_fake(tmp_path, body))
    assert "going away" in error.reason
    assert error.last_bytes.endswith(b"going away")


def test_eof_raises_transport_closed(tmp_path):
    async def body(fake):
        sock = await ws.UnixWebSocket.connect(fake.socket_path)
        await fake.conn.send_frame(ws.OP_TEXT, b'{"method":"thread/started"}')
        first = await sock.recv_text()
        await fake.drop()
        with pytest.raises(ws.TransportClosed) as info:
            await sock.recv_text()
        return first, info.value

    first, error = run(with_fake(tmp_path, body))
    assert first == '{"method":"thread/started"}'
    assert "closed" in error.reason
    assert error.last_bytes.endswith(b'{"method":"thread/started"}')
