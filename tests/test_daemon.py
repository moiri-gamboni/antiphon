import asyncio
import json
import logging

import pytest

from antiphon.codex import daemon as daemon_mod
from antiphon.codex.daemon import Daemon, DaemonError, DaemonUnavailable, Delivery, deliver, ensure_running
from antiphon.codex.ws import TransportClosed
from antiphon.rawlog import RawLog
from fake_daemon import FakeDaemon, load_fixture

THREAD_START = load_fixture("thread-start.jsonl")
APPROVAL = load_fixture("user-reviewer-request-approval.jsonl")
SUB_AGENT = load_fixture("sub-agent.jsonl")
ADOPTION = load_fixture("adoption.jsonl")
TURNS_LIST = load_fixture("turns-list.jsonl")
GUARDIAN = load_fixture("guardian-override.jsonl")

THREAD_ID = THREAD_START.result(2)["thread"]["id"]


def run(coro):
    return asyncio.run(coro)


async def until(condition, timeout: float = 2):
    """Poll a callable until it is truthy; a test that never gets there fails, not hangs."""
    async def poll():
        while not condition():
            await asyncio.sleep(0.01)

    await asyncio.wait_for(poll(), timeout)


class Harness:
    """A fake daemon plus a connected client, with every callback recorded."""

    def __init__(self, tmp_path):
        self.fake = FakeDaemon(tmp_path / "daemon.sock")
        self.notifications: list[tuple[str, dict]] = []
        self.server_requests: list[tuple[int, str, dict]] = []
        self.rawlog = RawLog(tmp_path / "raw.jsonl")
        self.daemon: Daemon | None = None

    async def on_notification(self, method, params):
        self.notifications.append((method, params))

    async def on_server_request(self, request_id, method, params):
        self.server_requests.append((request_id, method, params))

    async def __aenter__(self):
        await self.fake.start()
        self.daemon = await Daemon.connect(
            self.fake.socket_path, self.on_notification, self.on_server_request, rawlog=self.rawlog
        )
        return self

    async def __aexit__(self, *exc):
        await self.daemon.close()
        await self.fake.stop()

    def raw_lines(self) -> list[dict]:
        return [json.loads(line) for line in self.rawlog.path.read_text().splitlines()]


# --- connection ---------------------------------------------------------------


def test_connect_initializes_with_the_client_capabilities_then_says_initialized(tmp_path):
    async def body():
        async with Harness(tmp_path) as h:
            initialize = await h.fake.wait_request("initialize")
            initialized = await h.fake.wait_notification("initialized")
            return initialize, initialized, h.daemon.codex_version

    initialize, initialized, version = run(body())
    assert initialize["params"]["clientInfo"]["name"] == "antiphon"
    assert initialize["params"]["clientInfo"]["version"]
    assert initialize["params"]["capabilities"] == {
        "experimentalApi": True,
        "optOutNotificationMethods": [
            "item/agentMessage/delta",
            "item/reasoning/summaryTextDelta",
            "item/reasoning/summaryPartAdded",
            "item/reasoning/textDelta",
        ],
    }
    assert initialized["params"] == {}
    assert version == "0.155.1"


def test_each_connection_gets_a_higher_epoch(tmp_path):
    async def body():
        (tmp_path / "second").mkdir()
        async with Harness(tmp_path) as first:
            async with Harness(tmp_path / "second") as second:
                return first.daemon.epoch, second.daemon.epoch

    first, second = run(body())
    assert second > first


def test_every_frame_is_raw_logged_in_both_directions(tmp_path):
    async def body():
        async with Harness(tmp_path) as h:
            await h.fake.wait_notification("initialized")
            return h.raw_lines()

    lines = run(body())
    assert [(line["dir"], line["boundary"]) for line in lines[:3]] == [("out", "codex"), ("in", "codex"), ("out", "codex")]
    assert json.loads(lines[0]["data"])["method"] == "initialize"
    assert json.loads(lines[1]["data"])["result"]["userAgent"].startswith("codex-raw-ws/0.155.1")


# --- requests and routing -----------------------------------------------------


def test_request_returns_the_result_correlated_by_id(tmp_path):
    async def body():
        async with Harness(tmp_path) as h:
            h.fake.replies["thread/loaded/list"] = {"result": TURNS_LIST.result(2)}
            h.fake.replies["thread/read"] = {"result": SUB_AGENT.result(2, occurrence=1)}
            listed, read = await asyncio.gather(
                h.daemon.request("thread/loaded/list", {}),
                h.daemon.request("thread/read", {"threadId": "x"}),
            )
            return listed, read

    listed, read = run(body())
    assert listed == {"data": [], "nextCursor": None}
    assert read["thread"]["agentNickname"] == "Bernoulli"


def test_request_raises_daemon_error_with_the_error_object(tmp_path):
    async def body():
        async with Harness(tmp_path) as h:
            h.fake.replies["thread/resume"] = {"error": ADOPTION.error(3)}
            with pytest.raises(DaemonError) as info:
                await h.daemon.request("thread/resume", {"threadId": "01a0c484-5c6d-7d70-b714-4f7111641a9f"})
            return info.value

    error = run(body())
    assert error.method == "thread/resume"
    assert error.params == {"threadId": "01a0c484-5c6d-7d70-b714-4f7111641a9f"}
    assert error.error["code"] == -32600
    assert error.error["message"].startswith("no rollout found for thread id")


def test_request_times_out(tmp_path):
    async def body():
        async with Harness(tmp_path) as h:
            h.fake.replies["thread/read"] = lambda params: None  # never answered
            with pytest.raises(TimeoutError):
                await h.daemon.request("thread/read", {"threadId": "x"}, timeout=0.05)

    run(body())


def test_notifications_reach_the_handler_with_method_and_params(tmp_path):
    turn_completed = APPROVAL.notifications("turn/completed")[0]

    async def body():
        async with Harness(tmp_path) as h:
            await h.fake.notify("turn/completed", turn_completed)
            await until(lambda: h.notifications)
            return h.notifications

    assert run(body()) == [("turn/completed", turn_completed)]


def test_a_message_with_id_and_method_is_a_server_request_and_respond_answers_it(tmp_path):
    request = APPROVAL.server_requests("item/commandExecution/requestApproval")[0]

    async def body():
        async with Harness(tmp_path) as h:
            request_id = await h.fake.server_request(request["method"], request["params"])
            await until(lambda: h.server_requests)
            await h.daemon.respond(request_id, {"decision": "accept"})
            answer = await h.fake.response(request_id)
            return h.server_requests, h.notifications, answer, h.fake.conn.frames[-1][1]

    server_requests, notifications, answer, wire = run(body())
    assert server_requests == [(1, "item/commandExecution/requestApproval", request["params"])]
    assert notifications == []
    assert answer == {"jsonrpc": "2.0", "id": 1, "result": {"decision": "accept"}}
    assert json.loads(wire) == answer


def test_a_raising_notification_handler_is_logged_with_the_raw_frame_and_the_reader_continues(tmp_path, caplog):
    started, completed = APPROVAL.notifications("turn/started")[0], APPROVAL.notifications("turn/completed")[0]
    seen = []

    async def handler(method, params):
        if method == "turn/started":
            raise RuntimeError("handler bug")
        seen.append(method)

    async def no_server_requests(*args):
        raise AssertionError("no server request expected")

    async def body():
        fake = FakeDaemon(tmp_path / "daemon.sock")
        await fake.start()
        d = await Daemon.connect(fake.socket_path, handler, no_server_requests)
        with caplog.at_level(logging.ERROR, logger="antiphon.codex"):
            await fake.notify("turn/started", started)
            await fake.notify("turn/completed", completed)
            await until(lambda: seen)
        await d.close()
        await fake.stop()

    run(body())
    assert seen == ["turn/completed"]
    record = next(r for r in caplog.records if "handler bug" in r.getMessage() or r.exc_info)
    assert '"method": "turn/started"' in record.getMessage()


def test_dropped_connection_fails_pending_requests_and_marks_the_daemon_closed(tmp_path):
    async def body():
        async with Harness(tmp_path) as h:
            h.fake.replies["thread/read"] = lambda params: None
            pending = asyncio.ensure_future(h.daemon.request("thread/read", {"threadId": "x"}))
            await h.fake.wait_request("thread/read")
            await h.fake.drop()
            with pytest.raises(TransportClosed):
                await asyncio.wait_for(pending, 2)
            await asyncio.wait_for(h.daemon.closed.wait(), 1)
            return h.daemon.close_reason

    reason = run(body())
    assert isinstance(reason, TransportClosed)


def test_a_malformed_frame_from_the_daemon_ends_the_connection_loudly(tmp_path, caplog):
    async def body():
        async with Harness(tmp_path) as h:
            h.fake.replies["thread/read"] = lambda params: None
            pending = asyncio.ensure_future(h.daemon.request("thread/read", {"threadId": "x"}))
            await h.fake.wait_request("thread/read")
            with caplog.at_level(logging.ERROR, logger="antiphon.codex"):
                await h.fake.conn.send_frame(0x1, b"not json")
                with pytest.raises(TransportClosed) as info:
                    await asyncio.wait_for(pending, 2)
                await asyncio.wait_for(h.daemon.closed.wait(), 1)
            return info.value

    error = run(body())
    assert "reader stopped" in error.reason
    assert any("not json" in r.getMessage() for r in caplog.records)


def test_a_handler_can_make_requests_and_get_their_replies(tmp_path):
    read_results = []

    class Bridge(Harness):
        async def on_notification(self, method, params):
            read_results.append(await self.daemon.thread_read(params["threadId"]))

    async def body():
        async with Bridge(tmp_path) as h:
            h.fake.replies["thread/read"] = {"result": SUB_AGENT.result(2, occurrence=1)}
            await h.fake.notify("thread/status/changed", {"threadId": "t1", "status": {"type": "idle"}})
            await asyncio.wait_for(h.fake.wait_request("thread/read"), 1)
            await until(lambda: read_results)

    run(body())
    assert read_results[0]["thread"]["agentNickname"] == "Bernoulli"


def test_notifications_are_handled_in_arrival_order_even_when_a_handler_waits(tmp_path):
    order = []

    class Bridge(Harness):
        async def on_notification(self, method, params):
            if method == "turn/started":
                await asyncio.sleep(0.05)
            order.append(method)

    async def body():
        async with Bridge(tmp_path) as h:
            await h.fake.notify("turn/started", {})
            await h.fake.notify("turn/completed", {})
            await until(lambda: len(order) == 2)

    run(body())
    assert order == ["turn/started", "turn/completed"]


def test_a_failed_initialize_raises_and_releases_the_connection(tmp_path):
    async def body():
        fake = FakeDaemon(tmp_path / "daemon.sock")
        fake.replies["initialize"] = {"error": {"code": -32600, "message": "unsupported client"}}
        await fake.start()
        try:
            with pytest.raises(DaemonError):
                await Daemon.connect(fake.socket_path, None, None)
            await asyncio.wait_for(fake.conn.closed.wait(), 1)
            return [t for t in asyncio.all_tasks() if t.get_name().startswith("antiphon")]
        finally:
            await fake.stop()

    assert run(body()) == []


# --- thread verbs -------------------------------------------------------------

TURN_STARTED = APPROVAL.result(3)  # {"turn": {... "status": "inProgress" ...}}
TURN_ID = TURN_STARTED["turn"]["id"]
COMPLETED_TURN = TURNS_LIST.result(3)["data"][0]
NOT_LOADED = TURNS_LIST.error(4)  # "thread not loaded: <id>"
NOT_FOUND = SUB_AGENT.error(5)  # "thread not found: <id>" from turn/start on an unloaded thread
NO_ROLLOUT = ADOPTION.error(3)  # thread/resume on a thread that never took a turn
# turn/steer answers {"turnId": ...} in the protocol schema; no capture of a
# successful steer exists yet, so this reply is schema-shaped rather than recorded.
STEERED = {"turnId": TURN_ID}


def turns_list(*turns):
    return {"result": {"data": list(turns), "nextCursor": None, "backwardsCursor": None}}


def test_thread_start_sends_the_headless_thread_parameters_then_names_the_thread(tmp_path):
    async def body():
        async with Harness(tmp_path) as h:
            h.fake.replies["thread/start"] = {"result": THREAD_START.result(2)}
            h.fake.replies["thread/name/set"] = {"result": {}}
            result = await h.daemon.thread_start("/tmp/work", "helper", read_only=False, model=None, effort=None)
            return result, h.fake.received("thread/start")[0]["params"], h.fake.received("thread/name/set")[0]["params"]

    result, start_params, name_params = run(body())
    assert result["thread"]["id"] == THREAD_ID
    assert start_params == {
        "cwd": "/tmp/work",
        "approvalPolicy": "on-request",
        "approvalsReviewer": "auto_review",
        "sandbox": "workspace-write",
        "ephemeral": False,
        "serviceName": "antiphon",
        "developerInstructions": daemon_mod.HEADLESS_INSTRUCTIONS,
    }
    assert name_params == {"threadId": THREAD_ID, "name": "helper"}


def test_thread_start_read_only_model_and_parent_review_map_to_their_parameters(tmp_path):
    async def body():
        async with Harness(tmp_path) as h:
            h.fake.replies["thread/start"] = {"result": THREAD_START.result(2)}
            h.fake.replies["thread/name/set"] = {"result": {}}
            await h.daemon.thread_start("/tmp/work", "helper", read_only=True, model="gpt-5.6-terra", effort=None, review_by_parent=True)
            return h.fake.received("thread/start")[0]["params"]

    params = run(body())
    assert params["sandbox"] == "read-only"
    assert params["model"] == "gpt-5.6-terra"
    assert params["approvalsReviewer"] == "user"


def test_effort_is_sent_on_the_threads_first_turn_only(tmp_path):
    async def body():
        async with Harness(tmp_path) as h:
            h.fake.replies["thread/start"] = {"result": THREAD_START.result(2)}
            h.fake.replies["thread/name/set"] = {"result": {}}
            h.fake.replies["turn/start"] = {"result": TURN_STARTED}
            await h.daemon.thread_start("/tmp/work", "helper", read_only=False, model=None, effort="high")
            await h.daemon.turn_start(THREAD_ID, "first", None, "c1")
            await h.daemon.turn_start(THREAD_ID, "second", None, "c2")
            return [r["params"] for r in h.fake.received("turn/start")]

    first, second = run(body())
    assert first["effort"] == "high"
    assert "effort" not in second


def test_turn_start_builds_the_input_and_optional_sandbox_policy(tmp_path):
    async def body():
        async with Harness(tmp_path) as h:
            h.fake.replies["turn/start"] = {"result": TURN_STARTED}
            plain = await h.daemon.turn_start(THREAD_ID, "hello", None, "c1")
            await h.daemon.turn_start(THREAD_ID, "hello", {"type": "workspaceWrite", "networkAccess": False}, "c2")
            return plain, [r["params"] for r in h.fake.received("turn/start")]

    result, (plain, sandboxed) = run(body())
    assert result["turn"]["id"] == TURN_ID
    assert plain == {"threadId": THREAD_ID, "input": [{"type": "text", "text": "hello"}], "clientUserMessageId": "c1"}
    assert sandboxed["sandboxPolicy"] == {"type": "workspaceWrite", "networkAccess": False}


def test_active_turn_reads_the_latest_turn_and_reports_it_only_while_in_progress(tmp_path):
    async def body():
        async with Harness(tmp_path) as h:
            h.fake.replies["thread/turns/list"] = [turns_list(COMPLETED_TURN), turns_list(TURN_STARTED["turn"]), turns_list()]
            return (
                await h.daemon.active_turn(THREAD_ID),
                await h.daemon.active_turn(THREAD_ID),
                await h.daemon.active_turn(THREAD_ID),
                h.fake.received("thread/turns/list")[0]["params"],
            )

    completed, active, empty, params = run(body())
    assert completed is None
    assert active == TURN_ID
    assert empty is None
    assert params == {"threadId": THREAD_ID, "limit": 1, "sortDirection": "desc"}


@pytest.mark.parametrize(
    "call, method, params",
    [
        (lambda d: d.thread_resume("t1"), "thread/resume", {"threadId": "t1"}),
        (lambda d: d.thread_read("t1"), "thread/read", {"threadId": "t1"}),
        (lambda d: d.turn_steer("t1", "u1", "more", "c1"), "turn/steer",
         {"threadId": "t1", "expectedTurnId": "u1", "input": [{"type": "text", "text": "more"}], "clientUserMessageId": "c1"}),
        (lambda d: d.turn_interrupt("t1", "u1"), "turn/interrupt", {"threadId": "t1", "turnId": "u1"}),
        (lambda d: d.set_name("t1", "renamed"), "thread/name/set", {"threadId": "t1", "name": "renamed"}),
        (lambda d: d.loaded_list(), "thread/loaded/list", {}),
        (lambda d: d.unsubscribe("t1"), "thread/unsubscribe", {"threadId": "t1"}),
        (lambda d: d.approve_guardian_denied("t1", {"id": "r1", "status": "denied"}), "thread/approveGuardianDeniedAction",
         {"threadId": "t1", "event": {"id": "r1", "status": "denied"}}),
    ],
)
def test_thread_verbs_send_their_method_and_parameters(tmp_path, call, method, params):
    async def body():
        async with Harness(tmp_path) as h:
            h.fake.replies[method] = {"result": {"data": [], "nextCursor": None}}
            await call(h.daemon)
            return h.fake.received(method)[0]["params"]

    assert run(body()) == params


def test_loaded_list_returns_the_thread_ids(tmp_path):
    restart = load_fixture("daemon-restart.jsonl")

    async def body():
        async with Harness(tmp_path) as h:
            h.fake.replies["thread/loaded/list"] = {"result": restart.result(5)}
            return await h.daemon.loaded_list()

    assert run(body()) == ["01a0c399-780c-73f3-9209-e09112a796a0"]


def test_guardian_override_sends_the_captured_event_shape(tmp_path):
    sent = next(m for m in GUARDIAN.sent if m["method"] == "thread/approveGuardianDeniedAction")

    async def body():
        async with Harness(tmp_path) as h:
            h.fake.replies["thread/approveGuardianDeniedAction"] = {"result": GUARDIAN.result(4)}
            result = await h.daemon.approve_guardian_denied(sent["params"]["threadId"], sent["params"]["event"])
            return result, h.fake.received("thread/approveGuardianDeniedAction")[0]["params"]

    result, params = run(body())
    assert result == {}
    assert params == sent["params"]


# --- deliver ------------------------------------------------------------------


def deliver_with(tmp_path, replies):
    async def body():
        async with Harness(tmp_path) as h:
            h.fake.replies.update(replies)
            delivery = await deliver(h.daemon, THREAD_ID, "carry on", None)
            methods = [r["method"] for r in h.fake.requests if r["method"] != "initialize"]
            rungs = [line["data"] for line in h.raw_lines() if line["boundary"] == "codex.deliver"]
            return delivery, methods, rungs, h.fake.requests

    return run(body())


def test_deliver_starts_a_turn_on_an_idle_thread(tmp_path):
    delivery, methods, rungs, requests = deliver_with(tmp_path, {
        "thread/turns/list": turns_list(COMPLETED_TURN),
        "turn/start": {"result": TURN_STARTED},
    })
    assert delivery == Delivery("started", TURN_ID, delivery.client_id)
    assert methods == ["thread/turns/list", "turn/start"]
    started = requests[-1]["params"]
    assert started["input"] == [{"type": "text", "text": "carry on"}]
    assert started["clientUserMessageId"] == delivery.client_id
    assert rungs == []


def test_deliver_steers_an_active_turn(tmp_path):
    delivery, methods, rungs, requests = deliver_with(tmp_path, {
        "thread/turns/list": turns_list(TURN_STARTED["turn"]),
        "turn/steer": {"result": STEERED},
    })
    assert delivery == Delivery("steered", TURN_ID, delivery.client_id)
    assert methods == ["thread/turns/list", "turn/steer"]
    assert requests[-1]["params"]["expectedTurnId"] == TURN_ID
    assert rungs == []


def test_deliver_starts_a_turn_when_the_steer_is_refused(tmp_path):
    delivery, methods, rungs, _ = deliver_with(tmp_path, {
        "thread/turns/list": turns_list(TURN_STARTED["turn"]),
        "turn/steer": {"error": {"code": -32600, "message": "turn ended"}},
        "turn/start": {"result": TURN_STARTED},
    })
    assert delivery.kind == "started"
    assert methods == ["thread/turns/list", "turn/steer", "turn/start"]
    assert [r["rung"] for r in rungs] == ["steer-refused"]


def test_deliver_resumes_a_thread_the_daemon_has_not_loaded_then_retries_once(tmp_path):
    resume = load_fixture("daemon-restart.jsonl")
    delivery, methods, rungs, _ = deliver_with(tmp_path, {
        "thread/turns/list": [{"error": NOT_LOADED}, turns_list(COMPLETED_TURN)],
        "thread/resume": {"result": resume.result(2)},
        "turn/start": {"result": TURN_STARTED},
    })
    assert delivery.kind == "started"
    assert methods == ["thread/turns/list", "thread/resume", "thread/turns/list", "turn/start"]
    assert [r["rung"] for r in rungs] == ["not-loaded"]


def test_deliver_resumes_when_turn_start_reports_the_thread_not_found(tmp_path):
    resume = load_fixture("daemon-restart.jsonl")
    delivery, methods, rungs, _ = deliver_with(tmp_path, {
        "thread/turns/list": turns_list(COMPLETED_TURN),
        "turn/start": [{"error": NOT_FOUND}, {"result": TURN_STARTED}],
        "thread/resume": {"result": resume.result(2)},
    })
    assert delivery.kind == "started"
    assert methods == ["thread/turns/list", "turn/start", "thread/resume", "turn/start"]
    assert [r["rung"] for r in rungs] == ["not-loaded"]


def test_deliver_gives_up_when_the_resume_itself_fails(tmp_path):
    with pytest.raises(DaemonError) as info:
        deliver_with(tmp_path, {
            "thread/turns/list": {"error": NOT_LOADED},
            "thread/resume": {"error": NO_ROLLOUT},
        })
    assert info.value.method == "thread/resume"


def test_deliver_reads_again_and_steers_when_a_turn_started_in_between(tmp_path):
    # Provisional: no capture holds the error turn/start returns while a turn is
    # already active; the message is a guess and only its classification is tested.
    delivery, methods, rungs, _ = deliver_with(tmp_path, {
        "thread/turns/list": [turns_list(COMPLETED_TURN), turns_list(TURN_STARTED["turn"])],
        "turn/start": {"error": {"code": -32600, "message": "turn already active"}},
        "turn/steer": {"result": STEERED},
    })
    assert delivery == Delivery("steered", TURN_ID, delivery.client_id)
    assert methods == ["thread/turns/list", "turn/start", "thread/turns/list", "turn/steer"]
    assert [r["rung"] for r in rungs] == ["turn-active"]


def test_deliver_raises_when_the_turn_start_fails_for_another_reason(tmp_path):
    with pytest.raises(DaemonError) as info:
        deliver_with(tmp_path, {
            "thread/turns/list": turns_list(COMPLETED_TURN),
            "turn/start": {"error": {"code": -32600, "message": "direct app-server input is not allowed for multi-agent v2 sub-agents"}},
        })
    assert info.value.method == "turn/start"


# --- ensure_running -----------------------------------------------------------

START_OUTPUT = (
    '{"status":"alreadyRunning","backend":"pid","managedCodexPath":"~/.codex/packages/standalone/current/bin/codex",'
    '"managedCodexVersion":"0.155.1","socketPath":"~/.codex/app-server-control/app-server-control.sock",'
    '"cliVersion":"0.155.1","appServerVersion":"0.155.1"}\n'
)


def stub_run(monkeypatch, returncode=0, stdout=START_OUTPUT, stderr=""):
    import subprocess

    calls = []

    def fake_run(args, **kwargs):
        calls.append((args, kwargs))
        return subprocess.CompletedProcess(args, returncode, stdout, stderr)

    monkeypatch.setattr(subprocess, "run", fake_run)
    return calls


def test_ensure_running_starts_the_daemon_when_the_socket_is_missing(tmp_path, monkeypatch):
    calls = stub_run(monkeypatch)
    path = ensure_running(str(tmp_path))
    assert path == str(tmp_path / "app-server-control" / "app-server-control.sock")
    assert [c[0] for c in calls] == [["codex", "app-server", "daemon", "start"]]


def test_ensure_running_starts_the_daemon_when_the_socket_refuses(tmp_path, monkeypatch):
    stale = tmp_path / "app-server-control" / "app-server-control.sock"
    stale.parent.mkdir()
    stale.write_text("")
    calls = stub_run(monkeypatch)
    ensure_running(str(tmp_path))
    assert len(calls) == 1


def test_ensure_running_leaves_a_listening_daemon_alone(tmp_path, monkeypatch):
    calls = stub_run(monkeypatch)

    async def body():
        fake = FakeDaemon(tmp_path / "app-server-control" / "app-server-control.sock")
        (tmp_path / "app-server-control").mkdir()
        await fake.start()
        try:
            result = await asyncio.to_thread(ensure_running, str(tmp_path))
            # Let the fake finish accepting and releasing the probe connection
            # before the server goes away, or that socket leaks a warning.
            await until(lambda: fake.connections)
            await asyncio.wait_for(fake.conn.closed.wait(), 1)
            return result
        finally:
            await fake.stop()

    assert run(body()) == str(tmp_path / "app-server-control" / "app-server-control.sock")
    assert calls == []


def test_ensure_running_raises_with_the_command_output_when_the_start_fails(tmp_path, monkeypatch):
    stub_run(monkeypatch, returncode=1, stdout="", stderr="error: codex not logged in\n")
    with pytest.raises(DaemonUnavailable) as info:
        ensure_running(str(tmp_path))
    assert info.value.rc == 1
    assert info.value.stdout == ""
    assert info.value.stderr == "error: codex not logged in\n"


def test_ensure_running_logs_the_command_and_its_output(tmp_path, monkeypatch):
    stub_run(monkeypatch)
    rawlog = RawLog(tmp_path / "raw.jsonl")
    ensure_running(str(tmp_path), rawlog=rawlog)
    lines = [json.loads(line) for line in rawlog.path.read_text().splitlines()]
    assert lines[0]["dir"] == "out" and lines[0]["boundary"] == "codex-cli"
    assert lines[0]["data"] == ["codex", "app-server", "daemon", "start"]
    assert lines[1]["dir"] == "in" and lines[1]["data"]["stdout"] == START_OUTPUT
