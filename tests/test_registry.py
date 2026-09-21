import json
import os
import re
import subprocess
from pathlib import Path

import pytest

from antiphon.claude import registry

FIXTURES = Path(__file__).parent / "fixtures"

# The registry record a stub peer wrote, as captured in tests/fixtures/peer-frames.jsonl.
CAPTURED_RECORD = json.loads((FIXTURES / "peer-frames.jsonl").read_text().splitlines()[0])["data"]

# One line of /proc/<pid>/stat as captured from a running process, with the pid, comm and
# starttime (field 22) rewritten to match the captured record above.
STAT_LINE = (
    "1001 (claude) S 113565 1001 1001 34828 1001 4194304 4493423 30225908 44 3076 145499 20712 "
    "41840 45483 20 0 21 0 3095861 5738012672 155258 18446744073709551615 26356736 88112640 "
    "140726733560240 0 0 0 0 3149824 2072145151 0 0 0 17 6 0 0 0 0 0 88116736 236167168 "
    "1261539328 140726733565570 140726733565588 140726733565588 140726733570010 0\n"
)


def write_record(sessions_dir: Path, data: dict) -> Path:
    sessions_dir.mkdir(parents=True, exist_ok=True)
    path = sessions_dir / f"{data['pid']}.json"
    path.write_text(json.dumps(data))
    return path


def fake_proc(tmp_path: Path, pid: int, stat_line: str = STAT_LINE) -> Path:
    proc = tmp_path / "proc"
    (proc / str(pid)).mkdir(parents=True)
    (proc / str(pid) / "stat").write_text(stat_line)
    return proc


def own_record(sessions_dir: Path, sock_dir: Path, name: str = "claude-main") -> dict:
    data = dict(CAPTURED_RECORD, pid=os.getpid(), name=name)
    data["messagingSocketPath"] = str(sock_dir / f"{os.getpid()}.sock")
    write_record(sessions_dir, data)
    return data


def dead_pid() -> int:
    proc = subprocess.Popen(["true"])
    proc.wait()
    return proc.pid


def test_sessions_dir_follows_claude_config_dir(monkeypatch, tmp_path):
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path / "cc"))
    assert registry.sessions_dir() == tmp_path / "cc" / "sessions"


def test_sessions_dir_defaults_to_dot_claude_under_home(monkeypatch, tmp_path):
    monkeypatch.delenv("CLAUDE_CONFIG_DIR", raising=False)
    monkeypatch.setenv("HOME", str(tmp_path))
    assert registry.sessions_dir() == tmp_path / ".claude" / "sessions"


def test_proc_start_reads_starttime_after_the_comm_field(tmp_path):
    proc = fake_proc(tmp_path, 1001)
    assert registry.proc_start(1001, proc_root=proc) == "3095861"


def test_proc_start_survives_parentheses_in_the_command_name(tmp_path):
    line = STAT_LINE.replace("(claude)", "(a (weird) name)")
    proc = fake_proc(tmp_path, 1001, line)
    assert registry.proc_start(1001, proc_root=proc) == "3095861"


def test_ps_lstart_output_is_trimmed():
    assert registry.parse_ps_lstart(b"Mon Sep 21 15:30:18 2026\n") == "Mon Sep 21 15:30:18 2026"


def test_ps_lstart_of_a_live_process_is_one_english_utc_timestamp():
    out = subprocess.run(
        ["ps", "-o", "lstart=", "-p", str(os.getpid())],
        env=dict(os.environ, LC_ALL="C", TZ="UTC"),
        capture_output=True,
        check=True,
    ).stdout
    assert registry.ps_lstart(os.getpid()) == out.decode().strip()
    assert re.fullmatch(r"[A-Z][a-z]{2} [A-Z][a-z]{2} +\d{1,2} \d\d:\d\d:\d\d \d{4}", registry.ps_lstart(os.getpid()))


def test_live_records_keeps_live_pids_and_drops_dead_ones(tmp_path, caplog):
    sessions = tmp_path / "sessions"
    own_record(sessions, tmp_path)
    write_record(sessions, dict(CAPTURED_RECORD, pid=dead_pid()))
    (sessions / "1001.abc.key").write_text("not json")
    (sessions / "9.json").write_text("{broken")
    records = registry.live_records(sessions)
    assert [r.pid for r in records] == [os.getpid()]
    assert "9.json" in caplog.text


def test_live_records_with_no_directory_is_empty(tmp_path):
    assert registry.live_records(tmp_path / "missing") == []


def test_socket_dir_comes_from_a_live_record(tmp_path):
    sessions = tmp_path / "sessions"
    own_record(sessions, tmp_path / "socks")
    assert registry.socket_dir(sessions) == str(tmp_path / "socks")


def test_socket_dir_refuses_without_a_live_record(tmp_path):
    sessions = tmp_path / "sessions"
    write_record(sessions, dict(CAPTURED_RECORD, pid=dead_pid()))
    with pytest.raises(registry.NoLiveClaude):
        registry.socket_dir(sessions)


def test_pins_ok_accepts_the_captured_record(tmp_path):
    proc = fake_proc(tmp_path, 1001)
    path = write_record(tmp_path / "sessions", CAPTURED_RECORD)
    record = registry.Record(path, CAPTURED_RECORD)
    assert registry.pins_ok(record, proc_root=proc) == []


def test_pins_ok_names_a_missing_socket_path(tmp_path):
    proc = fake_proc(tmp_path, 1001)
    data = {k: v for k, v in CAPTURED_RECORD.items() if k != "messagingSocketPath"}
    path = write_record(tmp_path / "sessions", data)
    failures = registry.pins_ok(registry.Record(path, data), proc_root=proc)
    assert len(failures) == 1
    assert "messagingSocketPath" in failures[0] and str(path) in failures[0]


def test_pins_ok_names_an_unknown_protocol_number(tmp_path):
    proc = fake_proc(tmp_path, 1001)
    data = dict(CAPTURED_RECORD, peerProtocol=2)
    path = write_record(tmp_path / "sessions", data)
    failures = registry.pins_ok(registry.Record(path, data), proc_root=proc)
    assert len(failures) == 1
    assert "peerProtocol" in failures[0] and "2" in failures[0] and str(path) in failures[0]


def test_pins_ok_names_a_process_start_mismatch(tmp_path):
    proc = fake_proc(tmp_path, 1001)
    data = dict(CAPTURED_RECORD, procStart="1")
    path = write_record(tmp_path / "sessions", data)
    failures = registry.pins_ok(registry.Record(path, data), proc_root=proc)
    assert len(failures) == 1
    assert "procStart" in failures[0] and "3095861" in failures[0] and str(path) in failures[0]


def test_pins_ok_names_a_socket_path_not_ending_in_the_pid(tmp_path):
    proc = fake_proc(tmp_path, 1001)
    data = dict(CAPTURED_RECORD, messagingSocketPath="/run/user/1000/cc-socks/1002.sock")
    path = write_record(tmp_path / "sessions", data)
    failures = registry.pins_ok(registry.Record(path, data), proc_root=proc)
    assert len(failures) == 1
    assert "messagingSocketPath" in failures[0] and "1001.sock" in failures[0]


def test_unique_name_appends_a_counter_until_free():
    assert registry.unique_name("codex", set()) == "codex"
    assert registry.unique_name("codex", {"codex"}) == "codex-2"
    assert registry.unique_name("codex", {"codex", "codex-2"}) == "codex-3"
    assert registry.unique_name("codex", {"codex", "codex-3"}) == "codex-2"


def test_resolve_session_finds_a_live_record_by_session_id(tmp_path):
    sessions = tmp_path / "sessions"
    data = own_record(sessions, tmp_path)
    found = registry.resolve_session(data["sessionId"], sessions)
    assert found is not None and found.pid == os.getpid()
    assert registry.resolve_session("00000000-0000-0000-0000-000000000000", sessions) is None


def test_resolve_session_ignores_dead_records(tmp_path):
    sessions = tmp_path / "sessions"
    write_record(sessions, dict(CAPTURED_RECORD, pid=dead_pid()))
    assert registry.resolve_session(CAPTURED_RECORD["sessionId"], sessions) is None


def test_by_name_finds_a_live_record(tmp_path):
    sessions = tmp_path / "sessions"
    own_record(sessions, tmp_path, name="planner")
    found = registry.by_name("planner", sessions)
    assert found is not None and found.name == "planner"
    assert registry.by_name("nobody", sessions) is None
