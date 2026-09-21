import os
import sys

import pytest

from antiphon import callers
from antiphon.callers import Caller, classify, permits, forbidden_message, system_process_table


class FakeTable:
    """A process table built from explicit pid -> parent / pid -> comm maps."""

    def __init__(self, parents: dict[int, int], comms: dict[int, str]):
        self._parents = parents
        self._comms = comms

    def parent(self, pid: int) -> int | None:
        return self._parents.get(pid)

    def comm(self, pid: int) -> str | None:
        return self._comms.get(pid)


# --- classify: ancestry fixtures -------------------------------------------------


def test_classify_claude_ancestor():
    # cli(100) <- bash(101) <- claude(102) <- tmux(103) <- init(1)
    table = FakeTable(
        parents={100: 101, 101: 102, 102: 103, 103: 1},
        comms={100: "cli", 101: "bash", 102: "claude", 103: "tmux", 1: "systemd"},
    )
    caller = classify(
        100, None, claude_pids={102: "session-abc"}, known_threads=set(), table=table
    )
    assert caller.kind == "claude"
    assert caller.claude_pid == 102
    assert caller.claude_session_id == "session-abc"
    assert caller.codex_thread is None
    assert caller.owner_id == "session-abc"


def test_classify_codex_ancestor_known_thread():
    # cli(100) <- bash(101) <- codex(102) <- init(1)
    table = FakeTable(
        parents={100: 101, 101: 102, 102: 1},
        comms={100: "cli", 101: "bash", 102: "codex"},
    )
    caller = classify(
        100,
        "thread-xyz",
        claude_pids={},
        known_threads={"thread-xyz"},
        table=table,
    )
    assert caller.kind == "codex"
    assert caller.claude_pid is None
    assert caller.claude_session_id is None
    assert caller.codex_thread == "thread-xyz"


def test_classify_codex_ancestor_unknown_claimed_thread():
    table = FakeTable(
        parents={100: 101, 101: 102, 102: 1},
        comms={100: "cli", 101: "bash", 102: "codex"},
    )
    caller = classify(
        100,
        "made-up-thread",
        claude_pids={},
        known_threads={"thread-xyz"},
        table=table,
    )
    assert caller.kind == "codex"
    assert caller.codex_thread is None


def test_classify_no_claimed_thread_is_codex_with_no_thread():
    table = FakeTable(
        parents={100: 101, 101: 102, 102: 1},
        comms={100: "cli", 101: "bash", 102: "codex"},
    )
    caller = classify(
        100, None, claude_pids={}, known_threads={"thread-xyz"}, table=table
    )
    assert caller.kind == "codex"
    assert caller.codex_thread is None


def test_classify_human_ancestry():
    # cli(100) <- bash(101) <- tmux(102) <- init(1)
    table = FakeTable(
        parents={100: 101, 101: 102, 102: 1},
        comms={100: "cli", 101: "bash", 102: "tmux", 1: "init"},
    )
    caller = classify(100, None, claude_pids={}, known_threads=set(), table=table)
    assert caller.kind == "human"
    assert caller.claude_pid is None
    assert caller.claude_session_id is None
    assert caller.codex_thread is None
    assert caller.owner_id == "human"


def test_classify_nearest_ancestor_wins():
    # cli(100) <- bash(101) <- codex(102) <- bash(103) <- claude(104)
    table = FakeTable(
        parents={100: 101, 101: 102, 102: 103, 103: 104, 104: 1},
        comms={100: "cli", 101: "bash", 102: "codex", 103: "bash", 104: "claude"},
    )
    caller = classify(
        100,
        "t1",
        claude_pids={104: "session-far"},
        known_threads={"t1"},
        table=table,
    )
    assert caller.kind == "codex"
    assert caller.codex_thread == "t1"


def test_classify_compares_comm_by_basename():
    # a "codex" ancestor reported by full path still counts
    table = FakeTable(
        parents={100: 101, 101: 1},
        comms={100: "cli", 101: "/usr/local/bin/codex"},
    )
    caller = classify(100, None, claude_pids={}, known_threads=set(), table=table)
    assert caller.kind == "codex"


def test_classify_no_parent_chain_is_human():
    table = FakeTable(parents={}, comms={})
    caller = classify(100, None, claude_pids={}, known_threads=set(), table=table)
    assert caller.kind == "human"


# --- permits: ownership rule --------------------------------------------------


def test_permits_codex_with_unrecognised_thread_owns_nothing():
    caller = Caller(kind="codex", claude_pid=None, claude_session_id=None, codex_thread=None)
    assert permits(caller, "approve", "thread-1") is False
    assert permits(caller, "stop", "thread-1") is False


def test_permits_codex_approve_on_unspawned_thread_forbidden():
    caller = Caller(kind="codex", claude_pid=None, claude_session_id=None, codex_thread="thread-1")
    assert permits(caller, "approve", "thread-2") is False


def test_permits_codex_approve_on_own_thread_allowed():
    caller = Caller(kind="codex", claude_pid=None, claude_session_id=None, codex_thread="thread-1")
    assert permits(caller, "approve", "thread-1") is True


def test_permits_claude_approve_on_any_thread_allowed():
    caller = Caller(kind="claude", claude_pid=123, claude_session_id="session-1", codex_thread=None)
    assert permits(caller, "approve", "thread-not-mine") is True


def test_permits_codex_stop_on_unspawned_thread_forbidden():
    caller = Caller(kind="codex", claude_pid=None, claude_session_id=None, codex_thread="thread-1")
    assert permits(caller, "stop", "thread-2") is False


def test_permits_human_stop_on_anything_allowed():
    caller = Caller(kind="human", claude_pid=None, claude_session_id=None, codex_thread=None)
    assert permits(caller, "stop", "anything") is True


def test_permits_name_self_allowed_to_codex():
    codex_caller = Caller(kind="codex", claude_pid=None, claude_session_id=None, codex_thread="thread-1")
    assert permits(codex_caller, "name", None) is True


def test_permits_codex_interrupt_and_deny_follow_ownership():
    caller = Caller(kind="codex", claude_pid=None, claude_session_id=None, codex_thread="thread-1")
    assert permits(caller, "interrupt", "thread-2") is False
    assert permits(caller, "deny", "thread-2") is False
    assert permits(caller, "interrupt", "thread-1") is True
    assert permits(caller, "deny", "thread-1") is True


# --- forbidden_message --------------------------------------------------------


def test_forbidden_message_names_caller_and_spawner():
    caller = Caller(kind="codex", claude_pid=None, claude_session_id=None, codex_thread="thread-1")
    message = forbidden_message(caller, "approve", "thread-2")
    assert "codex" in message
    assert "thread-1" in message
    assert "thread-2" in message
    assert "approve" in message


def test_forbidden_message_names_unrecognised_codex_owner():
    caller = Caller(kind="codex", claude_pid=None, claude_session_id=None, codex_thread=None)
    message = forbidden_message(caller, "stop", "thread-2")
    assert "codex" in message
    assert "thread-2" in message


# --- /proc/<pid>/stat parsing ----------------------------------------------------


def test_parse_stat_handles_parens_and_spaces_in_comm():
    raw = "12345 (a) (b c) d) S 6789 6789 6789 0 -1 4194560\n"
    assert callers._parse_stat(raw) == ("a) (b c) d", 6789)


def test_parse_stat_rejects_unparseable_input():
    assert callers._parse_stat("not a stat line") is None


def test_exe_basename_strips_deleted_suffix(monkeypatch):
    def fake_readlink(path):
        assert path == "/proc/424242/exe"
        return "/usr/local/bin/codex (deleted)"

    monkeypatch.setattr(callers.os, "readlink", fake_readlink)
    assert callers._exe_basename(424242) == "codex"


# --- system_process_table -------------------------------------------------------


@pytest.mark.skipif(sys.platform != "linux", reason="Linux-only /proc reader")
def test_system_process_table_matches_os_on_linux():
    table = system_process_table()
    assert table.parent(os.getpid()) == os.getppid()
    comm = table.comm(os.getpid())
    assert comm


@pytest.mark.skipif(sys.platform != "linux", reason="ps availability assumed on this platform's CI")
def test_ps_process_table_matches_os():
    table = callers._PsProcessTable()
    assert table.parent(os.getpid()) == os.getppid()
    comm = table.comm(os.getpid())
    assert comm
