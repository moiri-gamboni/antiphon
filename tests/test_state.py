import json
import os

import pytest

from antiphon.state import State, SubAgent, ThreadState, home_dir


def full_thread() -> ThreadState:
    return ThreadState(
        thread_id="01a0c390-e298-7b53-87d2-3333c99c6ac4",
        name="helper",
        cwd="/work/repo",
        origin="spawned",
        spawner="8d0b3e2e-2f3f-4d33-9a5a-8f0b7d1a8e11",
        read_only=True,
        child_pid=4242,
        status="busy",
        active_turn_id="01a0c390-e2f0-7000-8000-000000000001",
        pending=[{"token": "a1b2c3", "since": 1790000000, "command": "rm -rf x"}],
        last_error={"message": "usage limit", "at": 1790000001.5},
        report=False,
        model="gpt-5.6-terra",
        effort="high",
        review_by_parent=True,
        worktree="/work/repo-worktrees/helper",
        outcome="completed",
        final="done: 42",
        sub_agents={
            "01a0c43e-3d95-7800-949d-06535c89fe5f": SubAgent(
                thread_id="01a0c43e-3d95-7800-949d-06535c89fe5f", nickname="Bernoulli", role=None, status="idle"
            )
        },
    )


def test_save_then_load_round_trips_every_field(tmp_path):
    path = tmp_path / "state.json"
    state = State()
    state.threads["01a0c390-e298-7b53-87d2-3333c99c6ac4"] = full_thread()
    state.stopped["old-name"] = "01a0c395-d415-70c0-bdce-5dcdf4319394"
    state.degraded = ["~/.claude/sessions/1002.json: required field pidDomain is missing"]
    state.save(path)

    loaded = State.load(path)
    assert loaded == state
    assert loaded.threads["01a0c390-e298-7b53-87d2-3333c99c6ac4"].sub_agents["01a0c43e-3d95-7800-949d-06535c89fe5f"].nickname == "Bernoulli"


def test_load_of_a_missing_file_is_an_empty_state(tmp_path):
    state = State.load(tmp_path / "state.json")
    assert state == State()


def test_save_leaves_no_temporary_file_and_the_file_is_complete_json(tmp_path):
    path = tmp_path / "state.json"
    state = State()
    state.threads["t"] = full_thread()
    state.save(path)
    state.save(path)
    assert sorted(os.listdir(tmp_path)) == ["state.json"]
    assert json.loads(path.read_text())["threads"]["t"]["name"] == "helper"


def test_thread_defaults_are_a_hosted_unregistered_idle_thread():
    thread = ThreadState(thread_id="t", name="n", cwd="/c", origin="adopted", spawner="human", read_only=False)
    assert thread.child_pid is None
    assert thread.status == "idle"
    assert thread.report is True
    assert thread.pending == []
    assert thread.sub_agents == {}


def test_home_dir_is_the_override_or_the_dotdir_under_home(monkeypatch, tmp_path):
    monkeypatch.setenv("ANTIPHON_HOME", str(tmp_path / "custom"))
    assert home_dir() == tmp_path / "custom"
    monkeypatch.delenv("ANTIPHON_HOME")
    monkeypatch.setenv("HOME", str(tmp_path))
    assert home_dir() == tmp_path / ".antiphon"


def test_ensure_home_creates_the_private_directory_tree(monkeypatch, tmp_path):
    monkeypatch.setenv("ANTIPHON_HOME", str(tmp_path / "h"))
    from antiphon.state import ensure_home

    home = ensure_home()
    assert home == tmp_path / "h"
    assert (home / "log").is_dir()
    assert os.stat(home).st_mode & 0o777 == 0o700


@pytest.mark.parametrize("status", ["idle", "busy", "approval", "unloaded"])
def test_every_status_value_survives_a_round_trip(tmp_path, status):
    state = State()
    state.threads["t"] = ThreadState(thread_id="t", name="n", cwd="/c", origin="spawned", spawner="human", read_only=False, status=status)
    state.save(tmp_path / "s.json")
    assert State.load(tmp_path / "s.json").threads["t"].status == status
