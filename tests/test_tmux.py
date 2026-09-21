import subprocess

import pytest

from antiphon.tmux import Attach, TmuxError, attach, new_window_argv, resume_command

THREAD_ID = "5b6f1c2e-8a3d-4b7e-9c1f-2d4e6a8b0c1d"


def test_resume_command_is_codex_resume_with_thread_id():
    assert resume_command(THREAD_ID) == f"codex resume {THREAD_ID}"


def test_resume_command_quotes_a_thread_id_with_shell_metacharacters():
    assert resume_command("a; rm -rf /") == "codex resume 'a; rm -rf /'"


def test_new_window_argv_matches_the_tmux_invocation():
    assert new_window_argv(THREAD_ID, "codex-antiphon") == [
        "tmux",
        "new-window",
        "-P",
        "-F",
        "#{session_name}:#{window_id}.#{pane_id}",
        "-n",
        "codex-antiphon",
        f"codex resume {THREAD_ID}",
    ]


def test_attach_in_tmux_runs_new_window_and_returns_the_pane():
    calls = []

    def fake_run(argv, **kwargs):
        calls.append((argv, kwargs))
        return subprocess.CompletedProcess(
            argv, returncode=0, stdout="main:@3.%7\n", stderr=""
        )

    result = attach(
        THREAD_ID,
        "codex-antiphon",
        env={"TMUX": "/tmp/tmux-1000/default,1,0"},
        run=fake_run,
    )

    assert result == Attach(pane="main:@3.%7", command=resume_command(THREAD_ID))
    assert len(calls) == 1
    argv, kwargs = calls[0]
    assert argv == new_window_argv(THREAD_ID, "codex-antiphon")
    assert kwargs == {"capture_output": True, "text": True, "check": False}


def test_attach_outside_tmux_returns_the_resume_command_without_running_anything():
    def fake_run(argv, **kwargs):
        raise AssertionError("tmux must not be invoked outside a tmux session")

    result = attach(THREAD_ID, "codex-antiphon", env={}, run=fake_run)

    assert result == Attach(pane=None, command=resume_command(THREAD_ID))


def test_attach_raises_tmux_error_when_tmux_fails():
    def fake_run(argv, **kwargs):
        return subprocess.CompletedProcess(
            argv, returncode=1, stdout="", stderr="no current session"
        )

    with pytest.raises(TmuxError) as exc_info:
        attach(
            THREAD_ID,
            "codex-antiphon",
            env={"TMUX": "/tmp/tmux-1000/default,1,0"},
            run=fake_run,
        )

    assert exc_info.value.rc == 1
    assert exc_info.value.stderr == "no current session"
