"""Attaching a human terminal to a Codex thread via tmux.

Outside tmux there is no window to open, so `attach` degrades to printing the
command a human can run by hand in any terminal.
"""

from __future__ import annotations

import os
import shlex
import subprocess
from dataclasses import dataclass

PANE_FORMAT = "#{session_name}:#{window_id}.#{pane_id}"


class TmuxError(Exception):
    """tmux exited non-zero while opening a window."""

    def __init__(self, rc: int, stderr: str):
        super().__init__(f"tmux exited {rc}: {stderr}")
        self.rc = rc
        self.stderr = stderr


@dataclass(frozen=True)
class Attach:
    pane: str | None
    command: str


def resume_command(thread_id: str) -> str:
    return f"codex resume {shlex.quote(thread_id)}"


def new_window_argv(thread_id: str, name: str) -> list[str]:
    return [
        "tmux",
        "new-window",
        "-P",
        "-F",
        PANE_FORMAT,
        "-n",
        name,
        resume_command(thread_id),
    ]


def attach(
    thread_id: str,
    name: str,
    *,
    env=os.environ,
    run=subprocess.run,
) -> Attach:
    command = resume_command(thread_id)
    if "TMUX" not in env:
        return Attach(pane=None, command=command)

    result = run(
        new_window_argv(thread_id, name), capture_output=True, text=True, check=False
    )
    if result.returncode != 0:
        raise TmuxError(result.returncode, result.stderr)
    return Attach(pane=result.stdout.strip(), command=command)
