"""Who is on the other end of a request: a Claude Code session, a Codex thread, or a human at a terminal.

Classification walks the calling process's parent chain looking for the nearest
ancestor that is a live Claude Code process or a Codex CLI process; anything else
is a human. Authorisation then mirrors Claude Code's own model: a caller may
always act on threads it spawned, Claude sessions and humans may act on any
thread, and a Codex caller acting on a thread it did not spawn is limited to a
labelled peer message.
"""

from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Collection, Mapping
from dataclasses import dataclass
from typing import Literal, Protocol

CallerKind = Literal["claude", "codex", "human"]


@dataclass(frozen=True)
class Caller:
    kind: CallerKind
    claude_pid: int | None
    claude_session_id: str | None
    codex_thread: str | None

    @property
    def owner_id(self) -> str:
        """The id this caller spawns threads under, for comparison against a thread's spawner."""
        if self.kind == "claude":
            assert self.claude_session_id is not None
            return self.claude_session_id
        if self.kind == "codex":
            # An unrecognised or absent claimed thread owns nothing: the empty
            # string never equals a real spawner id.
            return self.codex_thread or ""
        return "human"


class ProcessTable(Protocol):
    def parent(self, pid: int) -> int | None: ...
    def comm(self, pid: int) -> str | None: ...


def classify(
    pid: int,
    claimed_thread: str | None,
    *,
    claude_pids: Mapping[int, str],
    known_threads: Collection[str],
    table: ProcessTable,
) -> Caller:
    """Classify the process at `pid` by walking its ancestors (the pid itself excluded).

    The nearest ancestor that is either a live Claude Code process (a key of
    `claude_pids`) or a Codex process (comm basename "codex") decides the kind.
    """
    seen: set[int] = set()
    current = table.parent(pid)
    while current is not None and current not in seen:
        seen.add(current)

        session_id = claude_pids.get(current)
        if session_id is not None:
            return Caller(kind="claude", claude_pid=current, claude_session_id=session_id, codex_thread=None)

        comm = table.comm(current)
        if comm is not None and os.path.basename(comm) == "codex":
            thread = claimed_thread if claimed_thread in known_threads else None
            return Caller(kind="codex", claude_pid=None, claude_session_id=None, codex_thread=thread)

        if current == 1:
            break
        current = table.parent(current)

    return Caller(kind="human", claude_pid=None, claude_session_id=None, codex_thread=None)


def permits(caller: Caller, spawner: str | None) -> bool:
    """The ownership rule, the same for every gated op: whether `caller` may act on a
    thread spawned by `spawner` (`None` when the caller is acting on itself)."""
    if spawner is None:
        return True
    if caller.kind in ("claude", "human"):
        return True
    return spawner == caller.owner_id


def forbidden_message(caller: Caller, verb: str, spawner: str | None) -> str:
    """Explanation text for a denied `permits` call, naming the caller and the thread's spawner."""
    owner = caller.owner_id or "no thread it spawned"
    return f"{caller.kind} caller ({owner}) may not {verb} a thread spawned by {spawner!r}"


def _parse_stat(raw: str) -> tuple[str, int] | None:
    """Parse the text of /proc/<pid>/stat: comm is inside the (possibly parenthesis-containing) parens, ppid follows the state field."""
    open_paren = raw.find("(")
    close_paren = raw.rfind(")")
    if open_paren == -1 or close_paren == -1 or close_paren < open_paren:
        return None
    comm = raw[open_paren + 1 : close_paren]
    fields = raw[close_paren + 2 :].split()
    if len(fields) < 2:
        return None
    ppid = int(fields[1])  # fields[0] is state, fields[1] is ppid
    return comm, ppid


def _read_stat(pid: int) -> tuple[str, int] | None:
    try:
        with open(f"/proc/{pid}/stat") as f:
            raw = f.read()
    except OSError:
        return None
    return _parse_stat(raw)


def _exe_basename(pid: int) -> str | None:
    try:
        target = os.readlink(f"/proc/{pid}/exe")
    except OSError:
        return None
    # A replaced or removed binary under a still-running process reports its
    # old path with " (deleted)" appended; strip it so the name stays "codex"
    # instead of misclassifying the caller as human.
    target = target.removesuffix(" (deleted)")
    return os.path.basename(target)


class _LinuxProcessTable:
    def parent(self, pid: int) -> int | None:
        stat = _read_stat(pid)
        return stat[1] if stat is not None else None

    def comm(self, pid: int) -> str | None:
        # The kernel truncates /proc/<pid>/stat's comm to 15 characters; the
        # executable's basename is the fuller name when readable.
        exe = _exe_basename(pid)
        if exe is not None:
            return exe
        stat = _read_stat(pid)
        return stat[0] if stat is not None else None


class _PsProcessTable:
    def _lookup(self, pid: int) -> tuple[int, str] | None:
        try:
            result = subprocess.run(
                ["ps", "-o", "ppid=,comm=", "-p", str(pid)],
                capture_output=True,
                text=True,
                check=True,
            )
        except (OSError, subprocess.CalledProcessError):
            return None
        line = result.stdout.strip()
        if not line:
            return None
        parts = line.split(maxsplit=1)
        if len(parts) != 2:
            return None
        ppid_text, comm = parts
        try:
            ppid = int(ppid_text)
        except ValueError:
            return None
        return ppid, os.path.basename(comm)

    def parent(self, pid: int) -> int | None:
        looked_up = self._lookup(pid)
        return looked_up[0] if looked_up is not None else None

    def comm(self, pid: int) -> str | None:
        looked_up = self._lookup(pid)
        return looked_up[1] if looked_up is not None else None


def system_process_table() -> ProcessTable:
    """The real process table: /proc on Linux, `ps` elsewhere (e.g. macOS)."""
    if sys.platform == "linux":
        return _LinuxProcessTable()
    return _PsProcessTable()
