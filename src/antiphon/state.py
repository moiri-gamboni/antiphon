"""The bridge's durable state: the threads it hosts and whether it is degraded.

One JSON file under the antiphon home, written by the bridge alone. It holds
what must survive a bridge restart: which Codex threads are hosted, who
spawned them, and what the last turn on each produced. Everything tied to a
live process (child pipes, daemon subscriptions) is rebuilt at start.
"""

from __future__ import annotations

import dataclasses
import json
import os
from dataclasses import dataclass, field
from pathlib import Path

STATE_VERSION = 1


def home_dir() -> Path:
    override = os.environ.get("ANTIPHON_HOME")
    if override:
        return Path(override)
    return Path.home() / ".antiphon"


def ensure_home() -> Path:
    """The antiphon home with its log directory, private to the user."""
    home = home_dir()
    os.makedirs(home, 0o700, exist_ok=True)
    os.makedirs(home / "log", 0o700, exist_ok=True)
    return home


@dataclass
class SubAgent:
    """A thread Codex's own multi-agent tools spawned under one of ours; listed, never driven."""

    thread_id: str
    nickname: str | None
    role: str | None
    status: str


@dataclass
class ThreadState:
    thread_id: str
    name: str
    cwd: str
    origin: str  # "spawned" | "adopted"
    spawner: str  # a Claude session id, a Codex thread id, or "human"
    read_only: bool
    child_pid: int | None = None
    status: str = "idle"  # "idle" | "busy" | "approval" | "unloaded"
    active_turn_id: str | None = None
    pending: list[dict] = field(default_factory=list)
    last_error: dict | None = None
    report: bool = True
    effort: str | None = None
    effort_sent: bool = False  # the effort dial goes on the first turn the bridge starts, whichever daemon connection that is
    worktree: str | None = None
    outcome: str | None = None  # the last turn's status: "completed" | "failed" | "interrupted"
    final: str | None = None  # the last turn's outcome text: its final answer or "<status>: <error>"
    sub_agents: dict[str, SubAgent] = field(default_factory=dict)


@dataclass
class SessionState:
    """A Claude Code session antiphon started for a caller.

    The session writes its own registry record, so nothing here duplicates it: this is
    the ownership, the process to end on `stop`, and the escalations it is waiting on.
    """

    session_id: str
    name: str
    cwd: str
    spawner: str  # a Codex thread id or "human"
    job_id: str | None = None  # what `claude stop` and `claude attach` take
    pending: list[dict] = field(default_factory=list)


# What the bridge holds ownership and escalations for, whichever kind of session it is.
Peer = ThreadState | SessionState


@dataclass
class State:
    threads: dict[str, ThreadState] = field(default_factory=dict)
    sessions: dict[str, SessionState] = field(default_factory=dict)
    stopped: dict[str, str] = field(default_factory=dict)  # former name -> thread id, so `send` can name the resume command
    degraded: list[str] = field(default_factory=list)

    @classmethod
    def load(cls, path: Path | str) -> State:
        path = Path(path)
        if not path.exists():
            return cls()
        data = json.loads(path.read_text())
        threads = {}
        for thread_id, raw in data["threads"].items():
            sub_agents = {k: SubAgent(**v) for k, v in raw.pop("sub_agents").items()}
            threads[thread_id] = ThreadState(**raw, sub_agents=sub_agents)
        sessions = {k: SessionState(**v) for k, v in data.get("sessions", {}).items()}
        return cls(threads=threads, sessions=sessions, stopped=data["stopped"], degraded=data["degraded"])

    def save(self, path: Path | str) -> None:
        path = Path(path)
        data = {
            "v": STATE_VERSION,
            "threads": {k: dataclasses.asdict(t) for k, t in self.threads.items()},
            "sessions": {k: dataclasses.asdict(s) for k, s in self.sessions.items()},
            "stopped": self.stopped,
            "degraded": self.degraded,
        }
        # Written whole then renamed, so a crash mid-write leaves the previous file intact.
        tmp = path.with_name(f"{path.name}.{os.getpid()}.tmp")
        tmp.write_text(json.dumps(data, indent=1))
        os.replace(tmp, path)
