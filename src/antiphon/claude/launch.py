"""Starting a Claude Code session that the caller owns.

A Claude Code session registers itself in the registry, so antiphon has no peer
child for one and nothing to supervise: it builds the command, runs it, and then
waits for the record the session writes for itself. The **name** is the handle
between the two. A background session assigns its own session id and takes its
job id from that, ignoring any `--session-id` it was given (observed live: a
session asked for one id registered under another), so the name it was given is
the only thing the launch and the record share. The caller makes the name unique
before launching, and reads the session's real id back out of its record.

The form is `claude --bg`: a background session outlives the command that started
it, keeps its record while it lives, and Claude Code carries its whole lifecycle
(`claude agents`, `claude attach <job>`, `claude stop <job>`). A `claude -p`
session registers too, but only for its single turn, so nothing could follow up
with it. The `jobId` that `stop` and `attach` take is read from the session's own
record, not from the launch command's output.

The permission-forward hook is passed on the command line rather than written
into the user's settings, so it applies to this session and no other.
"""

from __future__ import annotations

import asyncio
import json
import logging
import shlex
import subprocess
from dataclasses import dataclass
from pathlib import Path

from antiphon.rawlog import RawLog

log = logging.getLogger(__name__)

CLAUDE = "claude"
HOOK_NAME = "claude-permission-forward.sh"
# Claude Code's own default for a command hook, and so the budget within which a
# forwarded permission request has to be answered.
HOOK_TIMEOUT = 600
# Kept back from that budget so the hook has time to reach the bridge, give up on a
# bridge that accepts but never answers, and still print a decision inside the timeout.
ASK_MARGIN = 15


class LaunchFailed(Exception):
    """The `claude` command refused to start a session."""


@dataclass(frozen=True)
class Spec:
    name: str
    cwd: str
    model: str | None
    hook: str | None  # the permission-forward hook script, or None to install none
    gate: str | None  # the tool names the hook applies to; None means every tool
    prompt: str | None = None


@dataclass(frozen=True)
class Launched:
    argv: list[str]
    stdout: str


def forward_hook() -> str | None:
    """The permission-forward hook: beside the package once installed, in the
    repository's `hooks/` directory when running from a checkout."""
    here = Path(__file__).resolve()
    for candidate in (here.parent / HOOK_NAME, here.parents[3] / "hooks" / HOOK_NAME):
        if candidate.exists():
            return str(candidate)
    log.error("%s is neither beside the package nor in hooks/; sessions start without it, "
              "so their permission decisions stay inside the session", HOOK_NAME)
    return None


def hook_settings(hook: str, gate: str | None) -> str:
    """The `--settings` document that installs the forward hook in this session alone."""
    command = f"{shlex.quote(hook)} {HOOK_TIMEOUT - ASK_MARGIN}"
    group: dict = {"hooks": [{"type": "command", "command": command, "timeout": HOOK_TIMEOUT}]}
    if gate:
        group = {"matcher": gate, **group}
    return json.dumps({"hooks": {"PreToolUse": [group]}})


def claude_argv(spec: Spec) -> list[str]:
    """The `claude` command that starts the session `spec` describes."""
    argv = [CLAUDE, "--bg", "--name", spec.name]
    if spec.model:
        argv += ["--model", spec.model]
    if spec.hook:
        argv += ["--settings", hook_settings(spec.hook, spec.gate)]
    if spec.prompt:
        # A brief may well start with a dash; `--` keeps `claude` from reading it as a flag.
        argv += ["--", spec.prompt]
    return argv


async def launch(spec: Spec, rawlog: RawLog) -> Launched:
    """Start the session; it is the caller that waits for its record to appear."""
    argv = claude_argv(spec)
    rawlog.log("out", "claude", {"argv": argv, "cwd": spec.cwd})
    proc = await asyncio.create_subprocess_exec(
        *argv, cwd=spec.cwd, stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    out, err = await proc.communicate()
    stdout, stderr = out.decode(errors="replace"), err.decode(errors="replace")
    rawlog.log("in", "claude", {"rc": proc.returncode, "stdout": stdout, "stderr": stderr})
    if proc.returncode != 0:
        raise LaunchFailed(f"{shlex.join(argv)} exited {proc.returncode}: {stderr.strip() or stdout.strip()}")
    return Launched(argv=argv, stdout=stdout)


def stop(job_id: str, rawlog: RawLog) -> None:
    """End a background session. Claude Code owns the process; this asks it to end one."""
    argv = [CLAUDE, "stop", job_id]
    rawlog.log("out", "claude", {"argv": argv})
    done = subprocess.run(argv, capture_output=True, text=True, check=False)
    rawlog.log("in", "claude", {"rc": done.returncode, "stdout": done.stdout, "stderr": done.stderr})


def attach_argv(job_id: str) -> list[str]:
    """The command that opens a background session in a terminal."""
    return [CLAUDE, "attach", job_id]
