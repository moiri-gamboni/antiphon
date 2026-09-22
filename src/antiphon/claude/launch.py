"""Starting a Claude Code session that the caller owns.

A Claude Code session registers itself in the registry, so antiphon has no peer
child for one and nothing to supervise: it builds the command, runs it, and then
waits for the record the session writes under the session id it was given.
`--session-id` is the handle throughout — a name can be taken and renamed, a
session id cannot.

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

log = logging.getLogger(__name__)

CLAUDE = "claude"
HOOK_NAME = "claude-permission-forward.sh"
# Claude Code's own default for a command hook, and so the budget within which a
# forwarded permission request has to be answered.
HOOK_TIMEOUT = 600
# Kept back from that budget so the hook prints its answer before Claude Code's timeout.
ASK_MARGIN = 5


class LaunchFailed(Exception):
    """The `claude` command refused to start a session."""


@dataclass(frozen=True)
class Spec:
    session_id: str
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
    argv = [CLAUDE, "--bg", "--session-id", spec.session_id, "--name", spec.name]
    if spec.model:
        argv += ["--model", spec.model]
    if spec.hook:
        argv += ["--settings", hook_settings(spec.hook, spec.gate)]
    if spec.prompt:
        argv += [spec.prompt]
    return argv


async def launch(spec: Spec) -> Launched:
    """Start the session; it is the caller that waits for its record to appear."""
    argv = claude_argv(spec)
    proc = await asyncio.create_subprocess_exec(
        *argv, cwd=spec.cwd, stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    out, err = await proc.communicate()
    stdout, stderr = out.decode(errors="replace"), err.decode(errors="replace")
    log.info("%s -> rc %s\n%s%s", shlex.join(argv), proc.returncode, stdout, stderr)
    if proc.returncode != 0:
        raise LaunchFailed(f"{shlex.join(argv)} exited {proc.returncode}: {stderr.strip() or stdout.strip()}")
    return Launched(argv=argv, stdout=stdout)


def stop(job_id: str) -> None:
    """End a background session. Claude Code owns the process; this asks it to end one."""
    done = subprocess.run([CLAUDE, "stop", job_id], capture_output=True, text=True, check=False)
    log.info("claude stop %s -> rc %s\n%s%s", job_id, done.returncode, done.stdout, done.stderr)


def attach_argv(job_id: str) -> list[str]:
    """The command that opens a background session in a terminal."""
    return [CLAUDE, "attach", job_id]
