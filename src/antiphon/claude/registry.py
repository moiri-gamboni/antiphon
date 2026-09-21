"""Claude Code's session registry: one JSON record per live session under
``$CLAUDE_CONFIG_DIR/sessions``, named ``<pid>.json``.

Claude Code treats a record as live when its pid is running and its ``procStart`` matches the
process's own start time, so a peer that wants to be listed must reproduce that computation
exactly; ``pins_ok`` checks a live record against everything this module assumes about the
record shape, which is how a protocol change under us is noticed instead of silently misread.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

log = logging.getLogger(__name__)

PEER_PROTOCOL = 1

REQUIRED_FIELDS = (
    "pid",
    "sessionId",
    "cwd",
    "startedAt",
    "procStart",
    "version",
    "peerProtocol",
    "peerFeatures",
    "kind",
    "pidDomain",
    "messagingSocketPath",
    "name",
)


class NoLiveClaude(Exception):
    """No registry record belongs to a running process."""


@dataclass(frozen=True)
class Record:
    path: Path
    data: dict

    @property
    def pid(self) -> int:
        return self.data["pid"]

    @property
    def session_id(self) -> str:
        return self.data["sessionId"]

    @property
    def name(self) -> str:
        return self.data["name"]

    @property
    def socket_path(self) -> str:
        return self.data["messagingSocketPath"]


def sessions_dir() -> Path:
    config_dir = os.environ.get("CLAUDE_CONFIG_DIR")
    if config_dir:
        return Path(config_dir) / "sessions"
    return Path.home() / ".claude" / "sessions"


def proc_start(pid: int, proc_root: Path | str = "/proc") -> str:
    """The process start time in the form Claude Code writes to ``procStart``."""
    if sys.platform.startswith("linux"):
        stat = (Path(proc_root) / str(pid) / "stat").read_text()
        # The comm field is parenthesised and may itself contain parentheses and spaces,
        # so the fields after it are counted from the last closing parenthesis.
        after_comm = stat.rsplit(")", 1)[1].split()
        return after_comm[19]
    return ps_lstart(pid)


def ps_lstart(pid: int) -> str:
    out = subprocess.run(
        ["ps", "-o", "lstart=", "-p", str(pid)],
        env={**os.environ, "LC_ALL": "C", "TZ": "UTC"},
        capture_output=True,
        check=False,
    ).stdout
    return parse_ps_lstart(out)


def parse_ps_lstart(output: bytes) -> str:
    return output.decode().strip()


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def live_records(directory: Path | None = None) -> list[Record]:
    directory = sessions_dir() if directory is None else Path(directory)
    if not directory.is_dir():
        return []
    records = []
    for path in sorted(directory.glob("*.json")):
        try:
            data = json.loads(path.read_text())
            pid = data["pid"]
        except (OSError, ValueError, KeyError, TypeError) as e:
            # A half-written or foreign file in the registry must not hide the other
            # sessions, so it is reported and skipped.
            log.warning("skipping unreadable registry record %s: %r", path, e)
            continue
        if isinstance(pid, int) and _pid_alive(pid):
            records.append(Record(path, data))
    return records


def socket_dir(directory: Path | None = None) -> str:
    for record in live_records(directory):
        if "messagingSocketPath" in record.data:
            return os.path.dirname(record.socket_path)
    raise NoLiveClaude("no live Claude Code session record to take the socket directory from")


def pins_ok(record: Record, proc_root: Path | str = "/proc") -> list[str]:
    """Every way the record departs from the shape this adapter was written against."""
    data = record.data
    failures = []
    for field in REQUIRED_FIELDS:
        if field not in data:
            failures.append(f"{record.path}: required field {field} is missing")
    if failures:
        return failures
    if data["peerProtocol"] != PEER_PROTOCOL:
        failures.append(
            f"{record.path}: peerProtocol is {data['peerProtocol']!r}, this adapter speaks {PEER_PROTOCOL}"
        )
    expected_suffix = f"/{data['pid']}.sock"
    if not data["messagingSocketPath"].endswith(expected_suffix):
        failures.append(
            f"{record.path}: messagingSocketPath {data['messagingSocketPath']!r} does not end in {expected_suffix}"
        )
    computed = proc_start(data["pid"], proc_root)
    if computed != data["procStart"]:
        failures.append(
            f"{record.path}: procStart is {data['procStart']!r} but the process start computes to {computed!r}"
        )
    return failures


def unique_name(wanted: str, taken: set[str]) -> str:
    if wanted not in taken:
        return wanted
    n = 2
    while f"{wanted}-{n}" in taken:
        n += 1
    return f"{wanted}-{n}"


def resolve_session(session_id: str, directory: Path | None = None) -> Record | None:
    for record in live_records(directory):
        if record.data.get("sessionId") == session_id:
            return record
    return None


def by_name(name: str, directory: Path | None = None) -> Record | None:
    for record in live_records(directory):
        if record.data.get("name") == name:
            return record
    return None
