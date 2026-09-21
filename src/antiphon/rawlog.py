"""Append-only log of every raw message crossing an external boundary.

One JSON line per message: `{"t": <epoch seconds>, "dir": "in"|"out",
"boundary": <the external interface: "codex", "codex-cli", "git", "tmux", or a
client-side marker such as "codex.deliver">, "data": <the raw message>}`. Whoever
debugs the next protocol failure reads this file; nothing else does.
"""
import json
import os
import time
from pathlib import Path


class RawLog:
    def __init__(self, path, max_bytes: int = 10_000_000, keep: int = 3):
        self.path = Path(path)
        self.max_bytes = max_bytes
        self.keep = keep

    def log(self, direction: str, boundary: str, data) -> None:
        # A message that cannot be serialised still gets recorded (as its repr)
        # instead of taking down the reader that logged it.
        line = json.dumps({"t": time.time(), "dir": direction, "boundary": boundary, "data": data}, default=repr)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self.path.exists() and self.path.stat().st_size >= self.max_bytes:
            self._rotate()
        with self.path.open("a") as f:
            f.write(line + "\n")

    def _rotate(self) -> None:
        oldest = self.path.with_name(f"{self.path.name}.{self.keep}")
        if oldest.exists():
            oldest.unlink()
        for n in range(self.keep - 1, 0, -1):
            older = self.path.with_name(f"{self.path.name}.{n}")
            if older.exists():
                os.replace(older, self.path.with_name(f"{self.path.name}.{n + 1}"))
        os.replace(self.path, self.path.with_name(f"{self.path.name}.1"))
