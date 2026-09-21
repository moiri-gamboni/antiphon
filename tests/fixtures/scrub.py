#!/usr/bin/env python3
"""Strip personal and machine identity from a captured protocol log.

Usage: scrub.py < raw.jsonl > scrubbed.jsonl

Every fixture in this directory went through this filter. It rewrites, line by
line: home directories to `~`, the machine id inside Claude Code's `pidDomain`
to zeros, process ids to small stable integers (the same pid maps to the same
integer across the whole file), multi-word Claude session titles in
`from-name` attributes to `claude-main`, and the daemon's `serverName` and
`installationId` to placeholders. Codex thread and turn ids are random UUIDs
and stay as they are.
"""
import re
import sys

HOME_DIR = re.compile(r"(/home|/Users)/[^/\"'\s]+")
PID_DOMAIN_MACHINE_ID = re.compile(r"(linux:)[0-9a-f]{32}(:)")
PID_FIELD = re.compile(r'("(?:pid|peer_pid|processId)":\s*"?)(\d+)')
SOCKET_PID = re.compile(r"(cc-socks/)(\d+)(\.sock)")
FROM_NAME_TITLE = re.compile(r'(from-name=\\?")([^"\\]*\s[^"\\]*)(\\?")')
SERVER_NAME = re.compile(r'("serverName":\s*")[^"]*(")')
INSTALLATION_ID = re.compile(r'("installationId":\s*")[^"]*(")')

ZERO_UUID = "00000000-0000-0000-0000-000000000000"


class Scrubber:
    """Line filter that keeps one pid numbering for the whole file."""

    def __init__(self):
        self.pids: dict[str, int] = {}

    def _pid(self, raw: str) -> str:
        if raw not in self.pids:
            self.pids[raw] = 1001 + len(self.pids)
        return str(self.pids[raw])

    def line(self, text: str) -> str:
        text = HOME_DIR.sub("~", text)
        text = PID_DOMAIN_MACHINE_ID.sub(r"\g<1>" + "0" * 32 + r"\2", text)
        text = PID_FIELD.sub(lambda m: m.group(1) + self._pid(m.group(2)), text)
        text = SOCKET_PID.sub(lambda m: m.group(1) + self._pid(m.group(2)) + m.group(3), text)
        text = FROM_NAME_TITLE.sub(r"\1claude-main\3", text)
        text = SERVER_NAME.sub(r"\1codex-host\2", text)
        text = INSTALLATION_ID.sub(r"\g<1>" + ZERO_UUID + r"\2", text)
        return text


def scrub(text: str) -> str:
    """Scrub one line on its own; use a Scrubber for a whole file."""
    return Scrubber().line(text)


def main() -> None:
    scrubber = Scrubber()
    for line in sys.stdin:
        sys.stdout.write(scrubber.line(line))


if __name__ == "__main__":
    main()
