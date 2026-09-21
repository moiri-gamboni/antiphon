"""The `antiphon` command: a thin client of the bridge over the control socket.

Every verb is one function in `VERBS`, called with the parsed arguments and a
`Client`. The client starts the bridge when nothing answers on its socket and
prints the degraded banner whenever the bridge reports one.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

from antiphon import ipc
from antiphon.ipc import BridgeUnreachable, IpcError
from antiphon.state import ensure_home

START_TIMEOUT = 3.0
WAIT_RETRY_BACKOFF = 3.0
WAIT_DEFAULT_TIMEOUT = 600.0
LOG_TAIL_LINES = 20

# Exit codes: 1 bridge unreachable, 2 usage or precondition, 3 delivery rejected,
# 4 timeout, 5 daemon unreachable, 6 turn failed or interrupted.
EXIT_CODES = {
    "internal": 1,
    "usage": 2,
    "precondition": 2,
    "ambiguous": 2,
    "unknown_target": 2,
    "stopped": 2,
    "not_a_thread": 2,
    "unknown_op": 2,
    "forbidden": 2,
    "version": 2,
    "bad_request": 2,
    "delivery_rejected": 3,
    "timeout": 4,
    "daemon_unreachable": 5,
    "daemon": 5,
}


def caller_env() -> dict:
    """What the CLI knows about its caller that the bridge cannot read from the socket."""
    return {"claimed_thread": os.environ.get("CODEX_THREAD_ID")}


# The bridge outlives the CLI that started it; its Popen handle is kept so Python
# does not report the still-running child when the handle is collected.
_spawned: list[subprocess.Popen] = []


def spawn_bridge(home: Path) -> None:
    with open(home / "log" / "bridge.out", "ab") as log:
        _spawned.append(subprocess.Popen(
            [sys.executable, "-m", "antiphon", "bridge"],
            stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True,
        ))


class Client:
    def __init__(self, home: Path):
        self.home = home
        self.socket = str(home / "bridge.sock")
        self._banner_shown = False

    def call(self, op: str, args: dict | None = None, timeout: float = 30):
        try:
            reply = ipc.call_raw(self.socket, op, {**(args or {}), **caller_env()}, timeout)
        except IpcError as e:
            self.banner(e.degraded)
            raise
        self.banner(reply.get("degraded"))
        return reply["result"]

    def banner(self, degraded) -> None:
        if degraded and not self._banner_shown:
            self._banner_shown = True
            for reason in degraded:
                print(f"antiphon: DEGRADED — {reason}", file=sys.stderr)


def connect(home: Path) -> Client:
    """A client with a bridge behind it, started now if none answers."""
    client = Client(home)
    try:
        client.call("ping")
        return client
    except BridgeUnreachable:
        pass
    spawn_bridge(home)
    deadline = time.monotonic() + START_TIMEOUT
    while time.monotonic() < deadline:
        try:
            client.call("ping")
            return client
        except BridgeUnreachable:
            time.sleep(0.1)
    log_path = home / "log" / "bridge.out"
    tail = log_path.read_text(errors="replace").splitlines()[-LOG_TAIL_LINES:] if log_path.exists() else []
    raise BridgeUnreachable(
        f"the bridge did not answer within {START_TIMEOUT:g} s; last lines of {log_path}:\n"
        + "".join(f"  {line}\n" for line in tail)
    )


# --- verbs -----------------------------------------------------------------------


def verb_ping(args, client: Client) -> int:
    result = client.call("ping")
    if not result["daemon"]:
        print("bridge ok · codex unreachable")
        return 5
    if result["degraded"]:
        print("bridge DEGRADED")
        for reason in result["degraded"]:
            print(f"  {reason}")
        return 2
    print(f"bridge ok · codex {result['codex']} · claude {result['claude'] or 'none'} · peers {result['peers']}")
    return 0


def verb_start(args, client: Client) -> int:
    result = client.call("start", {
        "cwd": os.path.abspath(args.cwd), "name": args.name, "read_only": args.read_only, "model": args.model,
        "effort": args.effort, "report": not args.no_report, "worktree": args.worktree,
        "review_by_parent": args.review_by_parent,
    })
    print(f"started {result['name']} ({result['thread_id']}) in {result['cwd']}")
    if not args.prompt:
        return 0
    return _send(client, result["thread_id"], " ".join(args.prompt), args.wait, args.timeout)


def verb_send(args, client: Client) -> int:
    return _send(client, args.target, " ".join(args.text), args.wait, args.timeout)


def _send(client: Client, target: str, text: str, wait: bool, timeout: float | None) -> int:
    result = client.call("send", {"target": target, "text": text})
    if result["kind"] == "steered":
        print(f"steered {result['name']} (turn {result['turn_id']})")
    else:
        print(f"started turn {result['turn_id']} on {result['name']}")
    if not wait:
        return 0
    return _wait(client, result["thread_id"], timeout)


def verb_wait(args, client: Client) -> int:
    return _wait(client, args.target, args.timeout)


def _wait(client: Client, target: str, timeout: float | None) -> int:
    """Wait for the thread's turn to end; the bridge going away once is survived."""
    timeout = timeout or WAIT_DEFAULT_TIMEOUT
    thread_id = client.call("status", {"target": target})["thread_id"]
    for attempt in (1, 2):
        try:
            result = client.call("wait", {"target": thread_id, "timeout": timeout}, timeout=timeout + 5)
            break
        except BridgeUnreachable as e:
            if attempt == 2:
                print(f"antiphon: the bridge went away twice while waiting ({e}); rerun: antiphon wait {thread_id}", file=sys.stderr)
                return 1
            time.sleep(WAIT_RETRY_BACKOFF)
            client = connect(client.home)
    print(result["final"] if result["final"] is not None else "(no turn recorded)")
    return 6 if result["status"] in ("failed", "interrupted") else 0


def verb_interrupt(args, client: Client) -> int:
    result = client.call("interrupt", {"target": args.target})
    if "noop" in result:
        print(f"nothing to interrupt: {args.target} is idle")
    else:
        print(f"interrupted turn {result['turn_id']}")
    return 0


def verb_status(args, client: Client) -> int:
    result = client.call("status", {"target": args.target})
    for key, value in result.items():
        print(f"{key}: {value}")
    return 0


def verb_ls(args, client: Client) -> int:
    rows = client.call("ls")
    table = [["NAME", "KIND", "STATUS", "CWD"]]
    for row in rows:
        indent = "  " if row.get("parent") else ""
        table.append([indent + str(row["name"]), row["kind"], str(row["status"]), str(row["cwd"])])
    widths = [max(len(line[i]) for line in table) for i in range(3)]
    marks = [" "] + ["*" if row["self"] else " " for row in rows]
    for mark, line in zip(marks, table):
        print(mark + " " + " ".join(cell.ljust(width) for cell, width in zip(line, widths)) + " " + line[3])
    return 0


def verb_stop(args, client: Client) -> int:
    result = client.call("stop", {"target": args.target})
    print(f"stopped {result['name']}")
    if "worktree_kept" in result:
        print(f"worktree kept at {result['worktree_kept']}: {result['worktree_reason']}")
    return 0


def verb_resume(args, client: Client) -> int:
    result = client.call("resume", {"target": args.target})
    print(f"resumed {result['name']} ({result['thread_id']})")
    return 0


def verb_name(args, client: Client) -> int:
    result = client.call("name", {"target": args.target, "new": args.new})
    print(f"renamed {args.target} to {result['name']}")
    return 0


VERBS = {
    "ping": verb_ping,
    "start": verb_start,
    "send": verb_send,
    "wait": verb_wait,
    "interrupt": verb_interrupt,
    "status": verb_status,
    "ls": verb_ls,
    "stop": verb_stop,
    "resume": verb_resume,
    "name": verb_name,
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="antiphon", description="Codex threads as Claude Code peers, and back.")
    sub = parser.add_subparsers(dest="verb", required=True)
    sub.add_parser("ping", help="is the bridge up, and what does it speak to")
    sub.add_parser("bridge", help="run the bridge in the foreground")

    start = sub.add_parser("start", help="start a Codex thread (idle until sent to, unless a prompt follows --)")
    start.add_argument("-C", dest="cwd", default=os.getcwd(), help="working directory (default: the current one)")
    start.add_argument("-n", "--name")
    start.add_argument("--read-only", action="store_true")
    start.add_argument("-m", "--model")
    start.add_argument("--effort")
    start.add_argument("--no-report", action="store_true", help="do not deliver the final answer to the spawner at turn end")
    start.add_argument("--worktree", action="store_true", help="give the thread its own git worktree beside the repository")
    start.add_argument("--review-by-parent", action="store_true", help="escalations block until the spawner answers")
    start.add_argument("--wait", action="store_true")
    start.add_argument("--timeout", type=float)
    start.add_argument("prompt", nargs="*")

    send = sub.add_parser("send", help="steer a busy thread or start a turn on an idle one")
    send.add_argument("target")
    send.add_argument("--wait", action="store_true")
    send.add_argument("--timeout", type=float)
    send.add_argument("text", nargs="+")

    wait = sub.add_parser("wait", help="wait for the thread's turn to end and print the outcome")
    wait.add_argument("target")
    wait.add_argument("--timeout", type=float)

    sub.add_parser("interrupt", help="end the thread's current turn").add_argument("target")
    sub.add_parser("status", help="a thread's state, or the bridge's").add_argument("target", nargs="?")
    sub.add_parser("ls", help="every peer on this machine")
    sub.add_parser("stop", help="retire the thread as a peer (its transcript stays)").add_argument("target")
    sub.add_parser("resume", help="host a stopped or never-hosted thread again").add_argument("target")
    name = sub.add_parser("name", help="rename a thread")
    name.add_argument("target")
    name.add_argument("new")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.verb == "bridge":
        from antiphon import bridge

        bridge.main()
        return 0
    home = ensure_home()
    try:
        client = connect(home)
        return VERBS[args.verb](args, client)
    except IpcError as e:
        print(f"antiphon: {e.message}", file=sys.stderr)
        return EXIT_CODES.get(e.kind, 1)
    except BridgeUnreachable as e:
        print(f"antiphon: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
