"""The `antiphon` command: a thin client of the bridge over the control socket.

Every verb is one function in `VERBS`, called with the parsed arguments and a
`Client`. The client starts the bridge when nothing answers on its socket and
prints the degraded banner whenever the bridge reports one.
"""

from __future__ import annotations

import argparse
import os
import shlex
import subprocess
import sys
import time
from pathlib import Path

from antiphon import ipc
from antiphon.claude import launch
from antiphon.codex import hooks
from antiphon.ipc import BridgeUnreachable, IpcError
from antiphon.rawlog import RawLog
from antiphon.state import ensure_home

START_TIMEOUT = 3.0
# `start --claude` waits for the session to register itself, which the bridge gives 10 s.
REGISTER_WAIT = 30.0
WAIT_RETRY_BACKOFF = 3.0
WAIT_DEFAULT_TIMEOUT = 600.0
LOG_TAIL_LINES = 20

# IpcError kind to exit code. Beyond this map: an unlisted kind and a BridgeUnreachable both
# exit 1 (no bridge, or an internal bridge error); verb_ping exits 2 on a degraded reply and
# _wait exits 6 on a failed or interrupted turn, both directly rather than through this map.
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
}

EXIT_CODES_HELP = """exit codes:
  0  ok
  1  no bridge answered and none could be started, or an op failed inside the bridge
  2  usage, precondition, unknown or stopped target, or a caller not allowed on that thread; `ping`: degraded
  3  delivery rejected (the daemon or the receiving session refused a send, approve or deny)
  4  timeout
  5  Codex daemon unreachable
  6  turn failed or interrupted (or the thread stopped or unloaded during a wait)"""


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


MISPLACED_OPTIONS = ("--wait", "--timeout")


def _misplaced_option(tokens: list[str]) -> str | None:
    """A `--wait`/`--timeout` that landed in the prompt because it followed `--`; the first
    or the last two tokens (`--timeout` carries a value, so its number is the last token),
    since a legitimate prompt may mention one in its middle."""
    ends = {tokens[0], *tokens[-2:]} if tokens else set()
    return next((token for token in MISPLACED_OPTIONS if token in ends), None)


def verb_start(args, client: Client) -> int:
    misplaced = _misplaced_option(args.prompt)
    if misplaced is not None:
        print(f'antiphon: {misplaced} goes before the --: antiphon start {misplaced} -- "<brief>"', file=sys.stderr)
        return 2
    if args.claude:
        return _start_claude(args, client)
    result = client.call("start", {
        "cwd": os.path.abspath(args.cwd), "name": args.name, "read_only": args.read_only, "model": args.model,
        "effort": args.effort, "report": not args.no_report, "worktree": args.worktree,
        "review_by_parent": args.review_by_parent,
    })
    print(f"started {result['name']} ({result['thread_id']}) in {result['cwd']}")
    if args.visible:
        attached = _attach(client, result["thread_id"], result["name"], result["cwd"])
        if attached != 0:
            return attached
    if not args.prompt:
        return 0
    return _send(client, result["thread_id"], " ".join(args.prompt), args.wait, args.timeout)


def _start_claude(args, client: Client) -> int:
    """`start --claude`: a Claude Code session of the caller's own, in the background."""
    unsupported = [name for flag, name in (
        (args.read_only, "--read-only"), (args.effort, "--effort"), (args.worktree, "--worktree"),
        (args.review_by_parent, "--review-by-parent"), (args.wait, "--wait"), (args.no_report, "--no-report"),
    ) if flag]
    if unsupported:
        print(f"antiphon: {', '.join(unsupported)} apply to Codex threads, not to --claude sessions", file=sys.stderr)
        return 2
    result = client.call("start_claude", {
        "cwd": os.path.abspath(args.cwd), "name": args.name, "model": args.model,
        "gate": args.gate, "prompt": " ".join(args.prompt) or None,
    }, timeout=REGISTER_WAIT)
    print(f"started the Claude Code session {result['name']} ({result['session_id']}) in {result['cwd']}")
    if result["hook"] is None:
        print("antiphon: hooks/claude-permission-forward.sh was not found, so this session decides its own "
              "permissions instead of asking you", file=sys.stderr)
    if args.visible:
        return _attach_session(client, result["name"], result["job_id"], result["cwd"])
    return 0


def verb_send(args, client: Client) -> int:
    misplaced = _misplaced_option(args.text)
    if misplaced is not None:
        print(f'antiphon: {misplaced} goes before the --: antiphon send {args.target} {misplaced} -- "<text>"', file=sys.stderr)
        return 2
    return _send(client, args.target, " ".join(args.text), args.wait, args.timeout)


def _send(client: Client, target: str, text: str, wait: bool, timeout: float | None) -> int:
    result = client.call("send", {"target": target, "text": text})
    if result["kind"] == "sent":
        # A labelled peer message: there is no turn of ours to wait for.
        print("sent")
        return 0
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
    """Wait for the peer's turn to end; the bridge going away once is survived."""
    timeout = timeout or WAIT_DEFAULT_TIMEOUT
    info = client.call("status", {"target": target})
    # A name can be re-taken while the wait runs; the id the peer keeps cannot.
    thread_id = info["thread_id"] if info["kind"] == "codex" else info["session_id"]
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
    # A thread stopped or unloaded out from under the wait never produced the outcome asked
    # for, so it is not a success — the same non-zero code as a failed or interrupted turn.
    return 6 if result["status"] in ("failed", "interrupted", "stopped", "unloaded") else 0


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
    me = next((row for row in rows if row["self"]), None)
    if me is not None:
        print(f"you are {me['name']}")
    table = [["NAME", "KIND", "STATUS", "CWD"]]
    for row in rows:
        name = str(row["name"])
        if row.get("parent"):
            name = "  " + (f"{name} ({row['role']})" if row.get("role") else name)
        table.append([name, row["kind"], str(row["status"]), str(row["cwd"])])
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
    print(f"renamed {result['former']} to {result['name']}")
    return 0


def verb_attach(args, client: Client) -> int:
    info = client.call("status", {"target": args.target})
    if info["kind"] == "claude":
        return _attach_session(client, info["name"], info["job_id"], info["cwd"])
    return _attach(client, info["thread_id"], info["name"], info["cwd"])


def _attach(client: Client, thread_id: str, name: str, cwd: str) -> int:
    return _open_terminal(client, name, cwd, f"codex resume {shlex.quote(thread_id)}")


def _attach_session(client: Client, name: str, job_id: str | None, cwd: str) -> int:
    if job_id is None:
        print(f"antiphon: {name} has no background job recorded; find it with `claude agents`", file=sys.stderr)
        return 2
    return _open_terminal(client, name, cwd, shlex.join(launch.attach_argv(job_id)))


def _open_terminal(client: Client, name: str, cwd: str, command: str) -> int:
    """Run `command` in a tmux window when there is a tmux to open one in; else print it."""
    if "TMUX" not in os.environ:
        print(command)
        return 0
    # The window starts in the thread's own directory. One inheriting the caller's makes
    # Codex ask which directory to resume in, and ask to trust a directory nobody meant.
    argv = ["tmux", "new-window", "-P", "-F", "#{session_name}:#{window_id}.#{pane_id}",
            "-c", cwd, "-n", name, command]
    rawlog = RawLog(client.home / "log" / "raw.jsonl")
    rawlog.log("out", "tmux", argv)
    done = subprocess.run(argv, capture_output=True, text=True, check=False)
    rawlog.log("in", "tmux", {"rc": done.returncode, "stdout": done.stdout, "stderr": done.stderr})
    if done.returncode != 0:
        print(f"antiphon: tmux exited {done.returncode}: {done.stderr}", file=sys.stderr)
        return 2
    print(done.stdout.strip())
    return 0


def verb_notify(args, client: Client) -> int:
    result = client.call("notify", {"target": args.target})
    print(f"watching {result['name']}: its idle notice will arrive here as a message")
    return 0


def verb_approve(args, client: Client) -> int:
    result = client.call("approve", {"token": args.token})
    print(f"approved {result['token']} on {result['name']}: {result['command']}")
    return 0


def verb_deny(args, client: Client) -> int:
    result = client.call("deny", {"token": args.token, "why": " ".join(args.why)})
    print(f"denied {result['token']} on {result['name']}: {result['command']}")
    return 0


# --- Claude Code hooks as Codex hooks ------------------------------------------------


def verb_hook_run(args, home: Path) -> int:
    """The shim Codex runs: Codex hook input on stdin, Codex hook output on stdout. The
    bridge is contacted only for an `ask`, so allow and deny work with no bridge at all."""

    def ask(codex_in: dict, reason: str, budget: float) -> tuple[str, str | None]:
        client = connect(home)
        result = client.call("hook_ask", {
            "asker": codex_in.get("session_id") or os.environ.get("CODEX_THREAD_ID"), "turn_id": codex_in.get("turn_id"),
            "tool_name": codex_in["tool_name"], "command": hooks.summarize_tool_input(codex_in["tool_input"]),
            "cwd": codex_in["cwd"], "reason": reason, "hook": os.path.basename(args.script), "timeout": budget,
        }, timeout=budget + 2)
        return result["decision"], result["why"]

    try:
        out = hooks.run_shim(args.script, sys.stdin.read(), home=home, ask=ask, event=args.event, timeout=args.timeout)
    except hooks.HookInputError as e:
        # Without a readable event no JSON answer parses; exit 2 with stderr is the one
        # denial every Codex hook event understands.
        print(f"antiphon: {e}", file=sys.stderr)
        return 2
    if out:
        print(out)
    return 0


class ListedHooks:
    """The daemon's view of the hooks it loads from `path`: `hooks/list` once, keyed by command."""

    def __init__(self, client: Client, path: Path):
        entries = client.call("hook_list", {"cwd": os.getcwd()})
        self.warnings = [w for entry in entries for w in entry.get("warnings", [])]
        self.by_command = {h["command"]: h for entry in entries for h in entry["hooks"] if h["sourcePath"] == str(path)}

    def trust_status(self, command: str) -> str:
        listed = self.by_command.get(command)
        return listed["trustStatus"] if listed else "not listed by Codex"


def verb_hook_install(args, client: Client) -> int:
    script = os.path.abspath(args.script)
    path = hooks.hooks_file()
    data = hooks.load_hooks(path)
    hooks.remove_script(data, script)
    handler = hooks.add_script(data, script, args.event, args.matcher, args.timeout)
    hooks.save_hooks(path, data)
    catalog = ListedHooks(client, path)
    listed = catalog.by_command.get(handler["command"])
    if listed is None:
        for warning in catalog.warnings:
            print(f"antiphon: codex: {warning}", file=sys.stderr)
        print(f"antiphon: wrote {path}, but Codex does not list the hook; check `[features]` in your Codex config, then rerun", file=sys.stderr)
        return 2
    if listed["trustStatus"] != "trusted":
        client.call("hook_trust", {"key": listed["key"], "hash": listed["currentHash"]})
    status = ListedHooks(client, path).trust_status(handler["command"])
    print(f"installed {script} as a Codex {args.event} hook in {path} (key {listed['key']}): {status}")
    if args.timeout < hooks.IPC_MARGIN + 1:
        print(f"antiphon: a {args.timeout} s timeout leaves no time to forward an ask; an ask from this hook is denied", file=sys.stderr)
    return 0 if status == "trusted" else 2


def verb_hook_uninstall(args, client: Client) -> int:
    script = os.path.abspath(args.script)
    path = hooks.hooks_file()
    data = hooks.load_hooks(path)
    if not hooks.remove_script(data, script):
        print(f"antiphon: {script} is not installed in {path}", file=sys.stderr)
        return 2
    hooks.save_hooks(path, data)
    print(f"removed {script} from {path}; its trusted hash stays in the Codex config and matches nothing else")
    return 0


def verb_hook_list(args, client: Client) -> int:
    path = hooks.hooks_file()
    ours = hooks.installed(hooks.load_hooks(path))
    if not ours:
        print(f"no Claude hooks installed in {path}")
        return 0
    listed = ListedHooks(client, path)
    table = [["EVENT", "MATCHER", "TIMEOUT", "TRUST", "SCRIPT"]]
    for entry in ours:
        table.append([entry["event"], entry["matcher"] or "*", str(entry["timeout"]), listed.trust_status(entry["command"]), entry["script"]])
    widths = [max(len(row[i]) for row in table) for i in range(4)]
    for row in table:
        print(" ".join(cell.ljust(width) for cell, width in zip(row, widths)) + " " + row[4])
    return 0


def verb_hook(args, client: Client) -> int:
    return {"install": verb_hook_install, "uninstall": verb_hook_uninstall, "list": verb_hook_list}[args.hook_verb](args, client)


VERBS = {
    "hook": verb_hook,
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
    "attach": verb_attach,
    "notify": verb_notify,
    "approve": verb_approve,
    "deny": verb_deny,
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="antiphon", description="Codex threads as Claude Code peers, and back.",
        epilog=EXIT_CODES_HELP, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = parser.add_subparsers(dest="verb", required=True)
    sub.add_parser("ping", help="is the bridge up, and what does it speak to")
    sub.add_parser("bridge", help="run the bridge in the foreground")

    start = sub.add_parser("start", help="start a Codex thread (idle until sent to, unless a prompt follows --)")
    start.add_argument("-C", dest="cwd", default=os.getcwd(), help="working directory (default: the current one)")
    start.add_argument("-n", "--name")
    start.add_argument("--claude", action="store_true",
                       help="start a Claude Code session instead of a Codex thread; it runs in the background, "
                            "takes send/wait/stop/attach like a thread, and has no interrupt")
    start.add_argument("--gate", help="with --claude: the tool names whose calls the session asks you about "
                                      "(default: every tool it uses, bar the ones it answers you with)")
    start.add_argument("--read-only", action="store_true")
    start.add_argument("-m", "--model")
    start.add_argument("--effort")
    start.add_argument("--no-report", action="store_true", help="do not deliver the final answer to the spawner at turn end")
    start.add_argument("--worktree", action="store_true", help="give the thread its own git worktree beside the repository")
    start.add_argument("--review-by-parent", action="store_true", help="escalations block until the spawner answers")
    start.add_argument("--visible", action="store_true", help="then attach a terminal to it (see attach)")
    start.add_argument("--wait", action="store_true")
    start.add_argument("--timeout", type=float)
    start.add_argument("prompt", nargs="*", help="the first turn's prompt; put options (--wait, --timeout) before the --")

    send = sub.add_parser("send", help="steer a busy thread or start a turn on an idle one")
    send.add_argument("target")
    send.add_argument("--wait", action="store_true")
    send.add_argument("--timeout", type=float)
    send.add_argument("text", nargs="+", help="the message; put options (--wait, --timeout) before the --")

    wait = sub.add_parser("wait", help="wait for the thread's turn, or a Claude Code session's next idle, and print the outcome")
    wait.add_argument("target")
    wait.add_argument("--timeout", type=float)

    sub.add_parser("interrupt", help="end the thread's current turn (Claude Code sessions have no such surface)").add_argument("target")
    sub.add_parser("status", help="a thread's state, or the bridge's").add_argument("target", nargs="?")
    sub.add_parser("ls", help="every peer on this machine")
    sub.add_parser("stop", help="retire the thread as a peer, or end a Claude Code session started here (transcripts stay)").add_argument("target")
    sub.add_parser("resume", help="host a stopped or never-hosted thread again").add_argument("target")
    name = sub.add_parser("name", help="rename a thread (from inside a Codex thread, the thread itself when no target is given)")
    name.add_argument("target", nargs="?")
    name.add_argument("new")
    attach = sub.add_parser("attach", help="open the thread or session in a new tmux window, or print the command that opens it")
    attach.add_argument("target")
    notify = sub.add_parser("notify", help="from inside a Codex thread: be messaged when a peer's turn ends")
    notify.add_argument("target")
    sub.add_parser("approve", help="approve an escalation by its token (see the message or `ls`)").add_argument("token")
    deny = sub.add_parser("deny", help="deny an escalation by its token, telling the thread why")
    deny.add_argument("token")
    deny.add_argument("why", nargs="+")

    hook = sub.add_parser("hook", help="run Claude Code hook scripts as Codex hooks")
    hook_sub = hook.add_subparsers(dest="hook_verb", required=True)
    events = list(hooks.CLAUDE_EVENT)
    run = hook_sub.add_parser("run", help="the shim Codex runs: Codex hook input on stdin, Codex hook output on stdout")
    run.add_argument("script", help="the Claude Code hook script")
    run.add_argument("--event", choices=events, help="override the event named in the input")
    run.add_argument("--timeout", type=float, default=hooks.DEFAULT_TIMEOUT, help="the hook's timeout in hooks.json (the budget for an ask)")
    install = hook_sub.add_parser("install", help="register a Claude Code hook script in ~/.codex/hooks.json and trust it")
    install.add_argument("script")
    install.add_argument("--event", choices=events, default="PreToolUse")
    install.add_argument("--matcher", help="tool name(s) the hook applies to, e.g. Bash; default: every tool")
    install.add_argument("--timeout", type=int, default=hooks.DEFAULT_TIMEOUT, help="seconds Codex gives the hook (default: Codex's own 600)")
    hook_sub.add_parser("uninstall", help="remove the script's entry from hooks.json").add_argument("script")
    hook_sub.add_parser("list", help="the installed scripts and whether Codex trusts them")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.verb == "bridge":
        from antiphon import bridge

        bridge.main()
        return 0
    home = ensure_home()
    if args.verb == "hook" and args.hook_verb == "run":
        return verb_hook_run(args, home)
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
