# antiphon

antiphon makes Claude Code sessions and OpenAI Codex CLI sessions peers of each other on one machine. A bridge process speaks Codex's app-server protocol on one side and Claude Code's cross-session peer protocol on the other; a small `antiphon` CLI talks to the bridge. From a Claude Code session, Codex threads appear in `ListAgents`, take `SendMessage`, honour `notify_when_idle`, and send their sandbox escalations back for a decision. From a Codex thread, `antiphon` lists the sessions on the machine, messages any of them, subscribes to their idle notices, and starts Claude Code sessions of its own whose permission decisions come back to it. A human can attach a terminal to any thread or session, and a Codex TUI a human started is a peer too.

## Requirements

- Codex CLI 0.155 or later (the app-server daemon and `turn/steer`)
- Claude Code 2.1.278 or later (peer protocol 1 with `notify_when_idle`)
- Python 3.12 or later; no runtime dependencies beyond the standard library
- Linux or macOS. On macOS, peer registration is checked against a live session's record at runtime but has not been verified against a recorded capture.
- `claude` and `codex` on the bridge's PATH, and `python3` on the PATH of the Claude Code sessions it starts (their permission hook runs there)
- Optional: `tmux` for `attach`, `git` for `start --worktree`

Claude Code's side is documented at https://code.claude.com/docs/en/cross-session-messaging. Its registry record and socket frames are not, so the bridge checks their shape and stops registering peers when it drifts (see [Troubleshooting](#troubleshooting)).

For Codex threads that use Codex's own sub-agents, set `[features] multi_agent_v2 = true` in `~/.codex/config.toml`.

## Install

```
uv tool install .          # or: pipx install .
./install.sh install       # skills + background service; --no-service skips the service
```

`install.sh install` symlinks `skills/claude` to `$CLAUDE_CONFIG_DIR/skills/antiphon` (default `~/.claude/skills/antiphon`) and `skills/codex` to `~/.agents/skills/antiphon`, and enables the bridge as a systemd user service (Linux) or a launchd agent (macOS), copying `CLAUDE_CONFIG_DIR`, `CODEX_HOME` and `ANTIPHON_HOME` into it when they are set. Without the service, the CLI starts a bridge whenever none answers. To install the service by hand, follow the header of `contrib/antiphon.service` or `contrib/com.antiphon.bridge.plist`; its PATH must reach `codex`, which the bridge runs to start the Codex daemon. `--human-approvals` adds the [approval prompt hook](#a-permission-prompt-for-approvals). `./install.sh uninstall` removes the skills, the service and the hook, and stops the bridge.

To update, run `uv tool install --reinstall .` and restart the bridge (`systemctl --user restart antiphon`, or kill the `antiphon bridge` process and let the next command start one): a running bridge keeps the old code. Threads survive in the Codex daemon.

State, the control socket and logs live under `~/.antiphon/` (`$ANTIPHON_HOME` overrides it). The bridge reads Claude Code's session registry from `$CLAUDE_CONFIG_DIR/sessions` (default `~/.claude/sessions`) and reaches the Codex daemon through `$CODEX_HOME/app-server-control/app-server-control.sock` (default `~/.codex`).

## Delegating from Claude Code

Ask Claude to hand something to Codex ("ask codex to review this diff"); the installed skill tells it how. The pattern underneath is:

```
antiphon start -C ~/src/app -n reviewer --read-only
```

followed, in the Claude Code session, by `SendMessage(to: "reviewer", message: <brief>, notify_when_idle: true)`, which starts the thread's first turn and subscribes to its completion. Later `SendMessage` calls keep the thread's context: a busy thread is steered, an idle one starts a new turn. `--worktree` gives a writing thread its own git worktree on branch `codex/<name>`.

### How completion is signalled

When a turn ends, the thread's full final answer is delivered to the session that started it, as an ordinary cross-session message from the thread's name (`start --no-report` turns this off); then every session that subscribed with `notify_when_idle` gets the idle notice, with the first 200 characters of the answer as its detail. A failed or interrupted turn reports `failed: <error>` or `interrupted: ...` the same way. `start --wait`, `send --wait` and `wait` also print the answer in the command's output, and survive one bridge restart mid-wait.

Each hosted thread is represented by a small child process of the bridge, because Claude Code lists a peer only while the pid in its record is alive. The children exit with the bridge, telling their subscribers `exited`, and come back when it restarts.

A message from a Claude Code session arrives in the thread prefixed `[from <name> via antiphon]`, and the thread replies with `antiphon send <name> -- ...`.

### Sandbox

A spawned thread runs in Codex's `workspace-write` sandbox: it writes under its working directory and `/tmp`, reads everything your user can read, and has outbound network access, which is what lets `antiphon` inside the thread reach the bridge. `start --read-only` gives it Codex's `read-only` sandbox instead.

## Approvals

Spawned threads use Codex's automatic reviewer: escalations (a write outside the workspace, network use, a command the sandbox blocks) are decided inside the thread, and the turn never blocks. Only the reviewer's denials are forwarded, as a message to the spawning session with a six-character token:

```
Codex's automatic reviewer denied an action in "<name>" (token a1b2c3): <reason> (risk <level>)
  command: <command>
  cwd: <cwd>
The thread has continued without it. Reply with: antiphon approve a1b2c3   or   antiphon deny a1b2c3 -- <why>
```

`antiphon approve a1b2c3` records the approval with Codex and tells the thread to retry the command; `antiphon deny a1b2c3 -- <why>` tells the thread it stays denied. The reviewer reviews the retry again, and its own policy decides what the approval is worth: it lets a `high`-risk denial through, never a `critical` one (the captures are described in `tests/fixtures/README.md`). A critical denial therefore gets no token, only a notice for the user, who can run the action by hand outside Codex. The notice goes to the first Claude Code session up the chain of spawners; when the chain starts at a shell, only the bridge log and the thread's own answer record it.

To decide a thread's escalations yourself, start it `--review-by-parent`. There is no automatic reviewer then: each escalation blocks the turn until you answer, `approve` runs the command, and `deny` refuses it and leaves the turn running so the thread can be told why. Waiting never cancels a request, not even across a dropped daemon connection; the spawner is reminded once after ten minutes.

While an escalation is unanswered, `antiphon ls` shows `denied <token> <age>`, `approval <token> <age>` or `hook <token> <age>` in place of the thread's status, and `antiphon status <name>` lists it under `pending`. The session or thread that spawned the thread may answer it, and so may any Claude Code session and a human at a terminal. A Codex TUI that a human started keeps its own approvals: the TUI answers them, the bridge never does.

### A permission prompt for approvals

An approval that arrives as a message is decided by the receiving Claude, and a message from another session is not the user's consent. `hooks/approve-ask.sh` is a Claude Code PreToolUse hook on Bash that turns each `antiphon approve <token>` and `antiphon deny <token>` into a real permission prompt showing the command, the directory and Codex's reason; every other command passes through untouched. `./install.sh install --human-approvals` adds it to `$CLAUDE_CONFIG_DIR/settings.json`; by hand:

```json
{"hooks": {"PreToolUse": [{"matcher": "Bash", "hooks": [
  {"type": "command", "command": "<repo>/hooks/approve-ask.sh", "timeout": 5}]}]}}
```

## From a Codex thread

Inside a thread the bridge hosts, `antiphon ls` starts with `you are <name>`; `antiphon send <name> -- <text>` messages a Claude Code session or another thread; `antiphon notify <name>` brings `Peer <name> is idle: <summary>` back as a turn; `antiphon name <new>` renames the thread; `antiphon start --claude` starts a Claude Code session of its own (next section). Codex's built-in multi-agent tools reach only the thread's own sub-agents, and the Codex skill introduces each verb by analogy with one of them. A thread may drive, stop and answer for the threads and sessions it started; everything else accepts only its messages.

A Codex session a human started in a terminal lacks the network access spawned threads get, so `antiphon` inside it fails with `PermissionError: [Errno 1] Operation not permitted` until Codex runs with `-c sandbox_workspace_write.network_access=true` (or `[sandbox_workspace_write] network_access = true` in `~/.codex/config.toml`).

## Delegating from Codex to Claude Code

A Codex thread starts a Claude Code session with `antiphon start --claude` and drives it with the same verbs it uses on a Codex thread:

```
antiphon start --claude -C ~/src/app -n reviewer -- "review the diff on this branch and report back"
antiphon send reviewer -- "look at the migration too"
antiphon wait reviewer
antiphon stop reviewer
```

The session runs in the background, as `claude --bg` starts one: it outlives the command, takes follow-up messages, and appears in `antiphon ls`, in `claude agents` and in every other session's `ListAgents`. (A `claude -p` session loses its peer record when its single turn ends, so nothing could follow up with it.)

`send` reaches the session as a cross-session message labelled `[from <thread name> via antiphon]`; the session replies with `SendMessage(to: <thread name>)`, and the reply arrives in the thread as a turn. `wait` returns at the session's *next* idle notice, so use it after sending something; a session sitting still waits the whole timeout, and a timed-out wait's notice still arrives later as a message. Claude Code has no interrupt, so send a correction (a busy session takes it as its next message) or `antiphon stop` it. `antiphon attach reviewer`, or `--visible` on `start --claude`, opens the session in a tmux window through `claude attach`; the session keeps running when the window closes.

The thread that started a session may drive, stop and answer for it; any other caller can only message it. A Claude Code session cannot use `start --claude`: it has its own Agent tool.

### Permission decisions come back to the thread

`start --claude` installs `hooks/claude-permission-forward.sh` into that session alone (through `claude --settings`) as a PreToolUse hook. Before each tool call it matches, the call is held and put to the thread that started the session, as a `Permission needed: ...` message with a token; `antiphon approve <token>` lets the call run, and `antiphon deny <token> -- <why>` blocks it with your reason as the hook's message. An unanswered call is denied just before Claude Code's 600 s hook timeout.

Claude Code has no permission-request event, so the hook runs before *every* call of the tools it matches, not only the ones that would have prompted. `--gate Bash` narrows it to the shell tool, and `--gate 'Bash|Write|Edit'` takes the matcher syntax of any Claude Code hook; with no `--gate` every tool call is forwarded, which is thorough and slow. `SendMessage`, `ListAgents` and `ToolSearch` are never held, whatever the gate: the session needs all three to answer you.

Limits:

- A session started from a shell rather than a Codex thread has no spawner to ask: its held calls show in `antiphon ls`, and must be answered from a shell before the timeout.
- If nothing can be asked (no bridge, a session antiphon did not start, input the hook cannot read), the call is denied with the reason.
- The hook applies to that session only and leaves `$CLAUDE_CONFIG_DIR/settings.json` alone. A session resumed later by hand (`claude --resume`) does not carry it.
- A session antiphon started reports to a Codex thread or to nobody; it cannot report to another Claude Code session.

## Running your Claude Code hooks in Codex

A PreToolUse guard you already run under Claude Code (deny `rm -rf`, deny ad-hoc writes to some API, ask before touching a directory) runs unchanged on the Codex threads antiphon spawns and on any other Codex session on the machine. `antiphon hook run` translates Codex's hook input and output to Claude Code's, field by field. To install one guard:

```
antiphon hook install ~/.claude/hooks/no-force-push.sh --matcher Bash
```

This adds an entry to `~/.codex/hooks.json` (created if absent; other entries are left alone) that runs `antiphon hook run <script>`, and marks the hook trusted in `~/.codex/config.toml`, as the Codex TUI's `/hooks` command does. It exits 2, printing Codex's warnings, if Codex does not list the hook afterwards. Threads started after the install run the hook; a running thread keeps the hooks it started with. `antiphon hook list` shows each installed script and its trust status; `antiphon hook uninstall <script>` removes the entry (the trusted hash stays in `config.toml`, where it matches nothing).

Pick the event that matches the Claude Code event the script was written for: `--event PreToolUse` (the default) runs before every tool call; `--event PermissionRequest` runs only when a call needs approval, before Codex's automatic reviewer, and its decision settles the escalation; `--event PostToolUse` runs after the call, where `decision: block` replaces the tool result with the reason. `--matcher` takes a tool name, a `|`-separated list or a regex, as in Claude Code; Codex names its shell tool `Bash` too.

When the script answers `ask`, the tool call is held and the thread's spawner gets a `Permission needed: the Claude hook <script> in "<name>" ...` message with a token, answered with `antiphon approve` or `antiphon deny` as above. A thread a human started has no spawner, so the `ask` shows in `antiphon ls` for a shell to answer. Codex kills a hook at its timeout (600 s unless `hook install --timeout` sets another), so an unanswered `ask` ends as a deny naming the token; a timeout under about six seconds leaves no time to forward an `ask` at all, and such an `ask` is denied at once. If nothing can be asked (no bridge, a thread antiphon does not host), the call is denied with the script's reason.

What the script sees and what reaches Codex:

- Input: the Claude Code fields Codex can supply, `session_id` (the Codex thread id), `transcript_path` (may be `null`), `cwd`, `permission_mode`, `tool_name`, `tool_input`, `tool_use_id` (absent on a `PermissionRequest`), `tool_response`, `agent_id`, `agent_type`, plus Codex's own `model` and `turn_id`. `prompt_id`, `scratchpad_dir` and `effort` have no Codex source and are absent.
- On a Codex `PreToolUse`, a plain `allow` prints nothing (Codex accepts `allow` only together with an input rewrite). On a `PermissionRequest`, an input rewrite (`updatedInput`) cannot be applied, so the call is denied instead. `additionalContext` reaches Codex on `PreToolUse` and `PostToolUse`, not on `PermissionRequest`.
- Output that looks like JSON but is not denies with the raw text, and a deny with no reason gets one, since Codex would treat it as invalid and let the call proceed.

## Attaching a terminal

`antiphon attach <name>` inside tmux opens a new window running `codex resume <thread id>` and prints `session:@window.%pane`; outside tmux it prints the command for you to run anywhere. `start --visible` starts a thread and attaches in one step. The thread outlives the window.

The reverse holds too: a plain `codex` TUI you start is adopted by the bridge within 15 seconds, named `codex-<directory>` unless it has a name, and listed as a peer. Messages and `interrupt` drive it like any thread, `notify_when_idle` works on it, and closing the TUI retires it. Codex sub-agents (threads with a parent) are listed under their parent, never registered as peers, and driven only by Codex's own tools.

## Command reference

Options go before the `--` separator; everything after it is the prompt or message text (`antiphon send helper --wait -- "go"`). A target is a thread's or session's name, or a unique prefix of a thread id. Default names are `codex-<directory name>` for a thread and `claude-<directory name>` for a session, with `-2`, `-3` appended when the name is taken. `antiphon <verb> --help` lists each verb's options and defaults, and `antiphon --help` the exit codes; every error message names its cause.

| Verb | What it does |
|---|---|
| `ping` | Reports whether the bridge is up and what it talks to: `bridge ok · codex <version> · claude <version> · peers <n>` |
| `start [-- PROMPT]` | Starts a Codex thread in the current directory (or `-C DIR`), idle until sent to unless a prompt follows `--`. `--read-only`, `--worktree`, `--review-by-parent`, `--no-report` and `--wait` are described above; `--instructions FILE` adds the file's text, minus any leading YAML frontmatter, to the thread's developer instructions, so a Claude Code agent definition can brief it as is |
| `start --claude [-- PROMPT]` | Starts a background Claude Code session instead, and waits up to 10 s for it to register as a peer; `--gate` picks the tools whose calls it asks about |
| `send TARGET -- TEXT` | Steers the target's running turn, or starts a turn if it is idle. To a Claude Code session, or from a Codex thread to a thread it did not start, it sends a labelled cross-session message instead, which `--wait` cannot wait on |
| `wait TARGET` | Waits for the thread's turn to end (default 600 s) and prints its final answer, or `<status>: <error>`. From a Codex thread, also waits for the next idle notice of a Claude Code session antiphon started |
| `interrupt TARGET` | Ends the thread's current turn; prints `nothing to interrupt` when idle. Claude Code sessions have no interrupt |
| `status [TARGET]` | A thread's state (status, active turn, last outcome, pending escalations, sub-agents, worktree), a started session's (directory, spawner, job id, pending escalations), or with no target the bridge's |
| `ls` | Every peer on the machine: Claude Code sessions, Codex threads, and Codex sub-agents indented under their parent. The first line names the caller when it is a peer; `*` marks its row |
| `stop TARGET` | Retires a thread as a peer, or ends a started Claude Code session (`claude stop`). Transcripts and `--worktree` branches stay; a clean worktree checkout is removed, a dirty one kept |
| `resume TARGET` | Hosts a stopped thread again, or any thread id the daemon knows, with its context and former name |
| `name [TARGET] NEW` | Renames a thread; inside a Codex thread with no target, that thread |
| `attach TARGET` | Opens the thread or started session in a new tmux window, or prints the command that does |
| `notify TARGET` | From inside a Codex thread: one message back when the target's turn next ends |
| `approve TOKEN`, `deny TOKEN -- WHY` | Answer an escalation (see [Approvals](#approvals)) |
| `hook install\|uninstall\|list\|run` | Claude Code hook scripts as Codex hooks (see [Running your Claude Code hooks in Codex](#running-your-claude-code-hooks-in-codex)) |
| `bridge` | Runs the bridge in the foreground, as the service does |

Every verb prints `antiphon: DEGRADED — <reason>` on stderr while the bridge is degraded.

## Trust model

- Same-user domain. The bridge socket is `0600`, and the state file and logs sit in a `0700` directory under your home; anything running as your user can drive the bridge and answer approvals. There is no authentication between sessions beyond the operating system's user boundary, which is also Claude Code's own model for cross-session messages.
- A spawned Codex thread reads every file your user can read and has outbound network access; `--read-only` removes the writes, not the reads. Approvals gate sandbox escalations only: what the sandbox allows never asks.
- A Claude Code session started with `start --claude` runs under your own Claude Code settings and permission mode; antiphon adds no sandbox. The forward hook decides only the tool calls it matches, and the answer comes from a Codex thread: model output deciding what another model may run. Gate what you actually want a second opinion on, and check `status` rather than assuming a call was gated.
- Names are addresses, not identities. `[from <name> via antiphon]` says which registry record a message was sent from, and a name is whatever the session or the bridge set; it proves nothing about who is speaking.
- Callers are classified by process ancestry: a request whose ancestors include a live Claude Code session's pid is that session's; one whose ancestors include a `codex` process is a Codex caller, identified by the `CODEX_THREAD_ID` Codex exports into its shell; anything else is a human at a terminal. A caller may drive, stop, rename and answer for the threads it spawned; Claude Code sessions and humans may do so for any thread; a Codex thread reaches other threads and sessions by message only. A process that reparents itself out of the Codex tree looks like a human, so the check stops mistakes and prompt-injected shortcuts, not a determined caller with your user's rights.
- Messages from Codex threads, their reported answers and their idle-notice details are model output. The skills say so on both sides, with the same rule: never ask a peer to do what your own sandbox, reviewer or permissions refused.
- Nothing antiphon does deletes work: a stopped thread keeps its transcript and its `codex/<name>` branch.

## Troubleshooting

Start with `antiphon ping`. Exit 0 prints `bridge ok · codex <version> · claude <version or none> · peers <n>`. Exit 5 means the bridge is up but the Codex daemon is not answering; the bridge keeps reconnecting, and `cd ~ && codex app-server daemon start` starts it (`~/.codex/app-server-daemon/app-server.stderr.log` says why it is down). Exit 2 means the bridge is degraded and prints the reasons. Exit 1 means no bridge answered and none could be started; the message quotes `~/.antiphon/log/bridge.out`.

Degraded means Claude Code's registry record or Codex's app-server methods no longer look the way the bridge expects, usually after an update of either. While the Claude Code side fails, the bridge hosts threads but registers no new peers. Both clear themselves once everything is back in shape.

The logs, under `~/.antiphon/log/`:

- `bridge.out`: the bridge's own log (`journalctl --user -u antiphon` under systemd).
- `raw.jsonl`: every frame to and from the Codex daemon, and the tmux and git calls.
- `peer-<first 8 characters of the thread id>.log`: every frame on a thread's peer socket and every command between bridge and child; unknown frames are logged once per action with their bytes.
- `hooks.jsonl`: every input, mapped input, script result and answer of `antiphon hook run` and of the permission-forward hook in started sessions.

To see an exchange end to end: `antiphon start -n probe`, send it a message from a Claude Code session with `SendMessage`, read `raw.jsonl` and the probe's `peer-*.log`, then `antiphon stop probe`.

Known failures:

- A thread never appears in `ListAgents` while `ping` is fine, and `bridge.out` says `waiting for a Claude Code session to copy the peer record shape from`: the bridge registers peers only while a Claude Code session is running, and retries every 15 seconds.
- `send` exits 3 with `direct app-server input is not allowed for multi-agent v2 sub-agents`: the target is one of Codex's sub-agents; only its parent drives it.
- `start` exits 2 with `failed to load configuration: No such file or directory`: the Codex daemon was started in a directory that has since been removed (a worktree, a temporary directory), and it keeps its starting directory for life. Run `cd ~ && codex app-server daemon restart`. A daemon that antiphon starts is started in your home directory.
- Turns fail with an expired-token error (`token_expired`), or begin failing after a ChatGPT plan change: log in again (`codex login`, or `codex login --device-auth` on a machine without a browser), then `cd ~ && codex app-server daemon restart`, since a running daemon keeps the token it started with.
- `start --worktree` exits 2 with `a branch named 'codex/<name>' already exists`: a stopped thread of that name left its branch; pick another name or delete the branch.
- `approve` exits 2 with `arrived on a Codex connection that dropped`: the daemon has not re-sent the request on the new connection yet; run the command again in a moment. If the token has gone instead, the thread stopped waiting on the approval and the spawner was told so.
- `start --claude` exits 2 with `did not register as a peer`: the session started but wrote no registry record within 10 s. The message quotes the command and what `claude` printed; `claude agents --json` lists it if it is running, and `claude stop <id>` ends it. A `claude` that is not logged in, or a `--session-id` already in use, fails here.
- A started session's tool calls are never forwarded: `antiphon status <name>` shows no `hook` entries and `hooks.jsonl` has nothing from the session. Either `start --claude` could not find `hooks/claude-permission-forward.sh` (it warns on stderr, and the session then decides its own permissions), or the session's PATH has no `python3`.
- The service's bridge exits at once with `a bridge already answers`: a bridge the CLI started is running. Stop it and restart the service; `install.sh` does this itself when it enables the service.

## Development

`uv run pytest -q -W error` runs the suite against fakes of both sides in temporary directories; nothing touches your Codex, Claude Code or antiphon state. `tests/fixtures/README.md` describes every protocol capture and what remains uncaptured, `tests/live/README.md` the manual pass against the real Codex daemon and Claude Code, and `CLAUDE.md` the layout and conventions.

## License

AGPL-3.0-or-later, see `LICENSE`.
