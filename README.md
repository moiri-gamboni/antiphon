# antiphon

antiphon makes Claude Code sessions and OpenAI Codex CLI sessions peers of each other on one machine. A bridge process speaks Codex's app-server protocol on one side and Claude Code's cross-session peer protocol on the other; a small `antiphon` CLI talks to the bridge. From a Claude Code session, Codex threads appear in `ListAgents`, take `SendMessage`, honour `notify_when_idle`, and send their sandbox escalations back for a decision. From a Codex thread, `antiphon` lists the sessions on the machine, messages any of them, subscribes to their idle notices, and starts Claude Code sessions of its own whose permission decisions come back to it. A human attaches a terminal to any thread or session, and a Codex TUI a human started is a peer too.

## Requirements

- Codex CLI 0.155 or later (the app-server daemon and `turn/steer`)
- Claude Code 2.1.278 or later (peer protocol 1 with `notify_when_idle`)
- Python 3.12 or later; no runtime dependencies beyond the standard library
- Linux or macOS. Registration as a Claude Code peer on macOS is checked at runtime against a live session's record (the process-start pin below) and has no recorded fixture yet.
- `claude` and `codex` on the bridge's PATH (`start --claude` runs `claude --bg`, and the bridge starts the Codex daemon), and `python3` on the PATH of the sessions it starts (the permission-forward hook runs there)
- Optional: `tmux` for `attach`, `git` for `start --worktree`

Claude Code's side of this is documented at https://code.claude.com/docs/en/cross-session-messaging (`ListAgents`, `SendMessage`, `notify_when_idle`, session names). The registry record and socket frame shapes are not documented, which is why the bridge pins them (see [Trust model](#trust-model) and [Troubleshooting](#troubleshooting)) and refuses to register peers when they drift.

For Codex threads that use Codex's own sub-agents, set `[features] multi_agent_v2 = true` in `~/.codex/config.toml`.

## Install

```
uv tool install .          # or: pipx install .
./install.sh install       # skills + background service; --no-service skips the service
```

`install.sh install` symlinks `skills/claude` to `$CLAUDE_CONFIG_DIR/skills/antiphon` (default `~/.claude/skills/antiphon`) and `skills/codex` to `~/.agents/skills/antiphon`, and enables the bridge as a systemd user service (Linux) or a launchd agent (macOS) so it is always running; the CLI also starts a bridge on demand when none answers, so the service is the steady state and lazy start the safety net. `--human-approvals` adds the [approval prompt hook](#a-permission-prompt-for-approvals). `./install.sh uninstall` removes the skills, the service and the hook, and stops the bridge.

State, socket and logs live under `~/.antiphon/` (`$ANTIPHON_HOME` overrides it): `bridge.sock`, `state.json`, `log/bridge.out`, `log/raw.jsonl`, `log/peer-<thread id prefix>.log`. The bridge reads Claude Code's registry from `$CLAUDE_CONFIG_DIR/sessions` (default `~/.claude/sessions`) and the Codex daemon socket from `$CODEX_HOME/app-server-control/app-server-control.sock` (default `~/.codex`).

## The `antiphon` command

Options go before the `--` separator; everything after it is the prompt or message text (`antiphon send helper --wait -- "go"`, never `-- "go" --wait`). Targets are a thread's or session's name, or a unique prefix of a thread id. The default name is `codex-<directory name>` for a thread and `claude-<directory name>` for a session; a name already taken by a live session or thread gets `-2`, `-3` appended.

| Verb | What it does | Exit codes beyond 0 |
|---|---|---|
| `ping` | Is the bridge up, and what does it speak to: `bridge ok · codex <version> · claude <version> · peers <n>` | 1 no bridge; 2 degraded (reasons listed); 5 Codex daemon unreachable |
| `start [-C DIR] [-n NAME] [--read-only] [-m MODEL] [--effort LEVEL] [--no-report] [--worktree] [--review-by-parent] [--visible] [--wait] [--timeout S] [-- PROMPT]` | Starts a Codex thread in `DIR` (default: the current directory), idle until sent to unless a prompt follows `--`. `--worktree` gives it a git worktree beside the repository on branch `codex/NAME`; `--visible` attaches a terminal after starting; `--wait` waits for the prompt's turn | 2 precondition (`--worktree` outside a git repository, a branch that already exists, or the daemon refused the start); 5 daemon unreachable; with `--wait`: 4, 6 as `wait` |
| `start --claude [-C DIR] [-n NAME] [-m MODEL] [--gate TOOLS] [--visible] [-- PROMPT]` | Starts a background Claude Code session in `DIR` instead of a Codex thread, and waits up to 10 s for it to register as a peer. The prompt starts its first turn. `--gate` names the tools whose calls it asks the caller about (default: every tool); `--visible` opens it in a terminal after starting. `--read-only`, `--effort`, `--no-report`, `--worktree`, `--review-by-parent` and `--wait` do not apply | 2 `claude` refused to start it, it never registered (the message quotes the command and its output), or a flag that only fits a Codex thread was given |
| `send TARGET [--wait] [--timeout S] -- TEXT` | Steers the target's running turn, or starts a turn if it is idle. To a Claude Code session, and from a Codex thread to a thread it did not spawn: a labelled cross-session message instead, which has no turn of ours to wait for, so `--wait` does nothing there (use `wait`) | 2 unknown or stopped target (the message names `antiphon resume <id>`); 3 the daemon or the receiving session refused it; with `--wait`: 4, 6 |
| `wait TARGET [--timeout S]` | Waits for the thread's turn to end (default 600 s) and prints its final answer, or `<status>: <error>`. On a Claude Code session, waits for its *next* idle notice, so use it on one that has just been given something to do; a session sitting still waits the whole timeout | 2 on a session from anywhere but a Codex thread; 4 still busy at the timeout (a session's subscription stands, and its next idle arrives as a message); 6 the turn failed or was interrupted, or the thread was stopped or unloaded while waiting; 1 the bridge went away twice (the message names the command to rerun) |
| `interrupt TARGET` | Ends the current turn; prints `nothing to interrupt` when idle. Claude Code has no such surface, so this exits 2 on a session and says to send a correction or stop it | 2 the target is a Claude Code session, or the daemon refused the interrupt; 5 daemon unreachable |
| `status [TARGET]` | The thread's state (status, active turn, last outcome, pending escalations, sub-agents, worktree), a started session's (directory, spawner, job id, pending escalations), or the bridge's (daemon, connection epoch, thread count, degraded reasons) | 2 unknown target |
| `ls` | Every peer on the machine: Claude Code sessions, Codex threads, and Codex sub-agents indented under their parent with their role. The first line names the caller when the caller is a peer; `*` marks its row | |
| `stop TARGET` | Retires the thread as a peer: interrupts its turn, unsubscribes, removes its record. The transcript stays, and so does a `--worktree` branch; a clean worktree checkout is removed, a dirty one kept (it says where). On a started Claude Code session, runs `claude stop <job id>` and forgets it | 2 unknown target |
| `resume TARGET` | Hosts a stopped thread again, or any thread id the daemon knows, with its context; a stopped thread gets its former name back | 2 the daemon has no such thread |
| `name [TARGET] NEW` | Renames a thread; from inside a Codex thread with no target, renames that thread | 2 |
| `attach TARGET` | Inside tmux, opens a window running `codex resume <id>` for a thread or `claude attach <job id>` for a started session, and prints the pane; elsewhere prints that command | 2 tmux failed (its message follows) |
| `notify TARGET` | From inside a Codex thread: one message back when the target's turn next ends | 2 from a Claude session (use `notify_when_idle`) or a terminal |
| `approve TOKEN` | Approves the escalation with that token | 2 unknown token, already resolved, or its Codex connection dropped and the request is not back yet; 3 the retry could not be delivered |
| `deny TOKEN -- WHY` | Denies it, telling the thread why | as `approve` |
| `hook install SCRIPT [--event PreToolUse\|PermissionRequest\|PostToolUse] [--matcher TOOL] [--timeout S]` | Registers a Claude Code hook script in `~/.codex/hooks.json` (default event `PreToolUse`, every tool, 600 s) running through `antiphon hook run`, and trusts it through the Codex daemon | 2 Codex does not list the hook, or did not trust it |
| `hook uninstall SCRIPT` | Removes the script's entry from `hooks.json` | 2 not installed |
| `hook list` | The installed scripts with their event, matcher, timeout and Codex trust status | |
| `hook run SCRIPT [--event E] [--timeout S]` | The shim Codex runs: Codex hook input on stdin, Codex hook output on stdout (see [Running your Claude Code hooks in Codex](#running-your-claude-code-hooks-in-codex)) | 2 stdin is not a Codex hook input |
| `bridge` | Runs the bridge in the foreground (what the service runs) | |

Exit codes overall: 0 ok; 1 no bridge answered and none could be started (the message quotes the last lines of `log/bridge.out`), or an op failed inside the bridge (the message names the exception; the traceback is in the bridge log); 2 usage, precondition (including a `start`, `interrupt`, `name` or `resume` the daemon refused), unknown target, or a caller that may not act on that thread; 3 delivery rejected (the daemon or the receiving session refused a `send`, `approve` or `deny`); 4 timeout; 5 Codex daemon unreachable; 6 turn failed or interrupted (or the thread stopped or unloaded during a wait).

Every verb prints `antiphon: DEGRADED — <reason>` on stderr while the bridge is degraded.

## Delegating from Claude Code

The Claude skill carries the working pattern; in short: `antiphon start -C <dir> -n <name> [--read-only]` creates an idle thread, and `SendMessage(to: <name>, message: <brief>, notify_when_idle: true)` starts its first turn and subscribes to the completion signal. Later `SendMessage` calls keep the thread's context: a busy thread is steered, an idle one starts a new turn.

### How completion is signalled

When a turn ends, two things happen in order. The thread's full final answer is delivered to the session that spawned it as an ordinary cross-session message from the thread's name (`start --no-report` turns this off); then the idle notice goes to every session that subscribed with `notify_when_idle`, with the first 200 characters of the answer as its detail. A failed or interrupted turn reports `failed: <error>` or `interrupted: ...` the same way, and `wait` exits 6. `start --wait` / `send --wait` / `wait` print the answer in the command's output as well (the report message still goes out unless the thread was started with `--no-report`), and survive one bridge restart mid-wait.

The idle notice is sent by the thread's peer child, one per hosted thread. Claude Code lists a peer only while the pid in its record is alive, so each thread the bridge hosts gets a process of its own whose registry record and messaging socket are the thread's identity. Those children die with the bridge (stdin EOF) after telling their subscribers `exited`, and are rebuilt when it restarts.

Messages from a Claude session arrive in the thread prefixed `[from <name> via antiphon]`; the thread's developer instructions tell it to reply with `antiphon send <name> -- ...`.

### Sandbox

A spawned thread runs in Codex's `workspace-write` sandbox: it writes under its working directory and `/tmp`, reads everything your user can read, and has outbound network access, which is what lets `antiphon` inside the thread reach the bridge socket. `start --read-only` gives it the `read-only` sandbox instead. Every turn the bridge starts carries that policy.

A Codex session a human started in a terminal has no such grant; `antiphon` inside it fails with `PermissionError: [Errno 1] Operation not permitted` until Codex runs with `-c sandbox_workspace_write.network_access=true` (or `[sandbox_workspace_write] network_access = true` in `~/.codex/config.toml`).

## Approvals

Spawned threads use Codex's automatic reviewer: escalations (a write outside the workspace, network use, a command the sandbox blocks) are decided inside the thread and the turn never blocks. Only the reviewer's denials are forwarded, as a cross-session message to the spawning session with a six-character token:

```
Codex's automatic reviewer denied an action in "<name>" (token a1b2c3): <reason> (risk <level>)
  command: <command>
  cwd: <cwd>
The thread has continued without it. Reply with: antiphon approve a1b2c3   or   antiphon deny a1b2c3 -- <why>
```

`antiphon approve a1b2c3` records the approval in the thread through Codex's own override call (`thread/approveGuardianDeniedAction`, assembled the way Codex's TUI assembles it) then sends the thread ``I authorize you to retry this command: `<command>` ``; if the daemon refuses the override, that message alone carries the approval. `antiphon deny a1b2c3 -- <why>` tells the thread the action stays denied.

**What approving a refusal can and cannot do.** The reviewer re-reviews the retry, and for an action it rates `critical` (the `risk` in the forwarded message) it refuses again whatever the approval says. Two captured retries, one after "retry that exact command now" and one after antiphon's own "I authorize you to retry this command", were both denied again, each review rating the risk `critical` and the user's authorization `high`, and the reviewer states the rule itself: "explicit authorization cannot override the critical-risk denial" (`tests/fixtures/guardian-retry.jsonl`, `guardian-retry-authorized.jsonl`). That is Codex's guardian, not antiphon, and no message from a spawner overrides it. So approving cannot rescue a `critical` denial; whether it rescues one rated lower is untested, since no lower-rated denial could be provoked. A thread whose escalations you intend to decide yourself should be started `--review-by-parent`, where there is no guardian, the request is answered directly, and the approval runs the command.

`start --review-by-parent` makes the spawning session the reviewer instead: each escalation is a blocking request, forwarded as `Codex asks to run an action in "<name>" ... The turn is blocked until you answer.`; `approve` answers it `accept`, `deny` answers `decline`, which refuses the command and leaves the turn running so it can be told why. The spawner is reminded once after ten minutes; nothing is ever cancelled by waiting. A dropped daemon connection does not lose a request either: the daemon re-sends it when the bridge resubscribes, and the same token answers it on the new id. Only a request whose thread has stopped waiting on approval by then is retired, with a message to the spawner.

`antiphon ls` shows `denied <token> <age>` or `approval <token> <age>` in place of a thread's status while an escalation is unanswered; `antiphon status <name>` lists them under `pending`. Answering an escalation is allowed to the session (or thread) that spawned the thread, to any Claude Code session, and to a human at a terminal.

A thread that a human started (adopted by the bridge, see below) keeps its own approvals: its TUI answers them, the bridge never does.

### A permission prompt for approvals

An approval that arrives as a message is decided by the receiving Claude; a message from another session never counts as the user's consent. `hooks/approve-ask.sh` is a Claude Code PreToolUse hook on Bash that turns each `antiphon approve <token>` and `antiphon deny <token>` into a real permission prompt showing the command, the directory and Codex's reason, read from the pending record in `~/.antiphon/state.json`. Install it with `./install.sh install --human-approvals`, which adds it to `$CLAUDE_CONFIG_DIR/settings.json`:

```json
{"hooks": {"PreToolUse": [{"matcher": "Bash", "hooks": [
  {"type": "command", "command": "<repo>/hooks/approve-ask.sh", "timeout": 5}]}]}}
```

Any other command passes through the hook untouched.

## Delegating from Codex to Claude Code

A Codex thread starts a Claude Code session of its own with `antiphon start --claude`, and drives it with the same verbs it uses on a Codex thread:

```
antiphon start --claude -C ~/src/app -n reviewer -- "review the diff on this branch and report back"
antiphon send reviewer -- "look at the migration too"
antiphon wait reviewer
antiphon stop reviewer
```

The session runs in the background, exactly as `claude --bg` starts one: it outlives the command, it takes follow-up messages, and it appears in `antiphon ls`, in `claude agents`, and in every other session's `ListAgents`. A `claude -p` session registers as a peer too but its record goes away with its single turn, so nothing can follow up with it; that is why the background form is the only one.

`send` reaches the session as an ordinary cross-session message labelled `[from <thread name> via antiphon]`; the session replies with `SendMessage(to: <thread name>)` and that reply arrives in the thread as a turn. `wait` subscribes to the session's *next* idle notice and returns its detail, so it belongs after something was sent; on a session sitting still it waits the whole timeout, and a timed-out wait leaves its subscription standing, so the eventual notice arrives as a message the way `notify` delivers one. `interrupt` has no counterpart at all — Claude Code does not offer one — so send a correction, which a busy session takes as its next message, or `antiphon stop`.

`antiphon attach reviewer` opens the session in a tmux window through `claude attach <job id>`, and `--visible` does that as part of `start --claude`. The session keeps running when the window closes.

The thread that started a session may drive, stop and answer for it; any other caller can only message it. A Claude Code session cannot use `start --claude` at all: it has its own Agent tool for that.

### Permission decisions come back to the thread

`start --claude` installs `hooks/claude-permission-forward.sh` into that session alone, through `claude --settings`, as a PreToolUse hook. Before each matched tool call the session's own decision is held and the call is put to the thread that started it, with a token:

```
Permission needed: the Claude Code session "reviewer" asks before an action runs (token a1b2c3): Bash needs a decision
  command: rm -rf build
  cwd: /home/you/src/app
The tool call is blocked until you answer. Reply with: antiphon approve a1b2c3   or   antiphon deny a1b2c3 -- <why>
```

`antiphon approve a1b2c3` lets the call run; `antiphon deny a1b2c3 -- <why>` blocks it with your reason as the hook's message. `antiphon ls` and `antiphon status` show `hook a1b2c3 <age>` meanwhile, and the thread is reminded once after ten minutes. This is the same blocked-call path, and the same pair of answering commands, as a Claude hook that answers `ask` inside a Codex thread (below).

Claude Code has no permission-request event, so the hook runs before *every* call of the tools it matches, not only the ones that would have prompted. `--gate Bash` narrows it to the shell tool; `--gate 'Bash|Write|Edit'` takes the same matcher syntax as any Claude Code hook. With no `--gate` every tool call is forwarded, which is thorough and slow.

The tools a session answers you with — `SendMessage`, `ListAgents`, `ToolSearch` — are never held, whatever the gate says. Answering one message costs a peer listing, a tool lookup and the send, so holding them would leave the session unable to say anything to the very party being asked to decide.

Limits:

- A session you started from a terminal rather than from a Codex thread has no spawner to ask: the call still blocks and shows in `antiphon ls`; answer it from a shell before Claude Code's 600 s hook timeout, or it is denied.
- If nothing can be asked — no bridge, a session antiphon did not start, a document the hook cannot read — the call is denied with the reason. A guard that fails open is not a guard, and Claude Code lets a call through when a hook exits non-zero without JSON.
- The hook writes every document it reads and every decision it prints to `~/.antiphon/log/hooks.jsonl`, beside the Codex direction's.
- The hook is passed on the command line, so it applies to that session only and leaves `$CLAUDE_CONFIG_DIR/settings.json` alone. A session resumed later by hand (`claude --resume`) does not carry it.
- A session antiphon started reports to a Codex thread or to nobody; it cannot report to another Claude Code session, because the frame would have to leave a peer child and only hosted Codex threads have one.

## Running your Claude Code hooks in Codex

A PreToolUse guard you already run under Claude Code (deny `rm -rf`, deny ad-hoc writes to some API, ask before touching a directory) runs unchanged on the Codex threads antiphon spawns and on any Codex session on the machine. Codex's hook events mirror Claude Code's; `antiphon hook run` translates the input and output field by field, and a hook that answers `ask` blocks the tool call while the question goes to whoever spawned the thread.

To install one guard:

```
antiphon hook install ~/.claude/hooks/no-force-push.sh --matcher Bash
```

This appends an entry to `~/.codex/hooks.json` (created if absent; other entries are left alone) whose command is `antiphon hook run <script> --timeout 600`, asks the Codex daemon for the hook's key and hash, and records the hash as trusted in `~/.codex/config.toml` the way the Codex TUI's `/hooks` command does. The verb exits 2 if Codex does not list the hook afterwards, printing Codex's own warnings. Threads started after the install run the hook; a thread already running keeps the hook set it started with. `antiphon hook list` shows each installed script with its trust status; `antiphon hook uninstall <script>` removes the entry (the trusted hash stays in `config.toml`, where it matches nothing else).

Pick the event to match the Claude event the script was written for: `--event PreToolUse` (the default) runs before every tool call, as in Claude Code; `--event PermissionRequest` runs only when a call needs approval, before Codex's automatic reviewer, and a decision there settles the escalation; `--event PostToolUse` runs after the call, where `decision: block` replaces the tool result with the reason. `--matcher` takes a tool name (`Bash`), a `|`-separated list, or a regex, as in Claude Code; Codex names its shell tool `Bash` too.

When the script answers `ask`, the thread's spawner gets a message with a token:

```
The Claude hook no-force-push.sh asks before an action runs in "<name>" (token a1b2c3): <the script's reason>
  command: <command>
  cwd: <cwd>
The tool call is blocked until you answer. Reply with: antiphon approve a1b2c3   or   antiphon deny a1b2c3 -- <why>
```

`approve` lets the call run; `deny` blocks it with your reason as the hook's message; `antiphon ls` shows `hook a1b2c3 <age>` meanwhile, and the spawner is reminded once after ten minutes. Codex kills a hook at its timeout (600 s unless `--timeout` set another value), so an unanswered `ask` ends as a deny naming the token, and the record is dropped; a timeout under about six seconds leaves no time to forward an `ask` at all, and such an `ask` is denied at once (a script that only allows or denies is fine with any timeout). If nothing can be asked at all (no bridge, a thread antiphon does not host), the call is denied with the script's reason.

Limits:

- A thread a human started in a terminal has no spawner to message: the `ask` still blocks and shows in `antiphon ls`; answer it from a shell with `antiphon approve <token>` before the timeout, or it is denied.
- The script sees the Claude input fields Codex can supply: `session_id` (the Codex thread id), `transcript_path` (may be `null`), `cwd`, `permission_mode`, `tool_name`, `tool_input`, `tool_use_id` (absent on a `PermissionRequest`), `tool_response`, `agent_id`, `agent_type`, plus Codex's own `model` and `turn_id`. `prompt_id`, `scratchpad_dir` and `effort` have no Codex source and are absent.
- On a Codex `PreToolUse`, a plain `allow` prints nothing (Codex accepts `allow` only together with an input rewrite); on a `PermissionRequest`, an input rewrite (`updatedInput`) cannot be applied and the call is denied instead. `additionalContext` reaches Codex on `PreToolUse` and `PostToolUse`, not on `PermissionRequest`. Output that looks like JSON but is not denies with the raw text; a deny with no reason gets one, since Codex would treat it as invalid and proceed.
- The shim logs every hook input, the mapped input, the script's exit and output, and the answer to `~/.antiphon/log/hooks.jsonl`.

`tests/fixtures/README.md` (`permission-hook-order.jsonl`, `hooks-list.jsonl`, `codex-hook-schemas/`) records the hook input, the ordering before the reviewer, the `hooks/list` reply and the schemas this rests on.

## Attaching a terminal

`antiphon attach <name>` inside tmux opens a new window in the current session running `codex resume <thread id>` and prints `session:@window.%pane`; outside tmux it prints the command for you to run anywhere. `start --visible` starts a thread and attaches in one step. The thread outlives the window.

The reverse holds too: a plain `codex` TUI you start is adopted by the bridge within 15 seconds (or at once, on the daemon's `thread/started` broadcast), named `codex-<directory>` unless it has a name, and listed as a peer; it is driven by messages and `interrupt` like any thread, and closing the TUI retires it. The bridge subscribes to it as it does to its own threads, so its turn endings and status changes reach `antiphon ls` and `notify_when_idle` as they happen rather than at the next pass. Its escalations arrive on that subscription too, and the TUI still draws its own approval prompt for them; the bridge only watches, so answering stays with you at the terminal. Codex sub-agents (threads with a parent) are listed under their parent, never registered as peers, and driven only by Codex's own tools.

## Codex-side use

Inside a hosted thread, `antiphon ls` starts with `you are <name>`; `antiphon send <name> -- <text>` delivers a labelled cross-session message to a Claude Code session or to another thread; `antiphon notify <name>` brings `Peer <name> is idle: <summary>` back as a turn; `antiphon name <new>` renames the thread; `antiphon start --claude` starts a Claude Code session of its own (see [Delegating from Codex to Claude Code](#delegating-from-codex-to-claude-code)). The verbs mirror the built-in multi-agent tools (`list_agents`, `send_message`/`followup_task`, `wait_agent`, `spawn_agent`, `interrupt_agent`), which reach only the thread's own sub-agents; the Codex skill introduces each by analogy. A thread may drive, stop and answer for the threads and sessions it started itself; towards other threads and sessions it can only send messages.

## Running the bridge as a service

`install.sh install` does this; by hand, the commands from the headers of the two files:

Linux (systemd user unit):

```
sed -e "s|@ANTIPHON@|$(command -v antiphon)|" -e "s|@PATH@|$PATH|" -e "/@ENVIRONMENT@/d" \
    contrib/antiphon.service > ~/.config/systemd/user/antiphon.service
systemctl --user daemon-reload && systemctl --user enable --now antiphon
```

macOS (launchd agent):

```
sed -e "s|@ANTIPHON@|$(command -v antiphon)|" -e "s|@PATH@|$PATH|" -e "s|@LOG@|$HOME/.antiphon/log|" -e "/@ENVIRONMENT@/d" \
    contrib/com.antiphon.bridge.plist > ~/Library/LaunchAgents/com.antiphon.bridge.plist
launchctl load ~/Library/LaunchAgents/com.antiphon.bridge.plist
```

Both run `<absolute path to antiphon> bridge`; `python -m antiphon bridge` from the environment antiphon is installed in is the same thing. The service's PATH must reach `codex`, which the bridge runs to start the app-server daemon when it is not running. If `CLAUDE_CONFIG_DIR`, `CODEX_HOME` or `ANTIPHON_HOME` is set for your sessions, `install.sh` copies them into the service; by hand, add them where the placeholders sit. A second bridge that finds one already answering exits 0, so the units restart on failure only. The bridge reconnects to the daemon with backoff when it restarts and re-subscribes to its threads.

## Trust model

- Same-user domain. The bridge socket is `0600` and the state file and logs sit in a `0700` directory under your home; anything running as your user can drive the bridge and answer approvals. There is no authentication between sessions, only the operating system's user boundary, which is also Claude Code's own model for cross-session messages.
- A Codex thread you spawn reads every file your user can read and has outbound network access; `--read-only` removes the writes, not the reads. Approvals gate sandbox escalations only: what the sandbox allows never asks.
- A Claude Code session started with `start --claude` runs under your own Claude Code settings and permission mode; antiphon adds no sandbox. The forward hook decides the tool calls it matches and leaves the rest to those settings, and the answer comes from a Codex thread — model output deciding what another model may run. Choose `--gate` for what you actually want a second opinion on, and read `status` rather than assuming a call was gated.
- Names are addresses, not identities. A message `[from <name> via antiphon]` says which registry record it was sent from, and a session's name is whatever it or the bridge set; do not act on a name as proof of who is speaking.
- Callers are classified by process ancestry: a request whose ancestor chain contains a live Claude Code session's pid is that session's; one whose chain contains a `codex` process is a Codex caller, identified by the `CODEX_THREAD_ID` Codex exports into its shell; anything else is a human at a terminal. A caller may drive, stop, rename and answer for the threads it spawned; Claude sessions and humans may do so for any thread; a Codex thread reaches other threads and sessions by labelled message only. The limit: a process that reparents itself out of the Codex tree looks like a human, so the ancestry check gates mistakes and prompt-injected shortcuts, not a determined caller with your user's rights.
- Messages from Codex threads, their reported answers and their idle-notice details are model output. The skills say so on both sides, and both carry the same rule: never ask a peer to do what your own sandbox, reviewer or permissions refused.
- A stopped thread keeps its transcript and its `codex/<name>` branch; nothing antiphon does deletes work.

## Troubleshooting

`antiphon ping` first: exit 0 prints `bridge ok · codex <version> · claude <version or none> · peers <n>`; exit 5 means the bridge is up but the Codex daemon is not answering (the bridge keeps reconnecting; `cd ~ && codex app-server daemon start`, and `~/.codex/app-server-daemon/app-server.stderr.log` for why it is not up); exit 2 means the bridge is degraded and prints the reasons. Exit 1 means no bridge answered and none could be started; the message quotes `~/.antiphon/log/bridge.out`.

Degraded reasons come from the pins. On the Claude side a live registry record is checked for the required fields, `peerProtocol` 1, a socket path ending in `/<pid>.sock`, and a `procStart` that matches what the bridge computes for that pid; on the Codex side, a failed `initialize` or a "method not found" for `thread/loaded/list`, `turn/steer` or `thread/resume`. While a Claude-side pin fails the bridge hosts threads but registers no new peers; a Codex-side failure only marks the state and the banner. Both clear themselves once a pass finds everything in shape.

A thread that never appears in `ListAgents` while `ping` is fine: `log/bridge.out` says `waiting for a Claude Code session to copy the peer record shape from` when no Claude Code session is running; the bridge registers peers only alongside a live session (it copies the record's version and pid domain from one) and retries every 15 seconds.

The probe recipe for anything else: `antiphon start -n probe`, send it a message from a Claude session with `SendMessage`, then read `~/.antiphon/log/raw.jsonl` (every frame to and from the Codex daemon, the tmux and git calls) and `~/.antiphon/log/peer-<first 8 chars of the thread id>.log` (every frame on the peer socket and every command between bridge and child). Unknown frames on a peer socket are logged once per action with their bytes. `antiphon stop probe` afterwards.

Other things seen:

- `send` exits 3 with `direct app-server input is not allowed for multi-agent v2 sub-agents`: the target is one of Codex's sub-agents; only its parent drives it.
- `start` exits 2 with `failed to load configuration: No such file or directory`: the Codex daemon was started in a directory that has since been removed (a worktree, a temporary directory); it keeps its starting directory for life and cannot load its configuration without it. `cd ~ && codex app-server daemon restart`. A daemon that antiphon starts is started in your home directory.
- `start --worktree` exits 2 with `a branch named 'codex/<name>' already exists`: a stopped thread of that name left its branch; pick another name or delete the branch.
- `approve` exits 2 with `arrived on a Codex connection that dropped`: the daemon has not re-sent the request on the new connection yet; run the command again in a moment. If the token has gone instead, the thread stopped waiting on the approval and the spawner was told so.
- `attach` prints `codex resume <id>` or `claude attach <job id>` instead of opening a window: the caller is not inside tmux.
- `start --claude` exits 2 with `did not register as a peer`: the session started but wrote no registry record within 10 s. The message quotes the command and what `claude` printed; `claude agents --json` lists it if it is running, and `claude stop <id>` ends it. A `claude` that is not logged in, or a `--session-id` already in use, fails here.
- A started session's tool calls are never forwarded: `antiphon status <name>` shows no `hook` entries and `log/hooks.jsonl` has no line from the session. `start --claude` prints a warning on stderr when it could not find `hooks/claude-permission-forward.sh`; the session then decides its own permissions. A session whose PATH has no `python3` is the other cause, and the hook's log is empty there too.
- The bridge started by the service exits at once with `a bridge already answers`: a lazily started bridge is running; `install.sh` stops it before enabling the service, or stop it yourself and restart the service.

## Development

`uv run pytest -q -W error` runs the suite against fakes of both sides in temporary directories; nothing touches your Codex, Claude Code or antiphon state. `tests/fixtures/README.md` describes every protocol capture and what remains uncaptured. `CLAUDE.md` has the layout and conventions.

## License

AGPL-3.0-or-later, see `LICENSE`.
