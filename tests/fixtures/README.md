# Protocol captures

Every file here is a raw exchange with a real Codex app-server daemon or a real Claude Code session, recorded with `capture_daemon.py` (Codex side) or the peer stub used for the Claude side, then passed through `scrub.py`. Tests replay these; nothing in them is typed from memory.

**Codex CLI version: `codex-cli 0.155.1`** (`codex update` ran before the captures and reported 0.155.1 as the latest, so nothing changed; the captures are valid for that version only). Claude Code version in the peer captures: 2.1.278 (`version` in the registry record).

Codex `features.multi_agent_v2 = true` was set in the user config for every Codex capture, and `config: {"features": {"multi_agent_v2": true}}` was also passed on `thread/start` in `permission-hook-order.jsonl`. The `thread/start` result reads `multiAgentMode: "explicitRequestOnly"` with or without that parameter, so whether the daemon honours the per-thread `config` for that feature is not distinguishable from these captures; the user config is the route that is known to work.

## Reading a capture

One JSON value per line. A line is one of:

- a message received from the daemon, verbatim (`{"id":..,"result":..}`, `{"method":..,"params":..}`, or a server request with both `id` and `method`);
- `{"sent": {...}}`, a message the client sent;
- `{"note": "...", "t": <epoch seconds>}`, something that happened on the client side (a wait, a close, a reconnect).

The older captures from the first spike (`thread-start`, `user-reviewer-*`, `auto-review-*`, `codex-agent-tools`, the first part of `sub-agent`) have no `sent` lines, start with a `<<handshake>>` line, and record the one reply that spike sent as `<<answered server request>> {...}`; the peer captures are `{"t", "kind", "data"}` records written by the stub peer. Scrubbing rewrote home directories to `~`, the machine id to zeros, pids to small integers, session titles to `claude-main`, and the daemon's `serverName`/`installationId` to placeholders; thread ids, turn ids and item ids are untouched.

Thread `cwd` for the new captures is `~/antiphon-capture`, an empty git repository trusted in the Codex config; escalations are provoked by writing directly under `~`.

## Fixtures

### `thread-start.jsonl`

`thread/start` on an idle daemon, with the `initialize` result, the `thread/started` broadcast and the MCP startup notifications. Pins the `initialize` result shape (`userAgent`, `codexHome`), the thread object, and that `thread/start` subscribes the connection.

### `user-reviewer-request-approval.jsonl`, `user-reviewer-request-unanswered.jsonl`

`approvalsReviewer: "user"`, `approvalPolicy: "on-request"`: a write outside the workspace produces the server request `item/commandExecution/requestApproval` (`id` + `method` + `params` with `command`, `cwd`, `reason`, `availableDecisions`) and `thread/status/changed` with `activeFlags: ["waitingOnApproval"]`. Answered with `{"result": {"decision": "accept"}}` the daemon emits `serverRequest/resolved`, the command runs, the turn completes. Unanswered, the request stays pending and that capture ends there (the thread was unloaded later; its `thread/closed` is at the tail of `user-reviewer-request-approval.jsonl`). Pins the escalation path the bridge forwards and answers.

### `auto-review-approved.jsonl`, `auto-review-denied.jsonl`

`approvalsReviewer: "auto_review"`: an escalation produces `item/autoApprovalReview/started` then `item/autoApprovalReview/completed` (`review.status` `approved` or `denied`, `riskLevel`, `userAuthorization`, `rationale`, `action{type, source, command, cwd}`) plus a `guardianWarning` notification; no server request follows and the turn continues either way. The denial was provoked with an upload of the auth file to an unresolvable host.

### `permission-hook-order.jsonl` (partial)

Invocation (hook mode file set to `log`, `~/.codex/hooks.json` holding one `PermissionRequest` command hook that logs its stdin and prints nothing; the hook trusted through `[hooks.state."<key>"] trusted_hash` using the `currentHash` that `hooks/list` reports):

```
capture_daemon.py --listen 3 \
  thread/start '{"cwd":"~/antiphon-capture","approvalPolicy":"on-request","approvalsReviewer":"auto_review","sandbox":"workspace-write","ephemeral":false,"config":{"features":{"multi_agent_v2":true}}}' \
  turn/start '{"threadId":"$THREAD","sandboxPolicy":{"type":"workspaceWrite","networkAccess":false},"input":[{"type":"text","text":"<touch a file directly under ~>"}]}' @await turn/completed 240 \
  turn/start '{... "<upload the auth file to an unresolvable host>" ...}' @await turn/completed 240
```

The lines after the client's output are the hook's stdin, one `{"permission_hook_input": ...}` record per invocation.

What it pins: `hook/started` and `hook/completed` (`run.eventName: "permissionRequest"`) arrive **before** `item/commandExecution` starts and before `item/autoApprovalReview/started`; the hook input is `{session_id, turn_id, transcript_path, cwd, hook_event_name: "PermissionRequest", model, permission_mode: "default", tool_name: "Bash", tool_input: {command, description}}`. With no decision from the hook the automatic reviewer ran and approved; the turn completed.

Verdict (with the Codex source at this version, `codex-rs/core/src/tools/approvals.rs`, `Session::request_approval`): hooks run **first and only once**, before the automatic reviewer or the user prompt. A hook `allow` resolves the approval as approved with source "hook" and the reviewer never runs; a hook `deny` resolves it as denied (the command is rejected with the hook's message) and the reviewer never runs; only "no decision" reaches the reviewer. Nothing runs after a reviewer denial. So a Codex `PermissionRequest` hook cannot be the escalation surface for denials; the hook example ships as a guard only, and the bridge's approval path is the override-plus-retry below with the parent-as-reviewer fallback.

The second turn of that run failed with `error{codexErrorInfo: "usageLimitExceeded"}` and `turn/completed{status: "failed"}`, which is a valid capture of the turn-failure shape. The `allow` and `deny` short-circuits the verdict rests on are captured separately, below.

### `hook-allow.jsonl`, `hook-deny.jsonl`

The same hook answering a decision instead of staying silent, one escalation each (a write outside the workspace), driven by `slice0/hook-modes.sh`:

```
capture_daemon.py --listen 5 \
  thread/start '{"cwd":"~/antiphon-capture","approvalPolicy":"on-request","approvalsReviewer":"auto_review","sandbox":"workspace-write","ephemeral":false}' \
  turn/start '{"threadId":"$THREAD","sandboxPolicy":{"type":"workspaceWrite","networkAccess":false},"input":[{"type":"text","text":"<write a file directly under ~>"}]}' \
  @await turn/completed 300
```

What they pin, and the verdict the source predicted: a hook's own decision ends the approval. With `allow`, `hook/started` and `hook/completed` are followed straight by the command running (`item/completed` for a `commandExecution`, and the file exists afterwards) and the turn completing, with **no `item/autoApprovalReview/*` notification anywhere in the capture**. With `deny`, the same two hook notifications are followed by no command at all, no reviewer, and a completed turn; the file does not exist. So the automatic reviewer sees only what the hook declined to decide, and a hook cannot be the place a denial is escalated from, since after a denial nothing runs.

### `guardian-override.jsonl` (partial)

Invocation, against the thread of `auto-review-denied.jsonl` (loaded again with `thread/resume`; `$GUARDIAN_EVENT` is assembled by `capture_daemon.py` from that capture's `item/autoApprovalReview/completed` the way Codex's TUI does it, see `codex-rs/tui/src/chatwidget/protocol_requests.rs`, `on_guardian_review_notification`):

```
capture_daemon.py --listen 5 \
  thread/resume '{"threadId":"<thread>"}' thread/read '{"threadId":"<thread>"}' \
  thread/approveGuardianDeniedAction '{"threadId":"<thread>","event":"$GUARDIAN_EVENT"}' \
  thread/read '{"threadId":"<thread>","includeTurns":true}'
```

The `event` sent (visible in the `sent` line) is the core `GuardianAssessmentEvent`: snake_case keys `id` (the `reviewId`), `turn_id`, `started_at_ms`, `completed_at_ms`, `status: "denied"`, `risk_level`, `user_authorization`, `rationale`, `decision_source: "agent"`, and `action` with `source` remapped from the notification's `unifiedExec` to `unified_exec`; `review_reason`, `target_item_id`, `plugin_id`, `script_path` are left out as the TUI leaves them empty.

What it pins: the daemon answers `{"result": {}}` for that payload on 0.155.1. In the Codex source the handler (`codex-rs/core/src/session/handlers.rs`, `approve_guardian_denied_action`) ignores events whose `status` is not `denied` and otherwise injects one developer-role context item, "approved action: <action>, outcome: allowed", into the thread without starting a turn; nothing is retried by the call itself.

What the retried command does after the override is captured in `guardian-retry.jsonl` (see below). It reaches a dead end for a different reason than "not capturable": for an action rated `critical`, **the retry is re-reviewed and denied again even after the override**.

### `guardian-retry.jsonl`

A credentials-shaped file of random synthetic values (`curl --data @<file>` to a host that does not resolve, so nothing can leave the machine and the values never enter the frames) provokes a genuine guardian denial, then the override and a retry, from `slice0/guardian-retry-realistic.sh`:

```
capture_daemon.py --listen 5 \
  thread/start '{... "approvalsReviewer":"auto_review" ...}' \
  turn/start '{... "<upload the credentials-shaped file>" ...}' @await turn/completed 300 \
  thread/approveGuardianDeniedAction '{"threadId":"$THREAD","event":"$GUARDIAN_EVENT"}' \
  turn/start '{... "retry that exact command now" ...}' @await turn/completed 300
```

What it pins, the answer to the question the earlier `guardian-override.jsonl` left open: the first `item/autoApprovalReview/completed` is `denied` ("uploads a credentials file to an untrusted external destination, constituting obvious secret exfiltration") and the command is declined; the override call is accepted (`{"result": {}}`); the retried command is **reviewed again and denied again**, the second review saying so in as many words: "The user explicitly re-approved the exact command, but it still exfiltrates a credentials file to an untrusted external destination." Both reviews rate the action `riskLevel: "critical"` with `userAuthorization: "high"`, and the first already gives the rule: "explicit authorization cannot override the critical-risk prohibition". So for an action the guardian rates `critical`, `thread/approveGuardianDeniedAction` plus a retry does not make it run: the guardian re-denies, acknowledging the re-approval. This is a property of Codex's guardian, not of antiphon; the surface a spawner can actually approve through is the parent-as-reviewer path (`--review-by-parent`, `approvalsReviewer: "user"`), where there is no guardian and the request is answered directly, captured in `user-reviewer-request-approval.jsonl` and confirmed live. Whether a denial rated below `critical` honours the re-approval on retry is untested (only `critical` was provoked, because a benign action is not denied at all: see the decoy attempt in the history of this file).

### `guardian-retry-authorized.jsonl`

The same run with the retry worded as `antiphon approve` words it, ``I authorize you to retry this command: `$DENIED_COMMAND` `` (the placeholder is the reviewed `action.command`), so the text is byte-identical to what the tool sends. What it pins: the wording changes nothing. Both reviews are `denied`, `riskLevel: "critical"`, `userAuthorization: "high"`; the first rationale states the rule ("explicit authorization cannot override the critical-risk denial") and the second applies it to the retry ("critical-risk credential exfiltration, which explicit approval cannot authorize"). Also visible: the model ran the quoted command inside its own shell wrapper (`/bin/bash -lc "/bin/bash -lc '…'"`), because `action.command` carries Codex's `/bin/bash -lc` wrapper.

The earlier `guardian-override.jsonl` still pins that the override call is accepted and injects a developer note without starting a turn; a decoy of obvious dummy values, by contrast, is inspected and **approved** rather than denied (`rationale`: "the payload is verified dummy data"), which is why the denial has to come from a payload the reviewer cannot clear.

### `adoption.jsonl`, `adoption.txt` (partial)

Invocation (client connected first; then a plain `codex` TUI started in a scratch tmux server with `~/antiphon-capture` as its directory; pane text captured before and after the client's `thread/resume`):

```
capture_daemon.py --listen 15 @await thread/started 90 \
  thread/read '{"threadId":"$THREAD"}' @sleep 5 \
  thread/resume '{"threadId":"$THREAD"}' thread/read '{"threadId":"$THREAD"}'
```

What it pins: a TUI-created thread reaches a client connected beforehand as a `thread/started` broadcast at TUI startup, before any turn. Its `thread/read` shows `source: "vscode"` and `threadSource: "user"`, the same values a client-created thread has, and `originator` is the daemon-wide value (it changes with the client that first connects after a daemon start), so **none of these fields identifies a human TUI thread**; "ours" must be "thread id recorded in state". A `thread/resume` from the client on a thread that has no turn yet fails with `-32600 no rollout found for thread id ...` (the rollout file appears with the first turn); `thread/read` works throughout, and the TUI shows nothing (the two pane captures are identical).

Not captured: closing the TUI mid-turn and confirming the thread stays loaded (needs a running turn).

### `daemon-restart.jsonl` (partial)

Invocation (a second shell ran `codex app-server daemon restart` six seconds in; its output and timestamps are the last lines of the file):

```
capture_daemon.py --listen 20 thread/resume '{"threadId":"<thread>"}' @reconnect 90 \
  thread/loaded/list '{}' thread/resume '{"threadId":"<thread>"}' thread/read '{"threadId":"<thread>"}'
```

What it pins, for an idle subscribed thread: the client sees EOF (`connection closed by the daemon`) 0.2 s after the restart command starts, a second before it returns; the socket path accepts a new connection and drops it once (`BrokenPipeError` on `initialize`) before the new daemon answers; after the reconnect `thread/loaded/list` already lists the thread this client had resumed, `thread/resume` succeeds and `thread/read` reports `status: idle`. A second thread that had been loaded before the restart (created by a TUI that was already closed) was not in that list but broadcast `thread/status/changed{idle}` a second later, so the daemon reloads previously loaded threads after it starts answering.

Not captured: the same with a turn in progress (whether the turn continues or dies).

### `decline.jsonl`

`approvalsReviewer: "user"`, `approvalPolicy: "on-request"`: two phases on separate threads, each with a write outside the workspace that produces `item/commandExecution/requestApproval`. The first answers with `{"decision": "decline"}`, the second with `{"decision": "cancel"}`. Both are accepted by the daemon (`serverRequest/resolved` arrives), the command is declined, and the turn completes normally (`status: completed` and `status: interrupted` respectively). Verdict: both `decline` and `cancel` are valid decision values on 0.155.1; they have the same effect on the command but different turn completion statuses.

### `dropped-connection.jsonl`

`approvalsReviewer: "user"`: one `user`-reviewer thread with a write outside the workspace producing `item/commandExecution/requestApproval`. The client closes the connection without answering, reconnects, resumes the thread, reads it, and interrupts the turn. Verdict: the pending request is re-sent on the new subscription; `thread/read` shows `activeFlags: ["waitingOnApproval"]`; `turn/interrupt` returns `{"result": {}}` and immediately emits `serverRequest/resolved` (the request is cleared and the turn → `interrupted`).

### `tui-routing.jsonl`, `tui-routing.txt`

`approvalsReviewer: "user"`: two phases on two threads (`01a0c880-…` in phase A, `01a0c883-…` in phase B), each with its own pane capture appended to the `.txt` file. Phase A: a TUI raised a command escalation while a headless client was subscribed via `thread/resume`. Phase B: a TUI was attached while the headless client's own turn raised the escalation.

Verdict: the subscribed client's connection receives `item/commandExecution/requestApproval` in both phases, so an escalation reaches every subscribed client whichever side raised it. In phase A the TUI pane shows its own approval prompt ("Would you like to run the following command?" with the y/p/esc choices) for the request the client also holds — the two coexist. The bridge must therefore subscribe to adopted threads to see their escalations.

Not captured: what the TUI's own answer does to a request a silent second subscriber is holding. Phase A ends with the prompt still open — no answer to request id 0 and no `serverRequest/resolved` for it — and phase B's pane was captured about eight seconds before its escalation, showing "Working (4s)" rather than a prompt, while the client answered that request 2 ms after it arrived. The one `serverRequest/resolved` in the file is for the request the *client* answered, with `{"decision": "decline"}` — a second instance of the `decline.jsonl` verdict.

### `turn-on-a-busy-thread.jsonl`

A long turn (`sleep 45`) started, then a second `turn/start` sent on the same thread twelve seconds in, carrying an instruction the first turn had no reason to follow ("end your reply with the word BANANA"), from `slice0/turn-active-text.sh`.

What it pins: the daemon **does not refuse a `turn/start` while a turn is running**, and does not start a second turn. It answers with the turn already in progress (the same `turn.id` the first call returned) and puts the text into it: the transcript shows the second `userMessage` item mid-turn and the final answer is "SLEPT BANANA". So a second start behaves as a steer, and the race where a turn begins between reading a thread's status and acting on it costs a message nothing. An earlier run of the same shape (`slice0/turn-active.jsonl`) confirms the no-error half separately, by interrupting instead of waiting. There is accordingly no "turn already active" error: the delivery ladder's rung for one was removed, having been built on an invented message.

### `two-subscribers.jsonl`

`approvalsReviewer: "user"`: two connections on one thread, each frame tagged `A/` or `B/` by the connection that saw it (`{"from": "A"|"B", "sent"|"recv": ...}`), from `slice0/antiphon-two-subs.py`. A starts the thread (subscribing itself), runs a benign turn so a rollout exists, then B resumes it (subscribing itself); A then raises a write outside the workspace and answers its own `item/commandExecution/requestApproval` with `accept`, while B only watches.

What it pins, the case the bridge is in on an adopted thread it does not answer: both connections receive the `item/commandExecution/requestApproval`, and when A answers, **B receives `serverRequest/resolved` although B never answered**. So a silent second subscriber is told when someone else (a human in a terminal) answers, which is the signal the bridge listens for to clear a pending record it is holding.

### `unconsumed-steer.jsonl`

`approvalsReviewer: "auto_review"`: a fixed-length `sleep 40` turn. Midway through (at ~34s), a `turn/steer` call sends a `clientUserMessageId` with new input. The first turn completes with the steered text appended to the model's output. A follow-up turn asks whether the steer was received; the model confirms via the echoed `clientUserMessageId`. Verdict: `turn/steer` succeeds (result: `{"turnId": "..."}`); the echoed `userMessage` item with `clientId: "<clientUserMessageId>"` arrives **before** `turn/completed`; the steer consumed and the instruction applies to the current turn (late-arriving steers can still land if the turn is long enough).

### `peer-frames.jsonl`, `peer-frames-idle-notice.jsonl`

Claude Code's peer protocol as seen by a stub peer: the registry record it wrote, an inbound `user` frame (`<cross-session-message from=... from-name=... from-mode=...>` body), the `notify_when_idle` control frame, and the `peer_message_status` and `peer_idle_notice` frames the stub sent back, the idle notice being the shape a Claude session rendered. `peer-frames.jsonl` also holds the stub's own outbound `user` frame to the Claude session (`kind: sent`) and one `sandbox_probe` control frame that a probe script running inside a Codex sandbox sent to the stub's socket; neither comes from Claude Code.

### `claude-bg-session.jsonl`

The registry record a background Claude Code session writes for itself, from a real
`claude --bg -n probe-bg --model sonnet 'reply with the single word ok'` in an empty
directory, watched by polling `$CLAUDE_CONFIG_DIR/sessions` while it started. One
`{"t", "kind": "registered", "data"}` line, the same shape as the first line of
`peer-frames.jsonl`.

What it pins: a session nobody is sitting in front of registers as an ordinary peer.
`peerProtocol` is 1 and `messagingSocketPath` has the usual `<pid>.sock` shape, so the
record passes `registry.pins_ok` and the socket takes the same frames as any session's.
`kind` is `"bg"` (an interactive session's is `"interactive"`), `entrypoint` is `"cli"`,
`nameSource` is `"peer"`, and there is a `jobId` — the short id that `claude agents`
lists and `claude stop` and `claude attach` take, which is the first eight characters of
the session id. `peerFeatures` is three entries on 2.1.278
(`notify_idle`, `reply_across_default_dirs`, `artifact_yield`) where the older
`peer-frames.jsonl` record has one, so nothing may require a particular set.

Two other forms were watched in the same run and are not committed, because what they
show is a negative:

- `claude -p` (print) registers too, with `kind: "interactive"` and
  `entrypoint: "sdk-cli"`, but its record is removed the moment its single turn ends, so
  it cannot be a peer anything follows up with. Its first record also carries a name
  derived from the directory (`cwd-45`), replaced ~160 ms later by the `-n` name; that is
  why the bridge waits for a record whose `nameSource` is not `derived`.
- an interactive session is `kind: "interactive"`, `entrypoint: "cli"`.

The same run showed a session's `status` going `busy` then `idle` across its turn, with `statusUpdatedAt` moving with it; only the settled `idle` is in the committed line, and nothing in the package reads either value.

### `sub-agent.jsonl`

Two parts. First, a thread with multi-agent enabled asked to spawn one sub-agent with `spawn_agent`, wait for it and list its agents, seen from the connection subscribed to the root thread: the sub-agent's own `thread/status/changed`, `turn/started` and `turn/completed` arrive on that subscription, the root emits `subAgentActivity` items (`kind` `started`/`interacted`/`completed`, `agentThreadId`, `agentPath: "/root/helper"`) and `collabAgentToolCall` items, and the final answer carries `list_agents`' output (`{"agents":[{"agent_name":"/root","agent_status":"running"},{"agent_name":"/root/helper","agent_status":{"completed":"..."}}]}`). Second, appended later once both threads had been unloaded:

```
capture_daemon.py --listen 3 thread/read '{"threadId":"<child>"}' thread/resume '{"threadId":"<child>"}' \
  thread/read '{"threadId":"<child>"}' turn/start '{"threadId":"<child>","input":[{"type":"text","text":"Reply with the single word OK."}]}' \
  thread/read '{"threadId":"<root>"}'
```

What it pins: `thread/read` of a sub-agent works while it is unloaded and returns `parentThreadId`, `agentNickname` (auto-generated, "Bernoulli"), `agentRole: null`, `canAcceptDirectInput: null`, and a `source` that is an **object**, `{"subAgent": {"thread_spawn": {"parent_thread_id", "depth", "agent_path", "agent_nickname", "agent_role"}}}`, where top-level threads have the string `"vscode"`; a parser must accept both. `thread/resume` on an unloaded sub-agent fails with `-32600 cannot resume an unloaded multi-agent v2 sub-agent through its parent; resume the parent first, or use thread/read to inspect it`, and `turn/start` on it fails with `-32600 thread not found`. The refusal of direct input on a *loaded* sub-agent (`-32600 direct app-server input is not allowed for multi-agent v2 sub-agents`) was observed in the first spike but is not in any committed capture.

### `turns-list.jsonl`

Read-only calls, no model turn: `thread/loaded/list` (empty) then `thread/turns/list {threadId, limit: 1, sortDirection: "desc"}` on the thread of `guardian-override.jsonl` while it was unloaded, and on an unknown id.

What it pins: `thread/turns/list` answers from the rollout even for a thread the daemon has not loaded (one completed turn, a `backwardsCursor`), and an unknown id fails with `-32600 thread not loaded: <id>`. This is how the client finds a thread's active turn before steering.

### `codex-agent-tools.jsonl`, `codex-agent-tools.txt`

A thread with multi-agent enabled asked to list its tools verbatim; the `.txt` is the model's listing (`name: description`, one per line). The multi-agent tools are `spawn_agent`, `send_message`, `followup_task`, `wait_agent`, `interrupt_agent`, `list_agents`, all scoped to the thread's own agent tree. No tool addresses another top-level thread, so a Codex-side `send` to another Codex thread is not a duplicate of a native tool.

### `hooks-list.jsonl`, `codex-hook-schemas/`

Two read-only `hooks/list` calls, no model turn: one for a directory with no hooks configured, one for a trusted project directory holding a temporary `.codex/hooks.json` with a single `PermissionRequest` command hook (removed after the capture):

```
capture_daemon.py hooks/list '{"cwds": ["~"]}'
capture_daemon.py hooks/list '{"cwds": ["/tmp/codex-src"]}'
```

What it pins: the reply is `{"data": [{"cwd", "hooks": [...], "warnings": [], "errors": []}]}`, one entry per requested `cwd`; each hook is `{key, eventName ("permissionRequest"), handlerType ("command"), command, async, matcher, timeoutSec, statusMessage, additionalContextLimit, sourcePath, source ("project" here, "user" for `~/.codex/hooks.json`), pluginId, displayOrder, enabled, isManaged, currentHash ("sha256:<hex>"), trustStatus ("untrusted" | "trusted" | "modified" | "managed")}`. The `key` is `<absolute path of the hooks file>:<event in snake_case>:<matcher group index>:<handler index>`, the same string the user config's `[hooks.state."<key>"] trusted_hash` table is keyed by. `hooks/list` reloads the configuration for each call, so a hook written to `hooks.json` is listed by the next call without a daemon restart.

`codex-hook-schemas/` holds the six generated JSON schemas for the `PermissionRequest`, `PreToolUse` and `PostToolUse` command-hook stdin and stdout, copied unchanged from the Codex source at the installed version (`codex-rs/hooks/schema/generated/`); the one real hook input on record is the `permission_hook_input` line of `permission-hook-order.jsonl`, whose `session_id` equals the `threadId` of the surrounding `hook/started` notification.

Not captured: `config/batchWrite` (the daemon-side write of `hooks.state.<key>.trusted_hash` the Codex TUI uses to trust a hook; its request and reply shapes are taken from `codex-rs/app-server-protocol/src/protocol/v2/config.rs`), and a hook actually run by Codex through the shim.

## Not yet captured

- `mac/`: registry record, process-start line and socket directory from a macOS Claude Code install (optional).
- What a retried command does after a guardian override: not capturable with a harmless payload, for the reason under `guardian-override.jsonl`.
- What a terminal's own answer does to a request a silent second subscriber also holds (see `tui-routing.jsonl`).
- A hook actually run by Codex through the shim, and `config/batchWrite` on a real daemon (see `hooks-list.jsonl`).
