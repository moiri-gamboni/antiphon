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

Not captured: the second turn (the denial-shaped command) failed with `error{codexErrorInfo: "usageLimitExceeded"}` and `turn/completed{status: "failed"}` because the Codex account's 30-day window was exhausted; the live confirmation of the `allow`/`deny` short-circuits (hook modes `allow` and `deny`) is still to be run. The failed-turn frames at the end of the file are a valid capture of the turn-failure shape.

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

Not captured: the `turn/start` "retry it now" after the override, and the message-only retry on a fresh denial (usage limit, see below). Whether the retried command runs without a second review, is re-reviewed and approved, or is denied again stays open; the override is confirmed accepted, so it remains the primary path and the message the fallback until that turn is captured.

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

### `unconsumed-steer.jsonl`

`approvalsReviewer: "auto_review"`: a fixed-length `sleep 40` turn. Midway through (at ~34s), a `turn/steer` call sends a `clientUserMessageId` with new input. The first turn completes with the steered text appended to the model's output. A follow-up turn asks whether the steer was received; the model confirms via the echoed `clientUserMessageId`. Verdict: `turn/steer` succeeds (result: `{"turnId": "..."}`); the echoed `userMessage` item with `clientId: "<clientUserMessageId>"` arrives **before** `turn/completed`; the steer consumed and the instruction applies to the current turn (late-arriving steers can still land if the turn is long enough).

### `peer-frames.jsonl`, `peer-frames-idle-notice.jsonl`

Claude Code's peer protocol as seen by a stub peer: the registry record it wrote, an inbound `user` frame (`<cross-session-message from=... from-name=... from-mode=...>` body), the `notify_when_idle` control frame, and the `peer_message_status` and `peer_idle_notice` frames the stub sent back, the idle notice being the shape a Claude session rendered. `peer-frames.jsonl` also holds the stub's own outbound `user` frame to the Claude session (`kind: sent`) and one `sandbox_probe` control frame that a probe script running inside a Codex sandbox sent to the stub's socket; neither comes from Claude Code.

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
- Hook modes `allow` and `deny` on `permission-hook-order.jsonl`: the first usage window's captures showed `log` mode; `allow` and `deny` mode short-circuits remain to be confirmed.
- Guardian override retry turns: `guardian-override.jsonl` pins the override call acceptance, but the `turn/start` "retry it now" failed with `codexErrorInfo: "cyberPolicy"` (server-side content block); the message-only path also self-refused.
