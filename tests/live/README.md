# Live verification

The automated suite runs against fakes. This is the manual pass against the real Codex daemon and a real Claude Code session, which is the only way to check the two things no fake can: that Claude Code accepts a Codex thread as a peer, and that a Codex thread can reach back through its sandbox.

Run it from a Claude Code session, not from a subagent: several steps need that session's own peer listing, its cross-session messages, and its turn boundaries. Do not run it while something else is capturing on the same daemon — the bridge adopts every thread the daemon has loaded.

Prepare a scratch repository (`mkdir ~/antiphon-scratch && git -C ~/antiphon-scratch init`) and use `antiphon` from the checkout (`uv run --directory <checkout> antiphon …`) if it is not installed. Retire every thread with `antiphon stop` at the end.

| # | Step | What must happen |
|---|---|---|
| 1 | `antiphon ping` | the bridge starts by itself and prints both versions and the peer count |
| 2 | `antiphon start -C ~/antiphon-scratch -n codex-scratch` | a thread id comes back; the log says a peer child registered |
| 3 | the session's own peer listing | `codex-scratch` is listed like any other session |
| 4 | send it a brief with an idle subscription | the thread answers; its full answer arrives as a cross-session message; one idle notice renders at the next turn boundary, carrying the answer as its detail |
| 5 | `antiphon send <name> -- …` during a long turn | `steered`, and the turn ends on the new instruction instead of the old one |
| 6 | `antiphon interrupt <name>` during a turn, then again when idle | the turn ends as `interrupted`; the second call is a no-op with exit 0 |
| 7 | `antiphon attach <name>` | a terminal opens on the thread in the thread's own directory |
| 8 | a thread started `--review-by-parent`, asked for something outside its workspace | the escalation arrives as a message with a token, the command, the directory and the reason, and the action stays blocked |
| 9 | `antiphon approve <token>` | the command runs and the turn finishes |
| 10 | the same again, then `antiphon deny <token> -- <why>` | the command does not run and the thread reports that it was denied |
| 11 | from inside the thread: `antiphon send "<the Claude session's name>" -- …` | the message arrives in that session, attributed to the thread |
| 12 | `codex app-server daemon restart` during a turn | the bridge stays up, reconnects, and records that turn as interrupted |
| 13 | a Codex terminal someone else started | it is adopted and listed within one reconcile pass |
| 14 | `antiphon stop` each thread | the peers disappear from the listing and leave no registry record or socket behind |
| 15 | from a Codex thread: `antiphon start --claude -n <name> -m <model> -C <dir>` | a background Claude session registers under that name within ten seconds, with the Codex thread as its spawner |
| 16 | `antiphon send <session>` from that thread, asking for a reply | the message arrives in the session and its reply reaches the Codex thread as a turn, with nothing held |
| 17 | the same session runs a gated tool | the call is held, the request reaches the Codex thread with a token, and `antiphon approve <token>` lets it through |
| 18 | `antiphon stop <session>` | Claude Code ends the job and the record disappears |

## The run of 2026-09-22

Codex 0.155.1, Claude Code 2.1.278. Every step above behaved as described. Three defects surfaced and were fixed in the same session:

- **The first message to a freshly started thread was dropped.** A thread has no rollout file until its first turn, so asking the daemon for its turns failed with `invalid paginated history lineage … missing source rollout`, and the sender got a `dropped` receipt. This is the path the Claude skill documents, so it failed for the normal way of briefing a new session. Captured as `tests/fixtures/fresh-thread.jsonl`.
- **An attached terminal opened in the caller's directory**, so Codex asked which directory to resume in and asked the human to trust one nobody meant to open.
- **A turn ended by a daemon restart read as `interrupted: `**, a status with an empty reason after the colon, because such a turn carries no message of its own.

The reverse direction was verified the same way later the same day, and found three more:

- **A started session was reported as never registering, while it was in fact running.** A background session assigns its own session id and takes its job id from that, ignoring the one it is given, so waiting for a record under the id we chose could only ever time out. The name is the handle; the id is read back from the record. The stand-in launcher in the suite honoured the id it was given, which is exactly why the suite was quiet about it.
- **A session could not answer its spawner.** Holding every tool held the reply too: one answer costs a peer listing, a tool lookup and the send, so it took three approvals to say one word back. Those three tools are now never held.
- **A session inherits the default model**, which on a login without credit for it fails every turn while the session still registers and looks healthy. Pass `-m` when the default is not certain.

Two things neither run could cover: the Codex hook shim end to end, which needs a hook written into the Codex configuration, and a macOS machine.
