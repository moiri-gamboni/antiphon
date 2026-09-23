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

## Notes

Pass `-m` to `antiphon start --claude` (step 15) unless the default model is certain for the login: a session started on a model the login has no credit for registers and looks healthy, then fails every turn.

Two things this pass does not cover: the Codex hook shim end to end, which needs a hook written into the Codex configuration, and a macOS machine.
