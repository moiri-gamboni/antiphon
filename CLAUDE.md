# CLAUDE.md

antiphon makes Claude Code sessions and Codex CLI sessions peers of each other on one machine. README.md is the user documentation; this file is for working on the code.

## Layout

- `src/antiphon/` — the package, standard library only at runtime. `bridge.py` (the one bridge process: daemon connection, thread table, reconcile, peer children, the driving ops), `cli.py` (every verb, the exit codes, lazy bridge start), `ipc.py` (control socket), `state.py` (`~/.antiphon/state.json`), `callers.py` (who is calling, by process ancestry, and the ownership rule), `peers.py` (the Codex-side verbs), `rawlog.py`.
  - `codex/` — the app-server adapter: `ws.py` (WebSocket over the daemon's Unix socket), `daemon.py` (JSON-RPC client, thread verbs, the `deliver` ladder), `approvals.py` (escalations forwarded to the spawner and answered by token).
  - `claude/` — the peer-protocol adapter: `registry.py` (session records, pins), `peer.py` (the child process that is one Codex thread's peer identity).
- `skills/claude/`, `skills/codex/` — the two skills `install.sh` symlinks into place; `hooks/` — the optional Claude Code approval prompt hook and the Codex guard example; `contrib/` — the systemd unit and launchd plist.
- `tests/` — pytest; `tests/fake_daemon.py` and `tests/fake_claude.py` stand in for the two real sides; `tests/fixtures/` holds the scrubbed protocol captures, each described in `tests/fixtures/README.md`.

## Commands

```
uv run pytest -q -W error      # the whole suite; every test runs in temporary homes and never touches the user's Codex, Claude or antiphon state
./install.sh install            # skills + background service; --no-service, --human-approvals; ./install.sh uninstall reverses it
```

## Conventions

- Every external shape (a daemon reply, a registry record, a socket frame) is written from a capture in `tests/fixtures/`, never from memory; a shape no capture holds is marked as schema-derived where it is parsed and tested. Every boundary writes its raw request and response to `~/.antiphon/log/raw.jsonl` (the peer children to `peer-<id>.log`).
- Protocol expectations live in code, in two places: `claude/registry.py` (`REQUIRED_FIELDS`, `PEER_PROTOCOL`, `pins_ok`) pins the Claude Code registry record, and `bridge.py` (`PINNED_METHODS`, `_check_pins`, `_watch_pinned_methods`) turns a pin failure on either side into the degraded state the CLI reports.
- Prose in code, commits and docs uses the domain's words: no task numbers, review labels, personal names, employer or machine names. The publication gate runs before every commit over `src tests skills README.md CLAUDE.md install.sh contrib hooks` and must print nothing: a case-insensitive `grep -rIiE` for the personal, employer and machine names and the machine id of the box the captures were made on (kept out of this file for the same reason they are kept out of the tree), and the case-sensitive grep for planning vocabulary:

```
grep -rIE -e 'Task [0-9]' -e '\b(FM|SEC)-[CIS][0-9]' -e '\b(HC|AR|SC|OP|DC)[0-9]' -e '\[(user|fact|assumed)\]' src tests skills README.md CLAUDE.md install.sh contrib hooks
```

  Captures are scrubbed at capture time (`tests/fixtures/scrub.py`: home directories to `~`, pids to small integers, session titles and machine ids to placeholders).

- Nothing under `plans/` is committed.
