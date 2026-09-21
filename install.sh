#!/usr/bin/env bash
# Install or remove antiphon's skills, background service and optional approval hook.
#
#   ./install.sh install [--no-service] [--human-approvals]
#   ./install.sh uninstall
#
# install:   symlinks skills/claude into $CLAUDE_CONFIG_DIR/skills/antiphon (default
#            ~/.claude/skills) and skills/codex into ~/.agents/skills/antiphon; installs
#            and starts the bridge as a systemd user service (Linux) or launchd agent
#            (macOS) unless --no-service; with --human-approvals, adds hooks/approve-ask.sh
#            as a PreToolUse hook in $CLAUDE_CONFIG_DIR/settings.json.
# uninstall: removes all of the above and stops the bridge.
#
# The `antiphon` command must be on PATH first (`uv tool install .` or `pipx install .`).
# Nothing else is created: the bridge makes ~/.antiphon on its first run.
set -euo pipefail

repo=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
claude_dir=${CLAUDE_CONFIG_DIR:-$HOME/.claude}
antiphon_home=${ANTIPHON_HOME:-$HOME/.antiphon}
claude_skill=$claude_dir/skills/antiphon
codex_skill=$HOME/.agents/skills/antiphon
settings=$claude_dir/settings.json
hook=$repo/hooks/approve-ask.sh
unit=$HOME/.config/systemd/user/antiphon.service
plist=$HOME/Library/LaunchAgents/com.antiphon.bridge.plist

usage() {
    echo "usage: $0 install [--no-service] [--human-approvals] | uninstall" >&2
    exit 2
}

link_skill() {
    local target=$1 link=$2
    mkdir -p "$(dirname "$link")"
    if [ -e "$link" ] && [ ! -L "$link" ]; then
        echo "install.sh: $link exists and is not a symlink; leaving it alone" >&2
        return 1
    fi
    ln -sfn "$target" "$link"
    echo "linked $link -> $target"
}

unlink_skill() {
    if [ -L "$1" ]; then
        rm "$1"
        echo "removed $1"
    fi
}

# Fills the placeholders of a contrib file, with Environment= lines (systemd) or
# key/string pairs (launchd) for the directory overrides the installing shell carries, so
# the service reads the same places. The template's header comment, which holds the
# manual instructions, is dropped from the installed copy.
render() {
    local template=$1 destination=$2 antiphon=$3
    mkdir -p "$(dirname "$destination")"
    ANTIPHON_BIN=$antiphon LOG_DIR=$antiphon_home/log \
        python3 - "$template" "$destination" <<'PY'
import os, re, sys
from xml.sax.saxutils import escape

template, destination = sys.argv[1], sys.argv[2]
text = open(template).read()
names = [n for n in ("CLAUDE_CONFIG_DIR", "CODEX_HOME", "ANTIPHON_HOME") if os.environ.get(n)]
if destination.endswith(".plist"):
    text = re.sub(r"<!--.*?-->\n", "", text, flags=re.DOTALL)
    env = "\n".join(f"        <key>{n}</key>\n        <string>{escape(os.environ[n])}</string>" for n in names)
    text = text.replace("@LOG@", escape(os.environ["LOG_DIR"]))
    text = text.replace("@ANTIPHON@", escape(os.environ["ANTIPHON_BIN"])).replace("@PATH@", escape(os.environ["PATH"]))
else:
    text = "".join(line for line in text.splitlines(keepends=True) if not line.startswith("#"))
    env = "\n".join(f'Environment="{n}={os.environ[n]}"' for n in names)
    text = text.replace("@ANTIPHON@", os.environ["ANTIPHON_BIN"]).replace("@PATH@", os.environ["PATH"])
text = text.replace("@ENVIRONMENT@\n", env + "\n" if env else "")
open(destination, "w").write(text)
PY
    echo "wrote $destination"
}

stop_bridge() {
    # A bridge the CLI started lazily runs outside any service; the service's own bridge
    # was stopped by the caller. Both run as `... antiphon bridge`.
    if pkill -u "$(id -u)" -f 'antiphon bridge$'; then
        echo "stopped the running bridge"
    fi
}

service_install() {
    local antiphon
    antiphon=$(command -v antiphon || true)
    if [ -z "$antiphon" ]; then
        echo "install.sh: antiphon is not on PATH; run \`uv tool install .\` (or \`pipx install .\`) first" >&2
        exit 2
    fi
    case $(uname -s) in
        Linux)
            render "$repo/contrib/antiphon.service" "$unit" "$antiphon"
            stop_bridge
            systemctl --user daemon-reload
            systemctl --user enable --now antiphon
            echo "antiphon bridge enabled as a systemd user service (journalctl --user -u antiphon)"
            ;;
        Darwin)
            render "$repo/contrib/com.antiphon.bridge.plist" "$plist" "$antiphon"
            stop_bridge
            launchctl load "$plist"
            echo "antiphon bridge loaded as a launchd agent ($antiphon_home/log/bridge.out)"
            ;;
        *)
            echo "install.sh: no service file for $(uname -s); the CLI starts the bridge lazily" >&2
            ;;
    esac
}

service_uninstall() {
    if [ -f "$unit" ]; then
        systemctl --user disable --now antiphon || true
        rm "$unit"
        systemctl --user daemon-reload
        echo "removed $unit"
    fi
    if [ -f "$plist" ]; then
        launchctl unload "$plist" || true
        rm "$plist"
        echo "removed $plist"
    fi
    stop_bridge
}

# The hook entry is keyed by its command path, so repeating install changes nothing and
# uninstall removes only what install added; every other setting is left as it is.
edit_hook() {
    local action=$1
    mkdir -p "$(dirname "$settings")"
    HOOK=$hook python3 - "$settings" "$action" <<'PY'
import json, os, sys

path, action = sys.argv[1], sys.argv[2]
hook = os.environ["HOOK"]
data = json.load(open(path)) if os.path.exists(path) else {}
groups = data.setdefault("hooks", {}).setdefault("PreToolUse", [])
for group in groups:
    group["hooks"] = [h for h in group.get("hooks", []) if h.get("command") != hook]
groups[:] = [g for g in groups if g.get("hooks")]
if action == "add":
    groups.append({"matcher": "Bash", "hooks": [{"type": "command", "command": hook, "timeout": 5}]})
if not groups:
    del data["hooks"]["PreToolUse"]
if not data["hooks"]:
    del data["hooks"]
with open(path, "w") as f:
    json.dump(data, f, indent=2)
    f.write("\n")
PY
}

[ $# -ge 1 ] || usage
action=$1
shift
service=1
human_approvals=0
for flag in "$@"; do
    case $flag in
        --no-service) service=0 ;;
        --human-approvals) human_approvals=1 ;;
        *) usage ;;
    esac
done

case $action in
    install)
        link_skill "$repo/skills/claude" "$claude_skill"
        link_skill "$repo/skills/codex" "$codex_skill"
        if [ "$service" = 1 ]; then
            service_install
        fi
        if [ "$human_approvals" = 1 ]; then
            edit_hook add
            echo "added hooks/approve-ask.sh as a PreToolUse hook in $settings"
        fi
        ;;
    uninstall)
        [ $# -eq 0 ] || usage
        unlink_skill "$claude_skill"
        unlink_skill "$codex_skill"
        service_uninstall
        if [ -f "$settings" ] && grep -q "$hook" "$settings"; then
            edit_hook remove
            echo "removed the approve-ask hook from $settings"
        fi
        ;;
    *) usage ;;
esac
