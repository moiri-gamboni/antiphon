"""The Claude Code skills a Codex thread can load, as skill roots for the Codex daemon.

Claude Code and Codex read the same `SKILL.md` format, and the daemon's
`skills/extraRoots/set` adds directories to what every thread on it can load
(`tests/fixtures/skills-extra-roots.jsonl`): the call replaces the whole list,
reaches every connection, skips a root that does not exist, scans a root
recursively, accepts a skill's own directory as a root, and names a skill under
a `.claude-plugin/plugin.json` `<plugin>:<skill>`, as Claude Code does.

The roots are what Claude Code itself loads from its config directory:

- each `skills/<name>/` holding a `SKILL.md`, one by one, because Codex's
  recursive scan of `skills/` would also pick up what Claude Code does not load
  there (the copies under `skills/synced/`). The `antiphon` skill is left out:
  it is the Claude side of this tool, and Codex has its own.
- the `skills/` directory of each plugin that `settings.json` `enabledPlugins`
  turns on, at the install path `plugins/installed_plugins.json` records for
  it, with `scope: "user"` (a project-scope install applies to one project,
  and the roots apply to every thread). Shapes from
  `tests/fixtures/claude-config/`.
"""

from __future__ import annotations

import json
from pathlib import Path

OWN_SKILL = "antiphon"


# What reading a Claude Code config can raise: a missing or unreadable file, one caught
# half-written (Claude Code rewrites both while it runs), or one in a shape not captured.
READ_ERRORS = (OSError, ValueError, AttributeError, KeyError, TypeError)


def skill_roots(config_dir: Path) -> list[str]:
    """Raises one of READ_ERRORS when the config cannot be read whole: the daemon's list
    replaces the previous one, so a partial list would take skills from running threads."""
    return _user_skills(config_dir) + _plugin_skills(config_dir)


def _user_skills(config_dir: Path) -> list[str]:
    base = config_dir / "skills"
    if not base.is_dir():
        return []
    return [str(d) for d in sorted(base.iterdir()) if d.name != OWN_SKILL and (d / "SKILL.md").is_file()]


def _plugin_skills(config_dir: Path) -> list[str]:
    enabled = _read(config_dir / "settings.json").get("enabledPlugins") or {}
    installed = _read(config_dir / "plugins" / "installed_plugins.json").get("plugins") or {}
    roots = []
    for plugin, on in enabled.items():
        for install in installed.get(plugin) or []:
            skills = Path(install["installPath"]) / "skills"
            if on is True and install.get("scope") == "user" and skills.is_dir():
                roots.append(str(skills))
    return roots


def _read(path: Path) -> dict:
    return json.loads(path.read_text()) if path.exists() else {}
