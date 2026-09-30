import json
import shutil
from pathlib import Path

import pytest

from antiphon.claude import skills

FIXTURES = Path(__file__).parent / "fixtures"


def skill(directory: Path, name: str) -> Path:
    directory.mkdir(parents=True)
    (directory / "SKILL.md").write_text(f"---\nname: {name}\ndescription: test skill\n---\n")
    return directory


def config_from_fixture(tmp_path: Path) -> Path:
    """The captured Claude Code plugin records, with every `~/.claude` install path moved under tmp_path."""
    config = tmp_path / "claude"
    shutil.copytree(FIXTURES / "claude-config", config)
    installed = config / "plugins" / "installed_plugins.json"
    installed.write_text(installed.read_text().replace("~/.claude", str(config)))
    return config


def test_roots_are_each_user_skill_then_each_enabled_plugins_skills_directory(tmp_path):
    config = config_from_fixture(tmp_path)
    tidy = skill(config / "skills" / "tidy", "tidy")
    praxis = config / "plugins/cache/praxis-marketplace/praxis/2.3.5"
    skill(praxis / "skills" / "design", "design")
    notes = config / "plugins/cache/private-marketplace/notes/ed05ba005eee"
    skill(notes / "skills" / "draft", "draft")

    assert skills.skill_roots(config) == [str(tidy), str(praxis / "skills"), str(notes / "skills")]


def test_a_plugin_installed_but_not_enabled_is_left_out(tmp_path):
    config = config_from_fixture(tmp_path)
    serena = config / "plugins/cache/claude-plugins-official/serena/2a8ad9f74633"
    skill(serena / "skills" / "index", "index")

    assert str(serena / "skills") not in skills.skill_roots(config)


def test_a_plugin_disabled_in_settings_or_without_skills_is_left_out(tmp_path):
    config = config_from_fixture(tmp_path)
    settings = json.loads((config / "settings.json").read_text())
    settings["enabledPlugins"]["praxis@praxis-marketplace"] = False
    (config / "settings.json").write_text(json.dumps(settings))
    skill(config / "plugins/cache/praxis-marketplace/praxis/2.3.5/skills/design", "design")
    # notes is enabled but its install has no skills directory

    assert skills.skill_roots(config) == []


def test_user_skills_are_one_level_deep_and_antiphons_own_claude_side_skill_is_left_out(tmp_path):
    config = tmp_path / "claude"
    skill(config / "skills" / "antiphon", "antiphon")
    skill(config / "skills" / "synced" / "a1b2" / "pdf", "pdf")
    tidy = skill(config / "skills" / "tidy", "tidy")

    assert skills.skill_roots(config) == [str(tidy)]


def test_a_symlinked_user_skill_is_passed_by_its_link(tmp_path):
    config = tmp_path / "claude"
    target = skill(tmp_path / "elsewhere" / "tidy", "tidy")
    (config / "skills").mkdir(parents=True)
    (config / "skills" / "tidy").symlink_to(target)

    assert skills.skill_roots(config) == [str(config / "skills" / "tidy")]


def test_no_claude_config_gives_no_roots(tmp_path):
    assert skills.skill_roots(tmp_path / "absent") == []


def test_unreadable_plugin_records_raise_rather_than_give_a_partial_list(tmp_path):
    config = tmp_path / "claude"
    skill(config / "skills" / "tidy", "tidy")
    (config / "plugins").mkdir()
    (config / "plugins" / "installed_plugins.json").write_text("{not json")
    (config / "settings.json").write_text(json.dumps({"enabledPlugins": {"praxis@praxis-marketplace": True}}))

    with pytest.raises(skills.READ_ERRORS):
        skills.skill_roots(config)
