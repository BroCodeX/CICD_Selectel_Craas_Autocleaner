"""
Tests for load_cleanup_config() fail-fast behaviour.

Invalid regexp must be detected at config load time (re.compile),
not silently ignored until the first image is processed.
"""
import re
import textwrap

import pytest

from config.logger_config import setup_logging
from config.cleanup_config import load_cleanup_config

setup_logging()


def _write_config(tmp_path, content: str) -> str:
    """Write YAML content to a temp file and return its path as string."""
    path = tmp_path / "rules.yaml"
    path.write_text(textwrap.dedent(content))
    return str(path)


# ---------------------------------------------------------------------------
# Fail-fast: invalid regexp
# ---------------------------------------------------------------------------

def test_load_config_invalid_regexp_exits(tmp_path, monkeypatch):
    """Syntactically invalid regexp must cause sys.exit(1) at load time."""
    config_path = _write_config(tmp_path, """
        cleanup_rules:
          broken_rule:
            regexp: "([unclosed"
            keep_latest: 1
    """)
    monkeypatch.setenv("CLEAN_CONFIG_PATH", config_path)

    with pytest.raises(SystemExit) as exc_info:
        load_cleanup_config()

    assert exc_info.value.code == 1


def test_load_config_another_invalid_regexp_exits(tmp_path, monkeypatch):
    """Another common bad pattern: lone quantifier."""
    config_path = _write_config(tmp_path, """
        cleanup_rules:
          broken_rule:
            regexp: "*bad"
            keep_latest: 1
    """)
    monkeypatch.setenv("CLEAN_CONFIG_PATH", config_path)

    with pytest.raises(SystemExit) as exc_info:
        load_cleanup_config()

    assert exc_info.value.code == 1


def test_load_config_valid_regexp_does_not_exit(tmp_path, monkeypatch):
    """Syntactically valid regexp must load without error."""
    config_path = _write_config(tmp_path, """
        cleanup_rules:
          good_rule:
            regexp: "myapp:.*-review-.*"
            keep_latest: 3
    """)
    monkeypatch.setenv("CLEAN_CONFIG_PATH", config_path)

    config = load_cleanup_config()
    rules = config["cleanup_rules"]

    assert "good_rule" in rules
    assert rules["good_rule"]["regexp"] == "myapp:.*-review-.*"


def test_load_config_multiple_rules_one_invalid_exits(tmp_path, monkeypatch):
    """If any rule has an invalid regexp, config must fail fast even if others are valid."""
    config_path = _write_config(tmp_path, """
        cleanup_rules:
          good_rule:
            regexp: "myapp:.*-review-.*"
            keep_latest: 3
          bad_rule:
            regexp: "([broken"
            keep_latest: 1
    """)
    monkeypatch.setenv("CLEAN_CONFIG_PATH", config_path)

    with pytest.raises(SystemExit) as exc_info:
        load_cleanup_config()

    assert exc_info.value.code == 1


# ---------------------------------------------------------------------------
# save_regexps field validation
# ---------------------------------------------------------------------------

def test_load_config_with_save_regexps(tmp_path, monkeypatch):
    """save_regexps field must be parsed and returned as list."""
    config_path = _write_config(tmp_path, """
        cleanup_rules:
          review:
            regexp: ".*:review-.*"
            keep_latest: 1
            save_regexps:
              - "review-clusters-.+"
              - ".*-370252"
    """)
    monkeypatch.setenv("CLEAN_CONFIG_PATH", config_path)

    config = load_cleanup_config()
    rules = config["cleanup_rules"]

    assert rules["review"]["save_regexps"] == ["review-clusters-.+", ".*-370252"]


def test_load_config_save_regexps_empty_list(tmp_path, monkeypatch):
    """Empty save_regexps list must be accepted (no-op)."""
    config_path = _write_config(tmp_path, """
        cleanup_rules:
          review:
            regexp: ".*:review-.*"
            keep_latest: 1
            save_regexps: []
    """)
    monkeypatch.setenv("CLEAN_CONFIG_PATH", config_path)

    config = load_cleanup_config()
    rules = config["cleanup_rules"]

    assert rules["review"]["save_regexps"] == []


def test_load_config_without_save_regexps_defaults_to_empty(tmp_path, monkeypatch):
    """When save_regexps is absent, it must default to empty list."""
    config_path = _write_config(tmp_path, """
        cleanup_rules:
          review:
            regexp: ".*:review-.*"
            keep_latest: 1
    """)
    monkeypatch.setenv("CLEAN_CONFIG_PATH", config_path)

    config = load_cleanup_config()
    rules = config["cleanup_rules"]

    assert rules["review"].get("save_regexps") in (None, [])


def test_load_config_invalid_save_regexp_exits(tmp_path, monkeypatch):
    """Invalid regexp in save_regexps must cause sys.exit(1) at load time."""
    config_path = _write_config(tmp_path, """
        cleanup_rules:
          review:
            regexp: ".*:review-.*"
            keep_latest: 1
            save_regexps:
              - "([unclosed"
    """)
    monkeypatch.setenv("CLEAN_CONFIG_PATH", config_path)

    with pytest.raises(SystemExit) as exc_info:
        load_cleanup_config()

    assert exc_info.value.code == 1


def test_load_config_save_regexps_not_list_exits(tmp_path, monkeypatch):
    """save_regexps must be a list, not a string."""
    config_path = _write_config(tmp_path, """
        cleanup_rules:
          review:
            regexp: ".*:review-.*"
            keep_latest: 1
            save_regexps: "not-a-list"
    """)
    monkeypatch.setenv("CLEAN_CONFIG_PATH", config_path)

    with pytest.raises(SystemExit) as exc_info:
        load_cleanup_config()

    assert exc_info.value.code == 1


def test_load_config_multiple_rules_save_regexps(tmp_path, monkeypatch):
    """Multiple rules with save_regexps must all be parsed independently."""
    config_path = _write_config(tmp_path, """
        cleanup_rules:
          review:
            regexp: ".*:review-.*"
            keep_latest: 5
            save_regexps:
              - "review-clusters-.+"
          release:
            regexp: ".*:release-.*"
            keep_latest: 10
            save_regexps:
              - "release-stable-.+"
              - ".*-v2$"
    """)
    monkeypatch.setenv("CLEAN_CONFIG_PATH", config_path)

    config = load_cleanup_config()
    rules = config["cleanup_rules"]

    assert rules["review"]["save_regexps"] == ["review-clusters-.+"]
    assert rules["release"]["save_regexps"] == ["release-stable-.+", ".*-v2$"]
