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
