"""
Tests for exclude_repo filtering.

The exclude_repo field in the YAML config specifies a regexp that is matched
against repository names. Any repo whose name matches is excluded from cleanup.
If exclude_repo is absent or empty, all repos are processed (existing behaviour).
"""
import re
import textwrap

import pytest

from config.logger_config import setup_logging
from config.cleanup_config import load_cleanup_config
from core.cleanup_rules_parser import filter_repos_by_exclude

setup_logging()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_repos(*names):
    return [{"name": n} for n in names]


def _write_config(tmp_path, content: str) -> str:
    path = tmp_path / "rules.yaml"
    path.write_text(textwrap.dedent(content))
    return str(path)


# ---------------------------------------------------------------------------
# filter_repos_by_exclude — unit tests
# ---------------------------------------------------------------------------

def test_no_exclude_pattern_returns_all_repos():
    """None exclude_pattern must return all repos unchanged."""
    repos = _make_repos("app", "app-cache", "nginx", "devsec/scanner")
    result = filter_repos_by_exclude(repos, None)
    assert [r["name"] for r in result] == ["app", "app-cache", "nginx", "devsec/scanner"]


def test_empty_string_exclude_pattern_returns_all_repos():
    """Empty string exclude_pattern must return all repos unchanged."""
    repos = _make_repos("app", "app-cache", "nginx")
    result = filter_repos_by_exclude(repos, "")
    assert [r["name"] for r in result] == ["app", "app-cache", "nginx"]


def test_exclude_pattern_removes_matching_repos():
    """Repos matching the exclude_pattern must be filtered out."""
    repos = _make_repos(
        "logistics-service/app",
        "logistics-service/app-cache",
        "devsec/scanner",
        "build/buildkit-test",
    )
    result = filter_repos_by_exclude(repos, r"devsec\/.*")
    assert [r["name"] for r in result] == [
        "logistics-service/app",
        "logistics-service/app-cache",
        "build/buildkit-test",
    ]


def test_exclude_pattern_partial_name_match():
    """Partial regexp match (re.search) must exclude matching repos."""
    repos = _make_repos("app", "app-cache", "nginx", "nginx-cache")
    result = filter_repos_by_exclude(repos, r"-cache$")
    assert [r["name"] for r in result] == ["app", "nginx"]


def test_exclude_pattern_excludes_multiple_prefixes():
    """Pattern matching multiple prefixes must exclude all of them."""
    repos = _make_repos(
        "build/test",
        "build/test-cache",
        "devsec/scanner",
        "logistics-service/app",
    )
    result = filter_repos_by_exclude(repos, r"^(build|devsec)\/")
    assert [r["name"] for r in result] == ["logistics-service/app"]


def test_exclude_pattern_no_matches_returns_all():
    """Pattern that matches nothing must return the full list."""
    repos = _make_repos("app", "nginx", "worker")
    result = filter_repos_by_exclude(repos, r"nonexistent")
    assert [r["name"] for r in result] == ["app", "nginx", "worker"]


def test_exclude_pattern_matches_all_returns_empty():
    """Pattern that matches everything must return an empty list."""
    repos = _make_repos("app", "nginx", "worker")
    result = filter_repos_by_exclude(repos, r".*")
    assert result == []


def test_exclude_empty_repo_list_returns_empty():
    """Empty input must always return empty, regardless of pattern."""
    assert filter_repos_by_exclude([], r".*") == []
    assert filter_repos_by_exclude([], None) == []


def test_invalid_exclude_regexp_raises():
    """Invalid regexp must raise re.error, not silently pass."""
    repos = _make_repos("app")
    with pytest.raises(re.error):
        filter_repos_by_exclude(repos, r"([unclosed")


# ---------------------------------------------------------------------------
# load_cleanup_config — exclude_repo field parsing
# ---------------------------------------------------------------------------

def test_load_config_with_exclude_repo(tmp_path, monkeypatch):
    """exclude_repo field must be parsed and returned."""
    config_path = _write_config(tmp_path, """
        exclude_repo: "devsec/.*"
        cleanup_rules:
          cache_rule:
            regexp: ".*-cache:.*"
            keep_latest: 10
    """)
    monkeypatch.setenv("CLEAN_CONFIG_PATH", config_path)

    config = load_cleanup_config()

    assert config["exclude_repo"] == "devsec/.*"
    assert "cache_rule" in config["cleanup_rules"]


def test_load_config_without_exclude_repo_defaults_to_none(tmp_path, monkeypatch):
    """When exclude_repo is absent, it must default to None."""
    config_path = _write_config(tmp_path, """
        cleanup_rules:
          cache_rule:
            regexp: ".*-cache:.*"
            keep_latest: 10
    """)
    monkeypatch.setenv("CLEAN_CONFIG_PATH", config_path)

    config = load_cleanup_config()

    assert config["exclude_repo"] is None


def test_load_config_invalid_exclude_repo_exits(tmp_path, monkeypatch):
    """Invalid regexp in exclude_repo must cause sys.exit(1) at load time."""
    config_path = _write_config(tmp_path, """
        exclude_repo: "([unclosed"
        cleanup_rules:
          cache_rule:
            regexp: ".*-cache:.*"
            keep_latest: 10
    """)
    monkeypatch.setenv("CLEAN_CONFIG_PATH", config_path)

    with pytest.raises(SystemExit) as exc_info:
        load_cleanup_config()

    assert exc_info.value.code == 1


def test_load_config_empty_exclude_repo_treated_as_none(tmp_path, monkeypatch):
    """Empty string exclude_repo must be normalised to None."""
    config_path = _write_config(tmp_path, """
        exclude_repo: ""
        cleanup_rules:
          cache_rule:
            regexp: ".*-cache:.*"
            keep_latest: 10
    """)
    monkeypatch.setenv("CLEAN_CONFIG_PATH", config_path)

    config = load_cleanup_config()

    assert config["exclude_repo"] is None
