"""
Tests for the deletion loop in main():
- loop must complete all iterations even when delete_image returns False
- sys.exit(1) must be called only once, at the very end
"""
from unittest.mock import MagicMock, patch

import pytest

MODULE = "cleanup_registry"


def _make_image(digest: str, tag: str) -> dict:
    return {"digest": digest, "tags": tag}


def _run_main_with_delete_results(delete_side_effect):
    """Patch all external dependencies and run main(); return (delete_mock, exit_mock, logger_mock)."""
    fake_repos = [{"name": "repo-a"}, {"name": "repo-b"}]
    fake_images_per_repo = [_make_image("sha256:aaa111", "v1"), _make_image("sha256:bbb222", "v2")]

    delete_mock = MagicMock(side_effect=delete_side_effect)
    exit_mock = MagicMock()
    logger_mock = MagicMock()

    with (
        patch(f"{MODULE}.setup_logging"),
        patch(f"{MODULE}.Settings", return_value=MagicMock(registry_id="reg-1", dry_run=False, validate=MagicMock())),
        patch(f"{MODULE}.create_session", return_value=MagicMock()),
        patch(f"{MODULE}.load_cleanup_config", return_value={"cleanup_rules": [], "exclude_repo": []}),
        patch(f"{MODULE}.get_auth_token", return_value="token-abc"),
        patch(f"{MODULE}.get_repositories", return_value=fake_repos),
        patch(f"{MODULE}.filter_repos_by_exclude", side_effect=lambda repos, _: repos),
        patch(f"{MODULE}.get_images", return_value=[]),
        patch(f"{MODULE}.select_images_to_delete", return_value=fake_images_per_repo),
        patch(f"{MODULE}.delete_image", delete_mock),
        patch(f"{MODULE}.logger", logger_mock),
        patch(f"{MODULE}.sys") as mock_sys,
    ):
        mock_sys.exit = exit_mock
        from cleanup_registry import main
        main()

    return delete_mock, exit_mock, logger_mock


def _critical_message(logger_mock) -> str:
    """Extract the single logger.critical call argument."""
    logger_mock.critical.assert_called_once()
    return logger_mock.critical.call_args[0][0]


def test_all_failures_loop_completes_then_exits():
    """When every delete_image returns False the loop must process all images
    (2 repos × 2 images = 4 calls) and call sys.exit(1) exactly once at the end."""
    delete_mock, exit_mock, logger_mock = _run_main_with_delete_results([False, False, False, False])

    assert delete_mock.call_count == 4, "loop must not short-circuit on False"
    exit_mock.assert_called_once_with(1)
    assert "4 failed deletion(s)" in _critical_message(logger_mock)


def test_partial_failures_loop_still_completes():
    """When only some delete_image calls return False the loop still processes
    all images; sys.exit(1) is called once at the end, not mid-loop."""
    # repo-a img1=True, img2=False | repo-b img1=False, img2=True
    delete_mock, exit_mock, logger_mock = _run_main_with_delete_results([True, False, False, True])

    assert delete_mock.call_count == 4, "loop must not short-circuit on False"
    exit_mock.assert_called_once_with(1)
    assert "2 failed deletion(s)" in _critical_message(logger_mock)
