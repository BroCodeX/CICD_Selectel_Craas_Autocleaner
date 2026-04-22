"""
Tests for the cleanup loop in main():
- bulk cleanup is called once per repo (not once per image)
- the loop processes every repo even when some cleanup calls fail
- sys.exit(1) is called exactly once at the end, with the count of failed repos
"""
from unittest.mock import MagicMock, patch

MODULE = "cleanup_registry"


def _make_image(digest: str, tag: str) -> dict:
    return {"digest": digest, "tags": tag}


def _run_main_with_cleanup_results(cleanup_side_effect, dry_run=False, repo_count=2):
    """Patch all external dependencies and run main();
    returns (cleanup_mock, exit_mock, logger_mock, sleep_mock)."""
    fake_repos = [{"name": f"repo-{i}"} for i in range(repo_count)]
    fake_to_delete = [_make_image("sha256:aaa111", "v1"), _make_image("sha256:bbb222", "v2")]

    cleanup_mock = MagicMock(side_effect=cleanup_side_effect)
    exit_mock = MagicMock()
    logger_mock = MagicMock()
    sleep_mock = MagicMock()

    with (
        patch(f"{MODULE}.setup_logging"),
        patch(f"{MODULE}.Settings", return_value=MagicMock(registry_id="reg-1", dry_run=dry_run, validate=MagicMock())),
        patch(f"{MODULE}.create_session", return_value=MagicMock()),
        patch(f"{MODULE}.load_cleanup_config", return_value={
            "cleanup_rules": [],
            "exclude_repo": [],
            "cleanup_defaults": {"keep_latest": 10, "remove_older": 14},
            "unmatched_defaults": {"keep_latest": 10, "remove_older": 14},
        }),
        patch(f"{MODULE}.get_auth_token", return_value="token-abc"),
        patch(f"{MODULE}.get_repositories", return_value=fake_repos),
        patch(f"{MODULE}.filter_repos_by_exclude", side_effect=lambda repos, _: repos),
        patch(f"{MODULE}.get_images", return_value=[]),
        patch(f"{MODULE}.select_images_to_delete", return_value=fake_to_delete),
        patch(f"{MODULE}.cleanup_repository", cleanup_mock),
        patch(f"{MODULE}.logger", logger_mock),
        patch(f"{MODULE}.time.sleep", sleep_mock),
        patch(f"{MODULE}.sys") as mock_sys,
    ):
        mock_sys.exit = exit_mock
        from cleanup_registry import main
        main()

    return cleanup_mock, exit_mock, logger_mock, sleep_mock


def _critical_message(logger_mock) -> str:
    """Extract the single logger.critical call argument."""
    logger_mock.critical.assert_called_once()
    return logger_mock.critical.call_args[0][0]


def test_cleanup_called_once_per_repo_with_full_image_batch():
    """Bulk endpoint must be invoked exactly once per repo and receive the
    entire to-delete list — not per-image as the previous implementation did."""
    cleanup_mock, exit_mock, _, _ = _run_main_with_cleanup_results([True, True])

    assert cleanup_mock.call_count == 2, "cleanup must be called once per repo"
    for call in cleanup_mock.call_args_list:
        kwargs = call.kwargs
        assert len(kwargs["images"]) == 2, "every call must receive the full image batch"
    exit_mock.assert_not_called()


def test_all_failures_loop_completes_then_exits():
    """When every cleanup_repository call returns False the loop must process
    all repos (2) and call sys.exit(1) exactly once at the end."""
    cleanup_mock, exit_mock, logger_mock, _ = _run_main_with_cleanup_results([False, False])

    assert cleanup_mock.call_count == 2, "loop must not short-circuit on False"
    exit_mock.assert_called_once_with(1)
    assert "2 failed repo(s)" in _critical_message(logger_mock)


def test_partial_failures_loop_still_completes():
    """When only some cleanup calls fail the loop still processes every repo;
    sys.exit(1) is called once at the end, not mid-loop."""
    cleanup_mock, exit_mock, logger_mock, _ = _run_main_with_cleanup_results([True, False])

    assert cleanup_mock.call_count == 2, "loop must not short-circuit on False"
    exit_mock.assert_called_once_with(1)
    assert "1 failed repo(s)" in _critical_message(logger_mock)


# ---------------------------------------------------------------------------
# Inter-repo delay
# ---------------------------------------------------------------------------

def test_sleep_between_repos_uses_default_delay():
    """Between repo iterations the loop must sleep REPO_CLEANUP_DELAY_SEC seconds
    so the registry GC has time to settle before the next bulk cleanup."""
    from cleanup_registry import REPO_CLEANUP_DELAY_SEC

    _, _, _, sleep_mock = _run_main_with_cleanup_results(
        [True, True, True], repo_count=3,
    )

    # 3 repos -> 2 inter-iteration sleeps
    assert sleep_mock.call_count == 2
    for call in sleep_mock.call_args_list:
        assert call.args == (REPO_CLEANUP_DELAY_SEC,)


def test_no_sleep_after_last_repo():
    """A single repo must produce zero sleeps (no next iteration)."""
    _, _, _, sleep_mock = _run_main_with_cleanup_results([True], repo_count=1)
    sleep_mock.assert_not_called()


def test_sleep_skipped_in_dry_run():
    """In dry-run mode no API calls are made, so the inter-repo delay is pointless."""
    _, _, _, sleep_mock = _run_main_with_cleanup_results(
        [True, True, True], dry_run=True, repo_count=3,
    )
    sleep_mock.assert_not_called()


def test_sleep_still_runs_after_failed_cleanup():
    """A failed cleanup must not skip the delay — the next iteration still hits the API."""
    _, _, _, sleep_mock = _run_main_with_cleanup_results([False, True], repo_count=2)
    assert sleep_mock.call_count == 1


def test_default_delay_is_20_seconds():
    """Default lag is 20s per task requirements."""
    from cleanup_registry import REPO_CLEANUP_DELAY_SEC
    assert REPO_CLEANUP_DELAY_SEC == 20
