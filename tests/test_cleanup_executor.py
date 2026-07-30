from config.logger_config import setup_logging
from core.cleanup_executor import select_images_to_delete

setup_logging()


def test_select_images_to_delete_by_rule_keep_latest():
    rules = {
        "logistics_review": {
            "regexp": r"logistics-service:.*-review-.*",
            "keep_latest": 1,
            "remove_older": 0,
        }
    }

    images = [
        {
            "digest": "sha256:3",
            "createdAt": "2026-03-01T08:00:00Z",
            "tags": ["x-review-003"],
        },
        {
            "digest": "sha256:2",
            "createdAt": "2026-03-01T09:00:00Z",
            "tags": ["x-review-002"],
        },
        {
            "digest": "sha256:1",
            "createdAt": "2026-03-01T10:00:00Z",
            "tags": ["x-review-001"],
        },
    ]

    to_delete = select_images_to_delete(
        repo_name="logistics-service",
        images=images,
        cleanup_rules=rules,
    )

    assert [i["digest"] for i in to_delete] == ["sha256:2", "sha256:3"]


# ---------------------------------------------------------------------------
# save_regexps integration tests
# ---------------------------------------------------------------------------

def test_save_regexps_absent_backward_compatible():
    rules = {
        "review": {
            "regexp": r"logistics-service:.*-review-.*",
            "keep_latest": 1,
            "remove_older": 0,
        }
    }
    images = [
        {"digest": "sha256:3", "createdAt": "2026-03-01T08:00:00Z", "tags": ["x-review-003"]},
        {"digest": "sha256:2", "createdAt": "2026-03-01T09:00:00Z", "tags": ["x-review-002"]},
        {"digest": "sha256:1", "createdAt": "2026-03-01T10:00:00Z", "tags": ["x-review-001"]},
    ]
    to_delete = select_images_to_delete(
        repo_name="logistics-service",
        images=images,
        cleanup_rules=rules,
    )
    assert [i["digest"] for i in to_delete] == ["sha256:2", "sha256:3"]


def test_save_regexps_empty_list_backward_compatible():
    rules = {
        "review": {
            "regexp": r"logistics-service:.*-review-.*",
            "keep_latest": 1,
            "remove_older": 0,
            "save_regexps": [],
        }
    }
    images = [
        {"digest": "sha256:3", "createdAt": "2026-03-01T08:00:00Z", "tags": ["x-review-003"]},
        {"digest": "sha256:2", "createdAt": "2026-03-01T09:00:00Z", "tags": ["x-review-002"]},
        {"digest": "sha256:1", "createdAt": "2026-03-01T10:00:00Z", "tags": ["x-review-001"]},
    ]
    to_delete = select_images_to_delete(
        repo_name="logistics-service",
        images=images,
        cleanup_rules=rules,
    )
    assert [i["digest"] for i in to_delete] == ["sha256:2", "sha256:3"]


def test_save_regexps_splits_both_groups_get_keep_latest():
    """Saved and non-saved groups each get their own keep_latest allocation."""
    rules = {
        "review": {
            "regexp": r"gf-stock/app:.*",
            "keep_latest": 1,
            "remove_older": 0,
            "save_regexps": [r".*\:review-clusters-.*"],
        }
    }
    images = [
        {"digest": "sha256:a", "createdAt": "2026-03-01T10:00:00Z", "tags": ["review-clusters-005"]},
        {"digest": "sha256:b", "createdAt": "2026-03-01T09:00:00Z", "tags": ["review-clusters-004"]},
        {"digest": "sha256:c", "createdAt": "2026-03-01T08:00:00Z", "tags": ["review-clusters-003"]},
        {"digest": "sha256:d", "createdAt": "2026-03-01T10:00:00Z", "tags": ["review-master-005"]},
        {"digest": "sha256:e", "createdAt": "2026-03-01T09:00:00Z", "tags": ["review-master-004"]},
        {"digest": "sha256:f", "createdAt": "2026-03-01T08:00:00Z", "tags": ["review-master-003"]},
    ]
    to_delete = select_images_to_delete(
        repo_name="gf-stock/app",
        images=images,
        cleanup_rules=rules,
    )
    digests = {i["digest"] for i in to_delete}
    assert digests == {"sha256:b", "sha256:c", "sha256:e", "sha256:f"}


def test_save_regexps_keep_latest_zero_deletes_from_both():
    """With keep_latest=0 both groups delete all images (save_regexps follows keep_latest)."""
    rules = {
        "review": {
            "regexp": r"gf-stock/app:.*",
            "keep_latest": 0,
            "remove_older": 0,
            "save_regexps": [r".*\:review-clusters-.*"],
        }
    }
    images = [
        {"digest": "sha256:a", "createdAt": "2026-02-01T00:00:00Z", "tags": ["review-clusters-001"]},
        {"digest": "sha256:b", "createdAt": "2026-01-01T00:00:00Z", "tags": ["review-whmsk1-001"]},
    ]
    to_delete = select_images_to_delete(
        repo_name="gf-stock/app",
        images=images,
        cleanup_rules=rules,
    )
    digests = {i["digest"] for i in to_delete}
    assert digests == {"sha256:a", "sha256:b"}


def test_user_scenario_exact():
    """User's example: 3 saved + 3 non-saved, keep_latest=1 each."""
    rules = {
        "review": {
            "regexp": r"gf-stock/app:.*",
            "keep_latest": 1,
            "remove_older": 0,
            "save_regexps": [r"review-clusters-.+"],
        }
    }
    images = [
        {"digest": "sha256:1", "createdAt": "2026-03-01T10:00:00Z", "tags": ["review-clusters-005"]},
        {"digest": "sha256:2", "createdAt": "2026-03-01T09:00:00Z", "tags": ["review-clusters-004"]},
        {"digest": "sha256:3", "createdAt": "2026-03-01T08:00:00Z", "tags": ["review-clusters-003"]},
        {"digest": "sha256:4", "createdAt": "2026-03-01T10:00:00Z", "tags": ["review-master-005"]},
        {"digest": "sha256:5", "createdAt": "2026-03-01T09:00:00Z", "tags": ["review-master-004"]},
        {"digest": "sha256:6", "createdAt": "2026-03-01T08:00:00Z", "tags": ["review-master-003"]},
    ]
    to_delete = select_images_to_delete(
        repo_name="gf-stock/app",
        images=images,
        cleanup_rules=rules,
    )
    digests = {i["digest"] for i in to_delete}
    assert digests == {"sha256:2", "sha256:3", "sha256:5", "sha256:6"}


def test_save_regexps_with_keep_latest():
    """1 saved image + 3 non-saved, keep_latest=1 protects newest from each group."""
    rules = {
        "review": {
            "regexp": r"gf-stock/app:.*",
            "keep_latest": 1,
            "remove_older": 0,
            "save_regexps": [r".*\:review-clusters-.*"],
        }
    }
    images = [
        {"digest": "sha256:a", "createdAt": "2026-03-01T10:00:00Z", "tags": ["review-clusters-003"]},
        {"digest": "sha256:b", "createdAt": "2026-03-01T09:00:00Z", "tags": ["review-other-003"]},
        {"digest": "sha256:c", "createdAt": "2026-03-01T08:00:00Z", "tags": ["review-other-002"]},
        {"digest": "sha256:d", "createdAt": "2026-03-01T07:00:00Z", "tags": ["review-other-001"]},
    ]
    to_delete = select_images_to_delete(
        repo_name="gf-stock/app",
        images=images,
        cleanup_rules=rules,
    )
    assert [i["digest"] for i in to_delete] == ["sha256:c", "sha256:d"]


def test_multiple_rules_with_save_regexps():
    """Two rules, save_regexps splits each into two groups with keep_latest."""
    rules = {
        "review_clusters": {
            "regexp": r".*\:review-.*",
            "keep_latest": 1,
            "remove_older": 0,
            "save_regexps": [r"review-clusters-.+"],
        },
        "master_builds": {
            "regexp": r".*\:master-.*",
            "keep_latest": 1,
            "remove_older": 0,
            "save_regexps": [r"master-whmsk1-.+"],
        },
    }
    images = [
        {"digest": "sha256:a", "createdAt": "2026-03-01T10:00:00Z", "tags": ["review-clusters-001"]},
        {"digest": "sha256:b", "createdAt": "2026-03-01T09:00:00Z", "tags": ["review-clusters-002"]},
        {"digest": "sha256:c", "createdAt": "2026-03-01T08:00:00Z", "tags": ["review-other-001"]},
        {"digest": "sha256:d", "createdAt": "2026-03-01T07:00:00Z", "tags": ["review-other-002"]},
        {"digest": "sha256:e", "createdAt": "2026-03-01T10:00:00Z", "tags": ["master-whmsk1-001"]},
        {"digest": "sha256:f", "createdAt": "2026-03-01T09:00:00Z", "tags": ["master-other-001"]},
        {"digest": "sha256:g", "createdAt": "2026-03-01T08:00:00Z", "tags": ["master-other-002"]},
    ]
    to_delete = select_images_to_delete(
        repo_name="gf-stock/app",
        images=images,
        cleanup_rules=rules,
    )
    digests = {i["digest"] for i in to_delete}
    assert digests == {"sha256:b", "sha256:d", "sha256:g"}
