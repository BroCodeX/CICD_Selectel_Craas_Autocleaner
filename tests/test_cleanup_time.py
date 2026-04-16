from datetime import datetime, timezone

from core.cleanup_executor import select_images_to_delete


NOW_UTC = datetime(2026, 3, 4, 0, 0, tzinfo=timezone.utc)


def test_remove_older_applies_after_keep_latest():
    rules = {
        "all_release": {
            "regexp": r".*:.*-release-.*",
            "keep_latest": 1,
            "remove_older": 7,
        }
    }

    images = [
        {"digest": "sha256:newest", "createdAt": "2026-03-03T00:00:00Z", "tags": ["x-release-3"]},
        {"digest": "sha256:fresh", "createdAt": "2026-03-01T00:00:00Z", "tags": ["x-release-2"]},
        {"digest": "sha256:old", "createdAt": "2026-02-20T00:00:00Z", "tags": ["x-release-1"]},
    ]

    to_delete = select_images_to_delete(
        repo_name="logistics-service",
        images=images,
        cleanup_rules=rules,
        now=NOW_UTC,
    )

    assert [i["digest"] for i in to_delete] == ["sha256:old"]


def test_keep_latest_is_never_deleted_even_if_old():
    rules = {
        "all_release": {
            "regexp": r".*:.*-release-.*",
            "keep_latest": 2,
            "remove_older": 1,
        }
    }

    images = [
        {"digest": "sha256:1", "createdAt": "2026-02-21T00:00:00Z", "tags": ["x-release-3"]},
        {"digest": "sha256:2", "createdAt": "2026-02-19T00:00:00Z", "tags": ["x-release-2"]},
        {"digest": "sha256:3", "createdAt": "2026-02-18T00:00:00Z", "tags": ["x-release-1"]},
    ]

    to_delete = select_images_to_delete(
        repo_name="logistics-service",
        images=images,
        cleanup_rules=rules,
        now=NOW_UTC,
    )

    assert [i["digest"] for i in to_delete] == ["sha256:3"]


def test_missing_remove_older_uses_only_remove_older_default():
    rules = {
        "all_release": {
            "regexp": r".*:.*-release-.*",
            "keep_latest": 1,
        }
    }

    images = [
        {"digest": "sha256:newest", "createdAt": "2026-03-03T00:00:00Z", "tags": ["x-release-3"]},
        {"digest": "sha256:not-old-enough", "createdAt": "2026-02-28T00:00:00Z", "tags": ["x-release-2"]},
        {"digest": "sha256:old", "createdAt": "2026-02-10T00:00:00Z", "tags": ["x-release-1"]},
    ]

    to_delete = select_images_to_delete(
        repo_name="logistics-service",
        images=images,
        cleanup_rules=rules,
        now=NOW_UTC,
    )

    assert [i["digest"] for i in to_delete] == ["sha256:old"]


def test_missing_keep_latest_uses_only_keep_latest_default():
    rules = {
        "all_release": {
            "regexp": r".*:.*-release-.*",
            "remove_older": 7,
        }
    }

    images = [
        {"digest": f"sha256:{n}", "createdAt": f"2026-01-{n:02d}T00:00:00Z", "tags": [f"x-release-{n}"]}
        for n in range(1, 13)
    ]

    to_delete = select_images_to_delete(
        repo_name="logistics-service",
        images=images,
        cleanup_rules=rules,
        now=NOW_UTC,
    )

    assert [i["digest"] for i in to_delete] == ["sha256:2", "sha256:1"]


def test_invalid_created_at_is_not_deleted_by_age():
    rules = {
        "all_release": {
            "regexp": r".*:.*-release-.*",
            "keep_latest": 0,
            "remove_older": 1,
        }
    }

    images = [
        {"digest": "sha256:bad-date", "createdAt": "not-a-date", "tags": ["x-release-2"]},
        {"digest": "sha256:fresh", "createdAt": "2026-03-01T00:00:00Z", "tags": ["x-release-1"]},
        {"digest": "sha256:old", "createdAt": "2026-02-20T00:00:00Z", "tags": ["x-release-1"]},
    ]

    to_delete = select_images_to_delete(
        repo_name="logistics-service",
        images=images,
        cleanup_rules=rules,
        now=NOW_UTC,
    )

    assert [i["digest"] for i in to_delete] == [ "sha256:fresh", "sha256:old"]
