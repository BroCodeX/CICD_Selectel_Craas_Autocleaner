import re

import pytest

from cleanup_rules_parser import split_images_by_rules


def test_priority_top_rule_wins():
    rules = {
        "logistics_release_app": {
            "regexp": r"logistics-service:.*-release-.*-app-.*",
            "keep_latest": 10,
        },
        "logistics_release_nginx": {
            "regexp": r"logistics-service:.*-release-.*-nginx-.*",
            "keep_latest": 10,
        },
        "logistics_review": {
            "regexp": r"logistics-service:.*-review-.*",
            "keep_latest": 10,
        },
        "all_release": {
            "regexp": r".*:.*-release-.*",
            "keep_latest": 5,
        },
        "all_review": {
            "regexp": r".*:.*-review-.*",
            "keep_latest": 5,
        },
    }

    images = [
        {
            "digest": "sha256:1",
            "createdAt": "2026-03-01T10:00:00Z",
            "tags": ["abc-release-123-app-1"],
        },
        {
            "digest": "sha256:2",
            "createdAt": "2026-03-01T09:00:00Z",
            "tags": ["abc-release-123-service-1"],
        },
        {
            "digest": "sha256:3",
            "createdAt": "2026-03-01T08:00:00Z",
            "tags": ["abc-release-123-nginx-1"],
        },
        {
            "digest": "sha256:4",
            "createdAt": "2026-03-01T07:00:00Z",
            "tags": ["abc-review-123-api-1"],
        },
        {
            "digest": "sha256:5",
            "createdAt": "2026-03-01T06:00:00Z",
            "tags": ["abc-hotfix-123"],
        },
    ]

    grouped, unmatched = split_images_by_rules("logistics-service", images, rules)

    assert [i["digest"] for i in grouped["logistics_release_app"]] == ["sha256:1"]
    assert [i["digest"] for i in grouped["all_release"]] == ["sha256:2"]
    assert [i["digest"] for i in grouped["logistics_release_nginx"]] == ["sha256:3"]
    assert [i["digest"] for i in grouped["logistics_review"]] == ["sha256:4"]
    assert grouped["all_review"] == []
    assert [i["digest"] for i in unmatched] == ["sha256:5"]


def test_rule_matches_only_repo_name_admin_gf():
    rules = {
        "admin_repo_rule": {
            "regexp": r"^admin-gf:.*$",
            "keep_latest": 2,
        }
    }

    images = [
        {
            "digest": "sha256:10",
            "createdAt": "2026-03-01T10:00:00Z",
            "tags": ["latest"],
        },
        {
            "digest": "sha256:12",
            "createdAt": "2026-03-01T14:00:00Z",
            "tags": ["latest-2"],
        },
    ]

    grouped, unmatched = split_images_by_rules("admin-gf", images, rules)

    assert [i["digest"] for i in grouped["admin_repo_rule"]] == ["sha256:10", "sha256:12"]
    assert unmatched == []


def test_null_or_missing_regexp_do_not_match():
    rules = {
        "null_regexp_rule": {
            "regexp": None,
            "keep_latest": 1,
        },
        "missing_regexp_rule": {
            "keep_latest": 1,
        },
    }

    images = [
        {
            "digest": "sha256:20",
            "createdAt": "2026-03-01T10:00:00Z",
            "tags": ["None-release-1"],
        }
    ]

    grouped, unmatched = split_images_by_rules("admin-gf", images, rules)

    assert grouped["null_regexp_rule"] == []
    assert grouped["missing_regexp_rule"] == []
    assert [i["digest"] for i in unmatched] == ["sha256:20"]


def test_invalid_regexp_raises_error():
    rules = {
        "broken_rule": {
            "regexp": r"([",
            "keep_latest": 1,
        }
    }
    images = [
        {
            "digest": "sha256:30",
            "createdAt": "2026-03-01T10:00:00Z",
            "tags": ["latest"],
        }
    ]

    with pytest.raises(re.error):
        split_images_by_rules("admin-gf", images, rules)
