import re

import pytest

from core.cleanup_rules_parser import split_images_by_rules, filter_saved_images


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


def test_untagged_image_no_tags_field_goes_to_unmatched():
    """Image without 'tags' key at all must fall into unmatched, not silently disappear."""
    rules = {
        "catch_all": {
            "regexp": r".*:.*",
            "keep_latest": 1,
        }
    }
    images = [
        {
            "digest": "sha256:untagged-1",
            "createdAt": "2026-03-01T10:00:00Z",
            # no 'tags' key
        }
    ]

    grouped, unmatched = split_images_by_rules("myapp", images, rules)

    assert grouped["catch_all"] == []
    assert [i["digest"] for i in unmatched] == ["sha256:untagged-1"]


def test_untagged_image_empty_tags_goes_to_unmatched():
    """Image with tags=[] must fall into unmatched."""
    rules = {
        "catch_all": {
            "regexp": r".*:.*",
            "keep_latest": 1,
        }
    }
    images = [
        {
            "digest": "sha256:untagged-2",
            "createdAt": "2026-03-01T10:00:00Z",
            "tags": [],
        }
    ]

    grouped, unmatched = split_images_by_rules("myapp", images, rules)

    assert grouped["catch_all"] == []
    assert [i["digest"] for i in unmatched] == ["sha256:untagged-2"]


def test_untagged_image_null_tags_goes_to_unmatched():
    """Image with tags=null must fall into unmatched."""
    rules = {
        "catch_all": {
            "regexp": r".*:.*",
            "keep_latest": 1,
        }
    }
    images = [
        {
            "digest": "sha256:untagged-3",
            "createdAt": "2026-03-01T10:00:00Z",
            "tags": None,
        }
    ]

    grouped, unmatched = split_images_by_rules("myapp", images, rules)

    assert grouped["catch_all"] == []
    assert [i["digest"] for i in unmatched] == ["sha256:untagged-3"]


def test_mix_tagged_and_untagged_images():
    """Untagged images must not interfere with matching of tagged images."""
    rules = {
        "catch_all": {
            "regexp": r".*:.*",
            "keep_latest": 5,
        }
    }
    images = [
        {
            "digest": "sha256:tagged",
            "createdAt": "2026-03-01T10:00:00Z",
            "tags": ["v1.0"],
        },
        {
            "digest": "sha256:untagged",
            "createdAt": "2026-03-01T09:00:00Z",
            "tags": [],
        },
    ]

    grouped, unmatched = split_images_by_rules("myapp", images, rules)

    assert [i["digest"] for i in grouped["catch_all"]] == ["sha256:tagged"]
    assert [i["digest"] for i in unmatched] == ["sha256:untagged"]


# ---------------------------------------------------------------------------
# filter_saved_images — unit tests (sequential AND, returns images to protect)
# ---------------------------------------------------------------------------

def test_save_regexps_none_returns_empty():
    images = [
        {"digest": "sha256:1", "tags": ["master-001"]},
        {"digest": "sha256:2", "tags": ["review-001"]},
    ]
    result = filter_saved_images("gf-stock/app", images, None)
    assert result == []


def test_save_regexps_empty_list_returns_empty():
    images = [
        {"digest": "sha256:1", "tags": ["master-001"]},
        {"digest": "sha256:2", "tags": ["review-001"]},
    ]
    result = filter_saved_images("gf-stock/app", images, [])
    assert result == []


def test_single_save_regexp_filters_images():
    images = [
        {"digest": "sha256:1", "tags": ["review-clusters-370252"]},
        {"digest": "sha256:2", "tags": ["review-whmsk1-370040"]},
        {"digest": "sha256:3", "tags": ["master-whmsk1-370040"]},
    ]
    result = filter_saved_images(
        "gf-stock/app",
        images,
        [r".*\:review-.*"],
    )
    assert [i["digest"] for i in result] == ["sha256:1", "sha256:2"]


def test_two_save_regexps_narrows_sequentially():
    images = [
        {"digest": "sha256:1", "tags": ["review-clusters-370252"]},
        {"digest": "sha256:2", "tags": ["review-whmsk1-370040"]},
        {"digest": "sha256:3", "tags": ["master-whmsk1-370040"]},
    ]
    result = filter_saved_images(
        "gf-stock/app",
        images,
        [r".*\:review-.*", r"review-clusters-.+"],
    )
    assert [i["digest"] for i in result] == ["sha256:1"]


def test_three_save_regexps_narrows_fully():
    images = [
        {"digest": "sha256:a", "tags": ["review-clusters-370252"]},
        {"digest": "sha256:b", "tags": ["review-clusters-370040"]},
    ]
    result = filter_saved_images(
        "myapp",
        images,
        [r".*\:review-.*", r".*clusters-.*", r".*-370252$"],
    )
    assert [i["digest"] for i in result] == ["sha256:a"]


def test_save_regexp_matches_nothing_returns_empty():
    images = [
        {"digest": "sha256:1", "tags": ["release-001"]},
        {"digest": "sha256:2", "tags": ["release-002"]},
    ]
    result = filter_saved_images(
        "myapp",
        images,
        [r"no-match-at-all"],
    )
    assert result == []


def test_save_regexp_pass_all_then_none():
    images = [
        {"digest": "sha256:1", "tags": ["release-001"]},
        {"digest": "sha256:2", "tags": ["release-002"]},
    ]
    result = filter_saved_images(
        "myapp",
        images,
        [r".*", r"no-match"],
    )
    assert result == []


def test_untagged_image_filtered_out_by_save_regexp():
    images = [
        {"digest": "sha256:1", "tags": ["release-001"]},
        {"digest": "sha256:2", "tags": []},
    ]
    result = filter_saved_images(
        "myapp",
        images,
        [r".*\:release-.*"],
    )
    assert [i["digest"] for i in result] == ["sha256:1"]


def test_invalid_save_regexp_raises():
    images = [
        {"digest": "sha256:1", "tags": ["tag"]},
    ]
    with pytest.raises(re.error):
        filter_saved_images(
            "myapp",
            images,
            [r"([unclosed"],
        )


def test_save_regexp_only_matches_repo_name():
    images = [
        {"digest": "sha256:1", "tags": ["v1"]},
        {"digest": "sha256:2", "tags": ["v2"]},
    ]
    result = filter_saved_images(
        "gf-stock/app",
        images,
        [r"^gf-stock/app:.*"],
    )
    assert [i["digest"] for i in result] == ["sha256:1", "sha256:2"]


def test_save_regexp_wrong_repo_no_match():
    images = [
        {"digest": "sha256:1", "tags": ["v1"]},
    ]
    result = filter_saved_images(
        "gf-mcp/app",
        images,
        [r"^gf-stock/app:.*"],
    )
    assert result == []
