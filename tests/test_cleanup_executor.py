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
