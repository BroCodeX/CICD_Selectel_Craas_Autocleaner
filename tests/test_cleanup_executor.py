import os

from cleanup_executor import select_images_to_delete
from cleanup_repository import delete_image

os.environ.setdefault("SEL_REGISTRY_ID", "9975a430-0fd7-4ceb-a1c4-0e73a403ab57")


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


def test_delete_image_calls_registry_api():
    class FakeResponse:
        status_code = 204
        text = ""

    class FakeSession:
        def __init__(self):
            self.url = None

        def delete(self, url, headers=None, timeout=0):
            self.url = url
            return FakeResponse()

    session = FakeSession()

    delete_image(
        session=session,
        base_url="https://cr.selcloud.ru/api/v1",
        registry_id=os.environ["SEL_REGISTRY_ID"],
        token="token-1",
        repo_name="logistics-service",
        digest="sha256:abc",
        dry_run=False,
    )

    assert session.url == (
        f"https://cr.selcloud.ru/api/v1/registries/{os.environ['SEL_REGISTRY_ID']}/"
        "repositories/logistics-service/sha256:abc"
    )
