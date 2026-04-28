"""
Tests for the bulk cleanup endpoint:
POST /v1/registries/{id}/repositories/{name}/cleanup

Body:
    {
        "digests": [...],
        "tags": [...],
        "disable_gc": false
    }

Bulk cleanup is preferred over per-image DELETE because the per-image flow
breaks the remote API and garbage collector when called too many times in a row.

URL encoding rules (same as the previous DELETE flow):
- Slashes inside repo names must be percent-encoded (%2F), otherwise the API
  interprets the extra path segments as part of the route.
"""
from unittest.mock import call, patch

import pytest

from config.logger_config import setup_logging
from clients.cleanup_repository import get_images, cleanup_repository, init_gc

setup_logging()

BASE_URL = "https://cr.selcloud.ru/api/v1"
REGISTRY_ID = "9975a430-0fd7-4ceb-a1c4-0e73a403ab57"
TOKEN = "test-token"


@pytest.fixture(autouse=True)
def sleep_mock():
    """Silence real time.sleep in every test so retry loops run instantly.
    Tests can request this fixture as an argument to inspect sleep calls."""
    with patch("clients.cleanup_repository.time.sleep") as m:
        yield m


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class FakeResponse:
    def __init__(self, status_code=200, body=None):
        self.status_code = status_code
        self._body = body if body is not None else []
        self.text = str(self._body)

    def json(self):
        return self._body

    def raise_for_status(self):
        pass


class FakeSession:
    """Records the last URL/payload used for GET / POST calls.

    Construct with either a single response (reused for every call) or a
    `responses` queue (one response per call, in order) to exercise retries.
    """

    def __init__(self, response: FakeResponse | None = None, responses=None):
        self._response = response
        self._responses = list(responses) if responses is not None else None
        self.last_get_url: str | None = None
        self.last_post_url: str | None = None
        self.last_post_json: dict | None = None
        self.last_post_params: dict | None = None
        self.post_call_count: int = 0

    def get(self, url, headers=None, timeout=None):
        self.last_get_url = url
        return self._response

    def post(self, url, headers=None, json=None, params=None, timeout=None):
        self.last_post_url = url
        self.last_post_json = json
        self.last_post_params = params
        self.post_call_count += 1
        if self._responses is not None:
            return self._responses.pop(0)
        return self._response


def _img(digest, tags):
    return {"digest": digest, "tags": tags}


# ---------------------------------------------------------------------------
# get_images URL encoding (kept from the previous suite — still relevant)
# ---------------------------------------------------------------------------

def test_get_images_simple_repo_url():
    session = FakeSession(FakeResponse(200, []))
    get_images(session, BASE_URL, REGISTRY_ID, TOKEN, "myapp")

    expected = f"{BASE_URL}/registries/{REGISTRY_ID}/repositories/myapp/images"
    assert session.last_get_url == expected


def test_get_images_slash_repo_url_encoded():
    session = FakeSession(FakeResponse(200, []))
    get_images(session, BASE_URL, REGISTRY_ID, TOKEN, "build/buildkit-test")

    expected = f"{BASE_URL}/registries/{REGISTRY_ID}/repositories/build%2Fbuildkit-test/images"
    assert session.last_get_url == expected


def test_get_images_double_slash_repo_url_encoded():
    session = FakeSession(FakeResponse(200, []))
    get_images(session, BASE_URL, REGISTRY_ID, TOKEN, "a/b/c")

    expected = f"{BASE_URL}/registries/{REGISTRY_ID}/repositories/a%2Fb%2Fc/images"
    assert session.last_get_url == expected


# ---------------------------------------------------------------------------
# cleanup_repository — URL
# ---------------------------------------------------------------------------

def test_cleanup_repository_simple_repo_url():
    session = FakeSession(FakeResponse(204))
    cleanup_repository(
        session, BASE_URL, REGISTRY_ID, TOKEN,
        "myapp", [_img("sha256:abc", "v1")], dry_run=False, disable_gc=False,
    )

    expected = f"{BASE_URL}/registries/{REGISTRY_ID}/repositories/myapp/cleanup"
    assert session.last_post_url == expected


def test_cleanup_repository_slash_repo_url_encoded():
    session = FakeSession(FakeResponse(204))
    cleanup_repository(
        session, BASE_URL, REGISTRY_ID, TOKEN,
        "build/buildkit-test", [_img("sha256:abc", "v1")], dry_run=False, disable_gc=False,
    )

    expected = (
        f"{BASE_URL}/registries/{REGISTRY_ID}/repositories/build%2Fbuildkit-test/cleanup"
    )
    assert session.last_post_url == expected


# ---------------------------------------------------------------------------
# cleanup_repository — payload
# ---------------------------------------------------------------------------

def test_cleanup_repository_payload_collects_digests_and_tags():
    """All digests and tags from the input images must be sent in one POST."""
    session = FakeSession(FakeResponse(204))
    images = [
        _img("sha256:aaa", "v1"),
        _img("sha256:bbb", "v2"),
        _img("sha256:ccc", "v3"),
    ]
    cleanup_repository(
        session, BASE_URL, REGISTRY_ID, TOKEN, "myapp", images, dry_run=False, disable_gc=False,
    )

    assert session.post_call_count == 1, "bulk cleanup must issue exactly one request"
    payload = session.last_post_json
    assert sorted(payload["digests"]) == ["sha256:aaa", "sha256:bbb", "sha256:ccc"]
    assert sorted(payload["tags"]) == ["v1", "v2", "v3"]


def test_cleanup_repository_payload_handles_tags_as_list():
    """API returns `tags` as a list per image — every tag must be flattened in."""
    session = FakeSession(FakeResponse(204))
    images = [
        _img("sha256:aaa", ["v1", "v1-alias"]),
        _img("sha256:bbb", ["v2"]),
    ]
    cleanup_repository(
        session, BASE_URL, REGISTRY_ID, TOKEN, "myapp", images, dry_run=False, disable_gc=False,
    )

    payload = session.last_post_json
    assert sorted(payload["digests"]) == ["sha256:aaa", "sha256:bbb"]
    assert sorted(payload["tags"]) == ["v1", "v1-alias", "v2"]


def test_cleanup_repository_payload_skips_empty_tags():
    """Untagged images contribute their digest but no tag entry."""
    session = FakeSession(FakeResponse(204))
    images = [
        _img("sha256:aaa", None),
        _img("sha256:bbb", []),
        _img("sha256:ccc", "v3"),
    ]
    cleanup_repository(
        session, BASE_URL, REGISTRY_ID, TOKEN, "myapp", images, dry_run=False, disable_gc=False,
    )

    payload = session.last_post_json
    assert sorted(payload["digests"]) == ["sha256:aaa", "sha256:bbb", "sha256:ccc"]
    assert payload["tags"] == ["v3"]


def test_cleanup_repository_payload_disable_gc_default_false():
    session = FakeSession(FakeResponse(204))
    cleanup_repository(
        session, BASE_URL, REGISTRY_ID, TOKEN,
        "myapp", [_img("sha256:abc", "v1")], dry_run=False, disable_gc=False,
    )

    assert session.last_post_json["disable_gc"] == "false"


def test_cleanup_repository_payload_disable_gc_can_be_overridden():
    session = FakeSession(FakeResponse(204))
    cleanup_repository(
        session, BASE_URL, REGISTRY_ID, TOKEN,
        "myapp", [_img("sha256:abc", "v1")], dry_run=False, disable_gc=True,
    )

    assert session.last_post_json["disable_gc"] == "true"


# ---------------------------------------------------------------------------
# cleanup_repository — return value
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("status", [200, 204])
def test_cleanup_repository_returns_true_on_success_status(status):
    session = FakeSession(FakeResponse(status))
    ok = cleanup_repository(
        session, BASE_URL, REGISTRY_ID, TOKEN,
        "myapp", [_img("sha256:abc", "v1")], dry_run=False, disable_gc=False,
    )
    assert ok is True


@pytest.mark.parametrize("status", [202, 400, 401, 404, 409, 500, 502, 503])
def test_cleanup_repository_returns_false_on_failure_status(status):
    session = FakeSession(FakeResponse(status))
    ok = cleanup_repository(
        session, BASE_URL, REGISTRY_ID, TOKEN,
        "myapp", [_img("sha256:abc", "v1")], dry_run=False, disable_gc=False,
    )
    assert ok is False


# ---------------------------------------------------------------------------
# cleanup_repository — dry-run / empty
# ---------------------------------------------------------------------------

def test_cleanup_repository_dry_run_does_not_call_api():
    session = FakeSession(FakeResponse(204))
    ok = cleanup_repository(
        session, BASE_URL, REGISTRY_ID, TOKEN,
        "myapp", [_img("sha256:abc", "v1")], dry_run=True, disable_gc=False,
    )
    assert ok is True
    assert session.post_call_count == 0
    assert session.last_post_url is None


def test_cleanup_repository_no_images_does_not_call_api():
    """Empty input must short-circuit and never POST."""
    session = FakeSession(FakeResponse(204))
    ok = cleanup_repository(
        session, BASE_URL, REGISTRY_ID, TOKEN, "myapp", [], dry_run=False, disable_gc=False,
    )
    assert ok is True
    assert session.post_call_count == 0


# ---------------------------------------------------------------------------
# cleanup_repository — retry on non-success status
# ---------------------------------------------------------------------------

def test_cleanup_repository_retries_then_succeeds(sleep_mock):
    """A transient failure followed by success must return True and stop retrying."""
    from clients import cleanup_repository as client_module

    with patch.object(client_module, "CLEANUP_RETRY_INITIAL_DELAY", 10):
        session = FakeSession(responses=[FakeResponse(500), FakeResponse(204)])
        ok = cleanup_repository(
            session, BASE_URL, REGISTRY_ID, TOKEN,
            "myapp", [_img("sha256:abc", "v1")], dry_run=False, disable_gc=False,
        )

    assert ok is True
    assert session.post_call_count == 2
    sleep_mock.assert_called_once_with(10)


def test_cleanup_repository_retries_default_count_then_gives_up(sleep_mock):
    """With retry_count=2: 1 initial + 2 retries = 3 total attempts."""
    from clients import cleanup_repository as client_module

    with (
        patch.object(client_module, "CLEANUP_RETRY_COUNT", 2),
        patch.object(client_module, "CLEANUP_RETRY_INITIAL_DELAY", 10),
        patch.object(client_module, "CLEANUP_RETRY_DELAY_STEP", 5),
    ):
        session = FakeSession(responses=[FakeResponse(500)] * 3)
        ok = cleanup_repository(
            session, BASE_URL, REGISTRY_ID, TOKEN,
            "myapp", [_img("sha256:abc", "v1")], dry_run=False, disable_gc=False,
        )

    assert ok is False
    assert session.post_call_count == 3, "1 initial + 2 retries"
    assert sleep_mock.call_args_list == [call(10), call(15)], (
        "first gap is 10s, each subsequent gap adds 5s"
    )


def test_cleanup_repository_retry_delay_pattern_extends_with_step(sleep_mock):
    """With 3 retries the gaps would be 10s, 15s, 20s — confirms the +5s step."""
    from clients import cleanup_repository as client_module

    with (
        patch.object(client_module, "CLEANUP_RETRY_COUNT", 3),
        patch.object(client_module, "CLEANUP_RETRY_INITIAL_DELAY", 10),
        patch.object(client_module, "CLEANUP_RETRY_DELAY_STEP", 5),
    ):
        session = FakeSession(responses=[FakeResponse(500)] * 4)
        ok = cleanup_repository(
            session, BASE_URL, REGISTRY_ID, TOKEN,
            "myapp", [_img("sha256:abc", "v1")], dry_run=False, disable_gc=False,
        )

    assert ok is False
    assert session.post_call_count == 4
    assert sleep_mock.call_args_list == [call(10), call(15), call(20)]


def test_cleanup_repository_no_sleep_when_first_attempt_succeeds(sleep_mock):
    session = FakeSession(FakeResponse(204))
    cleanup_repository(
        session, BASE_URL, REGISTRY_ID, TOKEN,
        "myapp", [_img("sha256:abc", "v1")], dry_run=False, disable_gc=False,
    )

    assert session.post_call_count == 1
    sleep_mock.assert_not_called()


def test_cleanup_repository_dry_run_skips_retry_entirely(sleep_mock):
    """Dry-run never issues a request, therefore must not retry or sleep."""
    session = FakeSession(FakeResponse(500))
    ok = cleanup_repository(
        session, BASE_URL, REGISTRY_ID, TOKEN,
        "myapp", [_img("sha256:abc", "v1")], dry_run=True, disable_gc=False,
    )

    assert ok is True
    assert session.post_call_count == 0
    sleep_mock.assert_not_called()


def test_cleanup_repository_retry_sends_same_payload_each_attempt():
    """Each retry must re-post the same body — retries are not supposed to alter it."""
    session = FakeSession(responses=[FakeResponse(500), FakeResponse(500), FakeResponse(204)])
    cleanup_repository(
        session, BASE_URL, REGISTRY_ID, TOKEN,
        "myapp", [_img("sha256:abc", "v1"), _img("sha256:def", "v2")], dry_run=False, disable_gc=False,
    )

    assert session.post_call_count == 3
    assert sorted(session.last_post_json["digests"]) == ["sha256:abc", "sha256:def"]
    assert sorted(session.last_post_json["tags"]) == ["v1", "v2"]


# ---------------------------------------------------------------------------
# init_gc
# ---------------------------------------------------------------------------

def test_init_gc_disabled_does_not_call_api(sleep_mock):
    """disable_gc=False must short-circuit before issuing any request."""
    session = FakeSession(FakeResponse(201))
    init_gc(session, BASE_URL, REGISTRY_ID, TOKEN, disable_gc=False)

    assert session.post_call_count == 0
    assert session.last_post_url is None
    sleep_mock.assert_not_called()


def test_init_gc_initiates_on_201(sleep_mock):
    """A 201 response triggers GC and returns without retry/sleep."""
    session = FakeSession(FakeResponse(201))
    init_gc(session, BASE_URL, REGISTRY_ID, TOKEN, disable_gc=True)

    expected_url = f"{BASE_URL}/registries/{REGISTRY_ID}/garbage-collection"
    assert session.last_post_url == expected_url
    assert session.last_post_params == {"delete-untagged": "true"}
    assert session.post_call_count == 1
    sleep_mock.assert_not_called()


def test_init_gc_retries_on_409_then_succeeds(sleep_mock):
    """A 409 (GC already running) must trigger one sleep, then the retry succeeds."""
    from clients import cleanup_repository as client_module

    session = FakeSession(responses=[FakeResponse(409), FakeResponse(201)])
    init_gc(session, BASE_URL, REGISTRY_ID, TOKEN, disable_gc=True)

    assert session.post_call_count == 2
    sleep_mock.assert_called_once_with(client_module.GC_RETRY_DELAY)
