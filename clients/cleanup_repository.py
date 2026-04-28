import sys
import time
from urllib.parse import quote

from loguru import logger

from core.constants import ImageFields

GET_TIMEOUT = 15
CLEANUP_TIMEOUT = 120

CLEANUP_RETRY_COUNT = 2
CLEANUP_RETRY_INITIAL_DELAY = 10
CLEANUP_RETRY_DELAY_STEP = 5

GC_RETRY_DELAY = 300

SUCCESS_STATUSES = {200, 201, 204}


def _get_auth_header(token) -> dict:
    return {"X-Auth-Token": token}

def _handle_api_response(res, context):
    if res.status_code == 204:
        logger.warning(f"{context}: No content (204)")
        return []

    if res.status_code == 404:
        logger.warning(f"{context}: Resource not found (404)")
        return []

    if res.status_code >= 500:
        logger.warning(f"{context}: Registry API server error ({res.status_code})")
        return []

    res.raise_for_status()
    return res.json()

def get_repositories(session, base_url, registry_id, token):
    logger.log("HEADER", "Get repositories")

    url = f"{base_url}/registries/{registry_id}/repositories"
    res = session.get(url, headers=_get_auth_header(token), timeout=15)

    context = f"Registry {registry_id}"

    data = _handle_api_response(res, context)
    if not isinstance(data, list):
        logger.critical(f"Unexpected repositories response: {data}")
        return []

    logger.success(f"Repositories found: {[r['name'] for r in data]}")
    return data


def get_images(session, base_url, registry_id, token, repo_name):
    logger.log("HEADER", f"Get images in repository: {repo_name}")

    url = f"{base_url}/registries/{registry_id}/repositories/{quote(repo_name, safe='')}/images"
    res = session.get(url, headers=_get_auth_header(token), timeout=GET_TIMEOUT)

    context = f"Repo {repo_name}"

    data = _handle_api_response(res, context)
    if not isinstance(data, list):
        logger.critical(f"{repo_name}: unexpected images response {data}")
        return []

    logger.success(f"{repo_name}: images={len(data)}")
    return data


def _build_cleanup_payload(images, disable_gc):
    digests = []
    tags = []
    for img in images:
        digest = img.get(ImageFields.DIGEST.value)
        if digest:
            digests.append(digest)

        img_tags = img.get(ImageFields.TAGS.value)
        if isinstance(img_tags, list):
            tags.extend(t for t in img_tags if t)
        elif img_tags:
            tags.append(img_tags)

    return {"digests": digests, "tags": tags, "disable_gc": disable_gc}


def cleanup_repository(
    session, base_url, registry_id, token, repo_name, images, dry_run, disable_gc
) -> bool:
    disable_gc = str(disable_gc).lower()
    payload = _build_cleanup_payload(images, disable_gc)
    digests = payload["digests"]
    tags = payload["tags"]

    logger.log("HEADER", f"Cleanup repo={repo_name} digests={len(digests)} tags={len(tags)}")

    if not digests and not tags:
        logger.warning(f"{repo_name}: nothing to cleanup")
        return True

    if dry_run:
        logger.info(
            f"[DRY-RUN] Would cleanup {repo_name}: digests={len(digests)}, tags={len(tags)}"
        )
        return True

    url = f"{base_url}/registries/{registry_id}/repositories/{quote(repo_name, safe='')}/cleanup"
    total_attempts = CLEANUP_RETRY_COUNT + 1

    res = None
    for attempt in range(1, total_attempts + 1):
        res = session.post(url, headers=_get_auth_header(token), json=payload, timeout=CLEANUP_TIMEOUT)

        if res.status_code in SUCCESS_STATUSES:
            logger.success(
                f"Cleanup {repo_name}: removed digests={len(digests)} tags={len(tags)}"
            )
            return True

        if attempt < total_attempts:
            delay = CLEANUP_RETRY_INITIAL_DELAY + (attempt - 1) * CLEANUP_RETRY_DELAY_STEP
            logger.warning(
                f"Cleanup {repo_name} failed ({res.status_code}): "
                f"retry {attempt}/{CLEANUP_RETRY_COUNT} in {delay}s"
            )
            time.sleep(delay)

    logger.critical(
        f"Cleanup failed {repo_name} after {total_attempts} attempts: "
        f"{res.status_code} {res.text}"
    )
    return False

def init_gc(session, base_url, registry_id, token, disable_gc, dry_run=False, delete_untagged=True):
    if not disable_gc:
        return
    logger.info("Garbage collection is enabled. Initializing GC process...")

    url = f"{base_url}/registries/{registry_id}/garbage-collection"
    params = {"delete-untagged": str(delete_untagged).lower()}

    if dry_run:
        logger.info(f"[DRY-RUN] Would initiate GC for registry {registry_id} with params: {params}")
        return

    for attempt in range(1, 4):
        res = session.post(
            url,
            headers=_get_auth_header(token),
            params=params,
            timeout=GET_TIMEOUT,
        )

        if res.status_code in SUCCESS_STATUSES:
            logger.success(f"Registry {registry_id}: garbage collection initiated (201)")
            return

        if res.status_code == 409:
            logger.warning(
                f"Registry {registry_id}: GC already in progress (409). "
                f"Retry {attempt}/3 in {GC_RETRY_DELAY}s"
            )
            time.sleep(GC_RETRY_DELAY)
            continue

        if res.status_code in (404, 500):
            logger.critical(
                f"Registry {registry_id}: GC failed ({res.status_code}) {res.text}"
            )
            sys.exit(1)

        logger.critical(
            f"Registry {registry_id}: unexpected GC response ({res.status_code}) {res.text}"
        )
        sys.exit(1)

    logger.warning(
        f"Registry {registry_id}: GC still in progress after 3 attempts (409). Sleep = {GC_RETRY_DELAY}s"
    )
    time.sleep(GC_RETRY_DELAY)
