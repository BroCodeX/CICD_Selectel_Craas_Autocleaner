import time
from urllib.parse import quote

from loguru import logger

GET_TIMEOUT = 15
DELETE_TIMEOUT = 20
DELETE_RETRY_COUNT = 5
DELETE_RETRY_DELAY = 30

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


def delete_image(session, base_url, registry_id, token, repo_name, digest, tag, dry_run) -> bool:
    logger.log("HEADER", f"Image to delete: repo={repo_name} tag={tag}")
    short_digest = digest[:16]

    if dry_run:
        logger.info(f"[DRY-RUN] Would delete: {repo_name} {tag}:{short_digest}")
        return True

    url = f"{base_url}/registries/{registry_id}/repositories/{quote(repo_name, safe='')}/{digest}"

    for attempt in range(1, DELETE_RETRY_COUNT + 1):
        res = session.delete(url, headers=_get_auth_header(token), timeout=DELETE_TIMEOUT)
        logger.debug(f"Delete status {tag} {short_digest} : {res.status_code}")

        if res.status_code == 204:
            logger.success(f"Deleted {repo_name} {tag}:{short_digest}")
            return True

        if res.status_code == 500:
            logger.warning(f"Server is not responding. {repo_name} {tag}:{short_digest}")
            return False

        if res.status_code == 409:
            if attempt < DELETE_RETRY_COUNT:
                logger.warning(
                    f"Delete {tag}:{short_digest} got 409 error (garbage collection), "
                    f"retry {attempt}/{DELETE_RETRY_COUNT - 1} in {DELETE_RETRY_DELAY}s"
                )
                time.sleep(DELETE_RETRY_DELAY)
                continue
            logger.critical(f"Delete failed tag: {tag}:{short_digest} after {DELETE_RETRY_COUNT} attempts: {res.status_code} {res.text}")
            return False
