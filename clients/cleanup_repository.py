from loguru import logger

GET_TIMEOUT = 15
DELETE_TIMEOUT = 10

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
    logger.log("HEADER", "=== get_repositories() ===")

    url = f"{base_url}/registries/{registry_id}/repositories"
    res = session.get(url, headers=_get_auth_header(token), timeout=15)
    logger.debug(f"Repo status: {res.status_code}")
    
    context = f"Registry {registry_id}"

    data = _handle_api_response(res, context)
    if not isinstance(data, list):
        logger.critical(f"Unexpected repositories response: {data}")
        return []

    logger.success(f"Repositories found: {len(data)}")
    return data


def get_images(session, base_url, registry_id, token, repo_name):
    logger.log("HEADER", f"=== get_images() repo={repo_name} ===")

    url = f"{base_url}/registries/{registry_id}/repositories/{repo_name}/images"
    res = session.get(url, headers=_get_auth_header(token), timeout=GET_TIMEOUT)

    logger.debug(f"Images status {repo_name}: {res.status_code}")
    context = f"Repo {repo_name}"

    data = _handle_api_response(res, context)
    if not isinstance(data, list):
        logger.critical(f"{repo_name}: unexpected images response {data}")
        return []

    logger.success(f"{repo_name}: images={len(data)}")
    return data


def delete_image(session, base_url, registry_id, token, repo_name, digest, dry_run):
    logger.log("HEADER", f"=== delete_image() repo={repo_name} digest={digest} ===")

    if dry_run:
        logger.info(f"[DRY-RUN] Would delete {repo_name} {digest}")
        return

    url = f"{base_url}/registries/{registry_id}/repositories/{repo_name}/{digest}"
    res = session.delete(url, headers=_get_auth_header(token), timeout=DELETE_TIMEOUT)

    logger.debug(f"Delete status {digest}: {res.status_code}")

    if res.status_code == 204:
        logger.success(f"Deleted {repo_name} {digest}")
    else:
        logger.critical(f"Delete failed {digest}: {res.status_code} {res.text}")
