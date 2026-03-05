import os
for k in ["HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"]:
    os.environ.pop(k, None)

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import sys
from loguru import logger
from cleanup_config import load_cleanup_config
from cleanup_repository import get_repositories, get_images, delete_image
from cleanup_executor import select_images_to_delete

logger.remove(0)
logger.add(sys.stderr,
           format="<level>{time:HH:mm:ss}</level> | <level>{message}</level>",
           colorize=True)

# Env config
USERNAME = os.getenv("SEL_USERNAME")
PASSWORD = os.getenv("SEL_PASSWORD")
ACCOUNT_ID = os.getenv("SEL_ACCOUNT_ID")
PROJECT_NAME = os.getenv("SEL_PROJECT_NAME")
REGISTRY_ID = os.getenv("SEL_REGISTRY_ID")
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"

AUTH_URL = "https://cloud.api.selcloud.ru/identity/v3/auth/tokens"
BASE_URL = "https://cr.selcloud.ru/api/v1"

# Session
session = requests.Session()
session.trust_env = False
retries = Retry(total=3, backoff_factor=1, status_forcelist=[502, 503, 504])
session.mount("https://", HTTPAdapter(max_retries=retries))
session.headers.update({
    "User-Agent": "GitLab-Cleanup-Script/1.0",
    "Content-Type": "application/json"
})

def get_token():
    logger.info("=== get_token() ===")

    payload = {
        "auth": {
            "identity": {
                "methods": ["password"],
                "password": {
                    "user": {
                        "name": USERNAME,
                        "domain": {"name": ACCOUNT_ID},
                        "password": PASSWORD
                    }
                }
            },
            "scope": {
                "project": {
                    "name": PROJECT_NAME,
                    "domain": {"name": ACCOUNT_ID}
                }
            }
        }
    }

    res = session.post(AUTH_URL, json=payload, timeout=30)
    logger.debug(f"Auth status: {res.status_code}")

    if res.status_code != 201:
        logger.error(res.text)
        res.raise_for_status()

    token = res.headers.get("X-Subject-Token")
    if not token:
        logger.critical("Token not found")
        sys.exit(1)

    logger.success("OK: Token received")
    return token


def main():
    if not all([USERNAME, PASSWORD, ACCOUNT_ID, PROJECT_NAME, REGISTRY_ID]):
        logger.critical("Missing environment variables")
        sys.exit(1)

    try:
        rules = load_cleanup_config()
        token = get_token()
        repos = get_repositories(session, BASE_URL, REGISTRY_ID, token)

        if len(repos) > 0 : logger.info(f"Repositories: {[r['name'] for r in repos]}")
        for repo in repos:
            repo_name = repo["name"]
            logger.info(f"\nRepository: {repo_name}")

            images = get_images(session, BASE_URL, REGISTRY_ID, token, repo_name)
            to_delete = select_images_to_delete(repo_name, images, rules)
            
            if not to_delete:
                logger.debug(f"{repo_name}: no images to delete")
                continue

            logger.success(f"{repo_name}: deleting {len(to_delete)} images")

            for img in to_delete:
                delete_image(
                    session=session,
                    base_url=BASE_URL,
                    registry_id=REGISTRY_ID,
                    token=token,
                    repo_name=repo_name,
                    digest=img["digest"],
                    dry_run=DRY_RUN,
                )

    except requests.exceptions.ConnectionError as e:
        logger.exception(f"Connection error: {e}")
    except Exception as e:
        logger.exception(f"Unhandled error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
