import os
import sys
from dataclasses import dataclass

import requests
from loguru import logger
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from cleanup_config import load_cleanup_config
from cleanup_repository import get_repositories, get_images, delete_image
from cleanup_executor import select_images_to_delete
from constants import ImageFields

AUTH_URL = "https://cloud.api.selcloud.ru/identity/v3/auth/tokens"
BASE_URL = "https://cr.selcloud.ru/api/v1"
USER_AGENT = "GitLab-Cleanup-Script/1.1"
DEFAULT_TIMEOUT = 30

for k in ["HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"]:
    os.environ.pop(k, None)

@dataclass(frozen=True)
class Settings:
    username: str = os.getenv("SEL_USERNAME")
    password: str = os.getenv("SEL_PASSWORD")
    account_id: str = os.getenv("SEL_ACCOUNT_ID")
    project_name: str = os.getenv("SEL_PROJECT_NAME")
    registry_id: str = os.getenv("SEL_REGISTRY_ID")
    dry_run: bool = os.getenv("DRY_RUN", "false").lower() == "true"

    def validate(self):
        if not all([self.username, self.password, self.account_id, self.project_name, self.registry_id]):
            logger.critical("Missing environment variables!")
            sys.exit(1)

def setup_logging():
    logger.remove()
    logger.level("HEADER", no=21, color="<magenta><bold>")
    logger.add(
        sys.stderr,
        format="<level>{time:HH:mm:ss}</level> | <level>{message}</level>",
        colorize=True
    )

def create_session() -> requests.Session:
    session = requests.Session()
    session.trust_env = False
    
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Content-Type": "application/json"
    })
    return session

def get_auth_token(session: requests.Session, settings: Settings) -> str:
    logger.log("HEADER", "Requesting authentication token...")
    
    payload = {
        "auth": {
            "identity": {
                "methods": ["password"],
                "password": {
                    "user": {
                        "name": settings.username,
                        "domain": {"name": settings.account_id},
                        "password": settings.password
                    }
                }
            },
            "scope": {
                "project": {
                    "name": settings.project_name,
                    "domain": {"name": settings.account_id}
                }
            }
        }
    }

    response = session.post(AUTH_URL, json=payload, timeout=DEFAULT_TIMEOUT)
    
    if response.status_code != 201:
        logger.error(f"Auth failed: {response.status_code} - {response.text}")
        response.raise_for_status()

    token = response.headers.get("X-Subject-Token")
    if not token:
        logger.critical("Token not found in headers")
        sys.exit(1)

    logger.success("Token successfully received")
    return token

def main():
    setup_logging()
    settings = Settings()
    settings.validate()
    
    session = create_session()
    
    try:
        rules = load_cleanup_config()
        token = get_auth_token(session, settings)
        
        repos = get_repositories(session, BASE_URL, settings.registry_id, token)
        
        if not repos:
            logger.warning("No repositories found.")
            return

        logger.info(f"Repositories to process: {[r['name'] for r in repos]}")

        for repo in repos:
            repo_name = repo["name"]
            logger.info(f"\nProcessing repository: {repo_name}")

            images = get_images(session, BASE_URL, settings.registry_id, token, repo_name)
            to_delete = select_images_to_delete(repo_name, images, rules)
            
            if not to_delete:
                logger.debug(f"{repo_name}: No images match deletion criteria.")
                continue

            logger.warning(f"{repo_name}: Found {len(to_delete)} images for deletion")

            for img in to_delete:
                digest = img.get(ImageFields.DIGEST.value)
                
                delete_image(
                    session=session,
                    base_url=BASE_URL,
                    registry_id=settings.registry_id,
                    token=token,
                    repo_name=repo_name,
                    digest=digest,
                    dry_run=settings.dry_run,
                )

    except requests.exceptions.RequestException as e:
        logger.exception(f"Network error: {e}")
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
