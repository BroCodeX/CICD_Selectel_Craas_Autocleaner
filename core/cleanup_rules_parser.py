import re
from loguru import logger
from core.constants import ImageFields, ConfigFields


def filter_repos_by_exclude(repos, exclude_pattern):
    if not exclude_pattern:
        return repos
    return [r for r in repos if not re.search(exclude_pattern, r["name"])]


def get_image_tags(image):
    tags = image.get(ImageFields.TAGS.value)
    if isinstance(tags, list):
        return [t for t in tags if isinstance(t, str)]
    return []


def image_matches_regexp(repo_name, image, regexp):
    if not regexp:
        return False

    tags = get_image_tags(image)
    for tag in tags:
        image_ref = f"{repo_name}:{tag}"
        try:
            if re.search(regexp, image_ref):
                return True
        except re.error as e:
            logger.critical(f"Fail during match '{regexp}': {e} on image '{image_ref}'")
            raise
    return False


def split_images_by_rules(repo_name, images, cleanup_rules):
    grouped = {}
    assigned = set()

    # Rule order: first match wins - images matched by one rule won't be considered for the next ones
    for rule_name, rule in cleanup_rules.items():
        regexp = rule.get(ConfigFields.REGEXP.value)
        grouped[rule_name] = []

        for image in images:
            digest = image.get(ImageFields.DIGEST.value)
            if not digest or digest in assigned:
                continue

            if image_matches_regexp(repo_name, image, regexp):
                grouped[rule_name].append(image)
                assigned.add(digest)

    unmatched = []
    for image in images:
        digest = image.get(ImageFields.DIGEST.value)
        if digest and digest not in assigned:
            unmatched.append(image)

    return grouped, unmatched
