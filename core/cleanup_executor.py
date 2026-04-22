from datetime import datetime, timedelta, timezone
from loguru import logger
from core.cleanup_rules_parser import split_images_by_rules
from core.constants import ImageFields, ConfigFields

FALLBACK_CLEANUP_DEFAULTS = {
    ConfigFields.KEEP_LATEST.value: 10,
    ConfigFields.REMOVE_OLDER.value: 14,
}
FALLBACK_UNMATCHED_DEFAULTS = {
    ConfigFields.KEEP_LATEST.value: 10,
    ConfigFields.REMOVE_OLDER.value: 14,
}


def _parse_created_at(value):
    if not isinstance(value, str) or not value.strip():
        return None

    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)

    return parsed.astimezone(timezone.utc)


def _resolve_rule_limits(rule_name, rule, cleanup_defaults):
    def define_limit(key, default_value):
        raw = rule.get(key)
        if raw is None:
            logger.warning(
                f"Rule '{rule_name}' has no values for {key}. "
                f"Using default {key}={default_value}."
            )
            return default_value
        try:
            value = int(raw)
            if value < 0:
                raise ValueError(f"negative {key}")
            return value
        except (TypeError, ValueError):
            logger.warning(
                f"Rule '{rule_name}' has invalid {key} value '{raw}'. "
                f"Using default {key}={default_value}."
            )
            return default_value

    keep_latest = define_limit(
        ConfigFields.KEEP_LATEST.value, cleanup_defaults[ConfigFields.KEEP_LATEST.value]
    )
    remove_older_days = define_limit(
        ConfigFields.REMOVE_OLDER.value, cleanup_defaults[ConfigFields.REMOVE_OLDER.value]
    )

    return keep_latest, remove_older_days


def _is_older_than_days(image, days, now_utc):
    created_at = _parse_created_at(image.get(ImageFields.CREATED_AT.value))
    if created_at is None:
        logger.warning(f"Image with digest '{image.get(ImageFields.DIGEST.value)}' has invalid or missing {ImageFields.CREATED_AT.value}. Skipping age check.")
        return False

    return now_utc - created_at >= timedelta(days=days)


def select_images_to_delete(
    repo_name,
    images,
    cleanup_rules,
    cleanup_defaults=None,
    unmatched_defaults=None,
    now=None,
):
    cleanup_defaults = cleanup_defaults or FALLBACK_CLEANUP_DEFAULTS
    unmatched_defaults = unmatched_defaults or FALLBACK_UNMATCHED_DEFAULTS

    sorted_images = sorted(images, key=lambda x: x.get(ImageFields.CREATED_AT.value, ""), reverse=True)
    grouped, unmatched = split_images_by_rules(repo_name, sorted_images, cleanup_rules)

    now_utc = now.astimezone(timezone.utc) if now else datetime.now(timezone.utc)
    to_delete = []

    for rule_name, rule_images in grouped.items():
        rule = cleanup_rules.get(rule_name, {})
        keep_latest, remove_older_days = _resolve_rule_limits(rule_name, rule, cleanup_defaults)

        protected = rule_images[:keep_latest]
        candidates = rule_images[len(protected):]

        old_candidates = [
            image for image in candidates if _is_older_than_days(image, remove_older_days, now_utc)
        ]

        if candidates:
            logger.info(
                f"Rule: {rule_name}: matched images: {len(rule_images)}, "
                f"keep latest: {keep_latest}, remove older: {remove_older_days} days"
            )
            logger.info(
                f"Rule: {rule_name}: protected={len(protected)}, "
                f"candidates={len(candidates)}, to_delete_by_age={len(old_candidates)}"
            )

        to_delete.extend(old_candidates)

    unmatched_keep = unmatched_defaults[ConfigFields.KEEP_LATEST.value]
    unmatched_remove = unmatched_defaults[ConfigFields.REMOVE_OLDER.value]
    protected_unmatched = unmatched[:unmatched_keep]
    unmatched_candidates = unmatched[len(protected_unmatched):]

    old_unmatched = [
        image for image in unmatched_candidates
        if _is_older_than_days(image, unmatched_remove, now_utc)
    ]

    if unmatched_candidates:
        logger.info(
            f"Unmatched: total={len(unmatched)}, "
            f"keep latest: {unmatched_keep}, remove older: {unmatched_remove} days"
        )
        logger.info(
            f"Unmatched: protected={len(protected_unmatched)}, "
            f"candidates={len(unmatched_candidates)}, to_delete_by_age={len(old_unmatched)}"
        )

    to_delete.extend(old_unmatched)

    unique = []
    seen = set()
    for image in to_delete:
        digest = image.get(ImageFields.DIGEST.value)
        if not digest or digest in seen:
            continue
        unique.append(image)
        seen.add(digest)

    return unique
