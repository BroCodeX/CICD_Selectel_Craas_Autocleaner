import os
import re
import sys
import yaml
from loguru import logger
from core.constants import ConfigFields
from core.constants import RulesFields

DEFAULT_CONFIG_PATH = "rules/cleanup_rules_default.yaml"

FALLBACK_CLEANUP_DEFAULTS = {
    ConfigFields.KEEP_LATEST.value: 10,
    ConfigFields.REMOVE_OLDER.value: 14,
}
FALLBACK_UNMATCHED_DEFAULTS = {
    ConfigFields.KEEP_LATEST.value: 10,
    ConfigFields.REMOVE_OLDER.value: 14,
}


def validate_regexp(pattern, context):
    try:
        re.compile(pattern)
    except re.error as e:
        logger.critical(f"Invalid regexp in {context} {pattern}: {e}")
        sys.exit(1)


def _parse_defaults_section(raw: dict, section_key: str, fallback: dict) -> dict:
    section = raw.get(section_key)
    if section is None:
        return dict(fallback)

    if not isinstance(section, dict):
        logger.critical(f"'{section_key}' must be a dictionary")
        sys.exit(1)

    resolved = dict(fallback)
    for field_key in (ConfigFields.KEEP_LATEST.value, ConfigFields.REMOVE_OLDER.value):
        if field_key not in section:
            continue
        raw_value = section[field_key]
        try:
            value = int(raw_value)
        except (TypeError, ValueError):
            logger.critical(
                f"'{section_key}.{field_key}' must be a non-negative int, got {raw_value!r}"
            )
            sys.exit(1)
        if value < 0:
            logger.critical(
                f"'{section_key}.{field_key}' must be a non-negative int, got {raw_value!r}"
            )
            sys.exit(1)
        resolved[field_key] = value

    return resolved


def parse_and_validate(raw: dict) -> dict:
    rules = raw.get(RulesFields.CLEANUP_RULES.value, {})
    if not isinstance(rules, dict):
        logger.critical("cleanup_rules must be a dictionary")
        sys.exit(1)

    for rule_name, rule in rules.items():
        if not isinstance(rule, dict):
            logger.critical(f"Rule '{rule_name}' must be a dictionary")
            sys.exit(1)

        regexp = rule.get(ConfigFields.REGEXP.value, "")
        if not isinstance(regexp, str):
            logger.critical(f"Rule '{rule_name}' is not a string")
            sys.exit(1)

        validate_regexp(regexp, f"rule '{rule_name}'")

    exclude_repo = raw.get(RulesFields.EXCLUDE_REPO.value, "") or None
    if exclude_repo is not None:
        validate_regexp(exclude_repo,  f"exclude '{ RulesFields.EXCLUDE_REPO.value}'")

    cleanup_defaults = _parse_defaults_section(
        raw, RulesFields.CLEANUP_DEFAULTS.value, FALLBACK_CLEANUP_DEFAULTS,
    )
    unmatched_defaults = _parse_defaults_section(
        raw, RulesFields.UNMATCHED_DEFAULTS.value, FALLBACK_UNMATCHED_DEFAULTS,
    )

    return {
        RulesFields.CLEANUP_RULES.value: rules,
        RulesFields.EXCLUDE_REPO.value: exclude_repo,
        RulesFields.CLEANUP_DEFAULTS.value: cleanup_defaults,
        RulesFields.UNMATCHED_DEFAULTS.value: unmatched_defaults,
    }


def load_cleanup_config() -> dict:
    clean_config_path = os.getenv("CLEAN_CONFIG_PATH", DEFAULT_CONFIG_PATH)
    if not os.path.exists(clean_config_path):
        logger.critical(f"Cleanup config file not found: {clean_config_path}")
        sys.exit(1)

    with open(clean_config_path, "r", encoding="utf-8") as f:
        try:
            raw = yaml.safe_load(f) or {}
        except yaml.YAMLError as e:
            logger.exception(f"Error parsing cleanup config: {e}")
            sys.exit(1)

    config = parse_and_validate(raw)
    logger.success(f"Cleanup config from {clean_config_path} loaded")
    return config

