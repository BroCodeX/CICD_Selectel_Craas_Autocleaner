import os
import re
import sys
import yaml
from loguru import logger
from core.constants import ConfigFields
from core.constants import RulesFields

DEFAULT_CONFIG_PATH = "rules/cleanup_rules_default.yaml"

def validate_regexp(pattern, context):
    try:
        re.compile(pattern)
    except re.error as e:
        logger.critical(f"Invalid regexp in {context} {pattern}: {e}")
        sys.exit(1)


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

    return {
        RulesFields.CLEANUP_RULES.value: rules,
        RulesFields.EXCLUDE_REPO.value: exclude_repo,
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

