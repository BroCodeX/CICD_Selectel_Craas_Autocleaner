import os
import sys
import yaml
from loguru import logger
from constants import ConfigFields

DEFAULT_CONFIG_PATH = ".config/cleanup_rules.yaml"

def load_cleanup_config():
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

    rules = raw.get("cleanup_rules", {})
    if not isinstance(rules, dict):
        logger.critical("cleanup_rules must be a dictionary")
        sys.exit(1)

    for rule_name, rule in rules.items():
        if not isinstance(rule, dict):
            logger.critical(f"Rule '{rule_name}' must be a dictionary")
            sys.exit(1)

        regexp = rule.get(ConfigFields.REGEXP.value)
        if not isinstance(regexp, str) or not regexp.strip():
            logger.critical(f"Rule '{rule_name}' has no valid regexp")
            sys.exit(1)

    logger.success(f"Cleanup config from {clean_config_path} loaded")
    return rules
