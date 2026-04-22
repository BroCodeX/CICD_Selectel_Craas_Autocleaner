from enum import Enum

class ImageFields(Enum):
    # https://docs.selectel.ru/api/craas/#tag/Repositories/operation/listImages
    CREATED_AT = "createdAt"
    DIGEST = "digest"
    OS = "os"
    TAGS = "tags"

class ConfigFields(Enum):
    REGEXP = "regexp"
    KEEP_LATEST = "keep_latest"
    REMOVE_OLDER = "remove_older"

class RulesFields(Enum):
    EXCLUDE_REPO = "exclude_repo"
    CLEANUP_RULES = "cleanup_rules"
    CLEANUP_DEFAULTS = "cleanup_defaults"
    UNMATCHED_DEFAULTS = "unmatched_defaults"


FALLBACK_CLEANUP_DEFAULTS = {
    ConfigFields.KEEP_LATEST.value: 10,
    ConfigFields.REMOVE_OLDER.value: 14,
}
FALLBACK_UNMATCHED_DEFAULTS = {
    ConfigFields.KEEP_LATEST.value: 10,
    ConfigFields.REMOVE_OLDER.value: 14,
}
