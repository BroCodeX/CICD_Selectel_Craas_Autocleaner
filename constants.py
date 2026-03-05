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
