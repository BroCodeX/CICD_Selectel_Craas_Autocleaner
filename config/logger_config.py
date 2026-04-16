from loguru import logger
import sys

def setup_logging():
    logger.remove()
    logger.level("HEADER", no=21, color="<magenta><bold>")
    logger.add(
        sys.stderr,
        format="<level>{time:HH:mm:ss}</level> | <level>{message}</level>",
        colorize=True
    )
