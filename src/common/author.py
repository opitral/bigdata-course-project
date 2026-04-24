import os

from common.constants import DEFAULT_AUTHOR


def resolve_author() -> str:
    return os.environ.get("AUTHOR_SURNAME", DEFAULT_AUTHOR)
