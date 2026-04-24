from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

DEFAULT_TIMEOUT = 60
DEFAULT_USER_AGENT = "kma-bigdata-slobodian-ingest/1.0"


def build_session(user_agent: str = DEFAULT_USER_AGENT) -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=4,
        backoff_factor=1.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "POST"}),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({"User-Agent": user_agent, "Accept": "*/*"})
    return session


_shared: Optional[requests.Session] = None


def shared_session() -> requests.Session:
    global _shared
    if _shared is None:
        _shared = build_session()
    return _shared
