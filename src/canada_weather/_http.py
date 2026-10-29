"""HTTP session with retries and an identifying User-Agent."""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    __version__ = version("canada-weather-downloader")
except PackageNotFoundError:  # running from a source checkout
    __version__ = "0.0.0"

USER_AGENT = (
    f"canada-weather-downloader/{__version__} "
    "(+https://github.com/shenzj1994/weather_canada_downloader)"
)


def make_session(
    pool_size: int = 10,
    retries: int = 3,
    backoff_factor: float = 1.0,
    status_forcelist: tuple[int, ...] = (429, 500, 502, 503, 504),
) -> requests.Session:
    """Create a :class:`requests.Session` that retries transient failures.

    Retries connection errors, read timeouts and 429/5xx responses with
    exponential backoff (honouring ``Retry-After``). One session is meant to be
    shared by all worker threads of a download so connections are reused.
    """
    retry = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods={"GET"},
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=retry, pool_connections=pool_size, pool_maxsize=pool_size)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers["User-Agent"] = USER_AGENT
    return session
