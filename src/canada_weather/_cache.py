"""Location of the on-disk cache (downloaded CSVs and refreshed inventory)."""

from __future__ import annotations

import os
import sys
from pathlib import Path


def default_cache_dir() -> Path:
    """Return the cache directory, honouring ``CANADA_WEATHER_CACHE``.

    Defaults to ``%LOCALAPPDATA%\\canada_weather`` on Windows,
    ``~/Library/Caches/canada_weather`` on macOS and
    ``$XDG_CACHE_HOME/canada_weather`` (``~/.cache``) elsewhere.
    """
    env = os.environ.get("CANADA_WEATHER_CACHE")
    if env:
        return Path(env).expanduser()
    if sys.platform == "win32":
        base = Path(os.environ.get("LOCALAPPDATA", Path.home() / "AppData" / "Local"))
    elif sys.platform == "darwin":
        base = Path.home() / "Library" / "Caches"
    else:
        base = Path(os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache"))
    return base / "canada_weather"
