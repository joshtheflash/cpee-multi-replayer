"""CPEE multi-log replay package."""

from importlib import metadata

try:
    __version__ = metadata.version("cpee-multi-replay")
except metadata.PackageNotFoundError:  # pragma: no cover - local checkout fallback
    __version__ = "0.0.dev0"

__all__ = ["replay", "loadLogs"]
