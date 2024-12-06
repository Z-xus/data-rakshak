"""Unified API for Presidio services."""

import importlib.metadata

try:
    __version__ = importlib.metadata.version("presidio")
except importlib.metadata.PackageNotFoundError:
    __version__ = "0.1.0"

