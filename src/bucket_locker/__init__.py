"""Top-level package for Bucket Locker."""

from importlib.metadata import version as _version

try:
    __version__ = _version("bucket_locker")
except Exception:
    __version__ = "0.0.0"  # fallback for editable/dev checkouts

# Public API
from .bucket_locker import Locker, BlobNotFound, BlobExists, Handle

__all__ = ["Locker", "Handle", "BlobNotFound", "BlobExists", "__version__"]
