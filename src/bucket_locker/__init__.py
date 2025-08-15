"""Top-level package for Bucket Locker."""

from importlib.metadata import version as _version

try:
    __version__ = _version("bucket_locker")  # replace with your package name
except Exception:
    __version__ = "0.0.0"  # fallback for editable/dev checkouts

# Public API
from .bucket_locker import Locker  # expose what users should import

__all__ = ["Locker", "__version__"]
