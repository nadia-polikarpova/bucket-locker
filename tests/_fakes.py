import base64, itertools, google_crc32c

# tests/_fakes.py
import base64
import itertools
from datetime import datetime, timezone
import google_crc32c
from google.api_core.exceptions import PreconditionFailed
from typing import Dict, Any, Optional, Union

class FakeBlob:
    _gen_counter = itertools.count(1)

    def __init__(self, store: Dict[str, Dict[str, Any]], name: str) -> None:
        self._store = store
        self.name: str = name

    def exists(self) -> bool:
        return self.name in self._store

    def reload(self) -> None:
        # no-op in fake; attributes come from store
        pass

    @property
    def generation(self) -> int:
        return self._store[self.name]["gen"] if self.exists() else 0

    @property
    def crc32c(self) -> Optional[str]:
        return self._store[self.name]["crc_b64"] if self.exists() else None

    @property
    def updated(self) -> Optional[datetime]:
        return self._store[self.name]["updated"] if self.exists() else None

    def _enforce_if_generation_match(self, match: Optional[int]) -> None:
        """Emulate GCS precondition semantics for if_generation_match."""
        if match is None:
            return
        if self.generation != int(match):
            raise PreconditionFailed(
                f"if_generation_match={match} but current gen={self.generation}"
            )

    def _write_bytes(self, data: bytes) -> None:
        crc = google_crc32c.value(data)
        self._store[self.name] = {
            "bytes": data,
            "gen": next(self._gen_counter),
            "crc_b64": base64.b64encode(crc.to_bytes(4, "big")).decode("ascii"),
            "updated": datetime.now(timezone.utc),
        }

    # --- API used by your code ---

    def upload_from_filename(self, path: str, *, if_generation_match: Optional[int] = None, **kwargs: Any) -> None:
        self._enforce_if_generation_match(if_generation_match)
        with open(path, "rb") as f:
            self._write_bytes(f.read())

    def upload_from_string(self, s: Union[str, bytes, bytearray], *, if_generation_match: Optional[int] = None, **kwargs: Any) -> None:
        self._enforce_if_generation_match(if_generation_match)
        data = s if isinstance(s, (bytes, bytearray)) else str(s).encode()
        self._write_bytes(data)

    def download_to_filename(self, path: str) -> None:
        with open(path, "wb") as f:
            f.write(self._store[self.name]["bytes"])

    def delete(self, *, if_generation_match: Optional[int] = None) -> None:
        # Only delete if precondition passes (if provided)
        if if_generation_match is not None:
            if not self.exists() or int(self.generation) != int(if_generation_match):
                raise PreconditionFailed(
                    f"delete if_generation_match={if_generation_match} but current gen={self.generation}"
                )
        # Best-effort delete (no NotFound)
        self._store.pop(self.name, None)

class FakeBucket:
    def __init__(self) -> None:
        self._store: Dict[str, Dict[str, Any]] = {}

    def blob(self, name: str) -> FakeBlob:
        return FakeBlob(self._store, name)
