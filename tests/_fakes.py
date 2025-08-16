import base64, itertools, google_crc32c

class FakeBlob:
    _gen_counter = itertools.count(1)

    def __init__(self, store: dict, name: str):
        self._store = store
        self.name = name

    def exists(self):
        return self.name in self._store

    def reload(self):
        # no-op; attrs derived from store
        pass

    @property
    def generation(self):
        return self._store[self.name]["gen"] if self.exists() else None

    @property
    def crc32c(self):
        if not self.exists(): return None
        return self._store[self.name]["crc_b64"]

    def download_to_filename(self, path):
        data = self._store[self.name]["bytes"]
        with open(path, "wb") as f:
            f.write(data)

    def upload_from_filename(self, path, **kwargs):
        with open(path, "rb") as f:
            data = f.read()
        crc = google_crc32c.value(data)
        self._store[self.name] = {
            "bytes": data,
            "gen": next(self._gen_counter),
            "crc_b64": base64.b64encode(crc.to_bytes(4, "big")).decode("ascii"),
        }

    def upload_from_string(self, s, if_generation_match=None):
        # used for lock uploads
        if self.exists() and if_generation_match == 0:
            # emulate PreconditionFailed
            from google.api_core.exceptions import PreconditionFailed
            raise PreconditionFailed("already exists")
        data = s.encode()
        crc = google_crc32c.value(data)
        self._store[self.name] = {
            "bytes": data,
            "gen": next(self._gen_counter),
            "crc_b64": base64.b64encode(crc.to_bytes(4, "big")).decode("ascii"),
        }

    def delete(self):
        self._store.pop(self.name, None)

class FakeBucket:
    def __init__(self):
        self._store = {}
    def blob(self, name):
        return FakeBlob(self._store, name)
