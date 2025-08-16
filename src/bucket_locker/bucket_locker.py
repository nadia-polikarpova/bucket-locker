from collections import defaultdict
from contextlib import asynccontextmanager
import logging
import asyncio
from datetime import datetime, timezone
from pathlib import Path
import uuid
import base64
from google.cloud import storage
from google.api_core.exceptions import PreconditionFailed
import google_crc32c

PROCESS_ID = str(uuid.uuid4())

class BlobNotFound(Exception):
    def __init__(self, blob_name: str):
        super().__init__(f"Blob '{blob_name}' not found.")
        self.blob_name = blob_name

class Locker:
    """A class that provides concurrency-safe access to a local copy of a Google Cloud Storage bucket."""

    def __init__(self, bucket_name: str, local_dir: Path,
                 *, bucket=None # for testing purposes
                 ):
        self.bucket_name = bucket_name
        self.local_dir = local_dir
        self._client = storage.Client()
        self._bucket = bucket or self._client.bucket(bucket_name)
        self._file_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

    def local_path(self, blob_name: str) -> Path:
        """Path to the local copy of a blob."""
        return self.local_dir / blob_name

    @asynccontextmanager
    async def owned_local_copy(self, blob_name: str):
        """
            Context manager for safe read-write access to a blob via a local copy.
            It will download the blob if local copy is out of sync and upload it upon exiting the context if the content has changed.
            The local file will be locked for the duration of the context to prevent concurrent access.
            Will raise BlobNotFoundError if the blob does not exist in GCS.
        """
        local_path = self.local_path(blob_name)
        # Acquire local copy lock to prevent concurrent access on the same local copy
        async with self._local_lock(blob_name):
            # Acquire a lock on the session blob in GCS to prevent concurrent access on different local copies
            await self._acquire_blob_lock(blob_name)
            try:
                # Ensure the session file is available locally
                self._download_blob(blob_name)
                try:
                    # Let the called do its thing with the local copy
                    yield local_path
                finally:
                    # Once the caller is done, upload the file even if the caller raised an exception
                    # (but not if download failed!)
                    self._upload_blob(blob_name)
            finally:
                # Release the blob lock in GCS even if download failed or caller raised an exception
                self._release_blob_lock(blob_name)

    @asynccontextmanager
    async def readonly_local_copy(self, blob_name: str):
        """
            Context manager for safe read-only access to a blob via a local copy.
            It will download the blob if local copy is out of sync, but will not lock or upload the blob.
            The local file will be locked for the duration of the context (to prevent reading an incomplete file).
            Will raise BlobNotFoundError if the blob does not exist in GCS.
        """
        local_path = self.local_path(blob_name)
        # Acquire local copy lock to prevent reading while someone else is writing
        async with self._local_lock(blob_name):
            # Ensure the session file is available locally
            self._download_blob(blob_name)
            # Let the caller do its thing with the local copy
            yield local_path

    def _download_blob(self, blob_name: str):
        """
            Ensure the file is available locally; download from GCS if needed.
            Raise BlobNotFoundError if the blob does not exist in GCS.
            This function is not thread-safe: client must lock the local file before calling it.
        """
        local_path = self.local_path(blob_name)
        if self._local_in_sync(blob_name):
            # Local copy is up-to-date, no need to download
            self.logger.debug(f"[{PROCESS_ID}] Blob {blob_name} is up-to-date, no download needed")
        else:
            local_path.parent.mkdir(parents=True, exist_ok=True)
            blob = self._bucket.blob(blob_name)
            if blob.exists():
                blob.download_to_filename(local_path)
                self._save_generation(blob_name, blob)
                self.logger.info(f"[{PROCESS_ID}] Downloaded blob {blob_name} to {local_path}")
            else:
                self.logger.error(f"[{PROCESS_ID}] Blob {blob_name} does not exist in GCS.")
                raise BlobNotFound(blob_name)

    def _upload_blob(self, blob_name: str):
        """
            Upload the blob back to GCS if it has been modified.
            This function is not thread-safe: client must lock the blob file before calling it.
        """
        local_path = self.local_path(blob_name)
        if local_path.exists():
            blob = self._bucket.blob(blob_name)
            if self._files_differ_crc32c(local_path, blob):
                blob.upload_from_filename(local_path)
                self._save_generation(blob_name, blob)
                self.logger.info(f"[{PROCESS_ID}] Uploaded blob {blob_name} to GCS")
            else:
                self.logger.debug(f"[{PROCESS_ID}] Blob {blob_name} is up-to-date, no upload needed")
        else:
            self.logger.error(f"[{PROCESS_ID}] Tried to upload non-existent local blob file {local_path}.")

    async def _acquire_blob_lock(self,
                                blob_name: str,
                                timeout: float = 10.0, # wait this long for the lock before giving up
                                delay: float = 0.5): # wait this long between attempts to acquire the lock
        """
            Acquire a lock on the blob in GCS to prevent concurrent modifications that use different local copies.
            This function should be called before downloading the blob for rw access.
        """
        blob = self._bucket.blob(self._remote_lock_path(blob_name))
        content = f"{PROCESS_ID} {datetime.now(timezone.utc).isoformat()}"

        deadline = asyncio.get_event_loop().time() + timeout
        while asyncio.get_event_loop().time() < deadline:
            try:
                blob.upload_from_string(content, if_generation_match=0)
                return  # acquired
            except PreconditionFailed:
                self.logger.info(f"[{PROCESS_ID}] Waiting for blob lock on {blob_name}...")
                await asyncio.sleep(delay)  # non-blocking wait
            except Exception as e:
                raise RuntimeError(f"Unexpected error acquiring lock: {e}")

        raise TimeoutError(f"Could not acquire lock for blob {blob_name}")

    def _release_blob_lock(self, blob_name: str):
        """
            Release the lock on the blob in GCS.
            This function should be called after uploading the blob (once done with it).
        """
        blob = self._bucket.blob(self._remote_lock_path(blob_name))
        try:
            blob.delete()
        except Exception as e:
            self.logger.error(f"[{PROCESS_ID}] Failed to delete lock for {blob_name}: {e}")

    def _local_lock(self, blob_name: str) -> asyncio.Lock:
        """
            Get a file lock for the local copy of the blob.
        """
        return self._file_locks[str(self.local_path(blob_name).resolve())]

    def _meta_path(self, blob_name: str) -> Path:
        """Path to the local metadata file for a blob."""
        return self.local_dir / f"{blob_name}.meta"

    def _remote_lock_path(self, blob_name: str) -> str:
        """Path to the remote lock blob in GCS."""
        return f"locks/{blob_name}.lock"

    def _local_in_sync(self, blob_name: str) -> bool:
        """Check if the local copy of the blob is still in sync with GCS."""
        local_path = self.local_path(blob_name)
        meta_path = self._meta_path(blob_name)

        if not local_path.exists() or not meta_path.exists():
            # If we don't have a local copy or metadata, clearly not in sync
            return False

        try:
            with open(meta_path) as f:
                local_gen = f.read().strip()

            blob = self._bucket.blob(blob_name)
            blob.reload()
            return str(blob.generation) == local_gen
        except Exception:
            return False  # fail safe

    def _save_generation(self, blob_name: str, blob: storage.Blob):
        """
            Save the blob's generation number to a local metadata file.
            This is used to check if the local copy is still in sync with GCS.
        """
        blob.reload()
        with open(self._meta_path(blob_name), 'w') as f:
            f.write(str(blob.generation))

    def _crc32c_of_file(self, path: Path) -> bytes:
        """Calculate the CRC32C checksum of a file."""
        crc = google_crc32c.Checksum()
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                crc.update(chunk)
        return crc.digest()  # returns bytes

    def _files_differ_crc32c(self, local_path: Path, blob: storage.Blob) -> bool:
        """Check if the local file differs from the GCS blob using CRC32C."""
        blob.reload()
        if not blob.crc32c:
            return True  # can't compare

        local_crc = self._crc32c_of_file(local_path)
        remote_crc = base64.b64decode(blob.crc32c)

        return local_crc != remote_crc


