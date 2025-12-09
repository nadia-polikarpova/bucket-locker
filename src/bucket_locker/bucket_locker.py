from dataclasses import dataclass
from collections import defaultdict
from contextlib import asynccontextmanager
import asyncio
from datetime import datetime, timezone
from pathlib import Path
from typing import AsyncIterator, Optional
import uuid
import base64
from google.cloud import storage
from google.api_core.exceptions import PreconditionFailed
import google_crc32c
import logging
logger = logging.getLogger(__name__)

PROCESS_ID = str(uuid.uuid4())

class BlobNotFound(Exception):
    def __init__(self, blob_name: str):
        super().__init__(f"Blob '{blob_name}' not found.")
        self.blob_name = blob_name

class BlobExists(Exception):
    def __init__(self, blob_name: str):
        super().__init__(f"Blob '{blob_name}' already exists.")
        self.blob_name = blob_name

@dataclass
class Handle:
    path: Path
    _locker: "Locker"
    _blob_name: str
    async def flush(self): await self._locker._upload_blob(self._blob_name)

class Locker:
    """A class that provides concurrency-safe access to a local copy of a Google Cloud Storage bucket."""

    def __init__(self, bucket_name: str, local_dir: Path,
                 *, bucket=None # for testing purposes
                 ):
        self.bucket_name = bucket_name
        self.local_dir = local_dir.resolve()
        self._client = storage.Client()
        self._bucket = bucket or self._client.bucket(bucket_name)
        self._file_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

    def local_path(self, blob_name: str) -> Path:
        """Path to the local copy of a blob."""
        p = (self.local_dir / blob_name).resolve()
        if not p.is_relative_to(self.local_dir):
            raise ValueError("Invalid blob name (path traversal)")

        return p

    @asynccontextmanager
    async def owned_local_copy(self, blob_name: str, *, allow_missing: bool = False, verify_checksum: bool = False) -> AsyncIterator[Handle]:
        """
            Context manager for safe read-write access to a blob via a local copy.
            It will download the blob if local copy is out of sync and upload it upon exiting the context if the content has changed.
            The local file will be locked for the duration of the context to prevent concurrent access.
            Will raise BlobNotFoundError if the blob does not exist in GCS, unless allow_missing=True.

            Args:
                blob_name: Name of the blob in GCS
                allow_missing: If False, will raise BlobNotFoundError if the blob does not exist in GCS.
                               If True, getting an owned copy of a missing blob will delete any existing local copy.
                verify_checksum: If True, also verify that the local file's checksum matches the remote blob's checksum.
                                 This catches local modifications made out-of-band since the last download.
        """
        local_path = self.local_path(blob_name)
        # Acquire local copy lock to prevent concurrent access on the same local copy
        async with self._local_lock(blob_name):
            # Acquire a lock on the blob in GCS to prevent concurrent access on different local copies
            await self._acquire_blob_lock(blob_name)
            try:
                # Ensure the file is available locally
                await self._download_blob(blob_name, allow_missing=allow_missing, verify_checksum=verify_checksum)
                try:
                    # Create a Handle and yield it to the caller
                    handle = Handle(local_path, self, blob_name)
                    yield handle
                finally:
                    # Once the caller is done, upload the file even if the caller raised an exception
                    # (but not if download failed!)
                    await self._upload_blob(blob_name)
            finally:
                # Release the blob lock in GCS even if download failed or caller raised an exception
                await self._release_blob_lock(blob_name)

    @asynccontextmanager
    async def readonly_local_copy(self, blob_name: str, *, verify_checksum: bool = False) -> AsyncIterator[Handle]:
        """
            Context manager for safe read-only access to a blob via a local copy.
            It will download the blob if local copy is out of sync, but will not lock or upload the blob.
            The local file will be locked for the duration of the context (to prevent reading an incomplete file).
            Will raise BlobNotFoundError if the blob does not exist in GCS.

            Args:
                blob_name: Name of the blob in GCS
                verify_checksum: If True, also verify that the local file's checksum matches the remote blob's checksum.
                                 This catches local modifications made out-of-band since the last download.
        """
        local_path = self.local_path(blob_name)
        # Acquire local copy lock to prevent reading while someone else is writing
        async with self._local_lock(blob_name):
            # Ensure the session file is available locally
            await self._download_blob(blob_name, verify_checksum=verify_checksum)
            # Create a Handle and yield it to the caller
            handle = Handle(local_path, self, blob_name)
            yield handle

    @asynccontextmanager
    async def new(self, blob_name: str) -> AsyncIterator[Path]:
        """
            Context manager for creating a new blob in GCS.
            It will create the local parent dir (but not the file); it will upload the file to GCS when done.
            Because this local file is not flushable, we do not yield a Handle here, but rather the path itself.
            Unlike owned_local_copy, if the caller raises an exception, the blob will not be uploaded;
            this makes more sense since partially created blobs are not useful.
            The local file will be locked for the duration of the context to prevent concurrent access.
        """
        local_path = self.local_path(blob_name)
        async with self._local_lock(blob_name):
            # Create parent directory if it does not exist
            local_path.parent.mkdir(parents=True, exist_ok=True)
            if local_path.exists():
                logger.error(f"[{PROCESS_ID}] Tried to create blob, but local copy {local_path} already exists.")
                raise FileExistsError(f"Local file {local_path} already exists.")
            yield local_path  # Yield the local path to the caller
            if not local_path.exists():
                logger.error(f"[{PROCESS_ID}] Tried to upload non-existent local blob file {local_path}.")
                raise FileNotFoundError(f"Local file {local_path} does not exist after creation.")
            # Upload the blob to GCS
            blob = self._bucket.blob(blob_name)
            try:
                await self._io(blob.upload_from_filename, local_path, if_generation_match=0)  # 0 means 'create new'
                await self._save_generation(blob_name, blob)  # Save the generation number
                logger.info(f"[{PROCESS_ID}] Created new blob {blob_name} in GCS from {local_path}")
            except PreconditionFailed:
                # Blob already exists, raise an error
                logger.error(f"[{PROCESS_ID}] Blob {blob_name} already exists in GCS.")
                raise BlobExists(blob_name)

    async def _download_blob(self, blob_name: str, allow_missing: bool = False, verify_checksum: bool = False):
        """
            Ensure that the local file is in sync with the blob in GCS.
            This function is not thread-safe: client must lock the local file before calling it.

            Args:
                blob_name: Name of the blob in GCS
                allow_missing: If False, will raise BlobNotFoundError if the blob does not exist in GCS.
                               If True, "downloading" a missing blob means deleting the local copy if it exists.
                verify_checksum: If True, also verify that the local file's checksum matches the remote blob's checksum.
                                 This catches local modifications made out-of-band since the last download.
        """
        local_path = self.local_path(blob_name)
        if await self._local_in_sync(blob_name, verify_checksum=verify_checksum):
            # Local copy is up-to-date, no need to download
            logger.debug(f"[{PROCESS_ID}] Blob {blob_name} is up-to-date, no download needed")
        else:
            local_path.parent.mkdir(parents=True, exist_ok=True)
            blob = self._bucket.blob(blob_name)
            if await self._io(blob.exists):
                await self._io(blob.download_to_filename, local_path)
                await self._save_generation(blob_name, blob)
                logger.info(f"[{PROCESS_ID}] Downloaded blob {blob_name} to {local_path}")
            elif allow_missing:
                # If a local copy exists, delete it
                if local_path.exists():
                    await self._io(local_path.unlink)
                    logger.info(f"[{PROCESS_ID}] Downloaded missing blob {blob_name} by deleting the local copy {local_path}")
                # Save generation as 0 because we need to remember than the blob had not existed at download time
                self._save_generation_0(blob_name)
            else:
                logger.error(f"[{PROCESS_ID}] Blob {blob_name} does not exist in GCS.")
                raise BlobNotFound(blob_name)

    async def _upload_blob(self, blob_name: str):
        """
            Upload the blob back to GCS if it has been modified.
            This function is not thread-safe: client must lock the blob file before calling it.

            Args:
                blob_name: Name of the blob in GCS
                allow_missing: If True, needs to handle missing blobs and local files.
        """
        local_path = self.local_path(blob_name)
        local_gen = self._local_generation(blob_name) # Generation at the time of download
        # local_gen cannot be None because it is always set to a value (real gen or 0) by _download_blob
        assert local_gen is not None

        if not local_path.exists():
            if local_gen != 0:
                # The local file is missing but the blob existed at download time
                # Delete the remote blob to keep in sync
                blob = self._bucket.blob(blob_name)
                try:
                    await self._io(blob.delete, if_generation_match=local_gen)
                    self._save_generation_0(blob_name)
                    logger.info(f"[{PROCESS_ID}] Deleted remote blob {blob_name} because local file was deleted")
                except PreconditionFailed:
                    # Blob was modified remotely since download - this is a conflict
                    logger.error(f"[{PROCESS_ID}] Conflict detected while trying to delete blob {blob_name}")
                    # We don't update the generation, forcing a re-download next time
            # If the blob did not exist at download time (local_gen == 0) and the local file is missing, nothing to do
            return

        blob = self._bucket.blob(blob_name)
        # If the blob exists, reload to get the latest generation info before comparing
        blob_exists = await self._io(blob.exists)
        if blob_exists:
            await self._io(blob.reload)

        if await self._io(self._files_differ_crc32c, local_path, blob):
            # files differ (or remote does not exist) - upload and handle conflicts
            try:
                await self._io(blob.upload_from_filename, local_path, if_generation_match=local_gen)
                await self._save_generation(blob_name, blob)
                logger.info(f"[{PROCESS_ID}] Uploaded blob {blob_name} to GCS")
            except PreconditionFailed:
                logger.error(f"[{PROCESS_ID}] Conflict detected while uploading blob {blob_name} from {local_path}")
                await self.handle_conflict(blob_name, local_path)
                # In case of conflict we do not update the local generation to force a re-download next time
        else:
            logger.debug(f"[{PROCESS_ID}] Blob {blob_name} is up-to-date, no upload needed")

    async def handle_conflict(self, blob_name: str, local_path: Path):
        """
            If a blob has been modified in an unprotected manner,
            handle a conflict by uploading the local file to a new unique blob name.
        """
        new_blob_name = f"{blob_name}.conflict.{uuid.uuid4()}"
        logger.error(f"[{PROCESS_ID}] Blob {blob_name} was modified in an unprotected manner. Uploading to {new_blob_name} instead.")
        blob = self._bucket.blob(new_blob_name)
        await self._io(blob.upload_from_filename, local_path, if_generation_match=0)  # 0 means 'create new'

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
                await self._io(blob.upload_from_string, content, if_generation_match=0)
                return  # acquired
            except PreconditionFailed:
                logger.info(f"[{PROCESS_ID}] Waiting for blob lock on {blob_name}...")
                await asyncio.sleep(delay)  # non-blocking wait
            except Exception as e:
                raise RuntimeError(f"Unexpected error acquiring lock: {e}")

        raise TimeoutError(f"Could not acquire lock for blob {blob_name}")

    async def _release_blob_lock(self, blob_name: str):
        """
            Release the lock on the blob in GCS.
            This function should be called after uploading the blob (once done with it).
        """
        blob = self._bucket.blob(self._remote_lock_path(blob_name))
        try:
            await self._io(blob.delete)
        except Exception as e:
            logger.error(f"[{PROCESS_ID}] Failed to delete lock for {blob_name}: {e}")

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

    async def _local_in_sync(self, blob_name: str, verify_checksum: bool = False) -> bool:
        """Check if the local copy of the blob is in sync with GCS.

        Args:
            blob_name: Name of the blob in GCS
            verify_checksum: If True, also verify that the local file's checksum matches the remote blob's checksum.
                             This catches local modifications made out-of-band since the last download.
        """
        local_gen = self._local_generation(blob_name)
        if local_gen is None:
            return False # No local generation info: not in sync
        local_path = self.local_path(blob_name)
        if not local_path.exists():
            # This should not happen under normal operation,
            # but someone could have deleted the local file out of band
            return False
        blob = self._bucket.blob(blob_name)
        if await self._io(blob.exists):
            await self._io(blob.reload)
            if blob.generation != local_gen:
                return False
            if verify_checksum:
                # Generations match, but verify content hasn't been modified locally
                if await self._io(self._files_differ_crc32c, local_path, blob):
                    logger.warning(f"[{PROCESS_ID}] Local file {local_path} has been modified out-of-band (checksum mismatch)")
                    return False
            return True
        return False # Local copy exists but remote blob does not: not in sync

    def _local_generation(self, blob_name: str) -> Optional[int]:
        """Get the local generation number of the blob."""
        meta_path = self._meta_path(blob_name)
        if not meta_path.exists():
            return None
        with open(meta_path) as f:
            return int(f.read().strip())

    async def _save_generation(self, blob_name: str, blob: storage.Blob):
        """
            Save the blob's generation number to a local metadata file.
            This is used to check if the local copy is still in sync with GCS.
        """
        await self._io(blob.reload)
        with open(self._meta_path(blob_name), 'w') as f:
            f.write(str(blob.generation))

    def _save_generation_0(self, blob_name: str):
        """
            Save generation to be 0 (blob does not exist)
        """
        with open(self._meta_path(blob_name), 'w') as f:
            f.write("0")

    def _crc32c_of_file(self, path: Path) -> bytes:
        """Calculate the CRC32C checksum of a file."""
        crc = google_crc32c.Checksum()
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                crc.update(chunk)
        return crc.digest()  # returns bytes

    def _files_differ_crc32c(self, local_path: Path, blob: storage.Blob) -> bool:
        """Check if the local file differs from the GCS blob using CRC32C."""
        if not blob.crc32c:
            return True  # can't compare

        local_crc = self._crc32c_of_file(local_path)
        remote_crc = base64.b64decode(blob.crc32c)

        return local_crc != remote_crc

    async def _io(self, fn, *a, **kw):
        """Run a blocking function in a thread pool to avoid blocking the event loop."""
        return await asyncio.to_thread(fn, *a, **kw)
