import re
import pytest

from bucket_locker import Locker
from ._fakes import FakeBucket  # your in-memory fake

CONFLICT_RE = re.compile(r"^(.+)\.conflict\.[0-9a-fA-F-]+$")

def _list_conflicts(bucket: FakeBucket, blob_name: str) -> list[str]:
    # We peek into the fake's internal store to find conflict objects.
    return [k for k in bucket._store.keys() if CONFLICT_RE.match(k) and k.startswith(f"{blob_name}.conflict.")]

@pytest.mark.asyncio
async def test_conflict_writes_side_object_and_preserves_original(tmp_path):
    bucket = FakeBucket()
    # Seed original content v1
    bucket.blob("data.txt").upload_from_string("v1")

    lk1 = Locker("dummy", tmp_path, bucket=bucket)

    # Start a write session and modify local to v2
    async with lk1.owned_local_copy("data.txt") as handle1:
        handle1.path.write_text("v2")
        # Simulate an *unprotected* remote writer that ignored the lock
        bucket.blob("data.txt").upload_from_string("REMOTE")

    # After exiting, lk1 should have written to a conflict object, not the original
    # Original should remain "REMOTE"
    async with lk1.readonly_local_copy("data.txt") as handle2:
        assert handle2.path.read_text() == "REMOTE"

    # Exactly one conflict object should exist; its content should be "v2"
    conflicts = _list_conflicts(bucket, "data.txt")
    assert len(conflicts) == 1
    conflict_name = conflicts[0]
    cblob = bucket.blob(conflict_name)
    assert cblob.exists()
    # Read it back via a quick download
    tmp = tmp_path / "conflict_readback"
    cblob.download_to_filename(tmp)
    assert tmp.read_text() == "v2"

@pytest.mark.asyncio
async def test_conflict_forces_redownload_on_next_access(tmp_path):
    bucket = FakeBucket()
    bucket.blob("model.bin").upload_from_string("base")

    lk = Locker("dummy", tmp_path, bucket=bucket)

    # First session - create a conflict
    async with lk.owned_local_copy("model.bin") as handle:
        handle.path.write_text("local-change")
        # Cause an external write to force a conflict
        bucket.blob("model.bin").upload_from_string("remote-change")

    # There should be a conflict object
    conflicts = _list_conflicts(bucket, "model.bin")
    assert conflicts, "expected a conflict object to be created"

    # Next session should re-download the remote content
    async with lk.readonly_local_copy("model.bin") as handle:
        assert handle.path.read_text() == "remote-change", "Next access should download latest remote content"

@pytest.mark.asyncio
async def test_no_conflict_when_remote_changes_metadata_only_crc_equal(tmp_path):
    """
    This test assumes your upload path is CRC-first:
    if the remote generation changes but bytes are identical, no conflict should be created.
    If your policy is 'conflict on any gen change', skip/remove this test.
    """
    bucket = FakeBucket()
    bucket.blob("notes.txt").upload_from_string("same-bytes")

    lk = Locker("dummy", tmp_path, bucket=bucket)

    # Open session (download), then re-write identical bytes remotely (new gen, same CRC)
    async with lk.owned_local_copy("notes.txt") as handle:
        # re-upload same bytes to bump generation but keep CRC equal
        bucket.blob("notes.txt").upload_from_string("same-bytes")

    # If CRC-first, no conflict object should exist
    conflicts = _list_conflicts(bucket, "notes.txt")
    assert len(conflicts) == 0, "CRC-equal change should not create a conflict object"

    # And local_in_sync should say 'up-to-date' only if you saved the new gen on skip;
    # if not, it should force a re-download next time. Either is acceptable by policy.

@pytest.mark.asyncio
async def test_delete_local_file_conflict_when_remote_changed(tmp_path):
    """Test that deleting a local file when remote changed results in a conflict."""
    bucket = FakeBucket()
    bucket.blob("conflict_delete.txt").upload_from_string("v1")
    lk = Locker("dummy", tmp_path, bucket=bucket)

    async with lk.owned_local_copy("conflict_delete.txt") as handle:
        # Delete the local file
        handle.path.unlink()
        # Someone else modifies the remote blob
        bucket.blob("conflict_delete.txt").upload_from_string("v2")

    # Remote blob should still exist (conflict prevented deletion)
    assert bucket.blob("conflict_delete.txt").exists()
    # Content should be v2 (the remote change)
    tmp = tmp_path / "verify"
    bucket.blob("conflict_delete.txt").download_to_filename(tmp)
    assert tmp.read_text() == "v2"
