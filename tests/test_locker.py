import asyncio
import pytest
from bucket_locker import Locker, BlobNotFound, BlobExists
from ._fakes import FakeBucket

@pytest.mark.asyncio
async def test_readonly_downloads(tmp_path):
    bucket = FakeBucket()
    # seed remote blob
    bucket.blob("a.txt").upload_from_string("hello")
    lk = Locker("dummy", tmp_path, bucket=bucket)

    async with lk.readonly_local_copy("a.txt") as handle:
        assert handle.path.read_text() == "hello"

@pytest.mark.asyncio
async def test_owned_uploads_when_changed(tmp_path):
    bucket = FakeBucket()
    bucket.blob("b.txt").upload_from_string("v1")
    lk = Locker("dummy", tmp_path, bucket=bucket)

    async with lk.owned_local_copy("b.txt") as handle:
        handle.path.write_text("v2")

    # After context, upload should have happened
    assert bucket.blob("b.txt").exists()
    # A second readonly copy should see v2
    lk2 = Locker("dummy", tmp_path, bucket=bucket)
    async with lk2.readonly_local_copy("b.txt") as handle2:
        assert handle2.path.read_text() == "v2"

@pytest.mark.asyncio
async def test_missing_blob_raises(tmp_path):
    lk = Locker("dummy", tmp_path, bucket=FakeBucket())
    with pytest.raises(BlobNotFound):
        async with lk.readonly_local_copy("nope.txt"):
            pass

@pytest.mark.asyncio
async def test_local_lock_serializes_access(tmp_path):
    bucket = FakeBucket()
    bucket.blob("c.txt").upload_from_string("x")
    lk = Locker("dummy", tmp_path, bucket=bucket)

    started = asyncio.Event()
    order = []

    async def writer1():
        async with lk.owned_local_copy("c.txt") as handle:
            started.set()
            await asyncio.sleep(0.3)
            # Append lines
            with open(handle.path, "a") as f:
                f.write("one line 1\n")
                f.write("one line 2\n")
                f.write("one line 3\n")
            order.append("one")

    async def writer2():
        await started.wait()
        async with lk.owned_local_copy("c.txt") as handle:
            # Append lines
            with open(handle.path, "a") as f:
                f.write("two line 1\n")
                f.write("two line 2\n")
                f.write("two line 3\n")
            order.append("two")

    await asyncio.gather(writer1(), writer2())

    # Check the final blob content
    assert bucket.blob("c.txt").exists()
    # Read back through readonly path
    async with lk.readonly_local_copy("c.txt") as handle:
        content = handle.path.read_text()
        # The content should have both sets of changes in order
        expected = "x" + "one line 1\none line 2\none line 3\n" + "two line 1\ntwo line 2\ntwo line 3\n"
        assert content == expected
    # And they ran in order (lock forced serialization)
    assert order == ["one", "two"]

@pytest.mark.asyncio
async def test_remote_lock_serializes_access(tmp_path):
    bucket = FakeBucket()
    bucket.blob("c.txt").upload_from_string("x")

    # Create two lockers with different local paths
    path1 = tmp_path / "loc1"
    path2 = tmp_path / "loc2"
    path1.mkdir()
    path2.mkdir()

    lk1 = Locker("dummy", path1, bucket=bucket)
    lk2 = Locker("dummy", path2, bucket=bucket)

    started = asyncio.Event()
    order = []

    async def writer1():
        async with lk1.owned_local_copy("c.txt") as handle:
            started.set()
            await asyncio.sleep(0.3)
            # Append lines
            with open(handle.path, "a") as f:
                f.write("one line 1\n")
                f.write("one line 2\n")
                f.write("one line 3\n")
            order.append("one")

    async def writer2():
        await started.wait()
        async with lk2.owned_local_copy("c.txt") as handle:
            # Append lines
            with open(handle.path, "a") as f:
                f.write("two line 1\n")
                f.write("two line 2\n")
                f.write("two line 3\n")
            order.append("two")

    await asyncio.gather(writer1(), writer2())

    # Check the final blob content
    assert bucket.blob("c.txt").exists()
    # Read back through readonly path
    async with lk1.readonly_local_copy("c.txt") as handle:
        content = handle.path.read_text()
        # The content should have both sets of changes in order
        expected = "x" + "one line 1\none line 2\none line 3\n" + "two line 1\ntwo line 2\ntwo line 3\n"
        assert content == expected
    # And they ran in order (lock forced serialization)
    assert order == ["one", "two"]

@pytest.mark.asyncio
async def test_owned_context_exception_still_uploads_and_releases_lock(tmp_path):
    bucket = FakeBucket()
    bucket.blob("x.txt").upload_from_string("v1")
    lk = Locker("dummy", tmp_path, bucket=bucket)

    # cause an exception after modifying local copy
    with pytest.raises(RuntimeError, match="boom"):
        async with lk.owned_local_copy("x.txt") as handle:
            handle.path.write_text("v2")
            raise RuntimeError("boom")

    # change should still be uploaded
    async with lk.readonly_local_copy("x.txt") as handle2:
        assert handle2.path.read_text() == "v2"

    # remote lock should be gone (no deadlock)
    assert not bucket.blob("locks/x.txt.lock").exists()

@pytest.mark.asyncio
async def test_owned_context_exception_without_change_skips_upload_and_releases_lock(tmp_path):
    bucket = FakeBucket()
    bucket.blob("y.txt").upload_from_string("v1")
    lk = Locker("dummy", tmp_path, bucket=bucket)

    with pytest.raises(ValueError):
        async with lk.owned_local_copy("y.txt") as handle:
            _ = handle.path.read_text()  # no modification
            raise ValueError("oops")

    # content unchanged
    async with lk.readonly_local_copy("y.txt") as handle2:
        assert handle2.path.read_text() == "v1"

    # lock released
    assert not bucket.blob("locks/y.txt.lock").exists()

@pytest.mark.asyncio
async def test_no_deadlock_after_exception(tmp_path):
    bucket = FakeBucket()
    bucket.blob("z.txt").upload_from_string("start")
    lk1 = Locker("dummy", tmp_path, bucket=bucket)
    lk2 = Locker("dummy", tmp_path, bucket=bucket)

    # First context raises
    with pytest.raises(RuntimeError):
        async with lk1.owned_local_copy("z.txt") as handle:
            handle.path.write_text("mid")
            raise RuntimeError("boom")

    # Second context should proceed normally
    async with lk2.owned_local_copy("z.txt") as handle2:
        handle2.path.write_text("final")

    async with lk1.readonly_local_copy("z.txt") as handle3:
        assert handle3.path.read_text() == "final"
    assert not bucket.blob("locks/z.txt.lock").exists()

@pytest.mark.asyncio
async def test_download_failure_releases_lock(tmp_path):
    bucket = FakeBucket()  # empty; blob missing
    lk = Locker("dummy", tmp_path, bucket=bucket)

    with pytest.raises(BlobNotFound):
        async with lk.owned_local_copy("missing.txt"):
            pass

    assert not bucket.blob("locks/missing.txt.lock").exists()

@pytest.mark.asyncio
async def test_new_success(tmp_path):
    bucket = FakeBucket()
    lk = Locker("dummy", tmp_path, bucket=bucket)

    async with lk.new("fresh.txt") as p:
        p.write_text("hello")

    # Uploaded to GCS
    b = bucket.blob("fresh.txt")
    assert b.exists()
    tmp = tmp_path / "dl"
    b.download_to_filename(tmp)
    assert tmp.read_text() == "hello"

@pytest.mark.asyncio
async def test_new_conflicts_if_remote_already_exists(tmp_path):
    bucket = FakeBucket()
    bucket.blob("exists.txt").upload_from_string("old")
    lk = Locker("dummy", tmp_path, bucket=bucket)

    with pytest.raises(BlobExists):
        async with lk.new("exists.txt") as p:
            p.write_text("new")

@pytest.mark.asyncio
async def test_new_no_upload_on_exception(tmp_path):
    bucket = FakeBucket()
    lk = Locker("dummy", tmp_path, bucket=bucket)

    with pytest.raises(RuntimeError):
        async with lk.new("draft.bin") as p:
            p.write_bytes(b"partial")
            raise RuntimeError("boom")

    # Nothing uploaded
    assert not bucket.blob("draft.bin").exists()

@pytest.mark.asyncio
async def test_allow_missing_with_file_creation(tmp_path):
    """Test allow_missing=True when client creates the missing file."""
    bucket = FakeBucket()
    lk = Locker("dummy", tmp_path, bucket=bucket)

    # Blob doesn't exist in GCS initially
    assert not bucket.blob("new_file.txt").exists()

    async with lk.owned_local_copy("new_file.txt", allow_missing=True) as handle:
        # Client creates the file
        handle.path.write_text("created by client")

    # File should now exist in GCS after upload
    assert bucket.blob("new_file.txt").exists()

    # Verify the content was uploaded correctly
    tmp = tmp_path / "download_test"
    bucket.blob("new_file.txt").download_to_filename(tmp)
    assert tmp.read_text() == "created by client"

@pytest.mark.asyncio
async def test_allow_missing_without_file_creation(tmp_path):
    """Test allow_missing=True when client doesn't create the missing file."""
    bucket = FakeBucket()
    lk = Locker("dummy", tmp_path, bucket=bucket)

    # Blob doesn't exist in GCS initially
    assert not bucket.blob("missing_file.txt").exists()

    # This should not raise an exception even though the file doesn't exist
    # and the client doesn't create it
    async with lk.owned_local_copy("missing_file.txt", allow_missing=True) as handle:
        # Client doesn't create the file - just verify path exists but file doesn't
        assert not handle.path.exists()

    # File should still not exist in GCS
    assert not bucket.blob("missing_file.txt").exists()

@pytest.mark.asyncio
async def test_allow_missing_false_still_raises_for_missing_files(tmp_path):
    """Test that allow_missing=False (default) still raises BlobNotFound for missing files."""
    bucket = FakeBucket()
    lk = Locker("dummy", tmp_path, bucket=bucket)

    # Blob doesn't exist in GCS
    assert not bucket.blob("definitely_missing.txt").exists()

    # Should raise BlobNotFound with default allow_missing=False
    with pytest.raises(BlobNotFound):
        async with lk.owned_local_copy("definitely_missing.txt") as handle:
            handle.path.write_text("this won't work")

@pytest.mark.asyncio
async def test_allow_missing_with_existing_file_behaves_normally(tmp_path):
    """Test that allow_missing=True doesn't affect behavior for existing files."""
    bucket = FakeBucket()
    bucket.blob("existing.txt").upload_from_string("original content")
    lk = Locker("dummy", tmp_path, bucket=bucket)

    # File exists in GCS
    assert bucket.blob("existing.txt").exists()

    async with lk.owned_local_copy("existing.txt", allow_missing=True) as handle:
        # Should download the existing file
        assert handle.path.read_text() == "original content"
        # Client modifies it
        handle.path.write_text("modified content")

    # Should upload the modified content
    tmp = tmp_path / "verify_upload"
    bucket.blob("existing.txt").download_to_filename(tmp)
    assert tmp.read_text() == "modified content"

@pytest.mark.asyncio
async def test_delete_local_file_deletes_remote_blob(tmp_path):
    """Test that deleting a local file also deletes the remote blob."""
    bucket = FakeBucket()
    bucket.blob("to_delete.txt").upload_from_string("will be deleted")
    lk = Locker("dummy", tmp_path, bucket=bucket)

    # Remote blob exists
    assert bucket.blob("to_delete.txt").exists()

    async with lk.owned_local_copy("to_delete.txt") as handle:
        # Delete the local file
        handle.path.unlink()

    # Remote blob should also be deleted
    assert not bucket.blob("to_delete.txt").exists()

@pytest.mark.asyncio
async def test_delete_local_file_no_upload_when_blob_never_existed(tmp_path):
    """Test that deleting a local file that was created for a missing blob does nothing."""
    bucket = FakeBucket()
    lk = Locker("dummy", tmp_path, bucket=bucket)

    # Blob doesn't exist, create local file with allow_missing
    async with lk.owned_local_copy("never_existed.txt", allow_missing=True) as handle:
        # Create and then delete the local file
        handle.path.write_text("temporary")
        handle.path.unlink()

    # Remote blob should still not exist
    assert not bucket.blob("never_existed.txt").exists()


# --- verify_checksum tests ---

@pytest.mark.asyncio
async def test_verify_checksum_redownloads_when_local_modified(tmp_path):
    """Test that verify_checksum=True forces re-download when local file was modified out-of-band."""
    bucket = FakeBucket()
    bucket.blob("check.txt").upload_from_string("original")
    lk = Locker("dummy", tmp_path, bucket=bucket)

    # First, get a local copy to establish generation
    async with lk.readonly_local_copy("check.txt") as handle:
        assert handle.path.read_text() == "original"

    # Modify local file out-of-band (simulating corruption or accidental edit)
    local_path = lk.local_path("check.txt")
    local_path.write_text("modified out of band")

    # Without verify_checksum, generation matches so no re-download
    async with lk.readonly_local_copy("check.txt", verify_checksum=False) as handle:
        assert handle.path.read_text() == "modified out of band"

    # With verify_checksum=True, checksum mismatch forces re-download
    async with lk.readonly_local_copy("check.txt", verify_checksum=True) as handle:
        assert handle.path.read_text() == "original"


@pytest.mark.asyncio
async def test_verify_checksum_no_redownload_when_unchanged(tmp_path):
    """Test that verify_checksum=True doesn't re-download when local file is unchanged."""
    bucket = FakeBucket()
    bucket.blob("unchanged.txt").upload_from_string("content")
    lk = Locker("dummy", tmp_path, bucket=bucket)

    # Get initial copy
    async with lk.readonly_local_copy("unchanged.txt") as handle:
        assert handle.path.read_text() == "content"

    # Local file is unchanged, verify_checksum should still pass
    async with lk.readonly_local_copy("unchanged.txt", verify_checksum=True) as handle:
        assert handle.path.read_text() == "content"


@pytest.mark.asyncio
async def test_verify_checksum_owned_local_copy(tmp_path):
    """Test verify_checksum with owned_local_copy."""
    bucket = FakeBucket()
    bucket.blob("owned.txt").upload_from_string("remote content")
    lk = Locker("dummy", tmp_path, bucket=bucket)

    # First, get a local copy
    async with lk.owned_local_copy("owned.txt") as handle:
        assert handle.path.read_text() == "remote content"

    # Modify local file out-of-band
    local_path = lk.local_path("owned.txt")
    local_path.write_text("tampered")

    # With verify_checksum=True, should re-download and overwrite tampered content
    async with lk.owned_local_copy("owned.txt", verify_checksum=True) as handle:
        assert handle.path.read_text() == "remote content"
        # Now make a legitimate change
        handle.path.write_text("legitimate update")

    # Verify the legitimate change was uploaded
    tmp = tmp_path / "verify"
    bucket.blob("owned.txt").download_to_filename(tmp)
    assert tmp.read_text() == "legitimate update"


@pytest.mark.asyncio
async def test_verify_checksum_default_false(tmp_path):
    """Test that verify_checksum defaults to False (backward compatible)."""
    bucket = FakeBucket()
    bucket.blob("default.txt").upload_from_string("original")
    lk = Locker("dummy", tmp_path, bucket=bucket)

    # Get local copy
    async with lk.readonly_local_copy("default.txt") as handle:
        pass

    # Modify out-of-band
    lk.local_path("default.txt").write_text("modified")

    # Default behavior: no checksum verification, so modification persists
    async with lk.readonly_local_copy("default.txt") as handle:
        assert handle.path.read_text() == "modified"