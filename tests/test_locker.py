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