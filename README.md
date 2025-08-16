# bucket-locker

**Concurrency-safe local copies of Google Cloud Storage (GCS) blobs in Python.**

## Why?

You need this library if:

- You are using a GCS bucket.
- Your system, app, or service downloads files from that bucket, works on local copies, and then uploads them back.

## What it does

`bucket-locker` helps you:

- **Avoid data races**
  - between accesses to the same local copy (e.g. multiple threads/tasks), and
  - between different local copies of the same blob (e.g. multiple processes/instances).
- **Avoid redundant I/O**
  - skip downloads if your local copy is already in sync,
  - skip uploads if the blob hasn’t changed.

## Installation

```bash
pip install bucket-locker
```

## Usage

```python
import asyncio
from pathlib import Path
from bucket_locker import Locker, BlobNotFound

async def main():
    locker = Locker(bucket_name="my-bucket", local_dir=Path("/tmp/bucket"))

    # Safe read–write access
    try:
        async with locker.owned_local_copy("data.json") as handle:
            # work on the local file
            data = handle.path.read_text()
            handle.path.write_text(data + "\nextra line")
            # changes will be uploaded automatically on exit
    except BlobNotFound:
        print("Blob not found in GCS")

    # Safe read-only access: does not lock the blob
    async with locker.readonly_local_copy("config.yaml") as handle:
        config = handle.path.read_text()
        print(config)

asyncio.run(main())
```

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.