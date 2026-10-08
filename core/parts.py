"""Part sizes for multipart uploads and server-side copies.

S3 sets a floor on every part but the last and a ceiling on the count, so the
size is derived from the object rather than chosen.
"""


MIN_COPY_PART_BYTES = 5 * 1024 * 1024   # S3's floor for a non-final part


MAX_MULTIPART_PARTS = 10000             # S3's ceiling on part count


def copy_part_size(size, preferred=0) -> int:
    """
    Part size for a multipart copy of an object of *size* bytes.

    Large enough that the object fits in S3's 10 000 parts, never below
    its 5 MiB floor for a non-final part. The configured multipart chunk
    size is honoured when it satisfies both.
    """
    size = int(size)
    needed = (size + MAX_MULTIPART_PARTS - 1) // MAX_MULTIPART_PARTS
    return max(int(preferred or 0), needed, MIN_COPY_PART_BYTES)


def _multipart_upload_size(client, bucket, key, upload_id) -> int:
    """Sum the sizes of the parts already uploaded for one multipart upload."""
    total = 0
    paginator = client.get_paginator("list_parts")
    for page in paginator.paginate(Bucket=bucket, Key=key, UploadId=upload_id):
        for part in (page.get("Parts", []) or []):
            total += int(part.get("Size") or 0)
    return total
