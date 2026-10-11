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


def upload_part_size(size, preferred) -> int:
    """
    Part size for a multipart upload of *size* bytes.

    S3 numbers parts 1..10 000 and refuses anything past that, so a fixed
    part size is only usable up to ``part x 10 000`` — 80 GiB at the default
    8 MiB, which is exactly the range the resumable writer exists for. The
    size is raised just far enough to fit, and otherwise left alone.

    No 5 MiB floor here, unlike ``copy_part_size``: the configured chunk is
    already clamped to it by ``set_multipart_sizes``, and raising a smaller
    one would silently rewrite a part size a caller chose on purpose. The
    ceiling is what S3 enforces; the floor is a setting.
    """
    size = int(size)
    preferred = max(1, int(preferred or 0))
    needed = (size + MAX_MULTIPART_PARTS - 1) // MAX_MULTIPART_PARTS
    return max(preferred, needed)


def _multipart_upload_size(client, bucket, key, upload_id) -> int:
    """Sum the sizes of the parts already uploaded for one multipart upload."""
    total = 0
    paginator = client.get_paginator("list_parts")
    for page in paginator.paginate(Bucket=bucket, Key=key, UploadId=upload_id):
        for part in (page.get("Parts", []) or []):
            total += int(part.get("Size") or 0)
    return total
