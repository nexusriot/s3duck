"""ETags, checksums and fingerprints: deciding whether two objects match.

An ETag pins the content only sometimes -- a multipart upload's is a digest of
its parts' digests, so it depends on the part size the uploader chose. Every
rule about when a comparison is meaningful lives here.
"""

import hashlib
import os


# Server-side encryption under which S3's ETag is NOT an MD5 of the bytes.
# SSE-S3 ("AES256") still is one; these are not, and SSE-C is not either.
OPAQUE_ETAG_ENCRYPTION = ("aws:kms", "aws:kms:dsse")


def normalize_etag(etag) -> str:
    return (etag or "").replace('"', "").strip()


def etag_is_md5(etag) -> bool:
    """
    True when the ETag is a plain MD5 we can reproduce locally. Multipart
    uploads use a '<md5-of-md5s>-<parts>' form that cannot be checked
    without knowing the original part boundaries.
    """
    tag = normalize_etag(etag)
    return bool(tag) and "-" not in tag and len(tag) == 32


def etag_is_opaque(head) -> bool:
    """
    True when a HEAD response says the object's ETag is no digest of its bytes.

    S3 documents the ETag as an MD5 only for objects stored unencrypted or
    with SSE-S3. Under SSE-KMS, DSSE-KMS and SSE-C it is still 32 hex
    characters -- so etag_is_md5 cannot tell -- but it describes nothing a
    local file can reproduce, and comparing it reported every intact
    download of such an object as corrupt.
    """
    head = head or {}
    if head.get("SSECustomerAlgorithm"):
        return True
    mode = str(head.get("ServerSideEncryption") or "").lower()
    return mode in OPAQUE_ETAG_ENCRYPTION


def file_md5(path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.md5()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(chunk_size), b""):
            digest.update(block)
    return digest.hexdigest()


def composite_etag(md5_digests) -> str:
    """
    Rebuild a multipart ETag: MD5 over the concatenated part MD5s, then
    "-<count>". This is how S3 forms one, so it is how it is checked.
    """
    digests = list(md5_digests)
    if not digests:
        return ""
    combined = hashlib.md5(b"".join(digests)).hexdigest()
    return f"{combined}-{len(digests)}"


def file_part_md5s(path, part_size, chunk_size: int = 1024 * 1024) -> list:
    """Raw MD5 digests of a local file cut into *part_size* pieces."""
    part_size = int(part_size)
    digests = []
    with open(path, "rb") as handle:
        while True:
            digest = hashlib.md5()
            remaining = part_size
            while remaining:
                block = handle.read(min(chunk_size, remaining))
                if not block:
                    break
                digest.update(block)
                remaining -= len(block)
            if remaining == part_size:
                break
            digests.append(digest.digest())
            if remaining:
                break
    return digests


def file_matches_etag(path, etag, part_sizes=()):
    """
    Whether the local file at *path* holds the bytes *etag* describes:
    True, False, or None when the ETag cannot say.

    A plain MD5 settles it either way. A multipart ETag depends on the part
    size the uploader chose, and nothing records that, so it is rebuilt at
    each of *part_sizes* that yields the right number of parts: a match is
    proof, but no match is only None -- the same bytes split elsewhere
    digest differently. Whether the ETag is a digest at all is the caller's
    question (etag_is_opaque); this assumes it is.
    """
    tag = normalize_etag(etag)
    if etag_is_md5(tag):
        return file_md5(path) == tag
    _digest, sep, count = tag.rpartition("-")
    if not sep or not count.isdigit() or int(count) < 1:
        return None
    size = os.path.getsize(path)
    for part in sorted({int(p) for p in part_sizes or () if int(p) > 0}):
        if max(1, -(-size // part)) != int(count):
            continue
        if composite_etag(file_part_md5s(path, part)) == tag:
            return True
    return None


def checksum_is_composite(value) -> bool:
    """
    Whether a stored checksum is a per-part composite ('<digest>-<parts>').

    A composite cannot be reproduced without the original part boundaries,
    exactly like a multipart ETag. Full-object checksums (what CRC32 with
    ChecksumType=FULL_OBJECT produces) have no suffix and are comparable.
    """
    text = str(value or "")
    head, sep, tail = text.rpartition("-")
    return bool(sep and head and tail.isdigit())


def compare_fingerprints(fingerprints) -> str:
    """
    "same", "different" or "unknown" for a group of ``(kind, value)``.

    Only a whole-object digest can prove two objects differ; per-part
    digests can prove they match but never that they do not.

    Even then the digests have to be comparable. Every value carries the
    algorithm that produced it ("CRC32:…", "MD5:…"), and objects uploaded
    under different client settings routinely end up with different ones —
    a CRC32 next to an ETag-derived MD5. Those two strings differ for
    identical bytes, so a mixed group is "unknown", not "different";
    calling it "different" quietly ruled real duplicates out.
    """
    entries = list(fingerprints or [])
    if len(entries) < 2 or any(not kind for kind, _v in entries):
        return "unknown"
    values = {value for _kind, value in entries}
    if len(values) == 1:
        return "same"
    algorithms = {value.split(":", 1)[0] for _kind, value in entries}
    if all(kind == "full" for kind, _v in entries) and len(algorithms) == 1:
        return "different"
    return "unknown"
