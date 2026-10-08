"""ETags, checksums and fingerprints: deciding whether two objects match.

An ETag pins the content only sometimes -- a multipart upload's is a digest of
its parts' digests, so it depends on the part size the uploader chose. Every
rule about when a comparison is meaningful lives here.
"""

import hashlib


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
