"""Small readings of an object's state: restore, tags, delete results."""

from core.errors import DeleteRefused


def raise_on_delete_errors(resp, log_fn=None, limit=5):
    """
    Turn the per-key refusals in a DeleteObjects response into a failure.

    DeleteObjects answers HTTP 200 even when it refused individual keys:
    the refusals come back in ``Errors``, and with ``Quiet`` set that is
    the only thing the body carries. Not reading it reported an
    object-lock, MFA-delete or per-key AccessDenied refusal as a
    completed delete — the queue row went green and the objects were
    still there.
    """
    errors = (resp or {}).get("Errors") or []
    if not errors:
        return
    described = []
    for err in errors[:limit]:
        code = err.get("Code") or "error"
        described.append(f"{err.get('Key', '?')} ({code})")
    if len(errors) > limit:
        described.append(f"and {len(errors) - limit} more")
    detail = ", ".join(described)
    if log_fn:
        log_fn(f"delete refused for {len(errors)} object(s): {detail}")
    raise DeleteRefused(
        f"The service refused to delete {len(errors)} object(s): {detail}"
    )


def _object_already_there(exc) -> bool:
    """Whether a conditional write was refused because the key is taken."""
    response = getattr(exc, "response", None) or {}
    code = (response.get("Error") or {}).get("Code", "")
    status = (response.get("ResponseMetadata") or {}).get("HTTPStatusCode")
    return code in ("PreconditionFailed", "ConditionalRequestConflict") \
        or status in (412, 409)


def merge_tags(current, add=None, remove=None, replace=False) -> list:
    """
    Work out the new TagSet for an object.

    current/result are boto3's [{'Key':k,'Value':v}] form. With replace the
    result is exactly *add*; otherwise *add* is merged over the existing
    tags and any key in *remove* is dropped. Insertion order is kept so a
    rewrite does not shuffle unrelated tags.
    """
    add = dict(add or {})
    remove = {str(k) for k in (remove or [])}
    if replace:
        merged = dict(add)
    else:
        merged = {}
        for tag in current or []:
            key = tag.get("Key")
            if key is None:
                continue
            merged[str(key)] = str(tag.get("Value") or "")
        merged.update({str(k): str(v) for k, v in add.items()})
        for key in remove:
            merged.pop(key, None)
    return [{"Key": k, "Value": v} for k, v in merged.items()]


def parse_restore_status(restore_header: str) -> str:
    """
    Turn the raw x-amz-restore header into a short human status.
      None / ""                                  -> ""  (not restored/archived)
      'ongoing-request="true"'                   -> "in-progress"
      'ongoing-request="false", expiry-date=...' -> "available until <date>"
    """
    if not restore_header:
        return ""
    if 'ongoing-request="true"' in restore_header:
        return "in-progress"
    if 'ongoing-request="false"' in restore_header:
        marker = 'expiry-date="'
        idx = restore_header.find(marker)
        if idx != -1:
            rest = restore_header[idx + len(marker):]
            end = rest.find('"')
            if end != -1:
                return f"available until {rest[:end]}"
        return "available"
    return restore_header
