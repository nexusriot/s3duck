"""Putting a prefix back the way it was at a given moment.

On a versioned bucket nothing is ever really overwritten or deleted: every
write leaves the previous version behind and every delete only adds a marker.
So "this folder as it was on Tuesday at 14:00" is recoverable for every key,
and the plan for getting there is pure bookkeeping over the version list:

* a key whose version at that moment is not the current one gets that
  version copied back on top (a new current version; history is kept);
* a key that did not exist then, or was deleted then, but exists now gets a
  delete marker (also reversible);
* everything else is left alone.

Nothing in the plan destroys a version, which is what makes it safe to run
on a whole prefix: the restore itself can be rolled back the same way.
"""

from core.naming import as_epoch

RESTORE, DELETE, SKIP = "restore", "delete", "skip"


def _stamp(entry):
    return as_epoch(entry.get("last_modified"))


def plan_point_in_time(versions, as_of) -> list:
    """
    The actions that make every key look as it did at *as_of*.

    ``versions`` are dicts with ``key``, ``version_id``, ``last_modified``,
    ``is_latest``, ``is_delete_marker`` and ``size`` -- one per version and
    per delete marker, in any order. Returns one action per key, sorted by
    key: ``{"action", "key", "version_id", "size", "reason"}``.
    """
    moment = as_epoch(as_of)
    by_key = {}
    for entry in versions or []:
        key = str(entry.get("key") or "")
        if key:
            by_key.setdefault(key, []).append(entry)

    actions = []
    for key in sorted(by_key):
        history = sorted(by_key[key], key=_stamp, reverse=True)
        current = next((v for v in history if v.get("is_latest")), history[0])
        then = next((v for v in history if _stamp(v) <= moment), None)
        exists_now = not current.get("is_delete_marker")
        existed_then = then is not None and not then.get("is_delete_marker")
        if not existed_then:
            if exists_now:
                actions.append({
                    "action": DELETE, "key": key, "version_id": "",
                    "size": int(current.get("size") or 0),
                    "reason": ("deleted at that time" if then is not None
                               else "did not exist yet")})
            else:
                actions.append({"action": SKIP, "key": key, "version_id": "",
                                "size": 0, "reason": "absent then and now"})
            continue
        if exists_now and then.get("version_id") == current.get("version_id"):
            actions.append({"action": SKIP, "key": key,
                            "version_id": then.get("version_id") or "",
                            "size": int(then.get("size") or 0),
                            "reason": "unchanged since"})
            continue
        actions.append({
            "action": RESTORE, "key": key,
            "version_id": str(then.get("version_id") or ""),
            "size": int(then.get("size") or 0),
            "reason": ("deleted since" if not exists_now
                       else "changed since")})
    return actions


def summarize_point_in_time(actions) -> dict:
    out = {RESTORE: 0, DELETE: 0, SKIP: 0, "bytes": 0}
    for action in actions or []:
        kind = action.get("action")
        out[kind] = out.get(kind, 0) + 1
        if kind == RESTORE:
            out["bytes"] += int(action.get("size") or 0)
    return out


def restore_job(actions) -> list:
    """Queue rows ``(key, action, version_id)`` for what is not a skip."""
    return [(a["key"], a["action"], a.get("version_id") or "")
            for a in actions or [] if a.get("action") in (RESTORE, DELETE)]
