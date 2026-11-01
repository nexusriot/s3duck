"""Reading, summarising and assembling bucket lifecycle rules.

A rule's filter has two historical shapes and three places a prefix can hide.
Getting that wrong turns "delete under logs/" into "delete everything", so the
reading is done once, here, and the editor and the summary both use it.
"""

import copy


# What the rule editor can hold. A rule outside these bounds cannot be shown
# in the form without the form changing it (a spin box clamps, a combo falls
# back to its first entry), so lifecycle_form_problem refuses it instead.
LIFECYCLE_TRANSITION_CLASSES = (
    "STANDARD_IA", "ONEZONE_IA", "INTELLIGENT_TIERING", "GLACIER_IR",
    "GLACIER", "DEEP_ARCHIVE",
)
LIFECYCLE_MAX_DAYS = 36500
LIFECYCLE_MAX_ABORT_DAYS = 3650


def lifecycle_rule_scope(rule) -> str:
    """The prefix a rule applies to, across both filter shapes."""
    if "Prefix" in rule and rule.get("Prefix") is not None:
        return str(rule.get("Prefix") or "")
    node = rule.get("Filter") or {}
    if "Prefix" in node:
        return str(node.get("Prefix") or "")
    combined = node.get("And") or {}
    return str(combined.get("Prefix") or "")


def lifecycle_rule_extras(rule) -> dict:
    """
    The parts of a filter that are NOT a prefix: tags and size bounds.

    Returns ``{"tags": [(key, value)], "min_size": int|None,
    "max_size": int|None}``. These narrow which objects a rule touches, so
    a summary that omits them describes a rule that deletes far more than
    the real one does.
    """
    node = dict((rule or {}).get("Filter") or {})
    combined = dict(node.get("And") or {})
    tags = list(combined.get("Tags") or [])
    if node.get("Tag"):
        tags = [node["Tag"]] + tags
    pairs = [(str(tag.get("Key") or ""), str(tag.get("Value") or ""))
             for tag in tags if isinstance(tag, dict)]
    minimum = node.get("ObjectSizeGreaterThan",
                       combined.get("ObjectSizeGreaterThan"))
    maximum = node.get("ObjectSizeLessThan",
                       combined.get("ObjectSizeLessThan"))
    return {
        "tags": pairs,
        "min_size": None if minimum is None else int(minimum),
        "max_size": None if maximum is None else int(maximum),
    }


def lifecycle_filter_is_simple(rule) -> bool:
    """
    Whether the prefix editor can represent this rule's filter without
    losing part of it.

    A rule filtered by tag or size that is rebuilt from a prefix form comes
    back matching every object under that prefix — which for an expiration
    rule means deleting things it never used to touch.
    """
    extras = lifecycle_rule_extras(rule)
    return not (extras["tags"] or extras["min_size"] is not None
                or extras["max_size"] is not None)


def lifecycle_form_problem(rule) -> str:
    """
    Why the rule editor cannot show this rule's actions faithfully, or "".

    The editor holds one transition by days, an expiration by days and the
    two clean-ups. Opening a rule it cannot hold showed the form's defaults
    in place of the real values — an unticked box for a second transition or
    a dated expiration, 30 for a day-0 transition — and OK then wrote the
    form back, quietly changing what the service does. The filter has its
    own guard (lifecycle_filter_is_simple); this one covers the actions.
    """
    rule = dict(rule or {})

    def _days_fit(value, upper):
        try:
            return 1 <= int(value) <= upper
        except (TypeError, ValueError):
            return False

    transitions = list(rule.get("Transitions") or [])
    if len(transitions) > 1:
        return ("It moves objects through more than one storage class, and "
                "the editor holds a single transition.")
    for transition in transitions:
        if transition.get("Days") is None:
            return "Its transition happens on a calendar date."
        if not _days_fit(transition.get("Days"), LIFECYCLE_MAX_DAYS):
            return (f"Its transition fires after {transition.get('Days')} "
                    "day(s), which the editor cannot hold.")
        if transition.get("StorageClass") not in LIFECYCLE_TRANSITION_CLASSES:
            return (f"It transitions to {transition.get('StorageClass')}, "
                    "which the editor does not offer.")
    expiration = rule.get("Expiration") or {}
    if expiration.get("Date") is not None:
        return "It expires objects on a calendar date."
    if (expiration.get("Days") is not None
            and not _days_fit(expiration["Days"], LIFECYCLE_MAX_DAYS)):
        return f"It expires objects after {expiration['Days']} day(s)."
    noncurrent = rule.get("NoncurrentVersionExpiration") or {}
    if (noncurrent.get("NoncurrentDays") is not None
            and not _days_fit(noncurrent["NoncurrentDays"], LIFECYCLE_MAX_DAYS)):
        return (f"It deletes old versions after "
                f"{noncurrent['NoncurrentDays']} day(s).")
    abort = rule.get("AbortIncompleteMultipartUpload") or {}
    if (abort.get("DaysAfterInitiation") is not None
            and not _days_fit(abort["DaysAfterInitiation"],
                              LIFECYCLE_MAX_ABORT_DAYS)):
        return (f"It aborts uploads after {abort['DaysAfterInitiation']} "
                "day(s).")
    return ""


def summarize_lifecycle_rule(rule) -> str:
    """One readable line for a rule, for a list the user can scan."""
    rule = dict(rule or {})
    parts = []
    scope = lifecycle_rule_scope(rule)
    parts.append(f"prefix '{scope}'" if scope else "whole bucket")
    extras = lifecycle_rule_extras(rule)
    for key, value in extras["tags"]:
        parts.append(f"tagged {key}={value}" if value else f"tagged {key}")
    if extras["min_size"] is not None:
        parts.append(f"larger than {extras['min_size']} B")
    if extras["max_size"] is not None:
        parts.append(f"smaller than {extras['max_size']} B")
    for transition in rule.get("Transitions") or []:
        days = transition.get("Days")
        target = transition.get("StorageClass") or "?"
        if days is not None:
            parts.append(f"to {target} after {days}d")
        else:
            parts.append(f"to {target} on {transition.get('Date')}")
    expiration = rule.get("Expiration") or {}
    if expiration.get("Days") is not None:
        parts.append(f"delete after {expiration['Days']}d")
    elif expiration.get("Date"):
        parts.append(f"delete on {expiration['Date']}")
    if expiration.get("ExpiredObjectDeleteMarker"):
        parts.append("clean up delete markers")
    noncurrent = rule.get("NoncurrentVersionExpiration") or {}
    # NewerNoncurrentVersions is what keeps the newest N old versions out of
    # reach of the rule. A summary without it describes a rule that deletes
    # every old version, which is not the one being confirmed.
    if noncurrent.get("NoncurrentDays") is not None:
        kept = noncurrent.get("NewerNoncurrentVersions")
        parts.append(
            f"delete old versions after {noncurrent['NoncurrentDays']}d"
            + (f" (keeping the newest {kept})" if kept else ""))
    for transition in rule.get("NoncurrentVersionTransitions") or []:
        kept = transition.get("NewerNoncurrentVersions")
        parts.append(
            f"old versions to {transition.get('StorageClass')} after "
            f"{transition.get('NoncurrentDays')}d"
            + (f" (beyond the newest {kept})" if kept else ""))
    abort = rule.get("AbortIncompleteMultipartUpload") or {}
    if abort.get("DaysAfterInitiation") is not None:
        parts.append(
            "abort incomplete uploads after "
            f"{abort['DaysAfterInitiation']}d")
    if len(parts) == 1:
        parts.append("does nothing")
    return " · ".join(parts)


def build_lifecycle_rule(rule_id, *, prefix="", enabled=True,
                         transition_days=None, transition_class="",
                         expire_days=None, noncurrent_days=None,
                         abort_days=None, expire_delete_markers=False,
                         base=None):
    """
    Assemble a rule document from the fields the editor collects.

    *base* is the rule being edited. The result starts from a copy of it and
    only the fields the form controls are overwritten, so whatever the form
    has no box for — NoncurrentVersionTransitions, NewerNoncurrentVersions,
    anything a newer API adds — comes back as it was. Rebuilding from the
    form alone dropped them on every Edit -> OK, and losing
    NewerNoncurrentVersions turns "keep the newest 3 old versions" into
    "delete every old version".

    Raises ValueError when the result would do nothing (S3 accepts such a
    rule and then silently never fires it), or asks for two expirations S3
    will not combine.
    """
    if expire_days is not None and expire_delete_markers:
        # One Expiration cannot carry both; the builder used to keep the days
        # and drop the clean-up without a word, though the box stayed ticked.
        raise ValueError(
            "A rule cannot both delete objects after a number of days and "
            "clean up expired delete markers — S3 refuses the combination. "
            "Untick one, or make a second rule for it.")
    rule = copy.deepcopy(dict(base or {}))
    rule["ID"] = str(rule_id or "").strip() or "rule"
    rule["Status"] = "Enabled" if enabled else "Disabled"
    # The modern shape. A bare "Prefix" is the deprecated one and is
    # rejected outright when mixed with Filter.
    rule.pop("Prefix", None)
    rule["Filter"] = {"Prefix": str(prefix or "")}
    if transition_days is not None and transition_class:
        rule["Transitions"] = [{"Days": int(transition_days),
                                "StorageClass": str(transition_class)}]
    else:
        rule.pop("Transitions", None)
    if expire_days is not None:
        rule["Expiration"] = {"Days": int(expire_days)}
    elif expire_delete_markers:
        rule["Expiration"] = {"ExpiredObjectDeleteMarker": True}
    else:
        rule.pop("Expiration", None)
    if noncurrent_days is not None:
        noncurrent = dict(rule.get("NoncurrentVersionExpiration") or {})
        noncurrent["NoncurrentDays"] = int(noncurrent_days)
        rule["NoncurrentVersionExpiration"] = noncurrent
    else:
        rule.pop("NoncurrentVersionExpiration", None)
    if abort_days is not None:
        rule["AbortIncompleteMultipartUpload"] = {
            "DaysAfterInitiation": int(abort_days)}
    else:
        rule.pop("AbortIncompleteMultipartUpload", None)
    if not any(key in rule for key in (
            "Transitions", "Expiration", "NoncurrentVersionExpiration",
            "NoncurrentVersionTransitions",
            "AbortIncompleteMultipartUpload")):
        raise ValueError(
            "A lifecycle rule needs at least one action, or it will never "
            "do anything.")
    return rule
