"""Reading, summarising and assembling bucket lifecycle rules.

A rule's filter has two historical shapes and three places a prefix can hide.
Getting that wrong turns "delete under logs/" into "delete everything", so the
reading is done once, here, and the editor and the summary both use it.
"""


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
    if noncurrent.get("NoncurrentDays") is not None:
        parts.append(
            f"delete old versions after {noncurrent['NoncurrentDays']}d")
    for transition in rule.get("NoncurrentVersionTransitions") or []:
        parts.append(
            f"old versions to {transition.get('StorageClass')} after "
            f"{transition.get('NoncurrentDays')}d")
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
                         abort_days=None, expire_delete_markers=False):
    """
    Assemble a rule document from the fields the editor collects.

    Raises ValueError when the result would do nothing: S3 accepts such a
    rule and then silently never fires it.
    """
    rule = {
        "ID": str(rule_id or "").strip() or "rule",
        "Status": "Enabled" if enabled else "Disabled",
        # The modern shape. A bare "Prefix" is the deprecated one and is
        # rejected outright when mixed with Filter.
        "Filter": {"Prefix": str(prefix or "")},
    }
    if transition_days is not None and transition_class:
        rule["Transitions"] = [{"Days": int(transition_days),
                                "StorageClass": str(transition_class)}]
    if expire_days is not None:
        rule["Expiration"] = {"Days": int(expire_days)}
    elif expire_delete_markers:
        rule["Expiration"] = {"ExpiredObjectDeleteMarker": True}
    if noncurrent_days is not None:
        rule["NoncurrentVersionExpiration"] = {
            "NoncurrentDays": int(noncurrent_days)}
    if abort_days is not None:
        rule["AbortIncompleteMultipartUpload"] = {
            "DaysAfterInitiation": int(abort_days)}
    if not any(key in rule for key in (
            "Transitions", "Expiration", "NoncurrentVersionExpiration",
            "AbortIncompleteMultipartUpload")):
        raise ValueError(
            "A lifecycle rule needs at least one action, or it will never "
            "do anything.")
    return rule
