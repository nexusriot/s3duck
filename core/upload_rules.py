"""Per-pattern upload settings, content-type overrides, and tag lists.

All are stored or edited as text the user types directly, so parsing has to
survive whatever they type: the parsers never raise.
"""


def parse_content_type_overrides(text) -> dict:
    """
    Read the user's ``ext = type`` lines into a mapping.

    Unparseable lines are dropped rather than raising: this is edited in a
    free-text box, and a half-typed line must not cost the whole table.
    """
    overrides = {}
    for line in str(text or "").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        separator = "=" if "=" in line else (":" if ":" in line else "")
        if not separator:
            continue
        ext, _, value = line.partition(separator)
        ext = ext.strip().lstrip("*").lstrip(".").lower()
        value = value.strip()
        if ext and value:
            overrides[ext] = value
    return overrides


def format_content_type_overrides(overrides) -> str:
    return "\n".join(
        f"{ext} = {value}"
        for ext, value in sorted(dict(overrides or {}).items()))


def parse_upload_rules(text) -> list:
    """
    Read ``glob -> class=X, tag:k=v`` lines into rules.

    Unparseable lines are dropped rather than raising: this is edited in a
    free-text box, and a half-typed rule must not cost the whole table.
    """
    rules = []
    for line in str(text or "").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "->" not in line:
            continue
        pattern, _, rest = line.partition("->")
        pattern = pattern.strip()
        if not pattern:
            continue
        rule = {"pattern": pattern, "storage_class": "", "tags": {}}
        for part in rest.split(","):
            name, _, value = part.strip().partition("=")
            name = name.strip()
            # Only the field kind is case-blind. A tag key is not: S3 keeps
            # "CostCenter" and "costcenter" apart, and cost-allocation reports
            # match on the exact key, so lowercasing it tagged every upload
            # with a key nothing was looking for.
            kind = name.lower()
            value = value.strip()
            if not value:
                continue
            if kind == "class":
                rule["storage_class"] = value.upper()
            elif kind.startswith("tag:"):
                tag = name[4:].strip()
                if tag:
                    rule["tags"][tag] = value
        if rule["storage_class"] or rule["tags"]:
            rules.append(rule)
    return rules


def format_upload_rules(rules) -> str:
    lines = []
    for rule in list(rules or []):
        parts = []
        if rule.get("storage_class"):
            parts.append(f"class={rule['storage_class']}")
        for key, value in sorted(dict(rule.get("tags") or {}).items()):
            parts.append(f"tag:{key}={value}")
        if parts:
            lines.append(f"{rule.get('pattern', '')} -> "
                         + ", ".join(parts))
    return "\n".join(lines)


def parse_tag_lines(text) -> dict:
    """
    Read ``key = value`` lines into a tag mapping, exactly as typed.

    Not parse_content_type_overrides, which bucket tags used to go through:
    that one lowercases the key and strips a leading "*" or "." (right for
    a file extension, wrong for "CostCenter"), and drops a line with no value
    — so saving the bucket's settings deleted every empty-valued tag and
    renamed the rest. Tag keys are case-sensitive and an empty value is a
    real tag. A line without "=" is a key with an empty value; the first
    " = " separates, so a key or a value may itself contain "=".
    """
    tags = {}
    for line in str(text or "").splitlines():
        line = line.strip()
        if not line:
            continue
        if " = " in line:
            key, _, value = line.partition(" = ")
        elif "=" in line:
            key, _, value = line.partition("=")
        else:
            key, value = line, ""
        key = key.strip()
        if key:
            tags[key] = value.strip()
    return tags


def format_tag_lines(tags) -> str:
    return "\n".join(
        f"{key} = {value}" for key, value in sorted(dict(tags or {}).items()))
