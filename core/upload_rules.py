"""Per-pattern upload settings, and content-type overrides.

Both are stored as text the user edits directly, so parsing has to survive
whatever they type: the parsers never raise, they drop what they cannot read.
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
            name = name.strip().lower()
            value = value.strip()
            if not value:
                continue
            if name == "class":
                rule["storage_class"] = value.upper()
            elif name.startswith("tag:"):
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
