"""Replication rules, read and summarised.

Read-only for the same reason event notifications are: a rule names an IAM
role and a destination bucket ARN this app cannot validate, and a wrong one
quietly stops replicating rather than failing anything visible.
"""


def summarize_replication(config) -> dict:
    """``{"role", "rules": [{…}]}`` from a GetBucketReplication response."""
    config = (config or {}).get("ReplicationConfiguration") or config or {}
    rules = []
    for raw in config.get("Rules") or []:
        rule_filter = raw.get("Filter") or {}
        prefix = rule_filter.get("Prefix")
        if prefix is None:
            prefix = (rule_filter.get("And") or {}).get("Prefix")
        if prefix is None:
            prefix = raw.get("Prefix", "")
        tags = []
        if rule_filter.get("Tag"):
            tags.append(rule_filter["Tag"])
        tags.extend((rule_filter.get("And") or {}).get("Tags") or [])
        destination = raw.get("Destination") or {}
        markers = (raw.get("DeleteMarkerReplication") or {}).get("Status", "")
        rules.append({
            "id": str(raw.get("ID") or ""),
            "status": str(raw.get("Status") or ""),
            "priority": raw.get("Priority"),
            "prefix": str(prefix or ""),
            "tags": [f"{t.get('Key')}={t.get('Value')}" for t in tags],
            "destination": str(destination.get("Bucket") or ""),
            "storage_class": str(destination.get("StorageClass") or ""),
            "account": str(destination.get("Account") or ""),
            "delete_markers": str(markers or ""),
        })
    return {"role": str(config.get("Role") or ""), "rules": rules}


def describe_replication_rule(rule) -> str:
    """One line per rule for the settings tab."""
    scope = f"prefix '{rule['prefix']}'" if rule.get("prefix") else "whole bucket"
    if rule.get("tags"):
        scope += " tagged " + ", ".join(rule["tags"])
    parts = [f"{rule.get('id') or '(unnamed)'}: {scope} → "
             f"{rule.get('destination') or '?'}"]
    if rule.get("storage_class"):
        parts.append(f"as {rule['storage_class']}")
    if rule.get("delete_markers") == "Enabled":
        parts.append("with delete markers")
    if rule.get("status") != "Enabled":
        parts.append("[disabled]")
    return " ".join(parts)
