"""Access control lists: reading grants, building the document, spotting the
public ones.

An ACL is the oldest and bluntest of S3's permission systems, and the one a
single click can make a whole bucket world-readable with. The rules here are
what the editor and its "are you sure" both rest on, kept free of Qt and boto3
so they can be tested on their own.
"""

ALL_USERS = "http://acs.amazonaws.com/groups/global/AllUsers"
AUTHENTICATED_USERS = "http://acs.amazonaws.com/groups/global/AuthenticatedUsers"
LOG_DELIVERY = "http://acs.amazonaws.com/groups/s3/LogDelivery"

GROUPS = {
    "Everyone (public)": ALL_USERS,
    "Any AWS account (authenticated)": AUTHENTICATED_USERS,
    "S3 log delivery": LOG_DELIVERY,
}
GROUP_LABELS = {uri: label for label, uri in GROUPS.items()}

PERMISSIONS = ("FULL_CONTROL", "READ", "WRITE", "READ_ACP", "WRITE_ACP")

# "Canonical user", "email" and "group" are what a grant can name; email
# grants are only accepted in a few old AWS regions, but they are still read.
GRANTEE_TYPES = ("CanonicalUser", "AmazonCustomerByEmail", "Group")

# Granting either of these to a public group publishes the object (READ) or
# lets anyone overwrite it (WRITE) -- or, for the ACP pair, rewrite who may.
_PUBLIC_GROUPS = (ALL_USERS, AUTHENTICATED_USERS)


def parse_acl(response) -> dict:
    """``{"owner": {"id", "display"}, "grants": [grant, …]}`` from a GetAcl."""
    response = response or {}
    owner = response.get("Owner") or {}
    grants = []
    for raw in response.get("Grants") or []:
        grantee = raw.get("Grantee") or {}
        grants.append({
            "type": str(grantee.get("Type") or ""),
            "id": str(grantee.get("ID") or ""),
            "display": str(grantee.get("DisplayName") or ""),
            "uri": str(grantee.get("URI") or ""),
            "email": str(grantee.get("EmailAddress") or ""),
            "permission": str(raw.get("Permission") or ""),
        })
    return {"owner": {"id": str(owner.get("ID") or ""),
                      "display": str(owner.get("DisplayName") or "")},
            "grants": grants}


def describe_grantee(grant) -> str:
    """How a grant reads in the table: a group by name, a user by name or id."""
    kind = grant.get("type")
    if kind == "Group":
        uri = grant.get("uri") or ""
        return GROUP_LABELS.get(uri, uri)
    if kind == "AmazonCustomerByEmail":
        return grant.get("email") or ""
    display = grant.get("display") or ""
    ident = grant.get("id") or ""
    if display and ident:
        return f"{display} ({ident[:12]}…)"
    return display or ident


def validate_grant(grant) -> str:
    """Why *grant* cannot be written, or "" if it can."""
    kind = grant.get("type")
    if kind not in GRANTEE_TYPES:
        return f"Unknown grantee type: {kind!r}"
    if grant.get("permission") not in PERMISSIONS:
        return f"Unknown permission: {grant.get('permission')!r}"
    if kind == "Group" and not grant.get("uri"):
        return "A group grant needs the group's URI."
    if kind == "CanonicalUser":
        ident = str(grant.get("id") or "").strip()
        if not ident:
            return "A canonical user grant needs the user's canonical ID."
        if len(ident) != 64 or any(c not in "0123456789abcdef" for c in ident.lower()):
            return ("A canonical user ID is 64 hexadecimal characters; an "
                    "account number or an email address is not one.")
    if kind == "AmazonCustomerByEmail" and "@" not in str(grant.get("email") or ""):
        return "An email grant needs an email address."
    return ""


def build_access_control_policy(owner, grants) -> dict:
    """The ``AccessControlPolicy`` a PutAcl call takes."""
    out = []
    for grant in grants or []:
        problem = validate_grant(grant)
        if problem:
            raise ValueError(problem)
        kind = grant["type"]
        grantee = {"Type": kind}
        if kind == "Group":
            grantee["URI"] = grant["uri"]
        elif kind == "AmazonCustomerByEmail":
            grantee["EmailAddress"] = grant["email"]
        else:
            grantee["ID"] = str(grant["id"]).strip()
        out.append({"Grantee": grantee, "Permission": grant["permission"]})
    owner = owner or {}
    if not owner.get("id"):
        raise ValueError("The ACL has no owner; read it again before saving.")
    owner_doc = {"ID": owner["id"]}
    if owner.get("display"):
        owner_doc["DisplayName"] = owner["display"]
    return {"Owner": owner_doc, "Grants": out}


def public_grants(grants) -> list:
    """The grants that hand something to everyone or to any AWS account."""
    return [g for g in grants or []
            if g.get("type") == "Group" and g.get("uri") in _PUBLIC_GROUPS]


def acl_is_editable(acl) -> bool:
    """
    Whether an ACL can be written back at all.

    A PutAcl must name the owner by canonical ID. Some S3-compatible servers
    (MinIO among them) answer GetAcl with an owner whose ID is empty -- they
    implement ACLs only as "the owner has FULL_CONTROL" -- and there is then
    nothing valid to send.
    """
    return bool(((acl or {}).get("owner") or {}).get("id"))


def owner_keeps_control(owner, grants) -> bool:
    """
    Whether the owner still holds FULL_CONTROL after this edit.

    Removing it is legal and is how people lock themselves out of changing
    an ACL again; the editor asks before saving one that does.
    """
    owner_id = (owner or {}).get("id") or ""
    return any(g.get("type") == "CanonicalUser" and g.get("id") == owner_id
               and g.get("permission") == "FULL_CONTROL"
               for g in grants or [])
