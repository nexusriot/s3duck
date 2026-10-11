"""One connection profile, by name rather than by position.

This used to be a 20-element tuple, padded to length on arrival::

    settings = tuple(settings) + ("",) * (20 - len(tuple(settings)))
    (current_dir, settings, profile_name, url, region, bucket, access_key,
     secret_key, no_ssl_check, use_path, session_token, read_only, accent,
     ...) = settings

Two things made that dangerous. Inserting a field anywhere but the end
shifted every field after it, silently -- an access key could land in the
secret key's slot and the only symptom would be a failed sign-in. And the
padding meant a caller that passed too few values got empty strings rather
than an error, so a genuine mistake looked exactly like an old caller.

Defaults do the padding's job now, and they do it per field.
"""

from dataclasses import dataclass, replace


@dataclass(frozen=True)
class ProfileSettings:
    """Everything about one S3 connection: where, as whom, and how."""

    # Identity
    profile_name: str = ""
    accent: str = ""            # the colour band the window wears

    # Endpoint
    url: str = ""
    region: str = ""
    bucket: str = ""            # empty means "start in bucket-list mode"
    use_path: bool = False      # path-style addressing rather than virtual host
    no_ssl_check: bool = False
    proxy_url: str = ""
    ca_bundle: str = ""
    public_base_url: str = ""

    # Credentials
    access_key: str = ""
    secret_key: str = ""
    session_token: str = ""
    session_expires: str = ""   # when a temporary session runs out
    aws_profile: str = ""       # a ~/.aws profile to re-read keys from
    credential_process: str = ""

    # Policy
    read_only: bool = False
    requester_pays: bool = False

    def replace(self, **changes) -> "ProfileSettings":
        """A copy with some fields changed; the original is frozen."""
        return replace(self, **changes)

    def without_secrets(self) -> "ProfileSettings":
        """A copy safe to log or show: the key material is blanked."""
        return replace(self, access_key="", secret_key="", session_token="")
