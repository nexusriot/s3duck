"""Shared setup for the end-to-end suite: where the server is, and a
throwaway bucket per test class."""

import os
import unittest
import uuid

from model import Model

# No sys.path juggling here on purpose: tests/__init__.py already puts the
# project root on the path, and Python runs it before this subpackage. That
# is the same arrangement the unit suite uses, and it is what keeps these
# imports contiguous at the top of the file.

ENDPOINT_VAR = "S3DUCK_TEST_ENDPOINT"


def endpoint() -> str:
    return os.environ.get(ENDPOINT_VAR, "").strip()


def _env(name, fallback):
    return os.environ.get(name, "").strip() or fallback


def requires_server(cls):
    """Skip a whole class unless an endpoint was configured.

    A tagged run on a machine with no server is a no-op, not a failure —
    the same bargain the sibling s3duck-tui suite makes.
    """
    return unittest.skipUnless(
        endpoint(), f"{ENDPOINT_VAR} not set; skipping end-to-end tests")(cls)


def make_model(bucket="", **overrides):
    """A Model pointed at the test server. Path-style addressing, because a
    MinIO on localhost has no per-bucket DNS."""
    kw = dict(
        endpoint_url=endpoint(),
        region_name=_env("S3DUCK_TEST_REGION", "us-east-1"),
        access_key=_env("S3DUCK_TEST_ACCESS_KEY", "minioadmin"),
        secret_key=_env("S3DUCK_TEST_SECRET_KEY", "minioadmin"),
        bucket=bucket,
        no_ssl_check=True,
        use_path=True,
    )
    kw.update(overrides)
    return Model(**kw)


def unique_bucket(tag="e2e") -> str:
    # Bucket names are DNS labels: lowercase, no underscores, 3-63 chars.
    return f"s3duck-{tag}-{uuid.uuid4().hex[:12]}"


def purge_bucket(client, name):
    """Remove a bucket and everything in it, including versions and
    in-flight uploads. Tolerant: this runs in tearDown, where the test has
    already reported whatever it found."""
    try:
        paginator = client.get_paginator("list_object_versions")
        for page in paginator.paginate(Bucket=name):
            doomed = [
                {"Key": entry["Key"], "VersionId": entry["VersionId"]}
                for group in ("Versions", "DeleteMarkers")
                for entry in (page.get(group) or [])
                if entry.get("Key") and entry.get("VersionId")
            ]
            if doomed:
                client.delete_objects(Bucket=name, Delete={"Objects": doomed})
    except Exception:
        pass
    try:
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=name):
            doomed = [{"Key": o["Key"]} for o in (page.get("Contents") or [])]
            if doomed:
                client.delete_objects(Bucket=name, Delete={"Objects": doomed})
    except Exception:
        pass
    try:
        for upload in client.list_multipart_uploads(
                Bucket=name).get("Uploads") or []:
            client.abort_multipart_upload(
                Bucket=name, Key=upload["Key"], UploadId=upload["UploadId"])
    except Exception:
        pass
    try:
        client.delete_bucket(Bucket=name)
    except Exception:
        pass


class ServerCase(unittest.TestCase):
    """A test class with its own bucket, created and destroyed around it.

    Per-class rather than per-test: bucket creation is the slow part, and
    the tests inside a class are written not to collide on keys.
    """

    OBJECT_LOCK = False

    @classmethod
    def setUpClass(cls):
        if not endpoint():
            raise unittest.SkipTest(f"{ENDPOINT_VAR} not set")
        cls.bucket = unique_bucket()
        cls.model = make_model(bucket=cls.bucket)
        params = {"Bucket": cls.bucket}
        if cls.OBJECT_LOCK:
            params["ObjectLockEnabledForBucket"] = True
        cls.model.client.create_bucket(**params)
        # enter_bucket is what the app calls; it also binds the client.
        cls.model.enter_bucket(cls.bucket)

    @classmethod
    def tearDownClass(cls):
        purge_bucket(cls.model.client, cls.bucket)

    def put(self, key, body=b"x", **kw):
        """Write an object straight through boto3, bypassing the app, so a
        test's fixture cannot be broken by the code under test."""
        self.model.client.put_object(
            Bucket=self.bucket, Key=key, Body=body, **kw)
        return key

    def keys(self, prefix=""):
        out = []
        paginator = self.model.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            out.extend(o["Key"] for o in (page.get("Contents") or []))
        return sorted(out)

    def body(self, key):
        return self.model.client.get_object(
            Bucket=self.bucket, Key=key)["Body"].read()
