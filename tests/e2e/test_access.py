"""Who may reach a bucket, asked of a real server.

Retention and upload forms are promises the *service* keeps -- a refused
delete, a refused oversized POST -- so only a server can show them kept. The
rest (Block Public Access, logging, replication, ACL writes, SSE-C over plain
HTTP) are documents a backend is free not to implement; for those this records
what the backend under test answers, and that the answer carries a code the
dialog can show, the same bargain test_bucket_admin makes.
"""

import base64
import hashlib
import os
import uuid
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

import botocore.exceptions

from core.acl import acl_is_editable
from core.retention import GOVERNANCE
from tests.e2e.harness import ServerCase


def _post_form(url, fields, filename, body):
    """Submit a presigned POST the way a browser would, multipart."""
    boundary = uuid.uuid4().hex
    chunks = []
    for name, value in fields.items():
        chunks.append(
            f'--{boundary}\r\nContent-Disposition: form-data; name="{name}"'
            f"\r\n\r\n{value}\r\n".encode())
    chunks.append(
        f'--{boundary}\r\nContent-Disposition: form-data; name="file"; '
        f'filename="{filename}"\r\nContent-Type: application/octet-stream'
        "\r\n\r\n".encode() + body + b"\r\n")
    chunks.append(f"--{boundary}--\r\n".encode())
    request = urllib.request.Request(
        url, data=b"".join(chunks), method="POST",
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"})
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read()


def _code(exc):
    return exc.response.get("Error", {}).get("Code", "")


class PresignedPostTests(ServerCase):

    def test_an_upload_through_the_form_lands(self):
        post = self.model.presigned_post("forms/exact.txt", 300, max_bytes=1024)
        status, _body = _post_form(post["url"], post["fields"], "x.txt",
                                   b"posted bytes")
        self.assertIn(status, (200, 204), _body)
        self.assertEqual(self.model.client.get_object(
            Bucket=self.bucket, Key="forms/exact.txt")["Body"].read(),
            b"posted bytes")

    def test_the_uploader_may_name_the_file_under_the_prefix(self):
        post = self.model.presigned_post("forms/any/", 300, max_bytes=1024,
                                         any_key_under_prefix=True)
        status, body = _post_form(post["url"], post["fields"], "chosen.bin",
                                  b"named by the uploader")
        self.assertIn(status, (200, 204), body)
        self.assertIn("forms/any/chosen.bin", self.keys("forms/any/"))

    def test_the_size_limit_is_enforced_by_the_service(self):
        post = self.model.presigned_post("forms/small.bin", 300, max_bytes=16)
        status, _body = _post_form(post["url"], post["fields"], "big.bin",
                                   os.urandom(4096))
        self.assertGreaterEqual(status, 400, "an oversized upload was accepted")
        self.assertNotIn("forms/small.bin", self.keys("forms/"))

    def test_the_content_type_condition_is_enforced(self):
        post = self.model.presigned_post("forms/typed.png", 300,
                                         content_type="image/png")
        fields = dict(post["fields"])
        fields["Content-Type"] = "text/html"
        status, _body = _post_form(post["url"], fields, "page.html", b"<p>")
        self.assertGreaterEqual(status, 400,
                                "a form condition was not enforced")


class RetentionTests(ServerCase):
    """GOVERNANCE only: a COMPLIANCE object in a test bucket could never be
    cleaned up again."""

    OBJECT_LOCK = True

    def _version(self, key):
        return [v for v in self.model.list_object_versions(key)
                if not v["is_delete_marker"]][0]["version_id"]

    def test_retention_is_set_and_read_back(self):
        key = self.put("retain/set.txt", b"x")
        until = datetime.now(timezone.utc) + timedelta(days=1)
        self.model.set_object_retention(key, GOVERNANCE, until)
        state = self.model.get_object_lock_state(key)
        self.assertEqual(state["mode"], GOVERNANCE)
        self.assertTrue(state["retain_until"])

    def test_a_retained_version_cannot_be_deleted(self):
        key = self.put("retain/kept.txt", b"kept")
        self.model.set_object_retention(
            key, GOVERNANCE, datetime.now(timezone.utc) + timedelta(days=1))
        version = self._version(key)
        with self.assertRaises(botocore.exceptions.ClientError):
            self.model.client.delete_object(Bucket=self.bucket, Key=key,
                                            VersionId=version)

    def test_shortening_governance_needs_the_bypass(self):
        key = self.put("retain/short.txt", b"x")
        far = datetime.now(timezone.utc) + timedelta(days=10)
        near = datetime.now(timezone.utc) + timedelta(days=1)
        self.model.set_object_retention(key, GOVERNANCE, far)
        with self.assertRaises(botocore.exceptions.ClientError) as caught:
            self.model.set_object_retention(key, GOVERNANCE, near)
        self.assertTrue(_code(caught.exception))
        self.model.set_object_retention(key, GOVERNANCE, near,
                                        bypass_governance=True)
        state = self.model.get_object_lock_state(key)
        self.assertTrue(state["retain_until"].startswith(near.strftime("%Y-%m-%d")),
                        state)


class AclTests(ServerCase):

    """MinIO answers GetAcl with an owner whose ID is empty: it implements
    ACLs only as "the owner has FULL_CONTROL". Nothing can be written back
    without an owner ID, so the editor opens read-only there -- and this
    records which kind of backend the run is talking to."""

    def test_an_object_acl_reads_back_with_full_control(self):
        key = self.put("acl/read.txt", b"x")
        acl = self.model.get_object_acl(key)
        self.assertIn("FULL_CONTROL", [g["permission"] for g in acl["grants"]])

    def test_writing_the_same_acl_back_where_it_can_be(self):
        key = self.put("acl/write.txt", b"x")
        acl = self.model.get_object_acl(key)
        if not acl_is_editable(acl):
            self.assertEqual(acl["owner"]["id"], "")
            return
        try:
            self.model.put_object_acl_grants(key, acl["owner"], acl["grants"])
        except botocore.exceptions.ClientError as exc:
            self.assertTrue(_code(exc), "a refusal with nothing to show")

    def test_the_bucket_acl_reads_back(self):
        acl = self.model.get_bucket_acl()
        self.assertTrue(acl["grants"], acl)


class BucketAccessDocumentTests(ServerCase):

    def test_block_public_access_reads_as_absent_or_flags(self):
        flags = self.model.get_public_access_block()
        self.assertTrue(flags is None or set(flags) == {
            "BlockPublicAcls", "IgnorePublicAcls", "BlockPublicPolicy",
            "RestrictPublicBuckets"}, flags)

    def test_block_public_access_writes_or_refuses_with_a_code(self):
        flags = {"BlockPublicAcls": True, "IgnorePublicAcls": True,
                 "BlockPublicPolicy": True, "RestrictPublicBuckets": True}
        try:
            self.model.put_public_access_block(flags)
        except botocore.exceptions.ClientError as exc:
            self.assertTrue(_code(exc))
            return
        self.assertEqual(self.model.get_public_access_block(), flags)

    def test_logging_reads_as_off(self):
        self.assertEqual(self.model.get_bucket_logging(), {})

    def test_logging_writes_or_refuses_with_a_code(self):
        try:
            self.model.put_bucket_logging(self.bucket + "-logs", "p/")
        except botocore.exceptions.ClientError as exc:
            self.assertTrue(_code(exc))

    def test_replication_reads_as_no_rules(self):
        self.assertEqual(self.model.get_bucket_replication()["rules"], [])


class SseCTests(ServerCase):
    """Over HTTPS this is a round trip; over plain HTTP (the compose MinIO)
    the service refuses to accept a key in the clear, and that refusal must
    reach the user as a code rather than as a mystery."""

    KEY = bytes(range(32))

    def _clone(self):
        clone = self.model.clone_for_worker()
        clone.set_sse_c_key(self.KEY)
        return clone

    def test_a_put_and_get_with_the_key(self):
        clone = self._clone()
        try:
            clone.client.put_object(Bucket=self.bucket, Key="ssec/a.txt",
                                    Body=b"secret")
        except botocore.exceptions.ClientError as exc:
            self.assertTrue(_code(exc), "SSE-C refused with nothing to show")
            self.assertTrue(self.model.endpoint_url.startswith("http://"),
                            f"SSE-C refused over TLS: {exc}")
            return
        body = clone.client.get_object(Bucket=self.bucket,
                                       Key="ssec/a.txt")["Body"].read()
        self.assertEqual(body, b"secret")
        with self.assertRaises(botocore.exceptions.ClientError):
            self.model.client.get_object(Bucket=self.bucket, Key="ssec/a.txt")
        head = clone.client.head_object(Bucket=self.bucket, Key="ssec/a.txt")
        self.assertEqual(head.get("SSECustomerKeyMD5"),
                         base64.b64encode(hashlib.md5(self.KEY).digest()).decode())
