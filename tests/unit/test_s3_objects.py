"""s3/object_admin.py and bucket_admin.py."""

import botocore.exceptions
import botocore.session
import io
import main_window
import unittest
from cryptography.fernet import Fernet
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from model import Model, ReadOnlyError
from tests.support import (
    FakeS3Client, _BucketAdminClient, _CopyPartClient, _DocumentClient, _dt,
    make_model,
)
from utils import CredentialError, Crypto


class ParseRestoreStatusTests(unittest.TestCase):
    def test_empty(self):
        self.assertEqual(Model.parse_restore_status(None), "")
        self.assertEqual(Model.parse_restore_status(""), "")

    def test_in_progress(self):
        self.assertEqual(
            Model.parse_restore_status('ongoing-request="true"'),
            "in-progress",
        )

    def test_available_with_expiry(self):
        header = ('ongoing-request="false", '
                  'expiry-date="Fri, 01 Jan 2027 00:00:00 GMT"')
        self.assertEqual(
            Model.parse_restore_status(header),
            "available until Fri, 01 Jan 2027 00:00:00 GMT",
        )

    def test_available_without_expiry(self):
        self.assertEqual(
            Model.parse_restore_status('ongoing-request="false"'),
            "available",
        )
class ListObjectVersionsTests(unittest.TestCase):
    def setUp(self):
        self.m = make_model(bucket="b")

    def _client(self):
        return FakeS3Client(versions_pages=[{
            "Versions": [
                {"Key": "a/f.txt", "VersionId": "v2", "IsLatest": True,
                 "LastModified": _dt(2), "Size": 20, "StorageClass": "STANDARD",
                 "ETag": '"etag2"'},
                {"Key": "a/f.txt", "VersionId": "v1", "IsLatest": False,
                 "LastModified": _dt(1), "Size": 10, "StorageClass": "STANDARD",
                 "ETag": '"etag1"'},
                {"Key": "a/other.txt", "VersionId": "x", "IsLatest": True,
                 "LastModified": _dt(3), "Size": 99, "StorageClass": "STANDARD"},
            ],
            "DeleteMarkers": [
                {"Key": "a/f.txt", "VersionId": "dm1", "IsLatest": False,
                 "LastModified": _dt(3)},
            ],
        }])

    def test_filters_to_exact_key(self):
        self.m._client = self._client()
        out = self.m.list_object_versions("a/f.txt")
        # v2, v1, dm1 -> the 'a/other.txt' entry must be excluded
        self.assertEqual({e["version_id"] for e in out}, {"v1", "v2", "dm1"})

    def test_newest_first_and_flags(self):
        self.m._client = self._client()
        out = self.m.list_object_versions("a/f.txt")
        # dm1 (day 3) newest, then v2 (day 2), then v1 (day 1)
        self.assertEqual([e["version_id"] for e in out], ["dm1", "v2", "v1"])
        dm = out[0]
        self.assertTrue(dm["is_delete_marker"])
        self.assertEqual(dm["size"], 0)
        latest = next(e for e in out if e["version_id"] == "v2")
        self.assertTrue(latest["is_latest"])
        self.assertEqual(latest["size"], 20)
        self.assertEqual(latest["etag"], "etag2")

    def test_rejects_folder_key(self):
        self.m._client = self._client()
        with self.assertRaises(ValueError):
            self.m.list_object_versions("a/")
class VersionMutationTests(unittest.TestCase):
    def setUp(self):
        self.m = make_model(bucket="b")

    def test_make_version_current_copies_with_version_id(self):
        c = FakeS3Client()
        self.m._client = c
        self.m.make_version_current("a/f.txt", "v1")
        copies = c.calls_of("copy_object")
        self.assertEqual(len(copies), 1)
        self.assertEqual(
            copies[0]["CopySource"],
            {"Bucket": "b", "Key": "a/f.txt", "VersionId": "v1"},
        )
        self.assertEqual(copies[0]["Key"], "a/f.txt")
        self.assertEqual(copies[0]["Bucket"], "b")

    def test_delete_object_version_passes_version_id(self):
        c = FakeS3Client()
        self.m._client = c
        self.m.delete_object_version("a/f.txt", "dm1")
        dels = c.calls_of("delete_object")
        self.assertEqual(dels, [{"Bucket": "b", "Key": "a/f.txt", "VersionId": "dm1"}])
class StorageClassTests(unittest.TestCase):
    def setUp(self):
        self.m = make_model(bucket="b")

    def test_change_storage_class_copies_onto_itself(self):
        c = FakeS3Client()
        self.m._client = c
        self.m.change_storage_class("a/f.txt", "GLACIER")
        copies = c.calls_of("copy_object")
        self.assertEqual(len(copies), 1)
        self.assertEqual(copies[0]["CopySource"], {"Bucket": "b", "Key": "a/f.txt"})
        self.assertEqual(copies[0]["Key"], "a/f.txt")
        self.assertEqual(copies[0]["StorageClass"], "GLACIER")
        self.assertEqual(copies[0]["MetadataDirective"], "COPY")

    def test_restore_object_success(self):
        c = FakeS3Client()
        self.m._client = c
        ok, reason = self.m.restore_object("a/f.txt", days=3, tier="Bulk")
        self.assertTrue(ok)
        self.assertIsNone(reason)
        req = c.calls_of("restore_object")[0]["RestoreRequest"]
        self.assertEqual(req["Days"], 3)
        self.assertEqual(req["GlacierJobParameters"], {"Tier": "Bulk"})

    def test_restore_object_already_in_progress(self):
        err = botocore.exceptions.ClientError(
            {"Error": {"Code": "RestoreAlreadyInProgress", "Message": "busy"}},
            "RestoreObject",
        )
        self.m._client = FakeS3Client(restore_error=err)
        ok, reason = self.m.restore_object("a/f.txt")
        self.assertFalse(ok)
        self.assertIn("already in progress", reason.lower())
class GetObjectPreviewTests(unittest.TestCase):
    def setUp(self):
        self.m = make_model(bucket="b")

    def test_ranged_reports_total_and_truncation(self):
        resp = {
            "Body": io.BytesIO(b"hello"),
            "ContentType": "text/plain",
            "ContentLength": 5,
            "ContentRange": "bytes 0-4/10",
        }
        c = FakeS3Client(get_object_resp=resp)
        self.m._client = c
        out = self.m.get_object_preview("a/f.txt", max_bytes=5)
        self.assertEqual(out["data"], b"hello")
        self.assertEqual(out["content_type"], "text/plain")
        self.assertEqual(out["size"], 10)          # from Content-Range total
        self.assertTrue(out["truncated"])          # 5 fetched < 10 total
        # A Range header must be sent when max_bytes is set.
        self.assertEqual(c.calls_of("get_object")[0]["Range"], "bytes=0-4")

    def test_full_object_not_truncated(self):
        resp = {
            "Body": io.BytesIO(b"abc"),
            "ContentType": "application/octet-stream",
            "ContentLength": 3,
        }
        self.m._client = FakeS3Client(get_object_resp=resp)
        out = self.m.get_object_preview("a/f.bin", max_bytes=1024)
        self.assertEqual(out["size"], 3)
        self.assertFalse(out["truncated"])

    def test_empty_object_retries_without_range(self):
        """REGRESSION: a zero-byte object can't satisfy a byte range and must
        fall back to an unranged read instead of surfacing 416."""

        class RangeRejectingClient:
            def __init__(self):
                self.range_seen = []

            def get_object(self, **kw):
                self.range_seen.append("Range" in kw)
                if "Range" in kw:
                    raise botocore.exceptions.ClientError(
                        {"Error": {"Code": "InvalidRange", "Message": "n/a"}},
                        "GetObject",
                    )
                return {"Body": io.BytesIO(b""), "ContentType": "text/plain",
                        "ContentLength": 0}

        c = RangeRejectingClient()
        self.m._client = c
        out = self.m.get_object_preview("a/empty.txt", max_bytes=1024)
        self.assertEqual(out["data"], b"")
        self.assertFalse(out["truncated"])
        # first call ranged (rejected), second call unranged (ok)
        self.assertEqual(c.range_seen, [True, False])
class PublicAccessSummaryTests(unittest.TestCase):
    def test_reasons_explain_a_refused_acl(self):
        m = make_model(bucket="b")

        class Client:
            def get_public_access_block(self, **_kw):
                return {"PublicAccessBlockConfiguration": {
                    "BlockPublicAcls": True, "IgnorePublicAcls": False}}

            def get_bucket_ownership_controls(self, **_kw):
                return {"OwnershipControls": {"Rules": [
                    {"ObjectOwnership": "BucketOwnerEnforced"}]}}

        m._client = Client()
        out = m.public_access_summary()
        self.assertEqual(out["ownership"], "BucketOwnerEnforced")
        self.assertEqual(len(out["reasons"]), 2)
        self.assertTrue(any("BlockPublicAcls" in r for r in out["reasons"]))
        self.assertTrue(any("ACLs" in r for r in out["reasons"]))

    def test_unsupported_backend_reports_nothing(self):
        m = make_model(bucket="b")

        class Client:
            def get_public_access_block(self, **_kw):
                raise botocore.exceptions.ClientError(
                    {"Error": {"Code": "NotImplemented", "Message": "no"}},
                    "GetPublicAccessBlock")

            def get_bucket_ownership_controls(self, **_kw):
                raise botocore.exceptions.ClientError(
                    {"Error": {"Code": "NotImplemented", "Message": "no"}},
                    "GetBucketOwnershipControls")

        m._client = Client()
        out = m.public_access_summary()
        self.assertEqual(out["reasons"], [])
class SetBucketVersioningTests(unittest.TestCase):
    def setUp(self):
        self.m = make_model(bucket="b")

    def test_enable_sends_correct_config(self):
        c = FakeS3Client()
        self.m._client = c
        self.m.set_bucket_versioning("Enabled")
        put = c.calls_of("put_bucket_versioning")[0]
        self.assertEqual(put["Bucket"], "b")
        self.assertEqual(put["VersioningConfiguration"], {"Status": "Enabled"})

    def test_lowercase_is_normalized(self):
        c = FakeS3Client()
        self.m._client = c
        self.m.set_bucket_versioning("suspended")
        put = c.calls_of("put_bucket_versioning")[0]
        self.assertEqual(put["VersioningConfiguration"], {"Status": "Suspended"})

    def test_invalid_status_rejected(self):
        self.m._client = FakeS3Client()
        with self.assertRaises(ValueError):
            self.m.set_bucket_versioning("Nope")
class ObjectMetadataTests(unittest.TestCase):
    def setUp(self):
        self.m = make_model(bucket="b")

    def test_get_metadata_maps_head_fields(self):
        head = {
            "ContentType": "text/plain",
            "CacheControl": "max-age=60",
            "ContentDisposition": 'attachment; filename="x.txt"',
            "ContentEncoding": "gzip",
            "StorageClass": "STANDARD_IA",
            "Metadata": {"owner": "vlad"},
        }
        self.m._client = FakeS3Client(head_object_resp=head)
        meta = self.m.get_object_metadata("a/f.txt")
        self.assertEqual(meta["content_type"], "text/plain")
        self.assertEqual(meta["cache_control"], "max-age=60")
        self.assertEqual(meta["content_encoding"], "gzip")
        self.assertEqual(meta["storage_class"], "STANDARD_IA")
        self.assertEqual(meta["metadata"], {"owner": "vlad"})

    def test_set_metadata_replace_copy(self):
        c = FakeS3Client()
        self.m._client = c
        self.m.set_object_metadata(
            "a/f.txt",
            content_type="application/json",
            cache_control="no-cache",
            metadata={"k": "v"},
            storage_class="GLACIER",
        )
        cp = c.calls_of("copy_object")[0]
        self.assertEqual(cp["MetadataDirective"], "REPLACE")
        self.assertEqual(cp["CopySource"], {"Bucket": "b", "Key": "a/f.txt"})
        self.assertEqual(cp["Key"], "a/f.txt")
        self.assertEqual(cp["ContentType"], "application/json")
        self.assertEqual(cp["CacheControl"], "no-cache")
        self.assertEqual(cp["Metadata"], {"k": "v"})
        # non-STANDARD storage class must be preserved on the REPLACE copy
        self.assertEqual(cp["StorageClass"], "GLACIER")

    def test_set_metadata_always_sends_content_type(self):
        """REGRESSION: a REPLACE copy without ContentType makes S3 reset it to
        binary/octet-stream, so it must always be present."""
        c = FakeS3Client()
        self.m._client = c
        self.m.set_object_metadata("a/f.txt", content_type="")
        cp = c.calls_of("copy_object")[0]
        self.assertIn("ContentType", cp)
        # STANDARD is the default and must NOT be pinned on the copy
        self.assertNotIn("StorageClass", cp)
class StorageClassNoOpTests(unittest.TestCase):
    """Setting the class an object already has is a no-op, not an error."""

    def setUp(self):
        self.m = make_model(bucket="b")

    class _Refusing(FakeS3Client):
        """Answers a self-copy that changes nothing the way S3 does."""

        def __init__(self, current=None, **kw):
            super().__init__(**kw)
            self._current = current

        def copy_object(self, **kw):
            self.calls.append(("copy_object", kw))
            if (kw.get("StorageClass") or "STANDARD") == (
                    self._current or "STANDARD"):
                raise botocore.exceptions.ClientError(
                    {"Error": {"Code": "InvalidRequest",
                               "Message": "This copy request is illegal…"}},
                    "CopyObject")
            return {}

        def head_object(self, **kw):
            self.calls.append(("head_object", kw))
            return {"StorageClass": self._current} if self._current else {}

    def test_setting_the_same_class_returns_false_and_does_not_raise(self):
        c = self._Refusing(current=None)          # absent => STANDARD
        self.m._client = c
        logs = []
        self.assertIs(
            self.m.change_storage_class("a.txt", "STANDARD", log_fn=logs.append),
            False)
        self.assertTrue(any("already STANDARD" in line for line in logs))

    def test_a_real_change_returns_true_and_costs_a_head_and_a_copy(self):
        """The HEAD is the price of keeping the object's own encryption (see
        SelfCopyEncryptionTests); one, and only one, is paid."""
        c = self._Refusing(current="STANDARD")
        self.m._client = c
        self.assertIs(self.m.change_storage_class("a.txt", "GLACIER"), True)
        self.assertEqual(len(c.calls_of("head_object")), 1)
        self.assertEqual(len(c.calls_of("copy_object")), 1)

    def test_the_no_op_reuses_the_head_the_copy_already_paid_for(self):
        c = self._Refusing(current="GLACIER")
        self.m._client = c
        self.assertIs(self.m.change_storage_class("a.txt", "GLACIER"), False)
        self.assertEqual(len(c.calls_of("head_object")), 1,
                         "asked the class again after already reading it")

    def test_an_invalid_request_for_another_reason_still_raises(self):
        """Only "it is already there" is forgiven; anything else is a real
        failure and must reach the user."""
        class OtherwiseIllegal(FakeS3Client):
            def copy_object(self, **kw):
                raise botocore.exceptions.ClientError(
                    {"Error": {"Code": "InvalidRequest", "Message": "nope"}},
                    "CopyObject")

            def head_object(self, **kw):
                return {"StorageClass": "GLACIER"}

        self.m._client = OtherwiseIllegal()
        with self.assertRaises(botocore.exceptions.ClientError):
            self.m.change_storage_class("a.txt", "DEEP_ARCHIVE")

    def test_an_unsupported_class_still_raises(self):
        class NoSuchClass(FakeS3Client):
            def copy_object(self, **kw):
                raise botocore.exceptions.ClientError(
                    {"Error": {"Code": "InvalidStorageClass",
                               "Message": "Invalid storage class"}},
                    "CopyObject")

        self.m._client = NoSuchClass()
        with self.assertRaises(botocore.exceptions.ClientError):
            self.m.change_storage_class("a.txt", "GLACIER")

    def test_head_silence_reads_as_standard(self):
        c = FakeS3Client()
        self.m._client = c
        self.assertEqual(self.m.current_storage_class("a.txt"), "STANDARD")
class PresignedUrlModelTests(unittest.TestCase):
    def setUp(self):
        self.m = make_model(bucket="b")

    def test_put_url_uses_put_object(self):
        c = FakeS3Client()
        self.m._client = c
        self.m.presigned_put_url("a/f.txt", 120)
        call = c.calls_of("generate_presigned_url")[0]
        self.assertEqual(call["op"], "put_object")
        self.assertEqual(call["Params"], {"Bucket": "b", "Key": "a/f.txt"})
        self.assertEqual(call["ExpiresIn"], 120)

    def test_get_url_uses_get_object(self):
        c = FakeS3Client()
        self.m._client = c
        self.m.presigned_get_url("a/f.txt", 300)
        call = c.calls_of("generate_presigned_url")[0]
        self.assertEqual(call["op"], "get_object")
        self.assertEqual(call["ExpiresIn"], 300)
class EncryptionPropertiesTests(unittest.TestCase):
    """Properties a credential store must actually hold, as opposed to lines
    merely being executed: secrets must not be recoverable from the stored
    blob, and a value written in one process must read back in another."""

    SECRET = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

    def test_generate_key_yields_a_usable_and_unique_key(self):
        first, second = Crypto.generate_key(), Crypto.generate_key()
        self.assertNotEqual(first, second)
        self.assertIsInstance(first, str)
        crypto = Crypto(first)
        self.assertEqual(crypto.decrypt(crypto.encrypt(self.SECRET)), self.SECRET)

    def test_plaintext_never_appears_in_the_stored_blob(self):
        """The whole point of encrypting credentials at rest."""
        crypto = Crypto(Crypto.generate_key())
        token = crypto.encrypt(self.SECRET)
        self.assertNotIn(self.SECRET.encode(), token)
        self.assertNotIn(b"wJalrXUt", token)

    def test_the_same_value_encrypts_differently_each_time(self):
        """Fernet randomises the IV; identical ciphertexts would leak that two
        profiles share a secret."""
        crypto = Crypto(Crypto.generate_key())
        first = crypto.encrypt(self.SECRET)
        second = crypto.encrypt(self.SECRET)
        self.assertNotEqual(first, second)
        self.assertEqual(crypto.decrypt(first), crypto.decrypt(second))

    def test_a_token_survives_a_new_crypto_instance(self):
        """Written on save, read back by a later process — the real lifecycle."""
        key = Crypto.generate_key()
        token = Crypto(key).encrypt(self.SECRET)
        self.assertEqual(Crypto(key).decrypt(token), self.SECRET)

    def test_a_bytes_key_is_accepted(self):
        key = Fernet.generate_key()          # bytes, not str
        crypto = Crypto(key)
        self.assertEqual(crypto.decrypt(crypto.encrypt("v")), "v")

    def test_round_trips_awkward_values(self):
        crypto = Crypto(Crypto.generate_key())
        for value in ("", "ünïcødé-Ключ-🦆", "x" * 4096, "line\nbreak\ttab",
                      "aws/session+token=="):
            with self.subTest(value=value[:20]):
                self.assertEqual(crypto.decrypt(crypto.encrypt(value)), value)

    def test_empty_plaintext_round_trips_but_an_empty_token_errors(self):
        """Two different things that both look 'empty': a stored empty secret
        is valid and returns ''; a missing stored value is an error."""
        crypto = Crypto(Crypto.generate_key())
        token = crypto.encrypt("")
        self.assertTrue(token)
        self.assertEqual(crypto.decrypt(token), "")
        with self.assertRaises(CredentialError):
            crypto.decrypt("")

    def test_encrypt_coerces_none_to_empty(self):
        crypto = Crypto(Crypto.generate_key())
        self.assertEqual(crypto.decrypt(crypto.encrypt(None)), "")

    def test_decrypt_falls_back_to_str_for_exotic_values(self):
        """QSettings-like wrappers that are neither bytes nor str and cannot
        be bytes()-converted still have to decrypt."""
        crypto = Crypto(Crypto.generate_key())
        token = crypto.encrypt(self.SECRET)

        class Wrapped:
            def __init__(self, raw):
                self._raw = raw

            def __str__(self):
                return self._raw.decode()

        with self.assertRaises(TypeError):
            bytes(Wrapped(token))            # the fallback really is needed
        self.assertEqual(crypto.decrypt(Wrapped(token)), self.SECRET)

    def test_a_token_is_worthless_without_the_key(self):
        token = Crypto(Crypto.generate_key()).encrypt(self.SECRET)
        with self.assertRaises(CredentialError):
            Crypto(Crypto.generate_key()).decrypt(token)
class MergeTagsTests(unittest.TestCase):
    CURRENT = [{"Key": "env", "Value": "prod"}, {"Key": "team", "Value": "ops"}]

    def test_add_merges_over_existing(self):
        out = Model.merge_tags(self.CURRENT, add={"owner": "vlad"})
        self.assertEqual(
            {t["Key"]: t["Value"] for t in out},
            {"env": "prod", "team": "ops", "owner": "vlad"})

    def test_add_overwrites_the_same_key(self):
        out = Model.merge_tags(self.CURRENT, add={"env": "staging"})
        self.assertEqual(
            {t["Key"]: t["Value"] for t in out}["env"], "staging")

    def test_remove_drops_keys(self):
        out = Model.merge_tags(self.CURRENT, remove=["team", "absent"])
        self.assertEqual([t["Key"] for t in out], ["env"])

    def test_replace_discards_everything_else(self):
        out = Model.merge_tags(self.CURRENT, add={"only": "1"}, replace=True)
        self.assertEqual(out, [{"Key": "only", "Value": "1"}])

    def test_existing_order_is_preserved(self):
        out = Model.merge_tags(self.CURRENT, add={"z": "1"})
        self.assertEqual([t["Key"] for t in out], ["env", "team", "z"])

    def test_values_are_stringified(self):
        out = Model.merge_tags([], add={"n": 5})
        self.assertEqual(out, [{"Key": "n", "Value": "5"}])

    def test_update_reads_merges_and_writes(self):
        m = make_model(bucket="b")

        class TagClient(FakeS3Client):
            def get_object_tagging(self, **kw):
                self.calls.append(("get_object_tagging", kw))
                return {"TagSet": [{"Key": "env", "Value": "prod"}]}

            def put_object_tagging(self, **kw):
                self.calls.append(("put_object_tagging", kw))
                return {}

        c = TagClient()
        m._client = c
        m.update_object_tags("a.txt", add={"owner": "vlad"}, remove=["env"])
        put = c.calls_of("put_object_tagging")[0]
        self.assertEqual(put["Tagging"]["TagSet"],
                         [{"Key": "owner", "Value": "vlad"}])

    def test_replace_skips_the_read(self):
        m = make_model(bucket="b")

        class TagClient(FakeS3Client):
            def get_object_tagging(self, **kw):
                raise AssertionError("must not read when replacing")

            def put_object_tagging(self, **kw):
                self.calls.append(("put_object_tagging", kw))
                return {}

        m._client = TagClient()
        m.update_object_tags("a.txt", add={"a": "b"}, replace=True)

    def test_read_only_refuses(self):
        m = make_model(bucket="b", read_only=True)
        m._client = FakeS3Client()
        with self.assertRaises(ReadOnlyError):
            m.update_object_tags("a.txt", add={"a": "b"})
class RetypeObjectTests(unittest.TestCase):
    """Re-stamping objects uploaded before detection existed."""

    def _model(self, head):
        client = FakeS3Client(head_object_resp=head)
        m = make_model(bucket="bkt")
        m._client = client
        return m, client

    def test_wrong_type_is_replaced_and_the_rest_preserved(self):
        m, client = self._model({
            "ContentType": "binary/octet-stream",
            "CacheControl": "max-age=60",
            "StorageClass": "GLACIER",
            "Metadata": {"owner": "vlad"},
        })
        changed, content_type = m.retype_object("site/index.html")
        self.assertTrue(changed)
        self.assertEqual(content_type, "text/html")
        copy_kw = client.calls_of("copy_object")[0]
        self.assertEqual(copy_kw["ContentType"], "text/html")
        self.assertEqual(copy_kw["MetadataDirective"], "REPLACE")
        self.assertEqual(copy_kw["CacheControl"], "max-age=60")
        self.assertEqual(copy_kw["StorageClass"], "GLACIER")
        self.assertEqual(copy_kw["Metadata"], {"owner": "vlad"})

    def test_correct_type_is_left_alone(self):
        m, client = self._model({"ContentType": "text/html"})
        changed, content_type = m.retype_object("site/index.html")
        self.assertFalse(changed)
        self.assertEqual(content_type, "text/html")
        self.assertEqual(client.calls_of("copy_object"), [])

    def test_unguessable_key_is_not_stamped_with_the_fallback(self):
        m, client = self._model({"ContentType": "binary/octet-stream"})
        self.assertEqual(m.retype_object("dumps/LICENSE"), (False, ""))
        self.assertEqual(client.calls_of("copy_object"), [])
        self.assertEqual(client.calls_of("head_object"), [])

    def test_read_only_profile_refuses(self):
        m, _client = self._model({"ContentType": "x/y"})
        m.read_only = True
        with self.assertRaises(ReadOnlyError):
            m.retype_object("site/index.html")
class LifecycleTests(unittest.TestCase):
    """The rules that decide what a bucket costs next month."""

    def _model(self, **kw):
        m = make_model(bucket="bkt")
        m._client = _BucketAdminClient(**kw)
        return m, m._client

    def test_a_bucket_without_rules_reads_as_empty_not_an_error(self):
        m, _c = self._model()
        self.assertEqual(m.get_bucket_lifecycle(), [])

    def test_rules_are_returned_as_stored(self):
        rules = [{"ID": "r1", "Status": "Enabled"}]
        m, _c = self._model(lifecycle=rules)
        self.assertEqual(m.get_bucket_lifecycle(), rules)

    def test_writing_an_empty_set_removes_the_configuration(self):
        m, client = self._model(lifecycle=[{"ID": "r1"}])
        m.put_bucket_lifecycle([])
        self.assertEqual(len(client.calls_of("delete_bucket_lifecycle")), 1)
        self.assertEqual(client.calls_of("put_bucket_lifecycle_configuration"),
                         [])

    def test_writing_rules_sends_them_in_one_document(self):
        m, client = self._model()
        rules = [{"ID": "r1", "Status": "Enabled"}]
        m.put_bucket_lifecycle(rules)
        sent = client.calls_of("put_bucket_lifecycle_configuration")[0]
        self.assertEqual(sent["LifecycleConfiguration"], {"Rules": rules})

    def test_a_read_only_profile_refuses(self):
        m, _c = self._model()
        m.read_only = True
        with self.assertRaises(ReadOnlyError):
            m.put_bucket_lifecycle([{"ID": "r"}])

    class _SizedLifecycle(_BucketAdminClient):
        """Stores the bucket-wide minimum object size beside the rules."""

        def __init__(self, minimum=None, read_error=None, **kw):
            super().__init__(**kw)
            self.minimum = minimum
            self.read_error = read_error

        def get_bucket_lifecycle_configuration(self, **kw):
            if self.read_error is not None:
                self.calls.append(("get_bucket_lifecycle_configuration", kw))
                raise botocore.exceptions.ClientError(
                    {"Error": {"Code": self.read_error, "Message": "no"}},
                    "GetBucketLifecycleConfiguration")
            resp = super().get_bucket_lifecycle_configuration(**kw)
            if self.minimum:
                resp["TransitionDefaultMinimumObjectSize"] = self.minimum
            return resp

    def test_saving_rules_keeps_the_minimum_object_size(self):
        """A PUT without it resets it to the service default, so editing any
        rule re-enabled transitions for small objects the owner had chosen."""
        m = make_model(bucket="bkt")
        m._client = client = self._SizedLifecycle(
            minimum="varies_by_storage_class", lifecycle=[{"ID": "old"}])
        self.assertEqual(m.get_bucket_lifecycle(), [{"ID": "old"}])
        m.put_bucket_lifecycle([{"ID": "new", "Status": "Enabled"}])
        sent = client.calls_of("put_bucket_lifecycle_configuration")[0]
        self.assertEqual(sent["TransitionDefaultMinimumObjectSize"],
                         "varies_by_storage_class")
        self.assertEqual(sent["LifecycleConfiguration"],
                         {"Rules": [{"ID": "new", "Status": "Enabled"}]})

    def test_a_first_configuration_sends_no_minimum(self):
        m, client = self._model()                  # NoSuchLifecycleConfiguration
        m.put_bucket_lifecycle([{"ID": "r1", "Status": "Enabled"}])
        sent = client.calls_of("put_bucket_lifecycle_configuration")[0]
        self.assertNotIn("TransitionDefaultMinimumObjectSize", sent)

    def test_an_unreadable_configuration_is_not_written_over_blind(self):
        m = make_model(bucket="bkt")
        m._client = client = self._SizedLifecycle(read_error="AccessDenied")
        with self.assertRaises(botocore.exceptions.ClientError):
            m.put_bucket_lifecycle([{"ID": "r1", "Status": "Enabled"}])
        self.assertEqual(client.calls_of("put_bucket_lifecycle_configuration"),
                         [])

    def test_scope_is_read_from_either_filter_shape(self):
        self.assertEqual(Model.lifecycle_rule_scope({"Prefix": "old/"}), "old/")
        self.assertEqual(
            Model.lifecycle_rule_scope({"Filter": {"Prefix": "logs/"}}),
            "logs/")
        self.assertEqual(
            Model.lifecycle_rule_scope(
                {"Filter": {"And": {"Prefix": "a/", "Tags": []}}}), "a/")
        self.assertEqual(Model.lifecycle_rule_scope({"Filter": {}}), "")

    def test_a_rule_summary_names_every_action(self):
        summary = Model.summarize_lifecycle_rule({
            "ID": "r", "Filter": {"Prefix": "logs/"},
            "Transitions": [{"Days": 30, "StorageClass": "GLACIER"}],
            "Expiration": {"Days": 365},
            "NoncurrentVersionExpiration": {"NoncurrentDays": 7},
            "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 3},
        })
        self.assertIn("prefix 'logs/'", summary)
        self.assertIn("to GLACIER after 30d", summary)
        self.assertIn("delete after 365d", summary)
        self.assertIn("old versions after 7d", summary)
        self.assertIn("abort incomplete uploads after 3d", summary)

    def test_a_tag_filter_is_named_in_the_summary(self):
        """A summary that omits the tag describes a rule that deletes far
        more than the real one does."""
        summary = Model.summarize_lifecycle_rule({
            "ID": "r",
            "Filter": {"And": {"Prefix": "logs/",
                               "Tags": [{"Key": "temp", "Value": "yes"}]}},
            "Expiration": {"Days": 7},
        })
        self.assertIn("prefix 'logs/'", summary)
        self.assertIn("tagged temp=yes", summary)

    def test_size_bounds_are_named_in_the_summary(self):
        summary = Model.summarize_lifecycle_rule({
            "ID": "r",
            "Filter": {"ObjectSizeGreaterThan": 1024,
                       "ObjectSizeLessThan": 4096},
            "Expiration": {"Days": 7},
        })
        self.assertIn("larger than 1024 B", summary)
        self.assertIn("smaller than 4096 B", summary)

    def test_a_prefix_only_filter_is_editable(self):
        for rule in ({"Filter": {"Prefix": "a/"}}, {"Filter": {}},
                     {"Prefix": "a/"}, {}):
            with self.subTest(rule=rule):
                self.assertTrue(Model.lifecycle_filter_is_simple(rule))

    def test_a_tag_or_size_filter_is_not_editable_here(self):
        """Rebuilding one from the prefix form would drop the filter and
        widen the rule to every object under the prefix."""
        for rule in (
            {"Filter": {"Tag": {"Key": "t", "Value": "v"}}},
            {"Filter": {"And": {"Prefix": "a/",
                                "Tags": [{"Key": "t", "Value": "v"}]}}},
            {"Filter": {"ObjectSizeGreaterThan": 1}},
            {"Filter": {"And": {"Prefix": "a/", "ObjectSizeLessThan": 10}}},
        ):
            with self.subTest(rule=rule):
                self.assertFalse(Model.lifecycle_filter_is_simple(rule))

    def test_a_rule_with_no_actions_says_so(self):
        self.assertIn(
            "does nothing",
            Model.summarize_lifecycle_rule({"ID": "r", "Filter": {}}))

    def test_building_a_rule_uses_the_modern_filter(self):
        rule = Model.build_lifecycle_rule(
            "expire", prefix="logs/", expire_days=90)
        self.assertEqual(rule["Filter"], {"Prefix": "logs/"})
        self.assertNotIn("Prefix", set(rule) - {"Filter"})
        self.assertEqual(rule["Expiration"], {"Days": 90})
        self.assertEqual(rule["Status"], "Enabled")

    def test_an_actionless_rule_is_refused_rather_than_stored(self):
        """S3 accepts one and then silently never fires it."""
        with self.assertRaises(ValueError):
            Model.build_lifecycle_rule("nothing", prefix="a/")

    def test_a_disabled_rule_keeps_its_status(self):
        rule = Model.build_lifecycle_rule("r", expire_days=1, enabled=False)
        self.assertEqual(rule["Status"], "Disabled")

    def test_delete_marker_cleanup_counts_as_an_action(self):
        rule = Model.build_lifecycle_rule("r", expire_delete_markers=True)
        self.assertEqual(rule["Expiration"],
                         {"ExpiredObjectDeleteMarker": True})
class CorsAndPolicyTests(unittest.TestCase):
    def _model(self, **kw):
        m = make_model(bucket="bkt")
        m._client = _BucketAdminClient(**kw)
        return m, m._client

    def test_absent_cors_reads_as_empty(self):
        m, _c = self._model()
        self.assertEqual(m.get_bucket_cors(), [])

    def test_empty_cors_removes_the_document(self):
        m, client = self._model(cors=[{"AllowedMethods": ["GET"]}])
        m.put_bucket_cors([])
        self.assertEqual(len(client.calls_of("delete_bucket_cors")), 1)

    def test_absent_policy_reads_as_empty_string(self):
        m, _c = self._model()
        self.assertEqual(m.get_bucket_policy(), "")

    def test_a_policy_must_be_a_json_object_with_statements(self):
        m, client = self._model()
        for bad in ("{not json", "[]", '{"Version":"2012-10-17"}'):
            with self.subTest(policy=bad):
                with self.assertRaises(ValueError):
                    m.put_bucket_policy(bad)
        self.assertEqual(client.calls_of("put_bucket_policy"), [])

    def test_a_valid_policy_is_sent_verbatim(self):
        m, client = self._model()
        policy = '{"Version": "2012-10-17", "Statement": []}'
        m.put_bucket_policy(policy)
        self.assertEqual(
            client.calls_of("put_bucket_policy")[0]["Policy"], policy)

    def test_an_empty_policy_removes_it(self):
        m, client = self._model(policy="{}")
        m.put_bucket_policy("   ")
        self.assertEqual(len(client.calls_of("delete_bucket_policy")), 1)

    def test_policy_status_is_reported_in_words(self):
        m, _c = self._model()
        self.assertEqual(m.get_bucket_policy_status(), "public")
class ObjectLockTests(unittest.TestCase):
    def _model(self, **kw):
        m = make_model(bucket="bkt")
        m._client = _BucketAdminClient(**kw)
        return m, m._client

    def test_a_bucket_without_object_lock_reads_as_empty(self):
        m, _c = self._model()
        self.assertEqual(m.get_object_lock_configuration(), {})

    def test_the_default_retention_is_reported(self):
        m, _c = self._model(lock={
            "ObjectLockEnabled": "Enabled",
            "Rule": {"DefaultRetention": {"Mode": "GOVERNANCE", "Days": 30}},
        })
        config = m.get_object_lock_configuration()
        self.assertTrue(config["enabled"])
        self.assertEqual(config["mode"], "GOVERNANCE")
        self.assertEqual(config["days"], 30)

    def test_an_unsupported_bucket_is_reported_not_raised(self):
        """Most buckets have no Object Lock, which is not an error."""
        m, _c = self._model()
        state = m.get_object_lock_state("a.txt")
        self.assertFalse(state["supported"])

    def test_retention_and_hold_are_read_together(self):
        m, _c = self._model(
            retention={"Mode": "COMPLIANCE",
                       "RetainUntilDate": "2027-01-01T00:00:00Z"},
            legal_hold="ON")
        state = m.get_object_lock_state("a.txt")
        self.assertTrue(state["supported"])
        self.assertEqual(state["mode"], "COMPLIANCE")
        self.assertIn("2027", state["retain_until"])
        self.assertEqual(state["legal_hold"], "ON")

    def test_a_missing_hold_is_simply_off(self):
        m, _c = self._model(retention={"Mode": "GOVERNANCE"})
        self.assertEqual(m.get_object_lock_state("a.txt")["legal_hold"], "")

    def test_placing_a_hold_sends_on(self):
        m, client = self._model(retention={"Mode": "GOVERNANCE"})
        m.set_object_legal_hold("a.txt", True)
        self.assertEqual(
            client.calls_of("put_object_legal_hold")[0]["LegalHold"],
            {"Status": "ON"})

    def test_clearing_a_hold_sends_off(self):
        m, client = self._model(retention={"Mode": "GOVERNANCE"})
        m.set_object_legal_hold("a.txt", False)
        self.assertEqual(
            client.calls_of("put_object_legal_hold")[0]["LegalHold"],
            {"Status": "OFF"})

    def test_a_read_only_profile_cannot_place_one(self):
        m, _c = self._model()
        m.read_only = True
        with self.assertRaises(ReadOnlyError):
            m.set_object_legal_hold("a.txt", True)
class BucketSettingsRenderTests(unittest.TestCase):
    """The four bucket documents are read in one background pass and each
    failure stays in its own tab."""

    class _DocModel:
        """Answers every bucket document; override one to make it fail."""

        bucket = "bkt"

        def get_bucket_lifecycle(self):
            return [{"ID": "r"}]

        def get_bucket_cors(self):
            return [{"AllowedMethods": ["GET"]}]

        def get_bucket_policy(self):
            return '{"Statement": []}'

        def get_object_lock_configuration(self):
            return {"enabled": True}

        def get_bucket_policy_status(self):
            return "public"

        def get_bucket_encryption(self):
            return {"sse": "AES256", "kms_key": "", "bucket_key": False}

        def get_bucket_tags(self):
            return {"team": "infra"}

        def get_bucket_website(self):
            return {"index": "index.html", "error": "", "redirect": ""}

        def get_bucket_notifications(self):
            return [("SQS queue", "arn:aws:sqs:x", ["s3:ObjectCreated:*"])]

        def get_bucket_acceleration(self):
            return "Enabled"

    def test_every_document_is_fetched_once(self):
        payload = main_window.BucketSettingsDialog.read_documents(
            self._DocModel())
        self.assertEqual(payload["lifecycle"], [{"ID": "r"}])
        self.assertEqual(payload["policy_status"], "public")
        self.assertTrue(payload["lock"]["enabled"])
        self.assertEqual(payload["encryption"]["sse"], "AES256")
        self.assertEqual(payload["tags"], {"team": "infra"})
        self.assertEqual(payload["website"]["index"], "index.html")
        self.assertEqual(payload["acceleration"], "Enabled")
        self.assertEqual(len(payload["events"]), 1)
        self.assertNotIn("policy_error", payload)

    def test_one_failure_does_not_hide_the_others(self):
        class _Failing(BucketSettingsRenderTests._DocModel):
            def get_bucket_lifecycle(self):
                raise RuntimeError("denied")

            def get_bucket_policy_status(self):
                raise RuntimeError("also denied")

            def get_bucket_tags(self):
                raise RuntimeError("no tag permission")

        payload = main_window.BucketSettingsDialog.read_documents(_Failing())
        self.assertEqual(payload["lifecycle_error"], "denied")
        self.assertIsNone(payload["lifecycle"])
        self.assertEqual(payload["tags_error"], "no tag permission")
        self.assertEqual(payload["cors"], [{"AllowedMethods": ["GET"]}])
        self.assertEqual(payload["policy_status"], "")
class BucketDocumentTests(unittest.TestCase):
    """Encryption, tags, website hosting, events and acceleration — each is
    absent far more often than it is set, and absent is not an error."""

    def _model(self, **docs):
        m = make_model(bucket="bkt")
        m._client = _DocumentClient(docs=docs)
        return m, m._client

    def test_absent_documents_all_read_as_empty(self):
        m, _c = self._model()
        self.assertEqual(m.get_bucket_encryption(), {})
        self.assertEqual(m.get_bucket_tags(), {})
        self.assertEqual(m.get_bucket_website(), {})
        self.assertEqual(m.get_bucket_notifications(), [])
        self.assertEqual(m.get_bucket_acceleration(), "")

    def test_default_encryption_is_flattened(self):
        m, _c = self._model(get_bucket_encryption={
            "ServerSideEncryptionConfiguration": {"Rules": [{
                "ApplyServerSideEncryptionByDefault": {
                    "SSEAlgorithm": "aws:kms",
                    "KMSMasterKeyID": "arn:key"},
                "BucketKeyEnabled": True}]}})
        self.assertEqual(m.get_bucket_encryption(), {
            "sse": "aws:kms", "kms_key": "arn:key", "bucket_key": True})

    def test_setting_kms_encryption_sends_the_key(self):
        m, client = self._model()
        m.put_bucket_encryption(sse="aws:kms", kms_key="arn:key",
                                bucket_key=True)
        rule = client.calls_of("put_bucket_encryption")[0][
            "ServerSideEncryptionConfiguration"]["Rules"][0]
        self.assertEqual(rule["ApplyServerSideEncryptionByDefault"],
                         {"SSEAlgorithm": "aws:kms",
                          "KMSMasterKeyID": "arn:key"})
        self.assertTrue(rule["BucketKeyEnabled"])

    def test_a_key_without_kms_mode_is_not_sent(self):
        m, client = self._model()
        m.put_bucket_encryption(sse="AES256", kms_key="arn:key")
        rule = client.calls_of("put_bucket_encryption")[0][
            "ServerSideEncryptionConfiguration"]["Rules"][0]
        self.assertNotIn("KMSMasterKeyID",
                         rule["ApplyServerSideEncryptionByDefault"])

    def test_clearing_encryption_deletes_the_document(self):
        m, client = self._model()
        m.put_bucket_encryption(sse="")
        self.assertEqual(len(client.calls_of("delete_bucket_encryption")), 1)

    def test_an_unknown_mode_is_refused(self):
        m, _c = self._model()
        with self.assertRaises(ValueError):
            m.put_bucket_encryption(sse="rot13")

    def test_bucket_tags_round_trip(self):
        m, client = self._model(get_bucket_tagging={"TagSet": [
            {"Key": "team", "Value": "infra"}]})
        self.assertEqual(m.get_bucket_tags(), {"team": "infra"})
        m.put_bucket_tags({"b": "2", "a": "1"})
        sent = client.calls_of("put_bucket_tagging")[0]["Tagging"]["TagSet"]
        self.assertEqual([t["Key"] for t in sent], ["a", "b"])  # sorted

    def test_empty_tags_delete_the_set(self):
        m, client = self._model()
        m.put_bucket_tags({})
        self.assertEqual(len(client.calls_of("delete_bucket_tagging")), 1)

    def test_website_hosting_round_trips(self):
        m, client = self._model(get_bucket_website={
            "IndexDocument": {"Suffix": "index.html"},
            "ErrorDocument": {"Key": "404.html"}})
        self.assertEqual(m.get_bucket_website(),
                         {"index": "index.html", "error": "404.html",
                          "redirect": ""})
        m.put_bucket_website(index="main.html", error="e.html")
        config = client.calls_of("put_bucket_website")[0][
            "WebsiteConfiguration"]
        self.assertEqual(config["IndexDocument"], {"Suffix": "main.html"})
        self.assertEqual(config["ErrorDocument"], {"Key": "e.html"})

    def test_a_redirect_replaces_the_documents(self):
        """The API rejects both at once, so the redirect wins rather than
        sending something the service refuses."""
        m, client = self._model()
        m.put_bucket_website(index="index.html", redirect="example.com")
        config = client.calls_of("put_bucket_website")[0][
            "WebsiteConfiguration"]
        self.assertEqual(config, {"RedirectAllRequestsTo":
                                  {"HostName": "example.com"}})

    def test_blank_website_settings_remove_it(self):
        m, client = self._model()
        m.put_bucket_website()
        self.assertEqual(len(client.calls_of("delete_bucket_website")), 1)

    def test_notifications_are_flattened_by_destination(self):
        m, _c = self._model(get_bucket_notification_configuration={
            "QueueConfigurations": [
                {"QueueArn": "arn:sqs", "Events": ["s3:ObjectCreated:*"]}],
            "TopicConfigurations": [{"TopicArn": "arn:sns", "Events": []}],
            "EventBridgeConfiguration": {},
        })
        events = m.get_bucket_notifications()
        self.assertIn(("SQS queue", "arn:sqs", ["s3:ObjectCreated:*"]), events)
        self.assertIn(("SNS topic", "arn:sns", []), events)

    def test_acceleration_is_reported_and_set(self):
        m, client = self._model(
            get_bucket_accelerate_configuration={"Status": "Enabled"})
        self.assertEqual(m.get_bucket_acceleration(), "Enabled")
        m.set_bucket_acceleration(False)
        self.assertEqual(
            client.calls_of("put_bucket_accelerate_configuration")[0][
                "AccelerateConfiguration"], {"Status": "Suspended"})

    def test_a_read_only_profile_refuses_every_write(self):
        m, _c = self._model()
        m.read_only = True
        for call in (lambda: m.put_bucket_encryption(sse="AES256"),
                     lambda: m.put_bucket_tags({"a": "1"}),
                     lambda: m.put_bucket_website(index="i.html"),
                     lambda: m.set_bucket_acceleration(True)):
            with self.assertRaises(ReadOnlyError):
                call()
class SelfCopyEncryptionTests(unittest.TestCase):
    """A copy onto itself names no encryption unless told to, and S3 then
    encrypts the result with the bucket's DEFAULT: an object under its own
    KMS key came back SSE-S3 after a storage-class change, a metadata edit or
    a version restore, with nothing said."""

    KEY_ARN = "arn:aws:kms:eu-west-1:111122223333:key/app"
    KMS_HEAD = {"ServerSideEncryption": "aws:kms", "SSEKMSKeyId": KEY_ARN,
                "BucketKeyEnabled": True, "StorageClass": "STANDARD"}
    KMS_ARGS = {"ServerSideEncryption": "aws:kms", "SSEKMSKeyId": KEY_ARN,
                "BucketKeyEnabled": True}

    def _model(self, head):
        m = make_model(bucket="b")
        m._client = FakeS3Client(head_object_resp=head)
        return m, m._client

    @staticmethod
    def _sse_of(call):
        return {field: value for field, value in call.items()
                if field in ("ServerSideEncryption", "SSEKMSKeyId",
                             "BucketKeyEnabled")}

    def test_a_storage_class_change_keeps_the_kms_key(self):
        m, client = self._model(self.KMS_HEAD)
        self.assertTrue(m.change_storage_class("a.bin", "STANDARD_IA"))
        self.assertEqual(self._sse_of(client.calls_of("copy_object")[0]),
                         self.KMS_ARGS)

    def test_a_metadata_edit_keeps_the_kms_key(self):
        m, client = self._model(self.KMS_HEAD)
        m.set_object_metadata("a.bin", content_type="text/plain")
        self.assertEqual(self._sse_of(client.calls_of("copy_object")[0]),
                         self.KMS_ARGS)

    def test_a_metadata_edit_uses_the_encryption_it_is_handed(self):
        """What get_object_metadata already read is not read again."""
        m, client = self._model(self.KMS_HEAD)
        m.set_object_metadata("a.bin", content_type="text/plain",
                              sse=dict(self.KMS_ARGS))
        self.assertEqual(client.calls_of("head_object"), [])
        self.assertEqual(self._sse_of(client.calls_of("copy_object")[0]),
                         self.KMS_ARGS)

    def test_retype_carries_the_key_from_its_one_head(self):
        m, client = self._model(
            dict(self.KMS_HEAD, ContentType="binary/octet-stream"))
        changed, _content_type = m.retype_object("site/index.html")
        self.assertTrue(changed)
        self.assertEqual(len(client.calls_of("head_object")), 1)
        self.assertEqual(self._sse_of(client.calls_of("copy_object")[0]),
                         self.KMS_ARGS)

    def test_a_restored_version_keeps_that_versions_key(self):
        m, client = self._model(self.KMS_HEAD)
        m.make_version_current("a.bin", "v1")
        self.assertEqual(client.calls_of("head_object")[0].get("VersionId"),
                         "v1", "read the current object, not the version")
        self.assertEqual(self._sse_of(client.calls_of("copy_object")[0]),
                         self.KMS_ARGS)

    def test_sse_s3_is_restated_without_the_kms_only_fields(self):
        self.assertEqual(
            Model._sse_args({"ServerSideEncryption": "AES256",
                             "SSEKMSKeyId": "stray", "BucketKeyEnabled": True}),
            {"ServerSideEncryption": "AES256"})
        self.assertEqual(Model._sse_args({}), {})

    def test_an_unreadable_head_leaves_the_copy_as_it_was(self):
        """Best effort: not being allowed to HEAD is no reason to refuse."""
        class NoHead(FakeS3Client):
            def head_object(self, **kw):
                self.calls.append(("head_object", kw))
                raise botocore.exceptions.ClientError(
                    {"Error": {"Code": "403", "Message": "Forbidden"}},
                    "HeadObject")

        m = make_model(bucket="b")
        m._client = client = NoHead()
        self.assertTrue(m.change_storage_class("a.bin", "GLACIER"))
        self.assertEqual(self._sse_of(client.calls_of("copy_object")[0]), {})

    def test_a_huge_metadata_rewrite_keeps_the_key_in_parts(self):
        too_big = botocore.exceptions.ClientError(
            {"Error": {"Code": "InvalidRequest",
                       "Message": "copy source is larger than the maximum"}},
            "CopyObject")
        class Untagged(_CopyPartClient):
            # The in-parts route may read the source's tags; this one has none.
            def get_object_tagging(self, **kw):
                self.calls.append(("get_object_tagging", kw))
                return {"TagSet": []}

        m = make_model(bucket="b")
        m._client = client = Untagged(
            source_size=6 * 1024 ** 3, copy_error=too_big, head=self.KMS_HEAD)
        m.set_object_metadata("big.bin", content_type="text/csv")
        _bucket, _key, kw = client.created[0]
        self.assertEqual(self._sse_of(kw), self.KMS_ARGS)
class UndeleteSinceTests(unittest.TestCase):
    """Undo removes the markers the delete being undone wrote, not every
    newest marker under the prefix -- that also resurrected whatever had been
    deleted from the folder on purpose long before."""

    def setUp(self):
        self.m = make_model(bucket="b")
        self.start = datetime.now(timezone.utc)

    def _client(self, markers, server_now=None):
        page = {"DeleteMarkers": markers, "Versions": []}
        if server_now is not None:
            page["ResponseMetadata"] = {"HTTPHeaders": {
                "date": format_datetime(server_now, usegmt=True)}}
        self.m._client = FakeS3Client(versions_pages=[page])
        return self.m._client

    @staticmethod
    def _marker(key, version_id, written):
        return {"Key": key, "VersionId": version_id, "IsLatest": True,
                "LastModified": written}

    def test_a_folder_undo_leaves_older_deletes_deleted(self):
        client = self._client([
            self._marker("dir/old-secret.txt", "dm-old",
                         self.start - timedelta(days=365)),
            self._marker("dir/a.txt", "dm-now",
                         self.start + timedelta(seconds=2)),
        ])
        self.assertEqual(self.m.undelete("dir/", since=self.start), 1)
        self.assertEqual(
            [kw["VersionId"] for kw in client.calls_of("delete_object")],
            ["dm-now"])

    def test_epoch_seconds_mean_the_same_moment(self):
        client = self._client([
            self._marker("dir/old.txt", "dm-old",
                         self.start - timedelta(hours=1)),
            self._marker("dir/a.txt", "dm-now", self.start),
        ])
        self.assertEqual(
            self.m.undelete("dir/", since=self.start.timestamp()), 1)
        self.assertEqual(client.calls_of("delete_object")[0]["Key"],
                         "dir/a.txt")

    def test_one_second_timestamps_do_not_lose_the_first_marker(self):
        self._client([self._marker("a.txt", "dm",
                                   self.start - timedelta(seconds=1))])
        self.assertEqual(self.m.undelete("a.txt", since=self.start), 1)

    def test_a_skewed_local_clock_is_measured_not_trusted(self):
        """This machine runs ten minutes ahead of the service, so by its own
        clock every marker the delete wrote predates the delete."""
        ahead = timedelta(minutes=10)
        server_start = self.start - ahead
        client = self._client([
            self._marker("dir/a.txt", "dm-now",
                         server_start + timedelta(seconds=1)),
            self._marker("dir/b.txt", "dm-earlier",
                         server_start - timedelta(minutes=3)),
        ], server_now=datetime.now(timezone.utc) - ahead)
        self.assertEqual(self.m.undelete("dir/", since=self.start), 1)
        self.assertEqual(
            [kw["VersionId"] for kw in client.calls_of("delete_object")],
            ["dm-now"])

    def test_a_null_marker_is_neither_removed_nor_counted(self):
        """Suspended versioning: the delete destroyed the unversioned copy,
        and removing its marker would only promote an older version under a
        "restored" that is not true."""
        client = self._client([
            self._marker("dir/a.txt", "null",
                         self.start + timedelta(seconds=1)),
            self._marker("dir/b.txt", "dm-b",
                         self.start + timedelta(seconds=1)),
        ])
        logs = []
        self.assertEqual(
            self.m.undelete("dir/", log_fn=logs.append, since=self.start), 1)
        self.assertEqual([kw["Key"] for kw in client.calls_of("delete_object")],
                         ["dir/b.txt"])
        self.assertTrue(any("dir/a.txt" in line and "suspended" in line
                            for line in logs), logs)
class WebsiteReadModifyWriteTests(unittest.TestCase):
    """PutBucketWebsite replaces the whole document; the dialog edits three
    fields of it. Everything else has to be carried over, or saving the
    error page deletes the site's redirects."""

    RULES = [{"Condition": {"KeyPrefixEquals": "docs/"},
              "Redirect": {"ReplaceKeyPrefixWith": "documents/"}}]

    def _model(self, stored=None):
        m = make_model(bucket="bkt")
        docs = {} if stored is None else {"get_bucket_website": stored}
        m._client = _DocumentClient(docs=docs)
        return m, m._client

    def _written(self, client):
        return client.calls_of("put_bucket_website")[0]["WebsiteConfiguration"]

    def test_routing_rules_survive_an_edit_of_the_documents(self):
        m, client = self._model({
            "IndexDocument": {"Suffix": "index.html"},
            "ErrorDocument": {"Key": "404.html"},
            "RoutingRules": self.RULES,
            "ResponseMetadata": {"HTTPStatusCode": 200},
        })
        m.put_bucket_website(index="index.html", error="oops.html")
        self.assertEqual(self._written(client), {
            "IndexDocument": {"Suffix": "index.html"},
            "ErrorDocument": {"Key": "oops.html"},
            "RoutingRules": self.RULES,
        })

    def test_an_https_redirect_stays_https(self):
        m, client = self._model({"RedirectAllRequestsTo": {
            "HostName": "old.example.com", "Protocol": "https"}})
        m.put_bucket_website(redirect="www.example.com")
        self.assertEqual(self._written(client), {"RedirectAllRequestsTo": {
            "HostName": "www.example.com", "Protocol": "https"}})

    def test_a_failed_read_is_not_written_over(self):
        class Denied(_DocumentClient):
            def get_bucket_website(self, **kw):
                raise botocore.exceptions.ClientError(
                    {"Error": {"Code": "AccessDenied", "Message": "no"}},
                    "GetBucketWebsite")

        m = make_model(bucket="bkt")
        m._client = client = Denied()
        with self.assertRaises(botocore.exceptions.ClientError):
            m.put_bucket_website(index="index.html")
        self.assertEqual(client.calls_of("put_bucket_website"), [])
class PurgeWithoutMultiDeleteTests(unittest.TestCase):
    """A folder delete already fell back to single deletes on a backend
    without DeleteObjects; emptying the whole bucket did not."""

    class _NoMultiDelete(FakeS3Client):
        def __init__(self, code="NotImplemented", **kw):
            super().__init__(**kw)
            self._code = code

        def delete_objects(self, **kw):
            self.calls.append(("delete_objects", kw))
            raise botocore.exceptions.ClientError(
                {"Error": {"Code": self._code, "Message": "no"}},
                "DeleteObjects")

    def _model(self, client):
        m = make_model(bucket="")
        m._try_bind_bucket = lambda name: (client, "ep", "us-east-1", True)
        return m

    def test_emptying_falls_back_to_one_delete_per_key(self):
        client = self._NoMultiDelete(
            list_pages=[{"Contents": [{"Key": "a.txt"}, {"Key": "b.txt"}]}],
            versions_pages=[{
                "Versions": [{"Key": "old.txt", "VersionId": "v0"}],
                "DeleteMarkers": [{"Key": "gone.txt", "VersionId": "dm0"}],
            }],
            mpu_pages=[{}])
        self._model(client).empty_bucket("bkt", batch_size=1)
        self.assertEqual(client.calls_of("delete_object"), [
            {"Bucket": "bkt", "Key": "a.txt"},
            {"Bucket": "bkt", "Key": "b.txt"},
            {"Bucket": "bkt", "Key": "old.txt", "VersionId": "v0"},
            {"Bucket": "bkt", "Key": "gone.txt", "VersionId": "dm0"},
        ])
        self.assertEqual(len(client.calls_of("delete_objects")), 1,
                         "asked again after being told no")

    def test_a_refusal_for_another_reason_still_fails(self):
        client = self._NoMultiDelete(
            code="AccessDenied",
            list_pages=[{"Contents": [{"Key": "a.txt"}]}],
            versions_pages=[{}], mpu_pages=[{}])
        with self.assertRaises(botocore.exceptions.ClientError):
            self._model(client).delete_bucket_recursive("bkt")
        self.assertEqual(client.calls_of("delete_object"), [])
        self.assertEqual(client.calls_of("delete_bucket"), [])
