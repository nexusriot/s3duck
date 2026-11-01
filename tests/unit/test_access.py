"""Tier 2 bucket administration: ACLs, retention, policy templates, Block
Public Access, logging, replication, presigned POST and SSE-C."""

import base64
import json
import types
import unittest
from datetime import datetime, timedelta, timezone

import botocore.exceptions
import main_window
from PyQt6.QtCore import QSettings
from PyQt6.QtWidgets import QDialogButtonBox, QMessageBox

from core.acl import (
    ALL_USERS, build_access_control_policy, describe_grantee,
    owner_keeps_control, parse_acl, public_grants, validate_grant,
)
from core.policy_templates import (
    build_template, grants_public_access, merge_statements,
)
from core.replication import describe_replication_rule, summarize_replication
from core.retention import (
    COMPLIANCE, GOVERNANCE, assess_retention_change, parse_retain_until,
)
from properties_window import describe_encryption
from s3.access import PUBLIC_ACCESS_FLAGS
from tests.support import (
    PROJECT_ROOT, _StubModel, _ensure_qapp, make_main_window,
    make_model,
)
from ui.dialogs.access import (
    AclDialog, PolicyTemplateDialog, RetentionDialog, SseCKeyDialog,
    parse_sse_c_key, sse_c_fingerprint,
)
from ui.window.access import AccessWindowMixin
from ui.dialogs.presigned import PresignedLinkDialog, post_form_curl, post_form_html
from unittest.mock import patch

OWNER_ID = "a" * 64
NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _grant(kind="CanonicalUser", permission="FULL_CONTROL", **kw):
    grant = {"type": kind, "id": "", "display": "", "uri": "", "email": "",
             "permission": permission}
    grant.update(kw)
    return grant


class AclCoreTests(unittest.TestCase):

    RESPONSE = {
        "Owner": {"ID": OWNER_ID, "DisplayName": "me"},
        "Grants": [
            {"Grantee": {"Type": "CanonicalUser", "ID": OWNER_ID,
                         "DisplayName": "me"}, "Permission": "FULL_CONTROL"},
            {"Grantee": {"Type": "Group", "URI": ALL_USERS},
             "Permission": "READ"},
        ],
    }

    def test_parse_and_rebuild_round_trip(self):
        acl = parse_acl(self.RESPONSE)
        self.assertEqual(acl["owner"], {"id": OWNER_ID, "display": "me"})
        self.assertEqual(len(acl["grants"]), 2)
        policy = build_access_control_policy(acl["owner"], acl["grants"])
        self.assertEqual(policy["Owner"]["ID"], OWNER_ID)
        self.assertEqual(policy["Grants"][1],
                         {"Grantee": {"Type": "Group", "URI": ALL_USERS},
                          "Permission": "READ"})

    def test_public_and_owner_checks(self):
        acl = parse_acl(self.RESPONSE)
        self.assertEqual(len(public_grants(acl["grants"])), 1)
        self.assertTrue(owner_keeps_control(acl["owner"], acl["grants"]))
        self.assertFalse(owner_keeps_control(acl["owner"], acl["grants"][1:]))

    def test_grantee_descriptions(self):
        self.assertEqual(describe_grantee(_grant("Group", uri=ALL_USERS)),
                         "Everyone (public)")
        self.assertEqual(describe_grantee(
            _grant("AmazonCustomerByEmail", email="x@y")), "x@y")
        self.assertIn("me (aaaa", describe_grantee(
            _grant(id=OWNER_ID, display="me")))

    def test_validation(self):
        self.assertEqual(validate_grant(_grant(id=OWNER_ID)), "")
        self.assertIn("64 hexadecimal", validate_grant(_grant(id="123456789012")))
        self.assertTrue(validate_grant(_grant("Group")))
        self.assertTrue(validate_grant(_grant("AmazonCustomerByEmail", email="no")))
        self.assertTrue(validate_grant(_grant(permission="EVERYTHING", id=OWNER_ID)))
        self.assertTrue(validate_grant(_grant("Robot")))
        with self.assertRaises(ValueError):
            build_access_control_policy({"id": OWNER_ID}, [_grant(id="x")])
        with self.assertRaises(ValueError):
            build_access_control_policy({}, [])


class RetentionCoreTests(unittest.TestCase):

    def _assess(self, cur_mode, cur_days, new_mode, new_days):
        cur = NOW + timedelta(days=cur_days) if cur_days is not None else None
        return assess_retention_change(cur_mode, cur, new_mode,
                                       NOW + timedelta(days=new_days), now=NOW)

    def test_new_retention(self):
        self.assertEqual(self._assess("", None, GOVERNANCE, 10), ("", False, False))
        self.assertEqual(self._assess("", None, COMPLIANCE, 10), ("", False, True))

    def test_the_date_must_be_in_the_future(self):
        problem, *_ = self._assess("", None, GOVERNANCE, -1)
        self.assertIn("future", problem)
        problem, *_ = assess_retention_change("", None, "FOREVER",
                                              NOW + timedelta(days=1), now=NOW)
        self.assertIn("mode", problem)

    def test_governance_shortening_needs_the_bypass(self):
        self.assertEqual(self._assess(GOVERNANCE, 30, GOVERNANCE, 10),
                         ("", True, False))
        self.assertEqual(self._assess(GOVERNANCE, 30, GOVERNANCE, 60),
                         ("", False, False))

    def test_compliance_cannot_be_shortened_or_downgraded(self):
        self.assertIn("cannot be shortened",
                      self._assess(COMPLIANCE, 30, COMPLIANCE, 10)[0])
        self.assertIn("cannot be changed",
                      self._assess(COMPLIANCE, 30, GOVERNANCE, 60)[0])
        self.assertEqual(self._assess(COMPLIANCE, 30, COMPLIANCE, 60),
                         ("", False, True))

    def test_an_expired_retention_is_no_constraint(self):
        self.assertEqual(self._assess(COMPLIANCE, -5, GOVERNANCE, 10),
                         ("", False, False))

    def test_parse_retain_until(self):
        self.assertEqual(parse_retain_until("2030-01-02 03:04:05+00:00"),
                         datetime(2030, 1, 2, 3, 4, 5, tzinfo=timezone.utc))
        self.assertEqual(parse_retain_until("2030-01-02T03:04:05Z").year, 2030)
        self.assertIsNone(parse_retain_until(""))
        self.assertIsNone(parse_retain_until("tomorrow"))


class PolicyTemplateTests(unittest.TestCase):

    def test_public_read_is_scoped_to_the_prefix(self):
        [statement] = build_template("public_read", "bkt", prefix="site/")
        self.assertEqual(statement["Action"], "s3:GetObject")
        self.assertEqual(statement["Resource"], "arn:aws:s3:::bkt/site/*")
        self.assertTrue(grants_public_access([statement]))

    def test_allow_account(self):
        statements = build_template("allow_account", "bkt",
                                    account_id="123456789012", write=True,
                                    prefix="in/")
        self.assertIn("s3:PutObject", statements[0]["Action"])
        self.assertEqual(statements[1]["Condition"]["StringLike"]["s3:prefix"],
                         ["in/*"])
        self.assertFalse(grants_public_access(statements))
        with self.assertRaises(ValueError):
            build_template("allow_account", "bkt", account_id="me@example.com")

    def test_deny_http_and_unknown(self):
        [statement] = build_template("deny_http", "bkt")
        self.assertEqual(statement["Effect"], "Deny")
        with self.assertRaises(ValueError):
            build_template("nope", "bkt")
        with self.assertRaises(ValueError):
            build_template("deny_http", "")

    def test_merge_replaces_by_sid_and_keeps_the_rest(self):
        existing = json.dumps({"Version": "2012-10-17", "Statement": [
            {"Sid": "Mine", "Effect": "Deny", "Principal": "*",
             "Action": "s3:DeleteObject", "Resource": "x"},
            {"Sid": "DenyInsecureTransport", "Effect": "Deny"}]})
        merged = json.loads(merge_statements(
            existing, build_template("deny_http", "bkt")))
        sids = [s["Sid"] for s in merged["Statement"]]
        self.assertEqual(sids, ["Mine", "DenyInsecureTransport"])
        self.assertIn("Condition", merged["Statement"][1])

    def test_merge_starts_a_policy_and_refuses_garbage(self):
        merged = json.loads(merge_statements("", build_template("deny_http", "b")))
        self.assertEqual(merged["Version"], "2012-10-17")
        with self.assertRaises(ValueError):
            merge_statements("{broken", [])
        with self.assertRaises(ValueError):
            merge_statements("[1]", [])
        single = json.dumps({"Statement": {"Sid": "One"}})
        self.assertEqual(len(json.loads(merge_statements(
            single, build_template("deny_http", "b")))["Statement"]), 2)


class ReplicationCoreTests(unittest.TestCase):

    def test_summary(self):
        summary = summarize_replication({"ReplicationConfiguration": {
            "Role": "arn:role",
            "Rules": [
                {"ID": "a", "Status": "Enabled", "Priority": 1,
                 "Filter": {"And": {"Prefix": "logs/",
                                    "Tags": [{"Key": "k", "Value": "v"}]}},
                 "Destination": {"Bucket": "arn:aws:s3:::dst",
                                 "StorageClass": "GLACIER"},
                 "DeleteMarkerReplication": {"Status": "Enabled"}},
                {"ID": "b", "Status": "Disabled", "Prefix": "old/",
                 "Destination": {"Bucket": "arn:aws:s3:::dst"}},
            ]}})
        self.assertEqual(summary["role"], "arn:role")
        first, second = summary["rules"]
        self.assertEqual(first["prefix"], "logs/")
        self.assertEqual(first["tags"], ["k=v"])
        line = describe_replication_rule(first)
        self.assertIn("as GLACIER", line)
        self.assertIn("with delete markers", line)
        self.assertEqual(second["prefix"], "old/")
        self.assertIn("[disabled]", describe_replication_rule(second))


class _Recorder:
    """A boto3 client stand-in that records calls and answers from a table."""

    def __init__(self, answers=None, errors=None):
        self.calls = []
        self.answers = answers or {}
        self.errors = errors or {}

    def __getattr__(self, name):
        def call(**kw):
            self.calls.append((name, kw))
            if name in self.errors:
                raise self.errors[name]
            return self.answers.get(name, {})
        return call


def _client_error(code):
    return botocore.exceptions.ClientError(
        {"Error": {"Code": code, "Message": code}}, "Op")


def _model(client=None, read_only=False):
    model = make_model(bucket="bkt", read_only=read_only)
    model._client = client or _Recorder()
    return model


class AccessModelTests(unittest.TestCase):

    def test_public_access_block(self):
        flags = {f: True for f in PUBLIC_ACCESS_FLAGS}
        model = _model(_Recorder({"get_public_access_block": {
            "PublicAccessBlockConfiguration": flags}}))
        self.assertEqual(model.get_public_access_block(), flags)
        model.put_public_access_block({"BlockPublicAcls": True})
        name, kw = model._client.calls[-1]
        self.assertEqual(name, "put_public_access_block")
        self.assertEqual(kw["PublicAccessBlockConfiguration"]["BlockPublicAcls"], True)
        self.assertEqual(kw["PublicAccessBlockConfiguration"]["IgnorePublicAcls"], False)
        model.delete_public_access_block()
        self.assertEqual(model._client.calls[-1][0], "delete_public_access_block")

    def test_an_absent_public_access_block_is_none(self):
        model = _model(_Recorder(errors={"get_public_access_block": _client_error(
            "NoSuchPublicAccessBlockConfiguration")}))
        self.assertIsNone(model.get_public_access_block())
        model = _model(_Recorder(errors={"get_public_access_block": _client_error(
            "AccessDenied")}))
        with self.assertRaises(botocore.exceptions.ClientError):
            model.get_public_access_block()

    def test_logging(self):
        model = _model(_Recorder({"get_bucket_logging": {"LoggingEnabled": {
            "TargetBucket": "logs", "TargetPrefix": "p/"}}}))
        self.assertEqual(model.get_bucket_logging(),
                         {"target_bucket": "logs", "target_prefix": "p/"})
        model.put_bucket_logging("logs", "x/")
        self.assertEqual(model._client.calls[-1][1]["BucketLoggingStatus"],
                         {"LoggingEnabled": {"TargetBucket": "logs",
                                             "TargetPrefix": "x/"}})
        model.put_bucket_logging("")
        self.assertEqual(model._client.calls[-1][1]["BucketLoggingStatus"], {})
        self.assertEqual(_model(_Recorder()).get_bucket_logging(), {})

    def test_replication_absent(self):
        model = _model(_Recorder(errors={"get_bucket_replication": _client_error(
            "ReplicationConfigurationNotFoundError")}))
        self.assertEqual(model.get_bucket_replication(), {"role": "", "rules": []})

    def test_acls(self):
        response = AclCoreTests.RESPONSE
        model = _model(_Recorder({"get_object_acl": response,
                                  "get_bucket_acl": response}))
        acl = model.get_object_acl("k", version_id="v1")
        self.assertEqual(model._client.calls[-1][1]["VersionId"], "v1")
        model.put_object_acl_grants("k", acl["owner"], acl["grants"])
        name, kw = model._client.calls[-1]
        self.assertEqual(name, "put_object_acl")
        self.assertEqual(len(kw["AccessControlPolicy"]["Grants"]), 2)
        bucket_acl = model.get_bucket_acl()
        model.put_bucket_acl(bucket_acl["owner"], bucket_acl["grants"][:1])
        self.assertEqual(model._client.calls[-1][0], "put_bucket_acl")

    def test_retention(self):
        model = _model()
        until = NOW + timedelta(days=1)
        model.set_object_retention("k", GOVERNANCE, until)
        name, kw = model._client.calls[-1]
        self.assertEqual(name, "put_object_retention")
        self.assertNotIn("BypassGovernanceRetention", kw)
        model.set_object_retention("k", GOVERNANCE, until, bypass_governance=True,
                                   version_id="v")
        kw = model._client.calls[-1][1]
        self.assertTrue(kw["BypassGovernanceRetention"])
        self.assertEqual(kw["VersionId"], "v")
        with self.assertRaises(ValueError):
            model.set_object_retention("k", "FOREVER", until)

    def test_writes_respect_read_only(self):
        model = _model(read_only=True)
        for call in (lambda: model.put_public_access_block({}),
                     lambda: model.put_bucket_logging("x"),
                     lambda: model.set_object_retention("k", GOVERNANCE, NOW),
                     lambda: model.put_object_acl_grants("k", {}, [])):
            with self.assertRaises(Exception):
                call()
        self.assertEqual(model._client.calls, [])

    def test_presigned_post_conditions(self):
        model = _model(_Recorder({"generate_presigned_post": {"url": "u",
                                                              "fields": {}}}))
        model.presigned_post("in/", 600, max_bytes=1024, content_type="image/png",
                             any_key_under_prefix=True)
        kw = model._client.calls[-1][1]
        self.assertEqual(kw["Key"], "in/${filename}")
        self.assertIn(["content-length-range", 0, 1024], kw["Conditions"])
        self.assertIn({"Content-Type": "image/png"}, kw["Conditions"])
        self.assertEqual(kw["Fields"], {"Content-Type": "image/png"})
        self.assertEqual(kw["ExpiresIn"], 600)
        model.presigned_post("exact.txt", 60)
        kw = model._client.calls[-1][1]
        self.assertEqual(kw["Key"], "exact.txt")
        self.assertIsNone(kw["Conditions"])

    def test_a_real_presigned_post_carries_the_policy(self):
        model = make_model(endpoint_url="http://localhost:9000", bucket="bkt",
                           use_path=True)
        post = model.presigned_post("in/", 60, max_bytes=10,
                                    any_key_under_prefix=True)
        self.assertIn("policy", post["fields"])
        policy = json.loads(base64.b64decode(post["fields"]["policy"]))
        self.assertIn(["content-length-range", 0, 10], policy["conditions"])
        self.assertIn(["starts-with", "$key", "in/"], policy["conditions"])


class _Sent(Exception):
    pass


class SseCTests(unittest.TestCase):

    KEY = bytes(range(32))

    def _capture(self, model, call):
        """The headers *call* would send, without sending it."""
        seen = {}

        def before_send(request, **_kw):
            seen.update(request.headers)
            raise _Sent()

        events = model.client.meta.events
        events.register("before-send.s3", before_send)
        try:
            with self.assertRaises(_Sent):
                call(model.client)
        finally:
            events.unregister("before-send.s3", before_send)
        return {k.lower(): (v.decode() if isinstance(v, bytes) else v)
                for k, v in seen.items()}

    def _model(self):
        return make_model(endpoint_url="http://localhost:9", bucket="bkt",
                          use_path=True)

    def test_the_key_rides_on_reads_and_writes(self):
        model = self._model()
        model.set_sse_c_key(self.KEY)
        headers = self._capture(model, lambda c: c.head_object(Bucket="bkt", Key="k"))
        self.assertEqual(
            headers["x-amz-server-side-encryption-customer-algorithm"], "AES256")
        self.assertEqual(headers["x-amz-server-side-encryption-customer-key"],
                         base64.b64encode(self.KEY).decode())
        self.assertIn("x-amz-server-side-encryption-customer-key-md5", headers)
        headers = self._capture(model, lambda c: c.put_object(
            Bucket="bkt", Key="k", Body=b"x", ServerSideEncryption="AES256"))
        self.assertNotIn("x-amz-server-side-encryption", headers,
                         "SSE-S3 was sent alongside SSE-C")
        self.assertIn("x-amz-server-side-encryption-customer-key", headers)

    def test_a_copy_names_the_key_for_both_ends(self):
        model = self._model()
        model.set_sse_c_key(self.KEY)
        headers = self._capture(model, lambda c: c.copy_object(
            Bucket="bkt", Key="b", CopySource={"Bucket": "bkt", "Key": "a"}))
        self.assertIn("x-amz-copy-source-server-side-encryption-customer-key",
                      headers)
        self.assertIn("x-amz-server-side-encryption-customer-key", headers)

    def test_clearing_the_key_stops_sending_it(self):
        model = self._model()
        model.set_sse_c_key(self.KEY)
        model.set_sse_c_key(b"")
        headers = self._capture(model, lambda c: c.head_object(Bucket="bkt", Key="k"))
        self.assertNotIn("x-amz-server-side-encryption-customer-key", headers)

    def test_the_key_follows_a_worker_clone(self):
        model = self._model()
        model.set_sse_c_key(self.KEY)
        clone = model.clone_for_worker()
        self.assertEqual(clone.sse_c_key, self.KEY)
        headers = self._capture(clone, lambda c: c.get_object(Bucket="bkt", Key="k"))
        self.assertIn("x-amz-server-side-encryption-customer-key", headers)

    def test_a_wrong_length_is_refused(self):
        with self.assertRaises(ValueError):
            self._model().set_sse_c_key(b"short")

    def test_verification_does_not_compare_an_sse_c_etag(self):
        model = self._model()
        model.set_sse_c_key(self.KEY)
        logged = []
        self.assertTrue(model.verify_download(
            __file__, "0" * 32, head={}, log_fn=logged.append))
        self.assertIn("SSE-C", logged[-1])

    def test_key_parsing(self):
        encoded = base64.b64encode(self.KEY).decode()
        self.assertEqual(parse_sse_c_key(encoded), self.KEY)
        self.assertEqual(parse_sse_c_key(" "), b"")
        with self.assertRaises(ValueError):
            parse_sse_c_key("not base64!!")
        with self.assertRaises(ValueError):
            parse_sse_c_key(base64.b64encode(b"x" * 16).decode())
        self.assertEqual(len(base64.b64decode(sse_c_fingerprint(self.KEY))), 16)


class AccessDialogTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _acl(self):
        return parse_acl(AclCoreTests.RESPONSE)

    def test_acl_dialog_lists_and_warns(self):
        saved = []
        dlg = AclDialog(None, "k", self._acl(),
                        save=lambda owner, grants: saved.append(grants))
        self.assertEqual(dlg.table.rowCount(), 2)
        self.assertIn("1 grant(s)", dlg.note.text())
        with patch.object(QMessageBox, "question",
                          return_value=QMessageBox.StandardButton.No):
            self.assertFalse(dlg.save())
        self.assertEqual(saved, [])
        dlg.table.selectRow(1)
        dlg.remove_selected()
        self.assertEqual(dlg.note.text(), "")
        self.assertTrue(dlg.save())
        self.assertEqual(len(saved[0]), 1)

    def test_dropping_owner_control_asks(self):
        saved = []
        dlg = AclDialog(None, "k", self._acl(),
                        save=lambda owner, grants: saved.append(grants))
        dlg.table.selectRow(0)
        dlg.remove_selected()
        dlg.table.selectRow(0)
        dlg.remove_selected()
        with patch.object(QMessageBox, "question",
                          return_value=QMessageBox.StandardButton.Yes) as ask:
            self.assertTrue(dlg.save())
        self.assertIn("FULL_CONTROL", ask.call_args.args[2])

    def test_an_acl_without_an_owner_id_is_not_editable(self):
        dlg = AclDialog(None, "k", {"owner": {"id": ""}, "grants": [
            _grant()]}, save=lambda *a: None)
        self.assertFalse(dlg.save_btn.isEnabled())
        self.assertIn("without an owner ID", dlg.note.text())

    def test_a_read_only_acl_dialog_cannot_save(self):
        dlg = AclDialog(None, "k", self._acl(), writable=False, save=lambda *a: None)
        self.assertFalse(dlg.save_btn.isEnabled())
        self.assertFalse(dlg.save())

    def test_a_refused_save_is_reported(self):
        def refuse(*_a):
            raise RuntimeError("AccessControlListNotSupported")
        dlg = AclDialog(None, "k", {"owner": {"id": OWNER_ID}, "grants": [
            _grant(id=OWNER_ID)]}, save=refuse)
        with patch.object(QMessageBox, "critical") as critical:
            self.assertFalse(dlg.save())
        critical.assert_called_once()

    def test_retention_dialog_gates_ok(self):
        state = {"mode": GOVERNANCE, "retain_until_dt": NOW + timedelta(days=30)}
        dlg = RetentionDialog(None, "k", state, now=NOW)
        ok = dlg.buttons.button(QDialogButtonBox.StandardButton.Ok)
        dlg.until.setDateTime(dlg.until.dateTime().addDays(-20))
        self.assertFalse(ok.isEnabled(), "a shortened GOVERNANCE needs bypass")
        self.assertTrue(dlg.bypass.isVisibleTo(dlg))
        dlg.bypass.setChecked(True)
        self.assertTrue(ok.isEnabled())
        dlg.mode.setCurrentText(COMPLIANCE)
        dlg.until.setDateTime(dlg.until.dateTime().addDays(60))
        self.assertFalse(ok.isEnabled(), "COMPLIANCE needs the acknowledgement")
        dlg.understood.setChecked(True)
        self.assertTrue(ok.isEnabled())
        mode, until, _bypass = dlg.chosen()
        self.assertEqual(mode, COMPLIANCE)
        self.assertEqual(until.tzinfo, timezone.utc)

    def test_policy_template_dialog(self):
        dlg = PolicyTemplateDialog(None, "bkt", "site/")
        dlg.template.setCurrentIndex(dlg.template.findData("allow_account"))
        dlg._accept()
        self.assertIn("12 digits", dlg.problem.text())
        dlg.account.setText("123456789012")
        self.assertEqual(len(dlg.statements()), 2)

    def test_sse_c_dialog(self):
        dlg = SseCKeyDialog(None)
        ok = dlg.buttons.button(QDialogButtonBox.StandardButton.Ok)
        self.assertEqual(dlg.key_bytes(), b"")
        dlg.key.setText("garbage")
        self.assertFalse(ok.isEnabled())
        dlg.generate()
        self.assertEqual(len(dlg.key_bytes()), 32)
        self.assertTrue(ok.isEnabled())
        self.assertIn("Key MD5", dlg.fingerprint.text())


class _PostModel:
    def __init__(self):
        self.posts = []

    def presigned_get_url(self, key, secs):
        return "https://get/" + key

    def presigned_put_url(self, key, secs):
        return "https://put/" + key

    def presigned_post(self, key, secs, **kw):
        self.posts.append((key, secs, kw))
        return {"url": "https://post/", "fields": {"key": key, "policy": "p\"q"}}


class PresignedPostDialogTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def test_post_mode(self):
        model = _PostModel()
        dlg = PresignedLinkDialog(None, model, "in/photo.png")
        self.assertIsNone(dlg.post_form())
        self.assertFalse(dlg._max_mb.isEnabled())
        dlg._type.setCurrentIndex(PresignedLinkDialog.POST)
        self.assertTrue(dlg._max_mb.isEnabled())
        self.assertEqual(dlg.post_form()["url"], "https://post/")
        key, _secs, kw = model.posts[-1]
        self.assertEqual(key, "in/photo.png")
        self.assertEqual(kw["max_bytes"], 100 * 1024 * 1024)
        dlg._any_name.setChecked(True)
        key, _secs, kw = model.posts[-1]
        self.assertEqual(key, "in/")
        self.assertTrue(kw["any_key_under_prefix"])
        self.assertIn("curl", dlg._fields.toPlainText())
        dlg._type.setCurrentIndex(PresignedLinkDialog.GET)
        self.assertIsNone(dlg.post_form())

    def test_curl_and_html(self):
        post = {"url": "https://h/b", "fields": {"key": "a b", "policy": 'x"y'}}
        curl = post_form_curl(post)
        self.assertTrue(curl.endswith("-F file=@/path/to/file https://h/b"))
        self.assertIn("--form-string 'key=a b'", curl)
        html = post_form_html(post)
        self.assertIn('value="x&quot;y"', html)
        self.assertIn('enctype="multipart/form-data"', html)


class DescribeEncryptionTests(unittest.TestCase):

    def test_kinds(self):
        self.assertIn("SSE-C", describe_encryption(
            {"SSECustomerAlgorithm": "AES256", "SSECustomerKeyMD5": "m"}))
        self.assertIn("SSE-KMS (k)", describe_encryption(
            {"ServerSideEncryption": "aws:kms", "SSEKMSKeyId": "k"}))
        self.assertEqual(describe_encryption({"ServerSideEncryption": "AES256"}),
                         "SSE-S3 (AES256)")
        self.assertEqual(describe_encryption({}), "")


class WindowAccessTests(unittest.TestCase):
    """The window-side handlers, with the model and dialogs stubbed."""

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def test_the_title_marks_an_sse_c_session(self):
        settings = QSettings("s3duck-tests-access", "s3duck-tests-access")
        settings.clear()
        self.addCleanup(settings.clear)
        with patch.object(main_window, "DataModel", _StubModel):
            win = make_main_window(PROJECT_ROOT, settings)
        self.addCleanup(win.close)
        win.apply_sse_c_key(SseCTests.KEY)
        self.assertIn("[SSE-C]", win.windowTitle())
        win.apply_sse_c_key(b"")
        self.assertNotIn("[SSE-C]", win.windowTitle())

    def test_an_acl_refusal_is_explained(self):
        text = AccessWindowMixin._explain_acl_refusal(
            types.SimpleNamespace(), Exception("AccessControlListNotSupported"))
        self.assertIn("BucketOwnerEnforced", text)


if __name__ == "__main__":
    unittest.main()
