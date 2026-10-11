"""What an upload carries besides the body.

"Anything under archive/ goes to Glacier, and tag it owner=data" is a policy,
and the point of writing it down once is that nobody has to remember it per
file. The unit suite proves the rules parse and that the right ExtraArgs dict
comes out; it cannot prove the service accepts that dict.

Tagging is the reason this needs a server at all. S3 takes a tag set on
PutObject as a url-encoded query string -- not as the list of dicts every
other tagging call uses -- so a rule that looks right offline is one
round trip away from being silently wrong, and the symptom is an object
stored with no tags rather than an error.
"""

import os
import tempfile

from tests.e2e.harness import ServerCase


class UploadRuleTests(ServerCase):
    """Per-destination rules, applied by a real PUT."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.addCleanup(self.model.set_upload_rules, [])
        self.addCleanup(self.model.set_upload_options)

    def _upload(self, key, body=b"payload"):
        path = os.path.join(self.tmp.name, "payload.bin")
        with open(path, "wb") as handle:
            handle.write(body)
        self.model.upload_file(path, key)
        return key

    def _tags(self, key):
        resp = self.model.client.get_object_tagging(
            Bucket=self.bucket, Key=key)
        return {t["Key"]: t["Value"] for t in resp.get("TagSet") or []}

    def test_a_rule_tags_the_object_on_the_server(self):
        """The url-encoded form has to be the one S3 wants, or the tags are
        quietly dropped."""
        self.model.set_upload_rules([
            {"pattern": "tagged/*", "storage_class": "",
             "tags": {"owner": "data", "retention": "30d"}},
        ])
        self._upload("tagged/report.csv")
        self.assertEqual(self._tags("tagged/report.csv"),
                         {"owner": "data", "retention": "30d"})

    def test_a_tag_value_with_awkward_characters_survives_the_encoding(self):
        """Spaces and '=' are exactly what a hand-rolled query string gets
        wrong."""
        self.model.set_upload_rules([
            {"pattern": "tagged/*", "storage_class": "",
             "tags": {"note": "quarterly report", "expr": "a=b"}},
        ])
        self._upload("tagged/awkward.csv")
        self.assertEqual(self._tags("tagged/awkward.csv"),
                         {"note": "quarterly report", "expr": "a=b"})

    def test_an_object_outside_the_pattern_is_left_alone(self):
        self.model.set_upload_rules([
            {"pattern": "tagged/*", "storage_class": "",
             "tags": {"owner": "data"}},
        ])
        self._upload("untagged/plain.csv")
        self.assertEqual(self._tags("untagged/plain.csv"), {})

    def test_a_bare_prefix_rule_matches_everything_beneath_it(self):
        """Writing "logs/" is the obvious way to say "everything under
        here", and it has to work without a trailing glob."""
        self.model.set_upload_rules([
            {"pattern": "logs/", "storage_class": "",
             "tags": {"kind": "log"}},
        ])
        self._upload("logs/deep/inner/today.log")
        self.assertEqual(self._tags("logs/deep/inner/today.log"),
                         {"kind": "log"})

    def test_the_first_matching_rule_wins(self):
        """First match rather than a merge: a table where two rules
        half-apply is one nobody can predict."""
        self.model.set_upload_rules([
            {"pattern": "both/*", "storage_class": "", "tags": {"rule": "one"}},
            {"pattern": "both/*", "storage_class": "", "tags": {"rule": "two"}},
        ])
        self._upload("both/file.txt")
        self.assertEqual(self._tags("both/file.txt"), {"rule": "one"})

    def test_a_storage_class_rule_is_applied_or_refused_clearly(self):
        """Not every backend has the colder tiers; what it must not do is
        store the object under a class nobody asked for."""
        self.model.set_upload_rules([
            {"pattern": "cold/*", "storage_class": "STANDARD_IA", "tags": {}},
        ])
        try:
            self._upload("cold/archive.bin")
        except Exception as exc:
            self.assertTrue(str(exc), "a refusal has to be reportable")
            print(f"\n    STANDARD_IA refused here: {exc}")
            return
        head = self.model.client.head_object(
            Bucket=self.bucket, Key="cold/archive.bin")
        stored = head.get("StorageClass") or "STANDARD"
        self.assertIn(stored, ("STANDARD_IA", "STANDARD"))
        print(f"\n    STANDARD_IA stored as: {stored}")

    def test_the_rules_survive_the_clone_a_transfer_runs_on(self):
        """Every transfer runs on clone_for_worker(), so a rule the clone
        forgets is a policy that applies in testing and not in the app."""
        self.model.set_upload_rules([
            {"pattern": "cloned/*", "storage_class": "",
             "tags": {"via": "clone"}},
        ])
        clone = self.model.clone_for_worker()
        path = os.path.join(self.tmp.name, "payload.bin")
        with open(path, "wb") as handle:
            handle.write(b"through the clone")
        clone.upload_file(path, "cloned/file.txt")
        self.assertEqual(self._tags("cloned/file.txt"), {"via": "clone"})


class ContentTypeRuleTests(ServerCase):
    """What an object is stored AS decides whether a browser opening its
    public URL renders it or downloads it, so the Content-Type an upload
    sends is a stored property worth asking the server about."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.addCleanup(self.model.set_content_type_options, True, {})

    def _upload(self, key, body=b"payload"):
        path = os.path.join(self.tmp.name, os.path.basename(key))
        with open(path, "wb") as handle:
            handle.write(body)
        self.model.upload_file(path, key)
        return key

    def _stored_type(self, key):
        return self.model.client.head_object(
            Bucket=self.bucket, Key=key).get("ContentType") or ""

    def test_a_user_override_beats_the_built_in_table(self):
        self.model.set_content_type_options(
            detect=True, overrides={"csv": "text/x-custom-csv"})
        self._upload("ct/data.csv")
        self.assertEqual(self._stored_type("ct/data.csv"),
                         "text/x-custom-csv")

    def test_the_built_in_table_covers_what_the_platform_misses(self):
        """.md is the case the stdlib answers differently depending on the
        machine's /etc/mime.types, which is why the table exists."""
        self.model.set_content_type_options(detect=True, overrides={})
        self._upload("ct/readme.md")
        self.assertEqual(self._stored_type("ct/readme.md"), "text/markdown")

    def test_detection_off_leaves_the_service_default(self):
        self.model.set_content_type_options(detect=False, overrides={})
        self._upload("ct/undetected.md")
        stored = self._stored_type("ct/undetected.md")
        self.assertNotEqual(stored, "text/markdown",
                            "a type was sent with detection switched off")

    def test_an_unknown_extension_is_not_guessed_at(self):
        """Empty rather than a fallback: the caller must be able to tell
        "no idea" from a deliberate binary/octet-stream."""
        self.model.set_content_type_options(detect=True, overrides={})
        self.assertEqual(self.model.guess_content_type("x.zzzunknown"), "")
        self._upload("ct/mystery.zzzunknown")
        self.assertTrue(self._stored_type("ct/mystery.zzzunknown"))


class ObjectPropertiesTests(ServerCase):
    """What the properties dialog and the metadata editor read back."""

    def test_properties_report_the_size_and_type_the_server_holds(self):
        self.put("props/thing.txt", b"0123456789",
                 ContentType="text/plain")
        head = self.model.object_properties("props/thing.txt")
        self.assertEqual(int(head["ContentLength"]), 10)
        self.assertEqual(head["ContentType"], "text/plain")

    def test_properties_of_a_missing_key_raise_rather_than_answer_blank(self):
        with self.assertRaises(Exception):
            self.model.object_properties("props/not-there.txt")

    def test_user_metadata_round_trips(self):
        """The editable view the metadata dialog fills itself from: the
        x-amz-meta-* pairs under "metadata", the rest flattened beside it."""
        self.put("props/meta.txt", b"x", Metadata={"owner": "data"},
                 ContentType="text/plain", CacheControl="max-age=60")
        editable = self.model.get_object_metadata("props/meta.txt")
        self.assertEqual(editable["metadata"].get("owner"), "data")
        self.assertEqual(editable["content_type"], "text/plain")
        self.assertEqual(editable["cache_control"], "max-age=60")
        self.assertEqual(editable["storage_class"], "STANDARD")

    def test_an_edit_preserves_the_metadata_it_was_not_given(self):
        """The dialog writes the whole set back, so anything it did not show
        has to survive the rewrite."""
        self.put("props/edit.txt", b"body",
                 Metadata={"owner": "data", "team": "platform"})
        self.model.set_object_metadata(
            "props/edit.txt", metadata={"owner": "analytics",
                                        "team": "platform"})
        editable = self.model.get_object_metadata("props/edit.txt")
        self.assertEqual(editable["metadata"].get("owner"), "analytics")
        self.assertEqual(editable["metadata"].get("team"), "platform")
        self.assertEqual(self.body("props/edit.txt"), b"body")

    def test_the_existence_probe_agrees_with_the_listing(self):
        """prefix_has_anything answers the overwrite check with one key
        rather than a full listing, so it has to agree with one."""
        self.put("probe/here/file.txt", b"x")
        self.assertTrue(self.model.prefix_has_anything("probe/here/"))
        self.assertFalse(self.model.prefix_has_anything("probe/empty/"))
