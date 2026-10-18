"""Writing a bucket's settings, not just reading them.

test_capabilities.py asks every optional document whether it can be *read*
without raising. Bucket settings is a dialog that writes them, and writing is
where a backend's answer actually differs: this MinIO stores lifecycle rules,
a policy and tags, and refuses CORS, website hosting and default encryption
outright. The app offers all six in the same dialog.

So each one here either round-trips or is refused with a code the dialog can
show. What must not happen is the third thing: a write that reports success
and stores nothing, or one that comes back as a bare traceback.
"""

import json

import botocore.exceptions

from core.lifecycle import build_lifecycle_rule, summarize_lifecycle_rule
from tests.e2e.harness import ServerCase, make_model, purge_bucket, unique_bucket


def _write(call, *args, **kwargs):
    """Run a write; return (True, None) or (False, error-code)."""
    try:
        call(*args, **kwargs)
        return True, None
    except botocore.exceptions.ClientError as exc:
        return False, exc.response.get("Error", {}).get("Code", "?")
    except Exception as exc:                       # noqa: BLE001
        return False, exc.__class__.__name__


class LifecycleTests(ServerCase):
    """The rule the editor builds has to be one the server accepts.

    core/lifecycle.py assembles the document from the dialog's fields; that
    it is well-formed is a claim about the S3 API, and the unit suite can
    only check it against itself.
    """

    def test_a_rule_the_editor_builds_is_accepted_and_comes_back(self):
        rule = build_lifecycle_rule("expire-logs", prefix="logs/",
                                    expire_days=30)
        stored, code = _write(self.model.put_bucket_lifecycle, [rule])
        if not stored:
            self.skipTest(f"backend refuses lifecycle rules: {code}")
        back = self.model.get_bucket_lifecycle()
        self.assertEqual(len(back), 1, back)
        self.assertEqual(back[0]["ID"], "expire-logs")
        self.assertEqual(back[0].get("Expiration", {}).get("Days"), 30)

    def test_the_stored_rule_summarises_the_way_the_list_shows_it(self):
        """The summary is what the user reads before deleting things. It has
        to survive the round trip, not just the shape the editor produced."""
        rule = build_lifecycle_rule("tidy", prefix="tmp/", expire_days=7)
        stored, code = _write(self.model.put_bucket_lifecycle, [rule])
        if not stored:
            self.skipTest(f"backend refuses lifecycle rules: {code}")
        back = self.model.get_bucket_lifecycle()
        summary = summarize_lifecycle_rule(back[0])
        self.assertIn("tmp/", summary)
        self.assertIn("delete after 7d", summary)

    def test_a_rule_with_a_transition_round_trips(self):
        rule = build_lifecycle_rule("cool-off", prefix="cold/",
                                    transition_days=10,
                                    transition_class="STANDARD_IA")
        stored, code = _write(self.model.put_bucket_lifecycle, [rule])
        if not stored:
            self.skipTest(f"backend refuses this transition: {code}")
        back = self.model.get_bucket_lifecycle()
        transitions = back[0].get("Transitions") or []
        self.assertTrue(transitions, back)
        self.assertEqual(transitions[0]["Days"], 10)

    def test_rules_can_be_removed_again(self):
        rule = build_lifecycle_rule("temporary", prefix="x/", expire_days=1)
        stored, code = _write(self.model.put_bucket_lifecycle, [rule])
        if not stored:
            self.skipTest(f"backend refuses lifecycle rules: {code}")
        self.model.put_bucket_lifecycle([])
        self.assertEqual(self.model.get_bucket_lifecycle(), [])

    def test_a_rule_that_would_do_nothing_is_refused_before_it_is_sent(self):
        """S3 accepts an action-less rule and then never fires it, so the
        editor refuses it here instead -- which only matters if the server
        really would have taken it."""
        with self.assertRaises(ValueError):
            build_lifecycle_rule("empty", prefix="logs/")


class PolicyTests(ServerCase):

    def test_a_policy_round_trips_and_is_reported_as_set(self):
        document = json.dumps({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"AWS": ["*"]},
                "Action": ["s3:GetObject"],
                "Resource": [f"arn:aws:s3:::{self.bucket}/public/*"],
            }],
        })
        stored, code = _write(self.model.put_bucket_policy, document)
        if not stored:
            self.skipTest(f"backend refuses bucket policies: {code}")
        back = self.model.get_bucket_policy()
        self.assertIn("s3:GetObject", back)

    def test_the_public_access_summary_stays_answerable(self):
        """It feeds a warning line in the dialog, and a bucket with a policy
        is exactly the case where that line matters."""
        summary = self.model.public_access_summary()
        self.assertIsInstance(summary, dict)
        self.assertIn("reasons", summary)

    def test_the_policy_status_is_a_string_either_way(self):
        status = self.model.get_bucket_policy_status()
        self.assertIsInstance(status, str)


class BucketTagTests(ServerCase):

    def test_tags_round_trip(self):
        stored, code = _write(self.model.put_bucket_tags, {"team": "data"})
        if not stored:
            self.skipTest(f"backend refuses bucket tags: {code}")
        self.assertEqual(self.model.get_bucket_tags(), {"team": "data"})

    def test_tags_can_be_replaced_wholesale(self):
        stored, code = _write(self.model.put_bucket_tags, {"a": "1", "b": "2"})
        if not stored:
            self.skipTest(f"backend refuses bucket tags: {code}")
        self.model.put_bucket_tags({"c": "3"})
        self.assertEqual(self.model.get_bucket_tags(), {"c": "3"})

    def test_tags_can_be_cleared(self):
        stored, code = _write(self.model.put_bucket_tags, {"a": "1"})
        if not stored:
            self.skipTest(f"backend refuses bucket tags: {code}")
        self.model.put_bucket_tags({})
        self.assertEqual(self.model.get_bucket_tags(), {})


class RefusedDocumentTests(ServerCase):
    """The three this MinIO does not implement.

    Whatever the backend answers, the dialog has to be able to say why. A
    refusal with no code is the failure mode: it reaches the user as an empty
    message box.
    """

    def test_cors_either_round_trips_or_refuses_with_a_code(self):
        rules = [{"AllowedMethods": ["GET"], "AllowedOrigins": ["*"]}]
        stored, code = _write(self.model.put_bucket_cors, rules)
        if stored:
            self.assertTrue(self.model.get_bucket_cors())
        else:
            self.assertTrue(code and code != "?", f"unreportable: {code}")
            print(f"\n    CORS not supported here: {code}")

    def test_website_either_round_trips_or_refuses_with_a_code(self):
        """This MinIO answers MalformedXML rather than NotImplemented, which
        reads like the app sent a bad document. It did not: raw boto3 is
        refused the same way for every shape the API allows, including a
        bare RedirectAllRequestsTo. Recorded here so the next person does
        not go looking for the bug in put_bucket_website."""
        stored, code = _write(self.model.put_bucket_website,
                              index="index.html")
        if stored:
            self.assertTrue(self.model.get_bucket_website())
        else:
            self.assertTrue(code and code != "?", f"unreportable: {code}")
            print(f"\n    website hosting not supported here: {code}")

    def test_encryption_either_round_trips_or_refuses_with_a_code(self):
        stored, code = _write(self.model.put_bucket_encryption, sse="AES256")
        if stored:
            self.assertTrue(self.model.get_bucket_encryption())
        else:
            self.assertTrue(code and code != "?", f"unreportable: {code}")
            print(f"\n    default encryption not supported here: {code}")

    def test_a_refused_document_leaves_the_bucket_usable(self):
        """A half-applied setting that breaks later reads would be worse
        than the refusal."""
        _write(self.model.put_bucket_cors,
               [{"AllowedMethods": ["GET"], "AllowedOrigins": ["*"]}])
        _write(self.model.put_bucket_encryption, sse="AES256")
        self.put("after/still-works.txt", b"x")
        self.assertEqual(self.body("after/still-works.txt"), b"x")


class BucketLifecycleOfTheBucketItselfTests(ServerCase):
    """Creating, emptying and removing a bucket.

    The app offers all three from the bucket list, and every one of them is
    destructive enough that nobody exercises it by hand twice.
    """

    def test_a_created_bucket_appears_in_the_listing(self):
        name = unique_bucket("made")
        model = make_model()
        model.create_bucket(name)
        self.addCleanup(purge_bucket, model.client, name)
        self.assertIn(name, [item.name for item in model.list_buckets()])

    def test_emptying_a_bucket_leaves_it_there_but_empty(self):
        name = unique_bucket("empty")
        model = make_model()
        model.create_bucket(name)
        self.addCleanup(purge_bucket, model.client, name)
        for index in range(5):
            model.client.put_object(Bucket=name, Key=f"k{index}.txt", Body=b"z")
        model.empty_bucket(name)
        self.assertEqual(
            model.client.list_objects_v2(Bucket=name).get("KeyCount"), 0)
        self.assertIn(name, [item.name for item in model.list_buckets()])

    def test_a_recursive_delete_removes_a_bucket_that_still_has_objects(self):
        """delete_bucket alone is refused on a bucket with anything in it,
        which is the whole reason the recursive form exists."""
        name = unique_bucket("recurse")
        model = make_model()
        model.create_bucket(name)
        for index in range(3):
            model.client.put_object(Bucket=name, Key=f"d/k{index}", Body=b"z")
        with self.assertRaises(botocore.exceptions.ClientError):
            model.client.delete_bucket(Bucket=name)
        model.delete_bucket_recursive(name)
        self.assertNotIn(name, [item.name for item in model.list_buckets()])
