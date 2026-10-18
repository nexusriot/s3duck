"""The two questions the app answers before it moves any bytes: can this
profile reach the bucket, and can this SDK send what the code wants to send.

Both are end-to-end questions for different reasons. "Can this profile reach
the bucket" is settled by a real round trip -- check_profile writes a probe
key and removes it again, and nothing offline can tell whether the server
accepted it. "Can this SDK send this parameter" is settled by the SDK rather
than the server, which is why the model suite runs twice: once on the boto3 in
requirements.txt and once on the oldest supported. A probe that answers the
same thing on both is a probe that is not reading anything.
"""

import os
import tempfile

from model import ObjectExists, client_accepts, installed_sdk_accepts
from tests.e2e.harness import ServerCase, make_model, unique_bucket


class ProfileCheckTests(ServerCase):
    """check_profile is the "Check" button in the profile list.

    It had no end-to-end coverage at all, which is how it came to report
    every read-only profile as broken: the probe it writes goes through
    _guard_write, so the check contradicted the setting beside it.
    """

    def test_a_writable_profile_passes(self):
        ok, reason = self.model.check_profile()
        self.assertTrue(ok, reason)
        self.assertIsNone(reason)

    def test_the_probe_key_does_not_outlive_the_check(self):
        """It writes into the user's own bucket, so it has to clean up."""
        before = set(self.keys())
        ok, reason = self.model.check_profile()
        self.assertTrue(ok, reason)
        self.assertEqual(set(self.keys()), before,
                         "the check left its probe key behind")

    def test_a_read_only_profile_passes_without_writing(self):
        """REGRESSION: the write probe is refused on a read-only profile, so
        the check reported "This profile is read-only" for every correctly
        configured one. It now proves read access instead."""
        reader = make_model(bucket=self.bucket, read_only=True)
        before = set(self.keys())
        ok, reason = reader.check_profile()
        self.assertTrue(ok, reason)
        self.assertIsNone(reason)
        self.assertEqual(set(self.keys()), before,
                         "the read-only check wrote to the bucket")

    def test_a_profile_with_no_bucket_is_checked_by_listing_buckets(self):
        ok, reason = make_model(bucket="").check_profile()
        self.assertTrue(ok, reason)

    def test_a_bucket_that_is_not_there_fails_with_a_reason(self):
        missing = make_model(bucket=unique_bucket("absent"))
        ok, reason = missing.check_profile()
        self.assertFalse(ok)
        self.assertTrue(reason, "a failed check must say why")

    def test_a_read_only_profile_on_a_missing_bucket_also_fails(self):
        """The read path needs its own failure case: answering True for every
        bucket would make the read-only check worthless."""
        missing = make_model(bucket=unique_bucket("absent"), read_only=True)
        ok, reason = missing.check_profile()
        self.assertFalse(ok)
        self.assertTrue(reason, "a failed check must say why")

    def test_bad_credentials_fail_with_a_reason(self):
        wrong = make_model(bucket=self.bucket, secret_key="not-the-password")
        ok, reason = wrong.check_profile()
        self.assertFalse(ok)
        self.assertTrue(reason, "a failed check must say why")


class SdkCapabilityTests(ServerCase):
    """What the installed boto3 will actually put on the wire.

    boto3 validates arguments client-side against the service model it ships
    with, so a parameter S3 gained after that build is refused before any
    request goes out. No backend can stand in for that, which is why this
    suite runs on two SDKs -- and why a probe has to be checked against the
    SDK rather than against a server.
    """

    def test_the_probe_reads_the_service_model(self):
        """REGRESSION: installed_sdk_accepts built its throwaway client with
        types.SimpleNamespace and s3/common.py never imported types. Every
        call raised NameError into the function's own "anything that cannot
        be asked answers True" handler, so it answered Yes on every build --
        including the old-SDK runner this service exists to cover.

        A parameter that cannot exist is the proof: it has to come back
        False on BOTH runners, and before the fix it came back True on both.
        """
        self.assertFalse(
            installed_sdk_accepts("PutObject", "NoSuchParameterAtAll"))
        self.assertTrue(installed_sdk_accepts("PutObject", "Bucket"))

    def test_an_unaskable_operation_still_answers_yes(self):
        """The documented fallback: a model this build cannot describe must
        not disable a feature the service does have."""
        self.assertTrue(installed_sdk_accepts("NoSuchOperation", "Whatever"))

    def test_the_probe_agrees_with_the_live_client(self):
        """The offline probe and the bound client answer the same question;
        Diagnostics asks the first and every upload asks the second."""
        for operation, parameter in (
                ("PutObject", "IfNoneMatch"),
                ("PutObject", "IfMatch"),
                ("CreateMultipartUpload", "ChecksumType")):
            with self.subTest(parameter=f"{operation}.{parameter}"):
                self.assertEqual(
                    installed_sdk_accepts(operation, parameter),
                    client_accepts(self.model.client, operation, parameter),
                    "Diagnostics and the real client disagree")

    def test_conditional_create_support_matches_both_halves(self):
        """supports_conditional_create is an AND over the two routes a
        conditional upload may take, so it cannot be looser than either."""
        put = client_accepts(self.model.client, "PutObject", "IfNoneMatch")
        complete = client_accepts(
            self.model.client, "CompleteMultipartUpload", "IfNoneMatch")
        self.assertEqual(self.model.supports_conditional_create(),
                         put and complete)
        print("\n    conditional-create support on this SDK: "
              f"{self.model.supports_conditional_create()}")

    def test_the_promise_matches_what_an_upload_actually_does(self):
        """Ties the probe to behaviour: when it says the parameter cannot be
        sent, a conditional upload must still land rather than fail -- that
        degradation is the whole reason the probe exists."""
        self.model.set_multipart_sizes(threshold_mb=16)
        self.put("sdk/taken.txt", b"original")
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "payload.bin")
            with open(path, "wb") as handle:
                handle.write(b"replacement")
            try:
                self.model.upload_file(path, "sdk/taken.txt",
                                       if_none_match=True)
            except ObjectExists:
                # Only a build that can send the precondition may refuse.
                self.assertTrue(self.model.supports_conditional_create(),
                                "refused a key on an SDK that cannot send "
                                "IfNoneMatch at all")
                self.assertEqual(self.body("sdk/taken.txt"), b"original")
                return
        # It went through. That is correct when either the SDK or the backend
        # cannot carry the precondition; the object must then be replaced
        # rather than half-written.
        self.assertEqual(self.body("sdk/taken.txt"), b"replacement")
