"""Object Lock and legal holds, asked of a real server.

Nothing stubbed can settle these. A legal hold's whole promise is that the
*server* refuses the delete -- "while it is on, the object cannot be deleted
or overwritten, including by you", in the app's own words -- so a fake client
that accepts whatever it is handed proves precisely nothing.

The shape is also not the obvious one. Object Lock implies versioning, so an
ordinary delete of a held object succeeds: it writes a delete marker, and the
held version survives underneath it. The protection shows up only when the
version itself is deleted. Anyone reading `delete() -> True` and concluding
the hold did not work would be wrong, which is why it is written down here.

The harness has carried an OBJECT_LOCK flag since it was written and nothing
had used it.
"""

import botocore.exceptions

from tests.e2e.harness import ServerCase


class LegalHoldTests(ServerCase):
    """A bucket created with Object Lock enabled."""

    OBJECT_LOCK = True

    def test_the_bucket_reports_object_lock_as_enabled(self):
        config = self.model.get_object_lock_configuration()
        self.assertTrue(config.get("enabled"),
                        f"Object Lock not reported enabled: {config}")

    def test_an_object_starts_with_no_hold_on_it(self):
        self.put("lock/clean.txt", b"x")
        state = self.model.get_object_lock_state("lock/clean.txt")
        self.assertTrue(state["supported"])
        self.assertEqual(state["legal_hold"], "")

    def test_a_hold_can_be_placed_and_is_reported_back(self):
        self.put("lock/held.txt", b"x")
        self.model.set_object_legal_hold("lock/held.txt", True)
        state = self.model.get_object_lock_state("lock/held.txt")
        self.assertEqual(state["legal_hold"], "ON")

    def test_a_hold_can_be_released(self):
        self.put("lock/released.txt", b"x")
        self.model.set_object_legal_hold("lock/released.txt", True)
        self.model.set_object_legal_hold("lock/released.txt", False)
        state = self.model.get_object_lock_state("lock/released.txt")
        self.assertEqual(state["legal_hold"], "OFF")

    def test_the_held_version_survives_an_ordinary_delete(self):
        """The surprising half, and the reason this is written down.

        Object Lock implies versioning, so the delete succeeds and writes a
        marker. The hold is not being ignored -- the version it protects is
        still there and still readable.
        """
        self.put("lock/survivor.txt", b"held bytes")
        self.model.set_object_legal_hold("lock/survivor.txt", True)
        held = [v for v in self.model.list_object_versions("lock/survivor.txt")
                if not v["is_delete_marker"]][0]["version_id"]

        self.assertTrue(self.model.delete("lock/survivor.txt"))

        versions = self.model.list_object_versions("lock/survivor.txt")
        self.assertTrue([v for v in versions if v["is_delete_marker"]],
                        "no delete marker was written")
        body = self.model.client.get_object(
            Bucket=self.bucket, Key="lock/survivor.txt",
            VersionId=held)["Body"].read()
        self.assertEqual(body, b"held bytes")

    def test_deleting_the_held_version_itself_is_refused(self):
        """This is where the protection actually bites."""
        self.put("lock/worm.txt", b"x")
        self.model.set_object_legal_hold("lock/worm.txt", True)
        held = [v for v in self.model.list_object_versions("lock/worm.txt")
                if not v["is_delete_marker"]][0]["version_id"]
        with self.assertRaises(botocore.exceptions.ClientError) as caught:
            self.model.delete_object_version("lock/worm.txt", held)
        code = caught.exception.response.get("Error", {}).get("Code", "")
        self.assertTrue(code, "the refusal carried no error code")
        # Still there afterwards: a refused delete must not be a partial one.
        body = self.model.client.get_object(
            Bucket=self.bucket, Key="lock/worm.txt",
            VersionId=held)["Body"].read()
        self.assertEqual(body, b"x")

    def test_a_hold_cannot_be_released_through_a_delete_marker(self):
        """After an ordinary delete the current version IS the marker, and a
        legal hold does not apply to one -- so the app's own release path
        fails here, and must fail with a reportable error rather than
        silently appearing to work."""
        self.put("lock/shadowed.txt", b"x")
        self.model.set_object_legal_hold("lock/shadowed.txt", True)
        self.model.delete("lock/shadowed.txt")
        with self.assertRaises(botocore.exceptions.ClientError) as caught:
            self.model.set_object_legal_hold("lock/shadowed.txt", False)
        self.assertTrue(
            caught.exception.response.get("Error", {}).get("Code", ""))


class NoObjectLockTests(ServerCase):
    """An ordinary bucket, where the feature is simply not available."""

    def test_the_state_reports_itself_unsupported_rather_than_raising(self):
        """The app asks before it offers, and the answer has to be a value.

        Raising here would take down the properties pane on every bucket
        that has no Object Lock, which is nearly all of them.
        """
        self.put("nolock/plain.txt", b"x")
        state = self.model.get_object_lock_state("nolock/plain.txt")
        self.assertFalse(state["supported"])
        self.assertEqual(state["legal_hold"], "")

    def test_the_configuration_reports_absence_rather_than_raising(self):
        config = self.model.get_object_lock_configuration()
        self.assertFalse(config.get("enabled"))

    def test_placing_a_hold_fails_with_something_reportable(self):
        """The menu entry is offered on any file; the refusal is what the
        user sees, so it has to carry an error code to show them."""
        self.put("nolock/try.txt", b"x")
        try:
            self.model.set_object_legal_hold("nolock/try.txt", True)
        except botocore.exceptions.ClientError as exc:
            self.assertTrue(exc.response.get("Error", {}).get("Code", ""))
        else:
            # A backend that allows it without Object Lock is unusual but
            # not wrong; then the state has to agree.
            state = self.model.get_object_lock_state("nolock/try.txt")
            self.assertEqual(state["legal_hold"], "ON")
