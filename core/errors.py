"""The failures the S3 layer raises by name.

They carry no boto3 and no Qt, so the planners and the workers can both
catch them without importing a client or a widget.
"""


class TransferCancelled(Exception):
    pass
class ReadOnlyError(Exception):
    """Raised when a write is attempted on a profile marked read-only."""
class ObjectExists(Exception):
    """
    A conditional write was refused because the key was already taken.

    Raised instead of overwriting, so a job the user asked to skip existing
    objects for keeps that promise even when the object appeared after the
    destination was checked.
    """
class DeleteRefused(Exception):
    """Raised when DeleteObjects answered 200 but refused individual keys."""
