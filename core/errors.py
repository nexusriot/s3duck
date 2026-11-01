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
class ArchivedObject(Exception):
    """
    A copy was refused because the source is archived and not restored.

    GLACIER and DEEP_ARCHIVE objects cannot be read at all until a restore
    has completed, and a server-side copy reads the source. The service says
    InvalidObjectState, which means nothing to anyone who has not met it
    before; this carries the object, its class and what to do instead.
    """
class DownloadIncomplete(Exception):
    """
    A folder download ended with objects that could not be written locally.

    Raised at the END, once everything that could be written was. One key
    the local filesystem cannot hold -- "logs" beside "logs/2024.txt", a
    ':' on Windows -- used to be the first error of the fan-out, which
    re-raised it and abandoned every file not yet started: one bad name cost
    the whole folder. Raising at all, rather than only logging, is what keeps
    the queue row from saying "done" over a folder that is not all there.

    *failures* is ``[(key, why)]``; *total* is how many objects the download
    covered, so the message can say how much DID arrive.
    """

    # More than this many lines makes the dialog taller than the screen;
    # the log has every one of them.
    SHOWN = 10

    def __init__(self, failures, total=0):
        self.failures = list(failures)
        self.total = int(total or 0)
        super().__init__(self._summary())

    def _summary(self) -> str:
        count = len(self.failures)
        of = f" of {self.total}" if self.total >= count else ""
        lines = [f"{count}{of} object(s) could not be written locally; "
                 "everything else was downloaded:"]
        lines += [f"  {key}: {why}" for key, why in self.failures[:self.SHOWN]]
        if count > self.SHOWN:
            lines.append(f"  …and {count - self.SHOWN} more (see the log)")
        return "\n".join(lines)
