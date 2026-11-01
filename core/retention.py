"""Object Lock retention: which changes the service allows, and which ones
cannot be taken back.

GOVERNANCE retention can be shortened or removed, but only by a caller with
``s3:BypassGovernanceRetention`` who sends the bypass header. COMPLIANCE
retention cannot be shortened or removed by anyone, the account root
included, until it expires -- and moving GOVERNANCE to COMPLIANCE is how an
object becomes undeletable for years by mistake. The editor asks this module
before it sends anything.
"""

from datetime import datetime, timezone

GOVERNANCE = "GOVERNANCE"
COMPLIANCE = "COMPLIANCE"
MODES = (GOVERNANCE, COMPLIANCE)


def _aware(value):
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def parse_retain_until(text):
    """The stored ``retain_until`` string back as an aware datetime, or None."""
    text = str(text or "").strip()
    if not text:
        return None
    try:
        return _aware(datetime.fromisoformat(text.replace("Z", "+00:00")))
    except ValueError:
        return None


def assess_retention_change(current_mode, current_until, new_mode, new_until,
                            now=None):
    """
    ``(problem, needs_bypass, irreversible)`` for a proposed retention.

    ``problem`` is a reason the service would refuse it (or "" if it would
    not); ``needs_bypass`` says the change only goes through with the
    governance bypass; ``irreversible`` says the result is COMPLIANCE, which
    nothing can undo before the date.
    """
    now = _aware(now) or datetime.now(timezone.utc)
    current_until = _aware(current_until)
    if current_until is not None:
        # The editor works in whole seconds, so an unchanged date read back
        # from "…07.500Z" would otherwise count as shortened.
        current_until = current_until.replace(microsecond=0)
    new_until = _aware(new_until)
    current_mode = str(current_mode or "")
    if new_mode not in MODES:
        return f"Choose a retention mode ({' or '.join(MODES)}).", False, False
    if new_until is None or new_until <= now:
        return "The retain-until date must be in the future.", False, False

    active = current_until is not None and current_until > now
    needs_bypass = False
    if active and current_mode == COMPLIANCE:
        if new_mode != COMPLIANCE:
            return ("COMPLIANCE retention cannot be changed to GOVERNANCE "
                    "before it expires.", False, True)
        if new_until < current_until:
            return ("COMPLIANCE retention cannot be shortened, by anyone, "
                    "before it expires.", False, True)
    elif active and current_mode == GOVERNANCE:
        if new_mode == GOVERNANCE and new_until < current_until:
            needs_bypass = True
    return "", needs_bypass, new_mode == COMPLIANCE
