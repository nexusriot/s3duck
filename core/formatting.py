"""Rendering numbers and bytes for people to read."""

from core.entries import FSObjectType
from core.naming import _human_bytes


def format_completion_notification(stats) -> tuple:
    """Build the (title, body) shown when the transfer queue drains."""
    done = int(stats.get("done", 0))
    errors = int(stats.get("error", 0))
    cancelled = int(stats.get("cancelled", 0))
    if errors:
        title = "S3 Duck — transfers failed"
    elif cancelled and not done:
        title = "S3 Duck — transfers cancelled"
    else:
        title = "S3 Duck — transfers finished"
    parts = []
    if done:
        parts.append(f"{done} completed")
    if errors:
        parts.append(f"{errors} failed")
    if cancelled:
        parts.append(f"{cancelled} cancelled")
    return title, ", ".join(parts) or "nothing to do"
def hex_dump(data, width: int = 16, max_bytes: int = 64 * 1024) -> str:
    """Classic offset / hex / ASCII rendering for binary previews."""
    data = data or b""
    truncated = len(data) > max_bytes
    view = data[:max_bytes]
    lines = []
    for offset in range(0, len(view), width):
        block = view[offset:offset + width]
        hex_part = " ".join(f"{b:02x}" for b in block)
        hex_part = hex_part.ljust(width * 3 - 1)
        text = "".join(chr(b) if 32 <= b < 127 else "." for b in block)
        lines.append(f"{offset:08x}  {hex_part}  |{text}|")
    if truncated:
        lines.append(f"… {len(data) - max_bytes} more byte(s) not shown")
    return "\n".join(lines)
def _scaled_bar_values(done, total, scale=1000):
    """Map raw byte counts onto a small fixed integer range for a QProgressBar.

    QProgressBar stores its range/value as C++ 32-bit ints, so feeding it raw
    byte counts overflows past ~2.1 GB (a single large file or a multi-file
    batch total). Scaling to a fixed range sidesteps that entirely.

    Returns ``(range_max, value)``. A ``range_max`` of 0 means the total size
    is unknown and the caller should render an indeterminate/busy bar.
    """
    if total <= 0:
        return 0, 0
    value = int(done / total * scale)
    return scale, min(scale, max(0, value))


def _listing_summary(items) -> str:
    """Status-bar summary of the current listing: counts + total file size."""
    folders = files = 0
    total = 0
    for i in items or []:
        if i.type_ == FSObjectType.FOLDER:
            folders += 1
        elif i.type_ == FSObjectType.FILE:
            files += 1
            total += int(i.size or 0)
    return f"{folders} dir(s), {files} file(s), {_human_bytes(total)}"
