"""The command palette and the shortcut sheet.

The action objects are passed in and only ever duck-typed -- ``text()``,
``shortcuts()``, ``isEnabled()`` -- so the ranking and the row building stay
testable without a QApplication.
"""

import re


# Keys with no QAction behind them — handled either in the list view's event
# filter or by a bare QShortcut — so scanning actions cannot find them.
LISTVIEW_KEY_HELP = (
    ("Ctrl+K", "Run any command by name"),
    ("Ctrl+P", "Go to a bucket or bookmark by name"),
    ("Ctrl+F", "Quick filter the current listing"),
    ("Ctrl+Shift+F", "Recursive search under this prefix"),
    ("Ctrl+E", "Sync with a local folder"),
    ("Shift+F2", "Rename multiple items"),
    ("Ctrl+/", "Show this shortcut list"),
    ("Ctrl+C", "Copy selection to the clipboard"),
    ("Ctrl+X", "Cut selection to the clipboard"),
    ("Ctrl+V", "Paste clipboard into this folder"),
    ("Ctrl+B", "Bookmark the current location"),
    ("Ctrl+Shift+D", "Find duplicate objects"),
    ("Ctrl+T", "Open another tab on this location"),
    ("Ctrl+W", "Close the current tab"),
    ("Ctrl+Tab", "Next tab (Ctrl+Shift+Tab for the previous one)"),
    ("F3", "Show or hide the second pane"),
    ("F5", "Dual pane: copy to the other pane — otherwise Refresh"),
    ("F6", "Dual pane: move to the other pane"),
    ("Enter", "Open the selected bucket / folder / file"),
    ("Backspace", "Go up one level"),
    ("Del", "Delete selection"),
    ("Insert", "Create bucket or folder"),
    ("F2", "Rename selected item"),
    ("Home", "Back to the bucket list"),
    ("Esc", "Clear the quick filter, else cancel transfers"),
    ("Any letter", "Start the quick filter (type-to-search)"),
)
def clean_action_label(text) -> str:
    """
    An action's text as a human-facing command name.

    Drops the mnemonic ampersand and the "(Ctrl+C)" hint several labels bake
    in — both are noise once the keys have a column of their own. The ellipsis
    is stripped first, or it holds the hint off the end of the string where
    the pattern anchors.
    """
    label = str(text or "").replace("&", "").strip().rstrip(" …")
    return re.sub(r"\s*\([^)]*\)\s*$", "", label).strip(" …")
def collect_shortcuts(actions, extra=()) -> list:
    """
    Build the (keys, description) rows for the shortcut help.

    Derived from the real QActions so the list cannot drift from the app, with
    the event-filter keys supplied separately.
    """
    rows = []
    seen = set()
    for action in actions or []:
        try:
            # shortcuts() (plural) — an action can carry several, and
            # shortcut() would only ever report the first (F5 but not Ctrl+R).
            bindings = [k.toString() for k in action.shortcuts()]
            label = action.text()
        except Exception:
            continue
        keys = ", ".join(k for k in bindings if k)
        if not keys or not label:
            continue
        label = clean_action_label(label)
        if (keys, label) in seen:
            continue
        seen.add((keys, label))
        rows.append((keys, label))
    rows.sort(key=lambda row: row[1].lower())
    return rows + list(extra)
def command_entries(actions) -> list:
    """
    ``(label, keys, action)`` for every action the palette can run.

    Built from the live QActions, like the shortcut sheet, so the palette
    cannot drift from the app. Disabled and hidden actions are dropped because
    running one would do nothing, and separators carry no label at all.
    """
    out, seen = [], set()
    for action in actions or []:
        try:
            # isEnabled() is False for a hidden action too, so this covers
            # both. Separators carry no label and cannot be run.
            if action.isSeparator() or not action.isEnabled():
                continue
            raw = action.text() or ""
            keys = ", ".join(
                k.toString() for k in action.shortcuts() if k.toString())
        except Exception:
            continue
        label = clean_action_label(raw)
        if not label or label.lower() in seen:
            continue
        seen.add(label.lower())
        out.append((label, keys, action))
    out.sort(key=lambda row: row[0].lower())
    return out
def palette_score(label, query):
    """
    Rank one command against the typed query; None when it does not match.

    Two tiers so the obvious answer wins: a contiguous substring beats a
    scattered subsequence — otherwise typing "co" would rank "Show incomplete
    uploads" alongside "Copy". Within a tier the earlier match wins, which is
    what puts a prefix match ("Copy") ahead of a later one ("Recopy").
    """
    text = str(label or "").lower()
    needle = str(query or "").strip().lower()
    if not needle:
        return (0, 0, text)
    position = text.find(needle)
    if position >= 0:
        return (0, position, text)
    # An iterator, not the string: the characters must appear IN ORDER, or
    # "yc" would match "Copy".
    remaining = iter(text)
    if all(char in remaining for char in needle):
        return (1, len(text), text)
    return None
def filter_commands(entries, query) -> list:
    """The palette's visible list: matches only, best first."""
    scored = []
    for entry in entries or []:
        score = palette_score(entry[0], query)
        if score is not None:
            scored.append((score, entry))
    scored.sort(key=lambda pair: pair[0])
    return [entry for _score, entry in scored]
