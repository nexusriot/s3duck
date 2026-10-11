"""Planning a bulk rename.

The plan is computed whole and shown before anything is renamed, so a
collision or a no-op is visible rather than discovered halfway through.
"""

import os
import re


BULK_RENAME_FIND = "find"
BULK_RENAME_TEMPLATE = "template"
def bulk_rename_plan(items, *, mode=BULK_RENAME_FIND, find="", replace="",
                     regex=False, case_sensitive=True,
                     template="{name}{ext}", start=1, padding=1):
    """
    Work out new names for a multi-selection.

    ``items`` is [(name, is_folder), ...]. Returns ``(plan, problems)`` where
    plan is [(old_name, new_name), ...] for entries that actually change and
    problems is a list of human-readable reasons a rename was rejected.

    Template placeholders: {name} (stem, or whole name for a folder), {ext}
    (".txt" or ""), {n} (counter, zero-padded to *padding*), {orig}.
    """
    plan = []
    problems = []
    counter = int(start)

    for name, is_folder in items:
        if is_folder:
            stem, ext = name, ""
        else:
            stem, ext = os.path.splitext(name)

        if mode == BULK_RENAME_FIND:
            if not find:
                continue
            if regex:
                try:
                    flags = 0 if case_sensitive else re.IGNORECASE
                    new = re.sub(find, replace, name, flags=flags)
                except re.error as exc:
                    return [], [f"Invalid regular expression: {exc}"]
            elif case_sensitive:
                new = name.replace(find, replace)
            else:
                # A lambda replacement keeps backslashes in *replace* literal.
                new = re.sub(re.escape(find), lambda _m: replace, name,
                             flags=re.IGNORECASE)
        else:
            try:
                new = template.format(
                    name=stem, ext=ext, orig=name,
                    n=str(counter).zfill(max(1, int(padding))),
                )
            except (KeyError, IndexError) as exc:
                return [], [f"Unknown placeholder in template: {exc}"]
            except ValueError as exc:
                # A half-typed template is the normal state of the box while
                # someone is typing in it: "{" alone, "{name!z}" and
                # "{name:d}" are all ValueError, and the preview recomputes
                # on every keystroke. PyQt6 turns an exception escaping that
                # slot into a process abort, so the dialog has to be told
                # what is wrong rather than the app dying mid-word.
                return [], [f"Invalid template: {exc}"]
            counter += 1

        new = (new or "").strip()
        if new == name:
            continue
        if not new:
            problems.append(f"{name}: new name would be empty")
            continue
        if "/" in new:
            problems.append(f"{name}: new name cannot contain '/'")
            continue
        plan.append((name, new))

    targets = {}
    for old, new in plan:
        targets.setdefault(new, []).append(old)
    for new, olds in sorted(targets.items()):
        if len(olds) > 1:
            problems.append(
                f"'{new}' would be produced by {len(olds)} items: "
                + ", ".join(sorted(olds))
            )
    return plan, problems
