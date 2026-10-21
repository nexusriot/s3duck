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
    kinds = []      # is_folder, parallel to plan
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
            except (ValueError, AttributeError, TypeError) as exc:
                # A half-typed template is the normal state of the box while
                # someone is typing in it: "{" alone, "{name!z}" and
                # "{name:d}" are all ValueError, and the preview recomputes
                # on every keystroke. PyQt6 turns an exception escaping that
                # slot into a process abort, so the dialog has to be told
                # what is wrong rather than the app dying mid-word. Field
                # access is format syntax too: "{name.ext}" (an easy slip
                # for "{name}{ext}") is AttributeError and "{name[x]}" is
                # TypeError, and both aborted the app the same way.
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
        kinds.append(bool(is_folder))

    targets = {}
    for old, new in plan:
        targets.setdefault(new, []).append(old)
    for new, olds in sorted(targets.items()):
        if len(olds) > 1:
            problems.append(
                f"'{new}' would be produced by {len(olds)} items: "
                + ", ".join(sorted(olds))
            )
    # A target that is the current name of another item being renamed is a
    # chain (img1 -> img2 -> img3, the shape of every "shift the numbering")
    # or a swap. A rename runs as copy-everything-then-delete-the-sources, so
    # the copy onto img2 clobbers it and the later delete of the source img2
    # removes the copy that landed there: three files in, one file out. The
    # overwrite prompt cannot help — "img2 already exists" reads as expected
    # when img2 is being renamed away too. A folder and a file of one name
    # are different keys ("x/" vs "x"), so the kind has to match as well.
    renamed = {(old, kind) for (old, _new), kind in zip(plan, kinds)}
    for (old, new), kind in zip(plan, kinds):
        if (new, kind) in renamed:
            problems.append(
                f"{old}: '{new}' is itself being renamed in this batch — "
                "rename in two steps (e.g. to a temporary name first)")
    return plan, problems
