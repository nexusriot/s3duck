"""Turning a user gesture into a list of transfer rows.

A drag-and-drop or a paste becomes ``(key, local_path)`` or
``(src, dst, is_folder, bucket)`` tuples here, before any worker sees it.
"""

import os
import pathlib

from core.naming import _dest_inside_source


def _build_upload_job_for_path(path: str, dest_prefix: str) -> list:
    """
    Build upload job entries [(key, local_path_or_None), ...] for one local
    file or directory tree, rooted under dest_prefix. Directories contribute
    a placeholder entry (local None) per directory level plus one entry per
    file. os.walk stays inside 'path'; a glob on path + "**/**" also matched
    sibling dirs sharing the name prefix.
    """
    if not path:
        return []
    path = os.path.normpath(path)
    base_path, _tail = os.path.split(path)
    dest_prefix = dest_prefix or ""
    job = []
    if os.path.isdir(path):
        for dirpath, _dirnames, filenames in os.walk(path):
            dir_key = pathlib.Path(
                os.path.join(dest_prefix, os.path.relpath(dirpath, base_path))
            ).as_posix()
            job.append((dir_key, None))
            for filename in filenames:
                full = os.path.join(dirpath, filename)
                key = pathlib.Path(
                    os.path.join(dest_prefix, os.path.relpath(full, base_path))
                ).as_posix()
                job.append((key, full))
    else:
        key = pathlib.Path(
            os.path.join(dest_prefix, os.path.relpath(path, base_path))
        ).as_posix()
        job.append((key, path))
    return job
def build_paste_job(clip, dst_bucket, dst_prefix):
    """
    Turn a clipboard payload into copy/move job entries.

    clip is {"mode", "bucket", "items": [(name, key, is_folder)], "classes":
    {key: storage_class}}. Returns ``(job, skipped)`` where job rows are
    ``(src_key, dst_key, is_folder, dst_bucket_or_None, storage_class)`` —
    dst_bucket is None when the paste stays inside the source bucket,
    matching what the copy/move workers expect.

    "classes" is what the listing the selection was made in already knew, so
    the paste keeps each object in its own tier without a HEAD per key. It
    is optional: a payload built somewhere that has no listing behind it
    (the other pane) leaves the class empty, and the copy asks the source.
    """
    job = []
    skipped = []
    if not clip:
        return job, skipped
    src_bucket = clip.get("bucket") or ""
    cross_bucket = bool(dst_bucket) and dst_bucket != src_bucket
    prefix = dst_prefix or ""
    classes = clip.get("classes") or {}

    for name, src_key, is_folder in clip.get("items") or []:
        dst_key = prefix + name + ("/" if is_folder else "")
        if not cross_bucket and _dest_inside_source(src_key, dst_key, is_folder):
            skipped.append(name)
            continue
        job.append((src_key, dst_key, is_folder,
                    dst_bucket if cross_bucket else None,
                    classes.get(src_key, "")))
    return job, skipped
