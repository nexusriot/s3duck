# S3 Duck 🦆 — Roadmap

Prioritized backlog of planned work. Shipped features live in
[README.md](README.md); this file tracks what is *not* built yet, plus known
limitations worth fixing. Items move up or down based on review findings.

## Tier 1 — high value

The 0.27.0 round shipped everything that was listed here: the local pane as a
file manager, the second pane against another profile, pause/resume and a
queue that survives a restart, and audio/video preview.

- **Compare panes across profiles** — Compare panes reads both sides through
  this window's model, so with the second pane on another profile it refuses
  and points at Sync to another profile, whose dry run is the same answer.
  Reading the right side through the pane's own model would make it one
  surface.
- **Persist cross-profile jobs** — a restored queue cannot hold another
  profile's connection, so copy/move/sync to a profile is not written down
  and is lost on close. Storing the destination profile's *name* and
  re-reading its credentials on restore would close that gap.

## Tier 2 — bucket administration

Block Public Access, server access logging, a read-only replication view,
ACL editing, Object Lock retention editing, policy templates, presigned POST
forms and SSE-C landed in 0.27.0. What is left:

- **Lifecycle filters beyond a prefix** — the editor writes a prefix filter.
  A rule filtered by tag or object size is listed and summarized (the tags and
  the size bounds appear in its one-line description) but refuses to open in
  the editor, because rebuilding it from the prefix form would drop the filter
  and widen the rule to every object under the prefix — for an expiration
  rule, that deletes things it never used to touch.
- **Replication editing** — the rules are shown, not written: each names an
  IAM role and a destination ARN this app cannot validate.
- **Inventory configuration editing** — reports are read; creating a
  configuration needs a destination bucket policy granting S3 write access,
  which deserves its own reviewed template.
- **Object Lock retention on a multipart copy** — a copy over 5 GiB is
  rebuilt by CreateMultipartUpload, which does not inherit the source's
  retention the way CopyObject does; the carried fields stop at the content
  headers, user metadata, encryption and the storage class.

## Tier 3 — polish

- **Content-hash duplicate confirmation by download** — "Confirm candidates"
  settles a group from the objects' stored digests where the backend supplies
  them. When it cannot, the only remaining answer is downloading and hashing
  both copies, which is worth offering explicitly for small files.
- **Event notification editing** — the destinations are listed but not
  editable: an ARN cannot be validated from here, and a wrong one silently
  stops every event.
- **Upload rules by tag or size** — the rule table matches the destination
  key. Matching on the local file's size or its existing tags would need a
  second matcher and a clear precedence story.
- **CLI parity** — `s3duck-cli` transfers one file at a time, prints no
  progress bar and has no `rm`/`mv`; the window's parallel transfer settings
  and the trash convention would be natural next steps.
- **Inventory in ORC or Parquet** — only CSV reports are read; the other two
  formats need libraries this app does not ship.
- **Localization** — externalize user-facing strings.

## Known limitations (accepted for now)

- Multipart server-side copies require a readable source HEAD and an ETag or
  version ID. A version ID pins the original revision across overwrites; an
  ETag precondition refuses a changed source. A backend that ignores these
  preconditions cannot provide the same protection.
- Resumable uploads check local file identity, size, and nanosecond modification
  and change times during the transfer and before completion. These checks
  detect ordinary concurrent edits, including an edit that restores mtime,
  but do not lock the file or provide an atomic filesystem snapshot. Keep
  source files stable while transferring and between resume attempts; existing
  resume records match size, mtime, and chunk size rather than a content hash.

- A multipart upload this app drives itself names CRC32 explicitly on
  `CreateMultipartUpload`, because botocore puts that same checksum on every
  `UploadPart` whether it is asked to or not, and a service that checks a
  part against the upload it belongs to refuses all of them otherwise. The
  object therefore carries a per-part composite CRC32 unless a whole-object
  checksum was configured, which is what the SDK was already sending.
- A byte-range GET has response-checksum validation taken off it. AWS omits
  the checksum header on a partial response; a backend that returns the
  whole-object one anyway would otherwise have botocore compare a slice
  against the whole and fail every ranged read — which is every download
  past the resume threshold. A whole-object GET is still validated.

- A copy, move or rename keeps the object's storage class, but a **copy to
  another profile** does not. That route cannot use `CopyObject` at all —
  no server-side copy spans two sets of credentials — so the bytes are
  written by the destination's client under the destination's own upload
  policy. The two ends are routinely different providers, and a class one
  of them has never heard of is a refused write rather than a preserved
  tier.
- An archived source — GLACIER or DEEP_ARCHIVE — cannot be copied, moved
  or renamed until its restore has completed, because a server-side copy
  still has to read the object. Nothing here can work around that, so the
  class is never used to refuse a copy in advance: a restored object copies
  normally (and lands back in the class it is archived in), and only the
  service's own refusal is translated, into "restore it first" rather than
  `InvalidObjectState`.
- A multipart server-side copy gives the new object a multipart ETag, even
  when the source had a plain one. The bytes are identical; the ETag is a
  function of how the object was written, and a 5 GiB-plus object cannot be
  written in one piece.
- CRC32C and CRC64NVME need `awscrt`, which is also the module botocore
  computes them with. Without it they are not offered at all, because an
  algorithm this app could send but not recompute would make verification
  pass every download unchecked. An object that already carries only a
  CRC64NVME is reported as not comparable and settled on its ETag instead.

- A cross-profile copy or sync cannot be re-run from transfer history: the
  stored record is JSON and never held the other profile's connection. The
  history dialog says so rather than queueing a job that must fail.
- No sync is re-run from history at all: its plan — including what to
  delete — was worked out from both sides as they were then. A re-run is also
  refused for a record made under another profile, and for one written before
  history recorded the bucket, rather than guessing the bucket that is open.
- Cancelling a resumable upload leaves its parts on the server on purpose,
  since that is what the next attempt resumes from. Abandoned ones are cleaned
  up from Tools -> Incomplete uploads.

- The preview's syntax highlighter is deliberately language-agnostic; a `#`
  inside a string literal is coloured as a comment.
- Editing in the preview is offered only for text that arrived whole and
  decoded cleanly as UTF-8: saving a truncated body would delete the tail, and
  saving a replacement-charactered decode would corrupt the object. Nor for
  text the editor would not hand back byte-for-byte — CR or CRLF line endings,
  a byte-order mark, non-breaking spaces, U+2028/U+2029 — since saving would
  rewrite them throughout the file. A save carries the object's headers,
  metadata, storage class, encryption and tags, but not its ACL or Object
  Lock settings, which need calls of their own.
- Drag-out must download the selection before the drag can start (Qt drags
  are synchronous); a large selection means a visible preparation dialog.
- A whole-folder download inside a parallel batch multiplies workers
  (files-in-flight × chunk fan-out); concurrency is bounded but can exceed
  the configured file parallelism.
- Verifying a multipart object needs its real part boundaries, which come
  from `GetObjectAttributes`. AWS answers it; the end-to-end suite calls it
  against MinIO and prints how many parts came back, but passes either way.
  On a backend that does not answer it, the old behaviour stands —
  verification reports the object as "not comparable" and passes. Uploading with an additional CRC32 checksum (Transfer settings)
  makes an object verifiable everywhere, since a full-object checksum needs
  no part boundaries at all.
- Bundled PNG twins are a fixed 48px raster, so on a Qt build without the SVG
  plugin icons do not scale as crisply as the SVGs would. Installing
  `python3-pyqt6.qtsvg` restores vector icons; the .deb depends on it, so
  this only affects source and pip installs on such a Qt.
- Bucket settings offers every document AWS does, and a backend is free to
  implement only some of them. The MinIO the end-to-end suite runs against
  stores lifecycle rules, a bucket policy and bucket tags, and refuses CORS
  and default encryption with `NotImplemented` and website hosting with
  `MalformedXML` — the last of which reads like a bad request from this app,
  though raw boto3 is refused the same way for every shape the API allows.
  The dialog reports whatever the server said rather than hiding the controls,
  because which ones work is a property of the endpoint, not of the app, and
  `tests/e2e/test_bucket_admin.py` asserts the refusal carries a code there is
  something to show.
- The command palette lists the toolbar and window-level actions. Context-menu
  entries are built on demand when the menu opens, so they are not in it.
- Cancelling a background scan (destination check, drag-out measure, duplicate
  scan) abandons the worker thread; it finishes quietly in the background.
- The duplicate finder compares ETags, so identical content uploaded with
  different multipart part sizes is reported as an unconfirmed candidate.
  "Confirm candidates" resolves those from stored digests, but only a
  whole-object digest can prove two objects DIFFER — matching per-part
  digests prove a match, while differing ones prove nothing, because the same
  bytes split at other boundaries digest differently.
- Tabs are remembered locations for one shared listing view, not independent
  views: switching tabs navigates, so a slow bucket is still slow to come
  back to.
- `F5` is Refresh with one pane and copy-to-the-other-pane with two; both
  bindings live at once would make Qt call the key ambiguous and fire
  neither, so the second pane takes it over while it is open. `Ctrl+R`
  refreshes in either mode.
- MinIO refuses an object key containing a `..` or `.` path component
  (`XMinioInvalidResourceName`), while AWS S3 accepts one — they are legal
  keys. The download paths therefore keep their own containment check rather
  than trusting the server to be the strict one; the end-to-end suite records
  which kind of backend it is talking to instead of assuming.
- MinIO answers `ListMultipartUploads` as if `Prefix` were an exact key
  match, so the prefix is filtered client-side. The list is bounded by
  in-flight uploads rather than by objects, so this costs nothing in
  practice.
- The bundled storage prices are AWS list prices for us-east-1 and cover
  storage only. They are shown for an `*.amazonaws.com` endpoint and hidden
  elsewhere, because quoting them at a MinIO or Ceph endpoint would be
  inventing a number.
- `.ts` is deliberately absent from the built-in content-type table: an HLS
  segment and a TypeScript source share it, and guessing wrong breaks video
  playback. Add an override if your bucket only holds one of them.
- Comparing two panes walks both sides in full — one listing each — and holds
  the result in memory. It is a snapshot, not a live view.
- The size explorer folds everything past the 80 biggest entries at a level
  into one "… n more" tile: a thousand slivers is not a picture of anything.
- A manifest records size and ETag from the listing, so it detects objects
  that went missing, appeared, or were rewritten. It cannot detect bit rot in
  an object the service still reports unchanged, and a multipart ETag only
  matches when the same part size was used again.
- An upload rule matches the destination key and the first match wins;
  writing tags with one needs the same permission as tagging by hand.
- Upload rules and the Content-Type override table are app-wide, not per
  profile: both live in Transfer settings and apply to every profile, so a
  rule written for one account's bucket layout also fires on another's.
- A job is retried automatically at most once, and only when the service
  itself asked (throttling, 5xx, a dropped connection). The wait is held by a
  timer owned by the window, so closing it cancels a retry that has not fired
  yet rather than starting one on the way out. Anything else is a
  refusal, and retrying a refusal on a timer is just a slower refusal.
- Only a launch that names a location is handed to a running window; a bare
  `s3duck` still opens its own, because two profiles side by side is a thing
  people legitimately want. A launch that also names a profile (`-p NAME`) is
  never handed over either — the running window would open the location
  under its own profile — so it starts its own launcher. Handover needs Qt's
  network module — where that is missing, the second launch simply opens its
  own window too.
- A watch never resumes on its own at startup, even though its settings are
  remembered — a background uploader that restarts unasked is how a folder
  ends up mirrored somewhere its owner forgot about.
- The back/forward trail is session state: tabs are restored on the next
  launch, the route taken to them is not.
- One proxy URL covers both http and https; botocore still honours the
  standard `HTTP_PROXY` / `HTTPS_PROXY` / `NO_PROXY` environment variables,
  and a per-profile setting overrides them for that profile only.
- A CA bundle is ignored while "No SSL check" is on, and the profile dialog
  says so rather than pretending both apply.

- A pause is a cancellation that keeps its place: the rows a worker
  reported finished are dropped on resume, and a file in flight resumes from
  its own record only when it is big enough to have one (past the resumable
  upload threshold or the ranged-download threshold). A smaller file restarts
  from zero, and a move restarts whole, because its deletes only happen
  after every copy.
- The persisted queue stores a job of up to 10 000 rows; a larger one (a
  prefix walk expanded to every key) is not written down, and neither is a
  cross-profile job. Each is named in the log rather than dropped silently.
- The media preview streams from a presigned link, which a profile with "No
  SSL check" (the player verifies TLS), a requester-pays bucket and an SSE-C
  key cannot use; the dialog says so and "Open with default app" still works.
- MinIO answers GetObjectAcl and GetBucketAcl with an owner whose ID is
  empty — it implements ACLs only as the owner's FULL_CONTROL — so nothing
  could be written back; the ACL editor opens read-only there and says why.
- SSE-C needs TLS: S3 and MinIO refuse a customer key sent over plain HTTP.
  While a key is set, objects not encrypted with it cannot be read, and the
  key is never stored, so losing it loses those objects.
- Trash is a server-side move, so it costs a copy and a delete per object,
  and an object in an infrequent-access or Glacier class keeps that class
  (and its minimum-duration charge) in the trash.
- A watch (either direction) refuses a pass that would delete more than 5
  files or 10% of the destination and stops itself, and `s3duck-cli sync
  --delete` refuses the same run without `--force`; a legitimate large
  cleanup is then done once through Sync with local folder, where the plan
  is shown first.
- A move to another profile whose two connections point at the same bucket
  of the same account is checked for overlap like an in-bucket move and
  refused when the destination lies inside the source.
- A restored job queued under an SSE-C key comes back paused, and resumes
  only once the window holds a key with the same MD5; the key itself is
  never written down.
- The QR code is offered for download links only: an upload link or form
  needs a file a phone camera cannot attach.
- A point-in-time restore copies each old version back with a plain
  CopyObject, so an object over 5 GiB cannot be restored that way yet, and
  the restored copy takes the bucket's default storage class.
- MinIO stalls for about 30 seconds on a write into a versioned prefix that
  already holds a folder-marker object. That is the server, not this app;
  the end-to-end suite orders its fixtures around it.
- Two-way sync compares local size and modification time with the remote
  size and ETag recorded at the last run. A local edit that keeps both the
  size and the mtime is not seen as a change, and a key with no single local
  name ("a//b", a backslash) is left out of a two-way run and named.
- Thumbnails are whole GETs of objects up to 8 MB, at most 300 per folder;
  bigger images and other types keep the generic icon.
- External editing uploads in one PUT and so stops at 100 MiB; an editor
  that keeps the file open and writes it in pieces may be uploaded more than
  once while it saves.
