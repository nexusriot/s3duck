# S3 Duck 🦆 — Roadmap

Prioritized backlog of planned work. Shipped features live in
[README.md](README.md); this file tracks what is *not* built yet, plus known
limitations worth fixing. Items move up or down based on review findings.

## Tier 1 — high value

- **Dual-pane mode** — two listings side by side (local↔remote or
  remote↔remote) with F5/F6-style copy/move between them; the clipboard and
  cross-bucket transfer plumbing already exists.

## Tier 2 — bucket administration

- **Lifecycle rules viewer/editor** — expiration, transitions,
  abort-incomplete-multipart; the natural follow-up to the incomplete-uploads
  cleaner.
- **CORS editor** — view and edit the bucket CORS document.
- **Bucket policy viewer/editor** — read-only display first (with a JSON
  syntax check), editing behind a confirmation.
- **Object Lock / legal hold** — surface retention state in Properties, allow
  setting/clearing legal holds where the bucket supports it.
- **Requester-pays support** — a per-profile toggle adding
  `RequestPayer=requester` to reads, without which such buckets are unusable.

## Launcher (profiles window)

The connect probe, the two-line rows and keyboard handling landed in 0.13.0;
the `[read-only]` / `[TLS unverified]` badges in 0.14.0; per-profile accent
colours in 0.15.0. What is left:

- **Reorder profiles** — drag to reorder, or sort by last used; the list is
  stored as a QSettings array, so order is already persisted and just needs a
  handle in the UI.
- **Filter box** — type to narrow the list once a user has more than a screen
  of profiles.
- **Duplicate-name validation** — Add/Edit accept a name already in use, and
  only import de-duplicates (`-imported` suffix).
- **Connection state per row** — show the result of the last probe (reachable
  / refused / never tried) so a broken profile is visible before Run.

## Tier 3 — polish

- **Pause / resume for the transfer queue** — cancel+retry exists; a true
  pause that keeps partial state would round it out.
- **Trash convention** — optional "move to `.trash/` prefix" instead of
  delete, with an empty-trash action (complements undo-delete, which needs
  versioning).
- **Content-hash duplicate confirmation** — the duplicate finder groups by
  size + ETag; confirming the "same size, ETags cannot compare" candidates
  would mean downloading and hashing them, which is worth offering explicitly
  for small files.
- **Same-bucket remote↔remote sync** — cross-profile sync landed, which also
  covers two prefixes in different accounts; comparing two prefixes inside one
  profile still goes through the local-folder dialog.
- **Cross-profile move** — copying between profiles landed; deleting the
  source afterwards (a true move) is the obvious follow-up, and needs the
  same are-you-sure care as any cross-account delete.
- **CRC32C checksums** — CRC32/SHA1/SHA256 are supported because they can be
  recomputed locally from the standard library. CRC32C would need
  `google-crc32c`; offering an algorithm we cannot verify would silently pass
  every download.
- **S3 Select preview** — run simple SQL over CSV/JSON objects in the preview
  dialog instead of downloading them.
- **QR code for presigned links** — hand a download link to a phone.
- **Watch mode** — monitor a local folder and auto-sync changes up on an
  interval (the sync engine already computes minimal plans).
- **Localization** — externalize user-facing strings.

## Known limitations (accepted for now)

- A cross-profile copy or sync cannot be re-run from transfer history: the
  stored record is JSON and never held the other profile's connection. The
  history dialog says so rather than queueing a job that must fail.
- Cancelling a resumable upload leaves its parts on the server on purpose,
  since that is what the next attempt resumes from. Abandoned ones are cleaned
  up from Tools -> Incomplete uploads.

- The preview's syntax highlighter is deliberately language-agnostic; a `#`
  inside a string literal is coloured as a comment.
- Drag-out must download the selection before the drag can start (Qt drags
  are synchronous); a large selection means a visible preparation dialog.
- A whole-folder download inside a parallel batch multiplies workers
  (files-in-flight × chunk fan-out); concurrency is bounded but can exceed
  the configured file parallelism.
- A multipart ETag still cannot be verified without the original part
  boundaries. Uploading with an additional CRC32 checksum (Transfer settings)
  makes those objects verifiable; without one, verification reports them as
  "not comparable" and passes.
- Bundled PNG twins are a fixed 48px raster, so on a Qt build without the SVG
  plugin icons do not scale as crisply as the SVGs would. Installing
  `python3-pyqt6.qtsvg` restores vector icons; the .deb recommends it.
- The command palette lists the toolbar and window-level actions. Context-menu
  entries are built on demand when the menu opens, so they are not in it.
- Cancelling a background scan (destination check, drag-out measure, duplicate
  scan) abandons the worker thread; it finishes quietly in the background.
- The duplicate finder compares ETags, so identical content uploaded with
  different multipart part sizes is reported as an unconfirmed candidate
  rather than a match.
