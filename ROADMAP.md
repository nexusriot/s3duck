# S3 Duck 🦆 — Roadmap

Prioritized backlog of planned work. Shipped features live in
[README.md](README.md); this file tracks what is *not* built yet, plus known
limitations worth fixing. Items move up or down based on review findings.

## Tier 1 — high value

- **Upload overwrite protection** — downloads, copies, moves, renames and
  pastes all detect existing destinations, but uploads (dialog, folder upload
  and drag-in) silently replace remote objects. Same Skip / Overwrite prompt,
  driven by one listing of the target prefix.
- **Restore last location** — reopen the previous bucket/prefix per profile on
  startup (bookmarks landed; this is the second half of that request).
- **Remote ↔ remote compare & sync** — the dry-run sync engine only pairs
  local↔remote today; comparing two prefixes (same or different buckets) would
  reuse `list_tree` + `build_sync_plan` nearly unchanged.
- **Resumable multipart uploads** — downloads resume from a `.s3duckpart`
  sidecar; interrupted large uploads still restart from zero. boto3's
  low-level multipart API + the incomplete-uploads machinery already in place
  make this tractable.
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

## Tier 3 — polish

- **Overwrite check for folder downloads** — the existing prompt covers file
  rows only; folder downloads overwrite local trees silently.
- **Pause / resume for the transfer queue** — cancel+retry exists; a true
  pause that keeps partial state would round it out.
- **Trash convention** — optional "move to `.trash/` prefix" instead of
  delete, with an empty-trash action (complements undo-delete, which needs
  versioning).
- **S3 Select preview** — run simple SQL over CSV/JSON objects in the preview
  dialog instead of downloading them.
- **QR code for presigned links** — hand a download link to a phone.
- **Watch mode** — monitor a local folder and auto-sync changes up on an
  interval (the sync engine already computes minimal plans).
- **Temp-file cleanup** — previews, drag-out staging and "open with default
  app" leave files in per-session temp dirs until the OS clears them.
- **Localization** — externalize user-facing strings.

## Known limitations (accepted for now)

- The preview's syntax highlighter is deliberately language-agnostic; a `#`
  inside a string literal is coloured as a comment.
- Drag-out must download the selection before the drag can start (Qt drags
  are synchronous); a large selection means a visible preparation dialog.
- A whole-folder download inside a parallel batch multiplies workers
  (files-in-flight × chunk fan-out); concurrency is bounded but can exceed
  the configured file parallelism.
- Multipart ETags cannot be checksum-verified without the original part
  boundaries; verification reports them as "not comparable" and passes.
- Cancelling a background scan (destination check, drag-out measure) abandons
  the worker thread; it finishes quietly in the background.
