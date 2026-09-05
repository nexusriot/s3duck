# S3 Duck 🦆 — Roadmap

Prioritized backlog of planned work. Shipped features live in
[README.md](README.md); this file tracks what is *not* built yet, plus known
limitations worth fixing. Items move up or down based on review findings.

## Tier 1 — high value

- **Local pane as a first-class side** — the dual pane (F3) can already show a
  local folder and copy either way, but the local side has no sorting, no
  context menu and no in-place rename. It is a viewer with transfer keys, not
  yet a file manager half.
- **Second pane against another profile** — the pane lists the current
  profile's account only; cross-profile copy and sync exist as dialogs, and
  wiring them into the pane would make the whole feature one surface.

## Tier 2 — bucket administration

Lifecycle, CORS, the read-only policy view and Object Lock landed in 0.18.0
under Tools → Bucket settings; default encryption, bucket tags, website
hosting, acceleration and a read-only events view joined them in 0.20.0, and
requester-pays is a per-profile toggle. What is left there:

- **Object Lock retention editing** — retention state and legal holds are
  shown and a hold can be placed or cleared, but setting a per-object
  retention mode and date is not offered. GOVERNANCE retention can be
  shortened only with a bypass permission, and COMPLIANCE cannot be shortened
  at all, so this needs more care than a spin box.
- **Lifecycle filters beyond a prefix** — the editor writes a prefix filter.
  A rule filtered by tag or object size is listed and summarized (the tags and
  the size bounds appear in its one-line description) but refuses to open in
  the editor, because rebuilding it from the prefix form would drop the filter
  and widen the rule to every object under the prefix — for an expiration
  rule, that deletes things it never used to touch.
- **Policy templates** — "make this prefix public", "allow this account", as
  reviewed snippets rather than a blank JSON box.

## Launcher (profiles window)

The connect probe, the two-line rows and keyboard handling landed in 0.13.0;
the `[read-only]` / `[TLS unverified]` badges in 0.14.0; per-profile accent
colours in 0.15.0; the credential-expiry badge, "Refresh credentials" and the
credential-storage choice in 0.18.0. What is left:

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
- **Scheduled sync** — the watch mirrors on an interval while the app is
  open. Running one at 02:00 with the app closed needs a daemon, which is a
  different program, not a bigger dialog.
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
- **Watch mode for downloads** — the interval mirror only runs local → remote;
  the reverse direction is the same planner with the arguments swapped, but it
  can delete local files, which deserves its own confirmation design.
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
- Editing in the preview is offered only for text that arrived whole and
  decoded cleanly as UTF-8: saving a truncated body would delete the tail, and
  saving a replacement-charactered decode would corrupt the object.
- Drag-out must download the selection before the drag can start (Qt drags
  are synchronous); a large selection means a visible preparation dialog.
- A whole-folder download inside a parallel batch multiplies workers
  (files-in-flight × chunk fan-out); concurrency is bounded but can exceed
  the configured file parallelism.
- Verifying a multipart object needs its real part boundaries, which come
  from `GetObjectAttributes`. AWS answers it; most S3-compatible backends do
  not, and there the old behaviour stands — verification reports the object as
  "not comparable" and passes. Uploading with an additional CRC32 checksum
  (Transfer settings) makes an object verifiable everywhere, since a
  full-object checksum needs no part boundaries at all.
- Bundled PNG twins are a fixed 48px raster, so on a Qt build without the SVG
  plugin icons do not scale as crisply as the SVGs would. Installing
  `python3-pyqt6.qtsvg` restores vector icons; the .deb recommends it.
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
- A job is retried automatically at most once, and only when the service
  itself asked (throttling, 5xx, a dropped connection). The wait is held by a
  timer owned by the window, so closing it cancels a retry that has not fired
  yet rather than starting one on the way out. Anything else is a
  refusal, and retrying a refusal on a timer is just a slower refusal.
- Only a launch that names a location is handed to a running window; a bare
  `s3duck` still opens its own, because two profiles side by side is a thing
  people legitimately want. Handover needs Qt's network module — where that
  is missing, the second launch simply opens its own window too.
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
