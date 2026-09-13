# S3Duck 🦆

Simple cross-platform GUI client for S3-compatible object storage (AWS S3, MinIO, Ceph, and others).

![ListBuckets](resources/buckets.png)
![Screenshot](resources/screenshot.png)

---

## Continuous integration

GitHub Actions (`.github/workflows/ci.yml`) runs three jobs on every push and
pull request:

- **tests** — the offscreen suite on Python 3.11 and 3.12, against both the
  pinned PyQt6 from `requirements.txt` and the newest wheel. The same code has
  behaved differently on a wheel, a pinned older wheel and Debian's
  `python3-pyqt6`, so more than one Qt is covered on purpose.
- **icons without the Qt SVG plugin** — strips PyQt6's SVG icon engine to
  reproduce Debian/Mint's `python3-pyqt6`, then requires every icon to still
  render from its bundled PNG twin. This is the environment that produced blank
  toolbar buttons.
- **.deb package** — builds the package and checks that the build substituted
  every control placeholder, that `Installed-Size` is measured rather than
  hardcoded, that the Qt SVG/PDF dependencies are declared, and that a PNG twin
  ships for every bundled SVG.

## Features

- **Multi-profile management** — create, edit, copy, and delete named connection profiles; credentials are encrypted at rest using Fernet symmetric encryption. Each row shows its endpoint, region and pinned bucket, with colour-coded `[read-only]` / `[TLS unverified]` badges beside the name, and the list is keyboard-driven (`Enter` opens, `F2` edits, `Del` removes). Connecting runs off the UI thread behind a cancellable dialog, so an unreachable endpoint no longer freezes the launcher
- **Profile export / import** — move profiles between machines as a bundle whose credentials are encrypted with a passphrase you choose (never written in the clear)
- **Read-only profiles** — mark a profile read-only to block every write and delete; the toolbar and context menus hide mutating actions, the title bar shows `[read-only]`, and the data layer refuses writes as a backstop
- **Temporary credentials** — session-token support for STS / SSO / assumed-role / MFA keys, with one-click import of any profile from `~/.aws/credentials`
- **Credential expiry** — a temporary key's lifetime is stored with the profile, badged in the launcher (`[expires in 12m]` / `[expired]`), counted down in the status bar, and checked before a transfer starts instead of failing half way through it
- **Credential refresh** (Tools → Refresh credentials, or the launcher's context menu) — mints a new session from the profile's `credential_process` (the AWS SDKs' own SSO / assume-role mechanism) or by re-reading the `~/.aws` profile it was imported from, without reconnecting or losing the current listing. A profile whose keys have lapsed is refreshed automatically when you open it
- **Credential storage** (launcher context menu → Credential storage…) — the key that protects saved credentials can live in the settings file (the default, and the historical behaviour), in the OS secret store via `keyring`, or sealed under a passphrase you enter once per launch; switching re-homes the same key so stored profiles stay readable
- **Icons that survive a minimal Qt** — the toolbar prefers your desktop icon theme, but falls back to bundled art whenever the theme has no entry *or* paints nothing visible; each bundled SVG has a PNG twin, so icons still appear on a Qt build without the SVG plugin (Debian/Mint's `python3-pyqt6` ships none). Run `python3 tools/icon_report.py` to see what your desktop resolves
- **Per-profile accent colour** — mark a profile with a colour; the launcher row and the open window both carry it, so prod is distinguishable from dev at a glance
- **Public base URL** — point a profile at the CloudFront distribution or custom domain in front of its bucket, and every public link and properties URL is shown against that instead of the raw endpoint. Presigned links deliberately keep the real endpoint, because their signature covers the host
- **Requester-pays buckets** — a per-profile toggle that adds `RequestPayer=requester` to every request that accepts one, without which such a bucket refuses every read
- **Diagnostics** (Tools → Diagnostics…) — Qt plugin availability, how every icon resolves, library versions and the active transfer settings, in one copyable report
- **Report a problem** (Tools → Report a problem…) — that report plus the last failure's request id and the tail of the session log, ready to copy or save. The log is also written to `~/.config/s3duck/s3duck.log` (rotated at 2 MB, switchable off in Transfer settings), because the log view is capped and dies with the window
- **Log view** — every operation is logged with a timestamp in the pane below the listing; its context menu clears it or saves what is on screen to a file
- **Open a location from anywhere** — `s3duck s3://bucket/prefix/` from a terminal, or an `s3://` link from a browser or file manager; an already-running window takes it as a new tab, while a bare launch still opens its own window
- **Quick open** (`Ctrl+P`) — type-to-jump across buckets and bookmarks, using the command palette's matcher
- **Sync to another profile** — compare this prefix against one in a different account or provider, review the dry-run plan, then run it; the bytes stream through this machine because no server-side copy can span two credentials
- **Command palette** (`Ctrl+K`) — type-to-run index of every available action, built from the live actions so it cannot drift
- **Copy to another profile** — stream objects (or whole prefixes) into a different account or provider; a server-side copy cannot use two sets of credentials, so the bytes travel through this machine
- **Bucket browser** — list, create, and delete buckets; recursive delete runs through the transfer queue (progress + cancel, UI stays responsive)
- **Empty bucket** — delete every object, version, delete marker and in-flight upload while keeping the bucket, queued with progress and cancel
- **Incomplete uploads** — find and abort in-flight multipart uploads that are invisible to normal listings but keep their parts billed; shows how much space they waste and can abort everything older than N days
- **Object browser** — navigate prefixes as a virtual folder tree with sorting by name, size, and modified date; optional Storage-class and ETag columns via the header context menu (both come free with the listing)
- **Upload** — single/multiple files via dialog or drag-and-drop from the OS file manager; whole directory trees via "Upload folder" (`Ctrl+Shift+U`) or drag-and-drop
- **Download** — single files or entire folder prefixes, recreating the directory tree locally
- **Download as ZIP** — stream a selection (files and whole folders) straight into one archive, without staging it on disk twice
- **Drag out** — drag objects from the list onto a file manager; the selection is downloaded to a temp folder first, with progress and a size warning. Staged payloads are removed when the app exits, and a crashed run's leftovers are reclaimed on the next start
- **Resumable uploads** — a large upload is sent part by part with its upload id recorded, so an interrupted transfer resumes instead of restarting; a cancelled one deliberately leaves the parts on the server (clean them up from Incomplete uploads)
- **Resumable downloads** — large files download as parallel ranges into a `.s3duckpart` file with a progress sidecar, so an interrupted transfer picks up where it stopped instead of restarting (a changed ETag discards the stale partial)
- **Upload rules by destination** — `archive/* -> class=GLACIER, tag:team=infra`: a small per-profile table that decides the storage class and tags from the key an upload is heading for, so a storage policy is applied rather than remembered
- **Content-Type detection** — every upload is stamped with a type derived from the file extension, so a public link renders in a browser instead of downloading as `binary/octet-stream`; the table is overridable per extension, and the object context menu's "Fix Content-Type from extension" re-stamps objects that were uploaded before (or by something else)
- **Additional checksums** — upload with CRC32/SHA1/SHA256; CRC32 is requested as a whole-object checksum so multipart objects stay verifiable, unlike a multipart ETag
- **Checksum verification** — optionally compare each downloaded file against the object's stored digest and fail the transfer on a mismatch. A multipart object is settled by asking the service for its real part boundaries (`GetObjectAttributes`) and rebuilding the composite digest from them, so "multipart, not comparable" is now the exception rather than the rule; where a backend does not implement that call, verification still degrades to reporting the object as not comparable rather than failing it
- **Parallel transfers** — configurable number of files moving at once *and* multipart connections within each file; applies to uploads, downloads (including whole prefixes) and sync
- **Transfer settings** — files in flight, connections per file, bandwidth limit, multipart part size and threshold, resumable uploads, automatic retry of transient failures, the per-listing entry limit, the session log file, Content-Type detection with its override table, the upload rules by destination, plus the storage class, checksum and server-side encryption (SSE-S3 / SSE-KMS) applied to uploads — all persisted across sessions
- **Overwrite protection** — uploads (dialog, folder upload and drag-in), downloads (single files *and* whole folders), copies, moves, renames and pastes all detect existing destinations and offer Skip / Overwrite / Cancel
- **Sync with a local folder** (`Ctrl+E`) — compare a directory against a prefix in either direction, review a dry-run plan (upload / download / delete / skip with a reason per file), then run it through the queue; supports exclude globs (`*.tmp`, `node_modules/`) and optionally deleting extras at the destination
- **Transfer queue** — queued jobs with per-row progress, cancel, and retry for failed or cancelled entries; "Retry failed" re-queues every failure at once, and a job the service itself asked us to repeat (throttling, 5xx, a dropped connection) is retried once automatically
- **Transfer history** — a persisted log of past jobs (when, what, bytes, outcome) with one-click re-run for small jobs, from the queue panel
- **Bandwidth limit** — optional ceiling on total transfer throughput, shared across every parallel file and chunk
- **Completion notifications** — a desktop notification when the queue drains while the window is in the background (toggle in Transfer settings)
- **Delete** — objects and folder prefixes (recursive, batched 1000 keys per call); confirmation shows the scanned object count and total size; recursive bucket delete also purges noncurrent versions, delete markers and in-flight uploads
- **Undo delete** (`Ctrl+Z`) — on a versioning-enabled bucket a delete only writes a delete marker, so the last delete can be rolled back by removing those markers
- **Clipboard** (`Ctrl+C` / `Ctrl+X` / `Ctrl+V`) — copy or cut a selection and paste it into any folder or bucket; copying also puts the `s3://` URIs on the system clipboard for use elsewhere
- **Copy / Move** — server-side copy or move of a multi-selection, within a bucket or **across buckets**; when the destination lives in another region or account (where a server-side copy is impossible) the object is streamed through instead of failing
- **Rename** — in-place rename of a file or folder (server-side copy + delete), on the context menu or `F2`
- **Bulk rename** (`Shift+F2`) — rename a whole selection by find-and-replace (optionally regex, with backreferences) or a `{name}/{ext}/{n}` numbering template, with a live preview and duplicate/invalid-name checks
- **Create folder** — creates an S3 prefix placeholder
- **Preview / open** — double-click a file to preview images, text, **PDFs** and syntax-highlighted code in-app, with a **hex dump** for binaries; anything can still be opened with the OS default application
- **Edit in place** — a text object that arrived whole can be edited in the preview and written back with an `If-Match` precondition, so a concurrent save is refused rather than silently overwritten (backends without conditional writes fall back to an ETag check)
- **Recursive search** — search a whole bucket/prefix by key substring or regular expression (`Ctrl+Shift+F` or "Search here…"), filtered by size range, extension and modified date, with jump-to-location on any result
- **Act on search results** — select any number of matches and download, delete, tag, re-type, restore or change the storage class of all of them through the transfer queue, or copy their keys and `s3://` URIs
- **Saved searches** — name a query with its filters and re-run it from a drop-down; stored per profile, so a recurring cleanup is one click rather than a re-typed form
- **Object versioning** — enable/suspend bucket versioning from the UI; list every version and delete marker of an object, download a specific version, promote an older version to current, or delete individual versions
- **Diff two versions** — select any two versions of a text object and see a coloured unified diff of what changed, which is the question a version list is usually opened with
- **Storage class** — view an object's storage class and change it (Standard, IA, Intelligent-Tiering, Glacier, Deep Archive, …); works on a multi-selection or whole folders and runs through the transfer queue
- **Glacier restore** — initiate a restore of archived objects (single, multi-select, or whole prefixes) with a chosen retrieval tier and retention window, queued like other transfers; restore status shown in properties
- **Bulk tagging** — add, overwrite or strip tags across a whole selection, expanding folders to every object beneath them
- **Edit metadata** — set `Content-Type`, `Cache-Control`, `Content-Disposition`, `Content-Encoding`, and custom `x-amz-meta-*` user metadata
- **Object properties** — key, size, ETag, storage class, restore status, Object Lock retention and legal hold (probed in the background, since most buckets have none), and public URL
- **Presigned links** — generate a temporary download (GET) or **upload (PUT)** link with a configurable expiry (up to the 7-day S3 maximum)
- **Make public** — set `public-read` ACL and copy direct URL; when the ACL is refused, Block Public Access and Object Ownership settings are reported as the reason
- **Location tabs** (`Ctrl+T`) — keep several buckets/prefixes a click apart in one window, reorder them by dragging, and reopen them next time; `Ctrl+W` closes one, `Ctrl+Tab` cycles. They are remembered locations for one shared listing view, not independent views, so switching tabs navigates
- **Compare panes** (Tools → Compare panes…) — what differs between the two panes, recursively: only-here, only-there, differs, identical, with the selected differences copyable either way. This is also how two prefixes in one account get compared, which previously had to go through a local folder
- **Dual pane** (`F3`) — a second pane showing a local folder or another remote prefix, with `F5` to copy the focused pane's selection to the other side and `F6` to move it, all through the transfer queue. `F5` belongs to Refresh while there is only one pane, and Refresh keeps `Ctrl+R` either way
- **Watch a folder** (Tools → Watch folder…) — mirror a local folder up to a prefix on an interval; each pass compares the two and queues only what differs, and a pass is skipped while the queue is still busy. The settings are remembered, but a watch is never resumed on its own at startup
- **Back / forward** (`Alt+←` / `Alt+→`) — a real navigation trail per tab, across buckets, that a new move truncates the way a browser does
- **Clickable breadcrumb** — jump straight to any parent prefix, the bucket root, or the bucket list from the path bar
- **Go to location** (`Ctrl+L`) — paste an `s3://bucket/prefix` (or a bare prefix) and jump straight there, across buckets
- **Restore last location** — each profile reopens the bucket and prefix you left it in, and the launcher preselects the profile you used last
- **Bookmarks** (`Ctrl+B`) — save any bucket/prefix and return to it from the toolbar menu, with rename/remove management; stored per profile
- **Quick filter** — type in any listing to narrow it by name, or use a glob (`*.log`, `2026-0?-*`); the bar shows how many of the rows match, so an over-narrow filter is not mistaken for an empty folder
- **Large prefixes** — a listing reports its running count while it loads and stops at a configurable limit, saying so rather than silently showing the first N of a much bigger prefix
- **Remembered layout** — splitter position, column widths and sort order persist between sessions
- **Listing summary** — folder/file counts and total size of the current listing in the status bar
- **Keyboard shortcuts** (`Ctrl+/`) — a searchable reference generated from the app's own actions, since plain letters are reserved for type-to-search
- **Theme** — Light, Dark, or system-default appearance, remembered across sessions
- **Duplicate finder** (Tools → Find duplicates, `Ctrl+Shift+D`) — group objects holding identical content by size + ETag at the cost of a listing, showing reclaimable space; select all-but-newest/oldest and delete the redundant copies. Objects whose ETags cannot settle the question (multipart vs single-part) are listed separately and never auto-selected — "Confirm candidates" then settles them from each object's stored digests, without downloading either copy, where the backend can supply one
- **Bucket usage stats** — total size and object count, breakdown by file category with a pie chart, top folder groups, a storage-class breakdown and the largest objects
- **Size explorer** (Tools → Size explorer…) — a treemap of the bucket by prefix, drilling down on a click. Area is the only encoding that makes a 400 GB folder look like one, which a breakdown by file category cannot
- **Integrity manifest** (Tools → Export manifest…, then Tools → Verify against manifest…) — record what a prefix holds as a CSV, then later ask what has gone missing, been added, or changed. Both directions cost one listing, so it is affordable on a bucket you would never hash
- **Cost estimate** — on an AWS endpoint the usage report also prices the storage per class and shows what the STANDARD bytes would cost in IA, Glacier IR or Deep Archive, with the minimum-duration catch spelled out beside each one (storage only, us-east-1 list price)
- **Bucket settings** (Tools → Bucket settings…) — lifecycle rules with a real editor (transitions, expiration, noncurrent versions, aborting incomplete uploads), the CORS document, the bucket policy (read-only until you tick "Allow editing", and JSON-checked before it is sent), default encryption, the bucket's own tags, static-website hosting, transfer acceleration, a read-only view of the event notifications, and Object Lock state — all read in one background pass. A rule filtered by tag or object size is listed and summarized but not editable here, because rebuilding it from the prefix form would silently widen it
- **Runtime profile switch** — switch S3 accounts without restarting the app
- **Cached bucket bindings** — the proven endpoint/region/addressing combination per bucket is remembered, so reopening an off-region bucket skips the probe round trips
- **Automatic region/endpoint detection** — when an operation fails due to a region or endpoint mismatch the app probes the server for the correct region, rebuilds the client, and retries transparently; applies to bucket open, listing, upload, download, and delete
- **HTTP proxy and private CAs** — a per-profile proxy, and a CA bundle to trust so a corporate MITM certificate or a self-signed MinIO one can be verified by path instead of switching verification off entirely; a bundle that is not there is reported before it becomes an opaque SSL error
- **Failure details** — a failed transfer keeps the error code, HTTP status, request id and host id the service returned, shown behind the row's ℹ button and copyable in one click. `str(exception)` alone can never be traced back to a request on the provider's side
- **S3-compatible storage** — path-style addressing option for MinIO and similar backends
- **Cross-platform** — Linux, macOS, Windows

Planned work and known limitations are tracked in [ROADMAP.md](ROADMAP.md).

---

## Requirements

| Dependency    | Version  | Purpose                          |
|---------------|----------|----------------------------------|
| Python        | ≥ 3.10   | Runtime                          |
| PyQt6         | ≥ 6.7    | GUI framework                    |
| boto3         | ≥ 1.42   | AWS / S3-compatible SDK          |
| cryptography  | ≥ 46.0   | Fernet credential encryption     |
| keyring       | optional | Keep the credential key in the OS secret store |
| pyinstaller   | ≥ 6.18   | Binary packaging (optional)      |

---

## Running from Source

**Quick start (system packages, Debian/Ubuntu):**
```bash
sudo apt install python3-boto3 python3-cryptography python3-pyqt6
python3 s3duck.py
```

**Recommended — virtualenv:**
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python3 s3duck.py
```

---

## Building

### Debian / Ubuntu package
```bash
sudo apt-get install git devscripts build-essential lintian upx-ucl
./build_deb.sh              # auto-detects amd64 / arm64
./build_deb.sh arm64        # explicit architecture
```
Output: `build/s3duck_<version>_<arch>.deb`

### Linux binary (PyInstaller)
```bash
./build_linux_bin.sh
```

### macOS binary + DMG
```bash
./build_macos_bin.sh                  # native arch
./build_macos_bin.sh universal2       # fat binary (x86_64 + arm64)
./build_dmg.sh
```

### Windows binary
```bat
build_win.cmd
```

Pre-built releases are available on the [GitHub releases page](https://github.com/nexusriot/s3duck/releases/latest).

---

## Project Architecture

```
s3duck/
├── s3duck.py            Entry point — QApplication bootstrap, Profiles dialog
├── main_window.py       Main window — file browser, toolbar, async workers
├── model.py             S3/data layer — all boto3 operations, region retry logic
├── settings.py          Profile create/edit dialog
├── properties_window.py Object properties dialog
├── profile_switcher.py  Runtime profile-switch dialog
├── theme.py             Light / Dark / system palette switching
├── diagnostics.py       Qt plugin / icon / library report (Tools → Diagnostics)
├── utils.py             Shared helpers — Crypto and CredentialStore, Qt thread
│                        plumbing, ~/.aws parsing, credential expiry, local tree scan
│
├── icons/               24 px SVG icons for toolbar and context menus
├── resources/           App icon (ico/icns/png), screenshots, .desktop file
├── DEBIAN/              Debian package metadata (control, postinst, prerm)
├── tests/               Offscreen unit suite (test_units.py)
├── tools/               CI helpers and the icon report
│
├── requirements.txt     Python dependencies
├── s3duck.spec          PyInstaller build spec
├── build_deb.sh         Build .deb package
├── build_linux_bin.sh   Build Linux self-contained binary
├── build_macos_bin.sh   Build macOS self-contained binary
├── build_dmg.sh         Pack macOS binary into .dmg
└── build_win.cmd        Build Windows self-contained binary
```

### Layers

```
┌─────────────────────────────────────────────────────────────────┐
│  Entry / Profile layer   s3duck.py                              │
│  Profiles dialog, SettingsItem, credential refresh              │
├─────────────────────────────────────────────────────────────────┤
│  Credential layer        utils.py                               │
│  Crypto (Fernet), CredentialStore (settings / keyring /         │
│  passphrase), credential_process, expiry_state                  │
├─────────────────────────────────────────────────────────────────┤
│  UI layer                main_window.py                         │
│  MainWindow, Tree, SecondPane, UpTopProxyModel, PieWidget,      │
│  BucketUsageDialog, ListItem, tab bar                           │
├─────────────────────────────────────────────────────────────────┤
│  Worker / Threading layer   main_window.py                      │
│  NavigationWorker  BucketEnterWorker  Worker  UsageWorker       │
│  (each runs in a QThread, communicates via pyqtSignal)          │
├─────────────────────────────────────────────────────────────────┤
│  Dialog layer                                                   │
│  SettingsWindow  PropertiesWindow  ProfileSwitchWindow          │
│  BucketSettingsDialog  WatchFolderDialog  …                     │
├─────────────────────────────────────────────────────────────────┤
│  Data / S3 layer         model.py                               │
│  Model — boto3 wrapper, adaptive region/endpoint probing        │
└─────────────────────────────────────────────────────────────────┘
```

### Key components

| Component | File | Responsibility |
|---|---|---|
| `Profiles` | s3duck.py | CRUD for connection profiles; launches MainWindow |
| `Crypto` | utils.py | Fernet encrypt/decrypt of stored credentials |
| `CredentialStore` | utils.py | Where the credential key lives — settings file, OS secret store, or sealed under a passphrase |
| `run_credential_process` | utils.py | Mints a session from a profile's `credential_process`, the AWS SDKs' own SSO / assume-role refresh |
| `MainWindow` | main_window.py | Root window — toolbar, splitter (tree + log), statusbar |
| `Tree` | main_window.py | Drag-and-drop `QTreeView`; hands drops to upload worker |
| `UpTopProxyModel` | main_window.py | Proxy that pins `[..]` to top and sorts BUCKET < FOLDER < FILE |
| `NavigationWorker` | main_window.py | Off-thread bucket/prefix listing; uses a private `Model` clone to avoid client races |
| `BucketEnterWorker` | main_window.py | Off-thread bucket entry with hints-based region/endpoint retry |
| `Worker` | main_window.py | Off-thread upload / download / delete with byte-level progress and cancellation |
| `UsageWorker` | main_window.py | Off-thread bucket size aggregation by file category, storage class and estimated cost |
| `SecondPane` | main_window.py | The other half of the dual pane — a local folder or a remote prefix, listing only |
| `PieWidget` | main_window.py | Custom `QPainter` pie chart for usage breakdown |
| `Model` | model.py | All boto3 calls; `_try_bind_bucket` probes addressing styles; `rebind_bucket` auto-corrects region mid-session; `get_object_parts` supplies the boundaries that make a multipart object verifiable |
| `describe_client_error` | utils.py | Error code, HTTP status, request id and host id from a botocore failure |
| `PreviewDialog` | main_window.py | In-app image/text/PDF preview, in-place editing of whole text objects, "open with default app" via a temp download |
| `VersionsDialog` | main_window.py | Per-object version manager (list / download / make-current / delete); lists off-thread |
| `IncompleteUploadsDialog` | main_window.py | Find and abort orphaned multipart uploads still holding billed parts |
| `TransferSettingsDialog` | main_window.py | Concurrency, upload storage class / encryption / checksum, Content-Type detection and the per-listing entry limit |
| `BucketSettingsDialog` | main_window.py | Lifecycle, CORS, policy, encryption, tags, website, events and Object Lock, read in one background pass |
| `PaneCompareDialog` | main_window.py | What differs between the two panes, and a copy either way |
| `VersionDiffDialog` | main_window.py | Coloured unified diff between two versions of one object |
| `SizeExplorerDialog` | main_window.py | Treemap of a bucket by prefix, drilling down on a click |
| `TreemapWidget` | main_window.py | One level of a prefix tree as proportional rectangles |
| `LogFile` | utils.py | Rotating session log behind Tools → Report a problem |
| `InstanceServer` | utils.py | Hands an `s3://` location to an already-running window |
| `LifecycleRuleDialog` | main_window.py | One lifecycle rule — scope plus transition / expiration / noncurrent / abort actions |
| `WatchFolderDialog` | main_window.py | Configures the interval mirror of a local folder up to a prefix |
| `OverwriteDialog` | main_window.py | Skip / Overwrite prompt for existing destinations |
| `ShortcutsDialog` | main_window.py | Keyboard reference derived from the live QActions |
| `TransferHistoryDialog` | main_window.py | Past transfers with re-run |
| `RateLimiter` | model.py | Shared token bucket capping total throughput |
| `BulkTagsDialog` | main_window.py | Add / replace / remove tags across a selection |
| `BookmarksDialog` | main_window.py | Rename / remove saved locations |
| `DuplicateFinderDialog` | main_window.py | Size+ETag duplicate scan with guarded deletion |
| `CodeHighlighter` | main_window.py | Language-agnostic syntax highlighting in previews |
| `BulkRenameDialog` | main_window.py | Find-replace / template rename with live preview |
| `SyncDialog` | main_window.py | Local↔remote comparison, dry-run plan, queued execution |
| `MetadataDialog` | main_window.py | Edit Content-Type / caching headers and custom user metadata |
| `SearchDialog` | main_window.py | Recursive key search over a bucket/prefix; results are multi-select and feed the transfer queue |
| `PresignedLinkDialog` | main_window.py | Generate GET/PUT presigned links with a configurable expiry |
| `Breadcrumb` | main_window.py | Clickable path bar for jumping to parent prefixes |
| `apply_theme` | theme.py | Light / Dark / system palette switching |
| `SettingsWindow` | settings.py | Profile form — name, URL, region, bucket, keys, session expiry, credential process, public base URL, requester-pays, HTTP proxy, CA bundle and the safety flags |
| `PropertiesWindow` | properties_window.py | Object metadata: key, size, ETag, storage class, restore status, Object Lock state (probed off-thread), public URL |
| `ProfileSwitchWindow` | profile_switcher.py | Runtime profile switch without app restart |

### Data flow

```
User action
  │
  ▼
MainWindow  ──spawn──►  QThread + Worker/NavigationWorker
                              │   (private Model clone or shared Model)
                              │
                              ▼
                         Model.method()
                              │  boto3 S3 API call
                              ▼
                         AWS S3 / MinIO / Ceph …
                              │
                         pyqtSignal (progress / finished / error)
                              │
                              ▼
                         MainWindow  ──update──►  UI (tree, log, progress bar)
```

### Region / endpoint auto-retry flow

```
Operation fails  (AuthorizationHeaderMalformed | PermanentRedirect)
  │
  ▼
get_bucket_hints()          HEAD Bucket → x-amz-bucket-region header
  │
  ▼
build_region_swapped_endpoint()   rewrite AWS endpoint for new region
  │
  ▼
rebind_bucket()             swap endpoint + region → enter_bucket() → validate
  │
  ▼
retry original operation    transparent to the caller
```

### Credential storage

```
New profile
  │  access_key, secret_key, session token
  ▼
Crypto.encrypt()  (Fernet)
  │  encrypted bytes
  ▼
QSettings  →  ~/.config/s3duck/s3duck.ini

Launch profile
  │  encrypted bytes from QSettings
  ▼
Crypto.decrypt()  →  plaintext creds  →  boto3.Session
```

The Fernet key itself has three possible homes, chosen from the launcher's
context menu (Credential storage…):

```
local        QSettings "common/key" — the default, and the historical
             behaviour: the key sits beside the ciphertext, which protects
             against a casual reader and nothing else
keyring      the OS secret store (needs the `keyring` package *and* a
             backend it can reach — a headless box often has neither)
passphrase   sealed with PBKDF2 under a passphrase entered once per launch,
             stored as "common/key_wrapped"
```

Switching modes re-homes the same key, so stored profiles stay readable; the
new home is written before the old one is cleared. Temporary credentials also
carry their expiry, which the launcher badges and "Refresh credentials"
renews from the profile's `credential_process` or its `~/.aws` profile.

---

## License

See [LICENSE](LICENSE).

## Author

[Vladislav Ananev](https://github.com/nexusriot) © 2022–2026
