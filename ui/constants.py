"""Values the window and its dialogs both have to agree on.

Kept out of main_window.py so a dialog can read one without importing the
window that imports the dialog.
"""

OS_FAMILY_MAP = {"Linux": "🐧", "Windows": "⊞ Win", "Darwin": " MacOS"}
UP_ENTRY_LABEL = "[..]"  # special row to go one level up
# Listing columns. Storage class and ETag come free with ListObjectsV2 but are
# hidden by default; the header context menu toggles them.
LIST_COLUMNS = ("Name", "Size", "Modified", "Storage class", "ETag")
LIST_COLUMN_DEFAULT_WIDTHS = (320, 80, 140, 110, 240)
# Where a copy, move or rename reads the class it has to keep the object in.
# Derived rather than written as 3 so reordering the columns cannot silently
# start copying ETags into StorageClass.
LIST_STORAGE_CLASS_COLUMN = LIST_COLUMNS.index("Storage class")
LIST_OPTIONAL_COLUMNS = (3, 4)
TICK_INTERVAL_MS = 600             # UI tick
# How often the credential countdown is refreshed. A minute is fine: the
# warning window is fifteen.
CREDENTIAL_CHECK_INTERVAL_MS = 60 * 1000
# How long to wait before re-queueing a job the service asked us to retry.
AUTO_RETRY_DELAY_MS = 5000
# Entries fetched for one listing before it stops and says so. A prefix that
# large is not browsable anyway; Search is the tool for it.
DEFAULT_LISTING_LIMIT = 50000
EMA_ALPHA = 0.15                   # smoother rate
RATE_WINDOW_SEC = 2.0              # window for instantaneous rate
STALL_DECAY_INTERVAL_SEC = 2.0     # when no progress, decay displayed rate
