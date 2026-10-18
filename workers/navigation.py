"""Listing a bucket or a prefix off the UI thread.

Each worker drives a private Model clone: two threads sharing one botocore
client race on its endpoint and region state, and the symptom is a listing
that belongs to the bucket you were looking at a moment ago.
"""

import time
from PyQt6.QtCore import QObject, pyqtSignal, pyqtSlot


# How often a long listing reports its running count.
LISTING_COUNT_INTERVAL_SEC = 0.4


class NavigationWorker(QObject):
    finished = pyqtSignal(int, object, str)  # seq, payload, err_str
    counted = pyqtSignal(int, int)           # seq, entries so far

    def __init__(self, data_model_clone, seq: int, bucket: str, prefix: str,
                 max_items: int = 0):
        super().__init__()
        self._max_items = int(max_items or 0)
        # Private model clone — the worker owns its own boto3 client so it
        # cannot race the main thread or other navigation workers on shared
        # client/region/endpoint state.
        self._dm = data_model_clone
        self._seq = seq
        self._bucket = bucket or ""
        self._prefix = prefix or ""

    def _capture_state(self):
        return {
            "endpoint_url": self._dm.endpoint_url,
            "region_name": self._dm.region_name,
            "use_path": self._dm.use_path,
        }

    @pyqtSlot()
    def run(self):
        try:
            if not self._bucket:
                buckets = self._dm.list_buckets()
                payload = {
                    "mode": "bucket_list",
                    "buckets": buckets,
                    "promoted": self._capture_state(),
                }
            else:
                # Emitted per page: a prefix with tens of thousands of direct
                # children used to spend a silent minute here.
                last = [0.0]

                def _page(count):
                    now = time.monotonic()
                    if count and (now - last[0]) >= LISTING_COUNT_INTERVAL_SEC:
                        last[0] = now
                        self.counted.emit(self._seq, int(count))

                items = self._dm.list(
                    self._prefix, page_cb=_page, max_items=self._max_items)
                payload = {
                    "mode": "bucket_items",
                    "items": items,
                    "bucket": self._bucket,
                    "prefix": self._prefix,
                    "truncated": bool(
                        getattr(self._dm, "last_listing_truncated", False)),
                    "promoted": self._capture_state(),
                }
            self.finished.emit(self._seq, payload, "")
        except Exception as exc:
            self.finished.emit(self._seq, None, str(exc))


class BucketEnterWorker(QObject):
    """Runs enter_bucket (+ endpoint retry) off the main thread."""
    success = pyqtSignal(str)         # bucket_name
    failure = pyqtSignal(str, str)    # bucket_name, error_message
    log_msg = pyqtSignal(str)
    finished = pyqtSignal()

    def __init__(self, data_model, name: str):
        super().__init__()
        self._dm = data_model
        self._name = name

    @pyqtSlot()
    def run(self):
        name = self._name
        first_exc = None
        try:
            self._dm.enter_bucket(name)
            self.success.emit(name)
            self.finished.emit()
            return
        except Exception as exc:
            first_exc = exc
            self.log_msg.emit(f"Open bucket failed for '{name}': {exc}")

        # Try to fetch hints (region/endpoint)
        region_hint, endpoint_hint = None, None
        try:
            region_hint, endpoint_hint = self._dm.get_bucket_hints(name)
        except Exception as hint_exc:
            self.log_msg.emit(f"While probing hints: {hint_exc}")

        if region_hint:
            self.log_msg.emit(f"Hint: bucket '{name}' region may be '{region_hint}'")
        else:
            self.log_msg.emit(f"Hint: bucket '{name}' region unknown (no header)")
        if endpoint_hint:
            self.log_msg.emit(f"Hint: suggested endpoint for '{name}': {endpoint_hint}")

        retried = False
        retry_err = None
        if region_hint:
            base_endpoint = self._dm.profile_endpoint_url or self._dm.endpoint_url
            swapped = self._dm.build_region_swapped_endpoint(base_endpoint, region_hint)
            candidate_endpoint = endpoint_hint or swapped
            if candidate_endpoint:
                old_endpoint = self._dm.endpoint_url
                old_region = self._dm.region_name
                old_use_path = self._dm.use_path
                old_client = self._dm._client
                try:
                    self.log_msg.emit(
                        f"Retry: temporarily switching endpoint to '{candidate_endpoint}' "
                        f"and region to '{region_hint}' for bucket '{name}'"
                    )
                    self._dm.endpoint_url = candidate_endpoint
                    self._dm.region_name = region_hint
                    self._dm._client = None
                    self._dm.enter_bucket(name)
                    retried = True
                except Exception as rexc:
                    retry_err = rexc
                    self.log_msg.emit(f"Retry failed for '{name}': {rexc}")
                    self._dm.endpoint_url = old_endpoint
                    self._dm.region_name = old_region
                    self._dm.use_path = old_use_path
                    self._dm._client = old_client

        if retried:
            self.success.emit(name)
        else:
            self.failure.emit(name, str(retry_err or first_exc))
        self.finished.emit()
