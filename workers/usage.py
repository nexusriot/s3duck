"""Totalling a bucket's size off the UI thread, by category and class.
"""

from PyQt6.QtCore import QObject, pyqtSignal, pyqtSlot
from core.naming import categorize_key
from model import Model as DataModel


class UsageWorker(QObject):
    finished = pyqtSignal(str, str, object)  # bucket, prefix, result_or_exc

    def __init__(self, data_model, bucket_name: str, prefix: str):
        super().__init__()
        self.data_model = data_model
        self.bucket_name = bucket_name
        self.prefix = prefix or ""

    @pyqtSlot()
    def run(self):
        try:
            total = 0
            count = 0
            by_cat = {"Documents": 0, "Media": 0, "Other": 0}
            by_top = {}
            by_class = {}
            largest = []

            pref = self.prefix or ""
            pref_len = len(pref)

            for k, s, storage in self.data_model.get_keys_for_bucket(
                    self.bucket_name, pref):
                if not k or str(k).endswith("/"):
                    continue
                key = str(k)
                sz = int(s or 0)
                total += sz
                count += 1

                cat = categorize_key(key)
                by_cat[cat] = by_cat.get(cat, 0) + sz

                cls = storage or "STANDARD"
                by_class[cls] = by_class.get(cls, 0) + sz

                rel = key[pref_len:] if key.startswith(pref) else key
                top = rel.split("/", 1)[0] if "/" in rel else "(files)"
                by_top[top] = by_top.get(top, 0) + sz

                largest.append((sz, rel))
                if len(largest) > 200:      # keep the scan memory-bounded
                    largest.sort(reverse=True)
                    del largest[50:]

            by_top = dict(sorted(by_top.items(), key=lambda kv: kv[1], reverse=True)[:12])
            largest.sort(reverse=True)

            self.finished.emit(self.bucket_name, self.prefix, {
                "total": total, "count": count, "by_cat": by_cat,
                "by_top": by_top, "by_class": by_class,
                "largest": largest[:10],
                "cost": (DataModel.estimate_storage_cost(by_class)
                         if self.data_model.is_aws_endpoint() else None),
                "colder": (DataModel.colder_class_options(by_class)
                           if self.data_model.is_aws_endpoint() else []),
            })
        except Exception as exc:
            self.finished.emit(self.bucket_name, self.prefix, exc)
