"""What a bucket holds, by category, storage class and estimated cost.
"""

import threading
from PyQt6.QtCore import QThread, Qt
from PyQt6.QtWidgets import (
    QDialog, QHBoxLayout, QLabel, QPushButton, QVBoxLayout,
)
from core.naming import _human_bytes
from core.treemap import build_prefix_tree, node_at_path, treemap_children
from model import TransferCancelled
from ui.widgets.charts import PieWidget, TreemapWidget
from utils import (
    DialogDismissMixin, FuncWorker, join_qthread, release_worker_on_finish,
)


class SizeExplorerDialog(DialogDismissMixin, QDialog):
    """Drill into a bucket by prefix, sized by what it actually holds."""

    def __init__(self, parent, main_window, model, prefix):
        super().__init__(parent)
        self._mw = main_window
        self._model = model
        self._prefix = prefix or ""
        self._root = {}
        self._path = []
        self._thread = None
        self._worker = None
        self._cancel = None

        self.setWindowTitle(f"Size explorer — {model.bucket}/{self._prefix}")
        self.resize(880, 620)

        self._breadcrumb = QLabel("")
        self._breadcrumb.setWordWrap(True)
        self._up = QPushButton("Up")
        self._up.clicked.connect(self.go_up)
        self._rescan = QPushButton("Rescan")
        self._rescan.clicked.connect(self._scan)

        top = QHBoxLayout()
        top.addWidget(self._up)
        top.addWidget(self._breadcrumb, 1)
        top.addWidget(self._rescan)

        self._map = TreemapWidget()
        self._map.drilled.connect(self.drill)
        self._info = QLabel("Scanning…")
        self._info.setWordWrap(True)

        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.reject)
        row = QHBoxLayout()
        row.addStretch(1)
        row.addWidget(close_btn)

        layout = QVBoxLayout(self)
        layout.addLayout(top)
        layout.addWidget(self._map, 1)
        layout.addWidget(self._info)
        layout.addLayout(row)
        self._scan()

    def stop_threads(self):
        if self._cancel is not None:
            self._cancel.set()
        thread, self._thread, self._worker = self._thread, None, None
        join_qthread(thread)

    def _scan(self):
        if self._thread is not None:
            return
        clone = self._model.clone_for_worker()
        bucket = self._model.bucket
        prefix = self._prefix
        cancel = threading.Event()
        self._cancel = cancel
        self._info.setText("Scanning…")
        self._rescan.setEnabled(False)

        def _run(_worker):
            entries = []
            for key, size, _storage in clone.get_keys_for_bucket(
                    bucket, prefix):
                if cancel.is_set():
                    raise TransferCancelled("cancelled")
                relative = key[len(prefix):] if key.startswith(prefix) else key
                entries.append((relative, size))
            return build_prefix_tree(entries)

        self._thread = QThread(self)
        self._worker = FuncWorker(_run)
        self._worker.moveToThread(self._thread)
        self._thread.started.connect(self._worker.run)
        self._worker.done.connect(self._on_scanned)
        self._worker.done.connect(self._thread.quit)
        release_worker_on_finish(self._thread, self._worker)
        self._thread.finished.connect(self._thread.deleteLater)
        self._thread.start()

    def _on_scanned(self, result, exc):
        thread, self._thread, self._worker = self._thread, None, None
        self._cancel = None
        join_qthread(thread)
        self._rescan.setEnabled(True)
        if exc is not None:
            self._info.setText(f"Scan failed: {exc}")
            return
        self._root = result or {}
        self._path = []
        self._render()

    def current_node(self) -> dict:
        return node_at_path(self._root, self._path)

    def drill(self, name):
        node = node_at_path(self._root, self._path + [name])
        if not node or not node.get("children"):
            return   # a leaf object, not a prefix
        self._path.append(name)
        self._render()

    def go_up(self):
        if self._path:
            self._path.pop()
            self._render()

    def _render(self):
        node = self.current_node()
        rows = treemap_children(node)
        self._map.set_rows(rows)
        where = "/".join(self._path)
        self._breadcrumb.setText(
            f"{self._model.bucket}/{self._prefix}{where}"
            + ("/" if where else ""))
        self._up.setEnabled(bool(self._path))
        self._info.setText(
            f"{_human_bytes(node.get('size', 0))} in "
            f"{node.get('count', 0)} object(s) · {len(rows)} entries here"
            + ("  ·  click a tile to go deeper" if rows else ""))


class BucketUsageDialog(QDialog):

    def __init__(self, bucket_name: str, prefix: str = "", parent=None):
        super().__init__(parent)
        self.setWindowTitle("Usage")
        self.setModal(False)

        self._bucket = bucket_name
        self._prefix = prefix or ""

        self.title = QLabel(self._title_html(bucket_name, self._prefix))
        self.total_lbl = QLabel("Total: <b>Calculating…</b>")

        self.pie = PieWidget({"Documents": 0, "Media": 0, "Other": 0}, self)

        self.legend_labels = {}
        legend = QVBoxLayout()
        for k in ["Documents", "Media", "Other"]:
            lbl = QLabel(f"{k}: Calculating…")
            self.legend_labels[k] = lbl
            legend.addWidget(lbl)
        legend.addStretch(1)

        self.top_groups = QLabel("<b>Top groups</b><br><pre>Calculating…</pre>")
        self.top_groups.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)

        self.by_class = QLabel("<b>Storage classes</b><br><pre>Calculating…</pre>")
        self.by_class.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        # Only ever filled for an AWS endpoint: the bundled prices are AWS
        # list prices, and quoting them at a MinIO endpoint invents a number.
        self.cost = QLabel("")
        self.cost.setTextInteractionFlags(
            Qt.TextInteractionFlag.TextSelectableByMouse)
        self.cost.hide()
        self.largest = QLabel("<b>Largest objects</b><br><pre>Calculating…</pre>")
        self.largest.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)

        top = QHBoxLayout()
        top.addWidget(self.pie, 0)
        top.addLayout(legend, 1)

        btn = QPushButton("Close")
        btn.clicked.connect(self.close)

        layout = QVBoxLayout()
        layout.addWidget(self.title)
        layout.addWidget(self.total_lbl)
        layout.addLayout(top)
        layout.addWidget(self.top_groups)
        layout.addWidget(self.by_class)
        layout.addWidget(self.cost)
        layout.addWidget(self.largest)
        layout.addWidget(btn)
        self.setLayout(layout)

    def _title_html(self, bucket: str, prefix: str) -> str:
        if prefix:
            return f"<b>{bucket}</b><br><span style='color:#666;'>/{prefix}</span>"
        return f"<b>{bucket}</b>"

    def set_calculating(self, bucket_name: str, prefix: str = ""):
        self._bucket = bucket_name
        title = f"<b>{bucket_name}</b>" + (f"<br><span style='color:#666'>/{prefix}</span>" if prefix else "")
        self.title.setText(title)

        self.total_lbl.setText("Total: <b>Calculating…</b>")
        self.pie.set_data({"Documents": 0, "Media": 0, "Other": 0})
        for k in ["Documents", "Media", "Other"]:
            self.legend_labels[k].setText(f"{k}: Calculating…")
        self.top_groups.setText("<b>Top groups</b><br><pre>Calculating…</pre>")
        self.by_class.setText("<b>Storage classes</b><br><pre>Calculating…</pre>")
        self.cost.hide()
        self.largest.setText("<b>Largest objects</b><br><pre>Calculating…</pre>")

    def set_error(self, bucket_name: str, prefix: str, err: Exception):
        self._bucket = bucket_name
        title = f"<b>{bucket_name}</b>" + (f"<br><span style='color:#666'>/{prefix}</span>" if prefix else "")
        self.title.setText(title)

        self.total_lbl.setText("Total: <b>n/a</b>")
        for k in ["Documents", "Media", "Other"]:
            self.legend_labels[k].setText(f"{k}: n/a")
        self.top_groups.setText(f"<b>Top groups</b><br><pre>n/a\n{err}</pre>")
        self.by_class.setText("<b>Storage classes</b><br><pre>n/a</pre>")
        self.cost.hide()
        self.largest.setText("<b>Largest objects</b><br><pre>n/a</pre>")

    @staticmethod
    def cost_html(cost, colder) -> str:
        """The estimated-cost panel, or "" when there is nothing to show."""
        if not cost:
            return ""
        lines = [f"{'Storage (estimated)':22s}  ${cost['total']:,.2f} / month"]
        for name, amount in sorted(cost.get("by_class", {}).items(),
                                   key=lambda kv: kv[1], reverse=True):
            if amount:
                lines.append(f"  {name:20s}  ${amount:,.2f}")
        if cost.get("unpriced"):
            lines.append("  not priced: " + ", ".join(cost["unpriced"]))
        if colder:
            lines.append("")
            lines.append("If the STANDARD bytes moved:")
            for name, amount, saving, caveat in colder:
                lines.append(
                    f"  {name:20s}  ${amount:,.2f}  "
                    f"(saves ${saving:,.2f})  — {caveat}")
        return ("<b>Estimated cost</b> "
                "<span style='color:#666;'>us-east-1 list price, storage "
                "only — requests, retrieval and transfer are not "
                "included</span><br><pre>" + "\n".join(lines) + "</pre>")

    def set_result(self, bucket_name: str, prefix: str, total: int, by_cat: dict,
                   by_top: dict, count: int = 0, by_class: dict = None,
                   largest=None, cost=None, colder=None):
        self._bucket = bucket_name
        title = f"<b>{bucket_name}</b>" + (f"<br><span style='color:#666'>/{prefix}</span>" if prefix else "")
        self.title.setText(title)

        self.total_lbl.setText(
            f"Total: <b>{_human_bytes(int(total))}</b>"
            + (f" in <b>{int(count)}</b> object(s)" if count else ""))

        norm_cat = {"Documents": 0, "Media": 0, "Other": 0}
        for k, v in (by_cat or {}).items():
            if k in norm_cat:
                norm_cat[k] = int(v or 0)

        self.pie.set_data(norm_cat)
        for k in ["Documents", "Media", "Other"]:
            self.legend_labels[k].setText(f"{k}: {_human_bytes(norm_cat[k])}")

        lines = []
        for k, v in (by_top or {}).items():
            lines.append(f"{k:24s}  {_human_bytes(int(v or 0))}")
        if not lines:
            lines = ["(empty)"]

        self.top_groups.setText("<b>Top groups</b><br><pre>" + "\n".join(lines) + "</pre>")

        class_lines = [
            f"{name:22s}  {_human_bytes(int(size or 0))}"
            for name, size in sorted((by_class or {}).items(),
                                     key=lambda kv: kv[1], reverse=True)
        ] or ["(empty)"]
        self.by_class.setText(
            "<b>Storage classes</b><br><pre>" + "\n".join(class_lines) + "</pre>")

        html = self.cost_html(cost, colder or [])
        self.cost.setText(html)
        self.cost.setVisible(bool(html))

        big_lines = [
            f"{_human_bytes(int(size or 0)):>10s}  {name}"
            for size, name in (largest or [])
        ] or ["(empty)"]
        self.largest.setText(
            "<b>Largest objects</b><br><pre>" + "\n".join(big_lines) + "</pre>")
