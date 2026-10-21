"""Copy and move within one profile (across buckets too), and copy or sync
into another profile.
"""

from PyQt6.QtCore import QThread, Qt
from PyQt6.QtWidgets import (
    QAbstractItemView, QCheckBox, QComboBox, QDialog, QDialogButtonBox,
    QFormLayout, QGroupBox, QHBoxLayout, QLabel, QLineEdit, QMessageBox,
    QPushButton, QRadioButton, QTableWidget, QTableWidgetItem, QVBoxLayout,
)
from core.locations import build_profile_sync_job
from core.naming import _human_bytes
from core.sync_plan import build_sync_plan, summarize_sync_plan
from ui.widgets.profile_picker import ProfilePicker, build_profile_model
from utils import (
    DialogDismissMixin, FuncWorker, join_qthread, release_worker_on_finish,
)


class CopyMoveDialog(DialogDismissMixin, QDialog):
    def __init__(self, parent, model, item_count: int, current_prefix: str):
        super().__init__(parent)
        self.setWindowTitle("Copy / Move")
        self.setMinimumWidth(520)
        self.setWindowModality(Qt.WindowModality.ApplicationModal)

        self._model = model
        self._source_bucket = model.bucket
        self._thread = None
        self._worker = None

        src_lbl = QLabel(
            f"<b>{item_count} item(s)</b> from "
            f"<code>s3://{self._source_bucket}/{current_prefix}</code>"
        )
        src_lbl.setWordWrap(True)

        # Editable so a bucket can be typed even if ListBuckets is denied or
        # still loading; the dropdown fills in from a background listing.
        self.bucket_combo = QComboBox()
        self.bucket_combo.setEditable(True)
        self.bucket_combo.addItem(self._source_bucket)
        self.bucket_combo.setCurrentText(self._source_bucket)

        self.dst_edit = QLineEdit(current_prefix)
        self.dst_edit.setPlaceholderText("e.g.  archive/2024/  (empty = bucket root)")

        form = QFormLayout()
        form.addRow(QLabel("Destination bucket"), self.bucket_combo)
        form.addRow(QLabel("Destination prefix"), self.dst_edit)

        self._note = QLabel("")
        self._note.setWordWrap(True)
        self.bucket_combo.currentTextChanged.connect(self._on_bucket_changed)

        op_group = QGroupBox("Operation")
        self.rb_copy = QRadioButton("Copy")
        self.rb_move = QRadioButton("Move  (copy then delete originals)")
        self.rb_copy.setChecked(True)
        op_lay = QVBoxLayout()
        op_lay.addWidget(self.rb_copy)
        op_lay.addWidget(self.rb_move)
        op_group.setLayout(op_lay)

        btns = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        btns.accepted.connect(self.accept)
        btns.rejected.connect(self.reject)

        lay = QVBoxLayout()
        lay.addWidget(src_lbl)
        lay.addSpacing(4)
        lay.addLayout(form)
        lay.addWidget(self._note)
        lay.addWidget(op_group)
        lay.addWidget(btns)
        self.setLayout(lay)

        self._load_buckets()

    def _load_buckets(self):
        """Populate the bucket dropdown without blocking the dialog."""
        clone = self._model.clone_for_worker()

        def _fetch(_w):
            return [b.name for b in clone.list_buckets()]

        self._thread = QThread(self)
        self._worker = FuncWorker(_fetch)
        self._worker.moveToThread(self._thread)
        self._thread.started.connect(self._worker.run)
        self._worker.done.connect(self._on_buckets)
        self._worker.done.connect(self._thread.quit)
        release_worker_on_finish(self._thread, self._worker)
        self._thread.finished.connect(self._thread.deleteLater)
        self._thread.start()

    def stop_threads(self):
        self._stop_loader()

    def _stop_loader(self):
        th, self._thread, self._worker = self._thread, None, None
        join_qthread(th)

    def _on_buckets(self, result, exc):
        self._stop_loader()
        if exc is not None or not result:
            return
        typed = self.bucket_combo.currentText()
        self.bucket_combo.blockSignals(True)
        self.bucket_combo.clear()
        self.bucket_combo.addItems(result)
        self.bucket_combo.setCurrentText(typed or self._source_bucket)
        self.bucket_combo.blockSignals(False)

    def _on_bucket_changed(self, name: str):
        if (name or "").strip() != self._source_bucket:
            self._note.setText(
                "Cross-bucket copy is server-side and requires the target "
                "bucket to be reachable on the same endpoint/region."
            )
        else:
            self._note.setText("")

    def destination(self) -> str:
        return self.dst_edit.text().strip()

    def destination_bucket(self) -> str:
        return self.bucket_combo.currentText().strip() or self._source_bucket

    def is_cross_bucket(self) -> bool:
        return self.destination_bucket() != self._source_bucket

    def is_move(self) -> bool:
        return self.rb_move.isChecked()


class CrossProfileCopyDialog(QDialog):
    """
    Choose another profile, bucket and prefix to copy the selection into.

    Server-side copy cannot cross credentials, so this is the only way to move
    objects between accounts or providers; the bytes travel through this
    process, which the note makes explicit.
    """

    def __init__(self, parent, settings, count, current_profile=""):
        super().__init__(parent)
        self.setWindowTitle("Copy to another profile")
        self.setMinimumWidth(480)
        self.picker = ProfilePicker(self, settings, current_profile)
        self.picker.note.setText(
            "Objects are streamed through this machine, because a server-side "
            "copy cannot use two sets of credentials.")

        self._buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok
            | QDialogButtonBox.StandardButton.Cancel)
        self._buttons.accepted.connect(self.accept)
        self._buttons.rejected.connect(self.reject)

        lay = QVBoxLayout(self)
        lay.addWidget(QLabel(f"Copying {count} item(s)."))
        lay.addWidget(self.picker)
        lay.addWidget(self.picker.note)
        lay.addWidget(self._buttons)

        if not self.picker.has_profiles():
            self.picker.note.setText("No other profile is configured.")
            self._buttons.button(
                QDialogButtonBox.StandardButton.Ok).setEnabled(False)

    def selected_profile(self):
        return self.picker.selected_profile()

    def destination_bucket(self) -> str:
        return self.picker.destination_bucket()

    def destination_prefix(self) -> str:
        return self.picker.destination_prefix()

    def closeEvent(self, event):
        self.picker.stop_loader()
        super().closeEvent(event)


class CrossProfileSyncDialog(QDialog):
    """
    Compare this prefix against a prefix in another profile, then run the plan.

    The dry run is the point: a cross-account sync moves bytes through this
    machine and can delete at the far end, so nothing happens until the plan
    has been seen.
    """

    def __init__(self, parent, main_window, model, prefix, settings,
                 current_profile=""):
        super().__init__(parent)
        self._mw = main_window
        self._model = model
        self._prefix = prefix or ""
        self._actions = []
        self._dest_model = None
        self.setWindowTitle(f"Sync to another profile — "
                            f"{model.bucket}/{self._prefix}")
        self.resize(820, 560)

        self.picker = ProfilePicker(self, settings, current_profile)

        self._direction = QComboBox()
        self._direction.addItem("Push: this profile → the other", "push")
        self._direction.addItem("Pull: the other → this profile", "pull")

        self._delete_extra = QCheckBox(
            "Delete items at the destination that the source does not have")
        self._exclude = QLineEdit()
        self._exclude.setPlaceholderText("exclude globs, comma separated")

        self._compare = QPushButton("Compare")
        self._compare.clicked.connect(self.compare)

        self._table = QTableWidget(0, 4)
        self._table.setHorizontalHeaderLabels(
            ["Action", "Path", "Size", "Why"])
        self._table.horizontalHeader().setStretchLastSection(True)
        self._table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)

        self._summary = QLabel("Nothing compared yet.")
        self._summary.setWordWrap(True)

        self._buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok
            | QDialogButtonBox.StandardButton.Cancel)
        self._buttons.button(
            QDialogButtonBox.StandardButton.Ok).setText("Run plan")
        self._buttons.accepted.connect(self.accept)
        self._buttons.rejected.connect(self.reject)
        self._ok_enabled(False)

        row = QHBoxLayout()
        row.addWidget(self._direction, 1)
        row.addWidget(self._compare)

        lay = QVBoxLayout(self)
        lay.addWidget(self.picker)
        lay.addLayout(row)
        lay.addWidget(self._exclude)
        lay.addWidget(self._delete_extra)
        lay.addWidget(self._table, 1)
        lay.addWidget(self._summary)
        lay.addWidget(self.picker.note)
        lay.addWidget(self._buttons)

        # A reviewed plan belongs to the inputs it was built from. Leaving it
        # runnable after they change let a plan reviewed as a push execute
        # after switching to pull, deleting on the side believed to be the
        # source.
        self._direction.currentIndexChanged.connect(self.invalidate_plan)
        self.picker.profile.currentIndexChanged.connect(self.invalidate_plan)
        self.picker.bucket.currentTextChanged.connect(self.invalidate_plan)
        self.picker.prefix.textChanged.connect(self.invalidate_plan)
        self._exclude.textChanged.connect(self.invalidate_plan)
        self._delete_extra.toggled.connect(self.invalidate_plan)

        if not self.picker.has_profiles():
            self.picker.note.setText("No other profile is configured.")
            self._compare.setEnabled(False)

    def invalidate_plan(self, *_args):
        """Drop the computed plan and everything derived from it."""
        if not self._actions:
            return
        self._actions = []
        self._dest_model = None
        self._source_model = None
        self._source_prefix = ""
        self._dest_prefix = ""
        self._table.setRowCount(0)
        self._summary.setText("Inputs changed — compare again.")
        self._ok_enabled(False)

    def _ok_enabled(self, enabled):
        self._buttons.button(
            QDialogButtonBox.StandardButton.Ok).setEnabled(bool(enabled))

    def direction(self) -> str:
        return self._direction.currentData() or "push"

    def compare(self):
        """Build the dry-run plan, listing both trees off the GUI thread."""
        bucket = self.picker.destination_bucket()
        if not bucket:
            self.picker.note.setText("Choose a bucket in the other profile.")
            return
        try:
            profile = self.picker.selected_profile()
        except Exception as exc:
            self.picker.note.setText(f"Could not read that profile: {exc}")
            return
        if profile is None:
            return

        other = build_profile_model(profile, bucket)
        here = self._model.clone_for_worker()
        other_prefix = self.picker.destination_prefix()
        mine_prefix = self._prefix
        excludes = self._exclude.text()
        delete_extra = self._delete_extra.isChecked()
        pushing = self.direction() == "push"

        def _scan(_worker):
            mine = here.list_tree(mine_prefix)
            theirs = other.list_tree(other_prefix)
            source, dest = (mine, theirs) if pushing else (theirs, mine)
            return build_sync_plan(
                source, dest, direction="upload",
                delete_extra=delete_extra, exclude=excludes)

        actions, exc = self._mw._run_with_progress("Comparing…", _scan)
        if exc is not None:
            QMessageBox.warning(self, "Sync to another profile",
                                f"Could not compare:\n{exc}")
            return
        if actions is None:
            return

        self._actions = actions
        self._dest_model = other if pushing else self._model.clone_for_worker()
        self._source_prefix = mine_prefix if pushing else other_prefix
        self._dest_prefix = other_prefix if pushing else mine_prefix
        self._source_model = here if pushing else other
        self._render(actions)

    def _render(self, actions):
        self._table.setRowCount(0)
        for action in actions:
            row = self._table.rowCount()
            self._table.insertRow(row)
            self._table.setItem(row, 0, QTableWidgetItem(action["action"]))
            self._table.setItem(row, 1, QTableWidgetItem(action["rel"]))
            self._table.setItem(
                row, 2, QTableWidgetItem(_human_bytes(action.get("size") or 0)))
            self._table.setItem(row, 3, QTableWidgetItem(action.get("reason", "")))
        # summarize_sync_plan returns counts, not text. Handing the dict
        # straight to setText raised TypeError inside the Compare slot, which
        # PyQt6 turns into a process abort -- every successful compare took
        # the app down. Spelled out the way the local sync dialog does it.
        summary = summarize_sync_plan(actions)
        bits = [f"{kind}: {count}" for kind, count in sorted(summary.items())
                if kind != "bytes" and count]
        self._summary.setText(
            ("  ".join(bits) or "Nothing to compare")
            + f"   ({_human_bytes(summary['bytes'])} to transfer)")
        self._ok_enabled(any(a["action"] != "skip" for a in actions))

    def plan(self) -> tuple:
        """``(source_model, dest_model, job)`` for the transfer queue."""
        job = build_profile_sync_job(
            self._actions, getattr(self, "_source_prefix", ""),
            getattr(self, "_dest_prefix", ""))
        return getattr(self, "_source_model", None), self._dest_model, job

    def closeEvent(self, event):
        self.picker.stop_loader()
        super().closeEvent(event)
