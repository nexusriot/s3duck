import os
import glob
import pathlib
from PyQt5.QtWidgets import *
from PyQt5.QtCore import *
from PyQt5.QtGui import QIcon, QStandardItemModel, QStandardItem
from PyQt5.QtWidgets import QApplication, QWidget, QFileDialog, QPlainTextEdit

from model import Model as DataModel
from model import FSObjectType

from properties_window import PropertiesWindow


OS_FAMILY_MAP = {"Linux": "🐧", "Windows": "⊞ Win", "Darwin": " MacOS"}

__VERSION__ = "0.0.9"


class Tree(QTreeView):
    def __init__(self, parent):
        self.parent = parent
        QTreeView.__init__(self)
        self.setDragDropMode(QAbstractItemView.InternalMove)
        self.enable_drag_drop()

    def enable_drag_drop(self):
        self.setAcceptDrops(True)
        self.setDropIndicatorShown(True)

    def disable_drag_drop(self):
        self.setAcceptDrops(False)
        self.setDropIndicatorShown(False)

    def dragEnterEvent(self, event):
        widget = event.source()

        if widget == self:
            event.ignore()
            return

        if event.mimeData().hasUrls:
            event.accept()
        else:
            event.ignore()
        event.accept()

    def dragMoveEvent(self, event):
        widget = event.source()
        if widget == self:
            event.ignore()
            return

        if event.mimeData().hasUrls:
            event.setDropAction(Qt.MoveAction)
            event.accept()
        else:
            event.ignore()

    def dropEvent(self, event):
        widget = event.source()
        if widget == self:
            event.ignore()
            return

        if event.mimeData().hasUrls:
            event.setDropAction(Qt.CopyAction)
            event.accept()

            job = list()
            for url in event.mimeData().urls():
                path = str(url.toLocalFile())
                base_path, tail = os.path.split(path)
                if os.path.isdir(path):
                    for filename in glob.iglob(path + "**/**", recursive=True):
                        key = pathlib.Path(
                            os.path.join(
                                self.parent.data_model.current_folder,
                                os.path.relpath(filename, base_path),
                            )
                        ).as_posix()
                        if os.path.isdir(filename):
                            # append folder
                            job.append((key, None))
                        else:
                            job.append((key, filename))
                else:
                    key = pathlib.Path(
                        os.path.join(
                            self.parent.data_model.current_folder,
                            os.path.relpath(path, base_path),
                        )
                    ).as_posix()
                    job.append((key, path))
            self.disable_drag_drop()
            self.parent.assign_thread_operation("upload", job)
            self.parent.thread.finished.connect(lambda: self.enable_drag_drop())
        else:
            event.ignore()


class ListItem(QStandardItem):
    def __init__(self, size, t, *args, **kwargs):
        self.size = size
        self.t = t
        super().__init__(*args, **kwargs)


class Worker(QObject):
    finished = pyqtSignal()
    progress = pyqtSignal(str)
    refresh = pyqtSignal()

    def __init__(self, data_model, job):
        self.data_model = data_model
        self.job = job
        super().__init__()

    def download(self):
        for i in self.job:
            key, local_name, size, folder_path = i
            if local_name:
                msg = "downloading %s -> %s (%s)" % (key, local_name, size)
            else:
                msg = "downloading directory: %s ->%s" % (key, folder_path)
            self.progress.emit(msg)
            self.data_model.download_file(key, local_name, folder_path)
        self.finished.emit()

    def delete(self):
        for key in self.job:
            msg = "moving %s -> /dev/null" % key
            self.progress.emit(msg)
            self.data_model.delete(key)
            self.refresh.emit()
        self.finished.emit()

    def upload(self):
        for i in self.job:
            key, local_name = i
            if local_name is not None:
                msg = "uploading %s -> %s" % (local_name, key)
            else:
                msg = "creating folder %s" % key
            self.progress.emit(msg)
            self.data_model.upload_file(local_name, key)
            self.refresh.emit()
        self.finished.emit()


class MainWindow(QMainWindow):
    def __init__(self, *args, **kwargs):
        settings = kwargs.pop("settings")
        super(MainWindow, self).__init__(*args, **kwargs)
        self.setWindowTitle("S3 Duck 🦆 %s PoC" % __VERSION__)
        self.setWindowIcon(QIcon.fromTheme("applications-internet"))

        (
            current_dir,
            settings,
            profile_name,
            url,
            region,
            bucket,
            access_key,
            secret_key,
            no_ssl_check,
            use_path,
        ) = settings
        self.settings = settings
        self.current_dir = current_dir
        self.data_model = DataModel(
            url, region, access_key, secret_key, bucket, no_ssl_check, use_path
        )
        self.logview = QPlainTextEdit(self)
        self.listview = Tree(self)
        self.clip = QApplication.clipboard()
        self.splitter = QSplitter()
        self.splitter.setOrientation(Qt.Vertical)
        self.splitter.addWidget(self.listview)
        self.splitter.addWidget(self.logview)
        self.logview.setReadOnly(True)
        self.logview.appendPlainText(
            "Welcome to S3 Duck 🦆 %s (on %s)"
            % (__VERSION__, OS_FAMILY_MAP.get(DataModel.get_os_family(), "❓"))
        )
        hlay = QHBoxLayout()
        hlay.addWidget(self.splitter)

        wid = QWidget()
        wid.setLayout(hlay)
        self.setCentralWidget(wid)
        self.setGeometry(0, 26, 900, 500)
        self.profile_name = profile_name
        self.copyPath = ""
        self.copyList = []
        self.copyListNew = ""
        self.createActions()

        self.tBar = self.addToolBar("Tools")
        self.tBar.setContextMenuPolicy(Qt.PreventContextMenu)
        self.tBar.setMovable(True)
        self.tBar.setIconSize(QSize(16, 16))
        self.tBar.addSeparator()
        self.tBar.addAction(self.btnHome)
        self.tBar.addAction(self.btnBack)
        self.tBar.addAction(self.btnUp)
        self.tBar.addAction(self.btnRefresh)
        self.tBar.addSeparator()
        self.tBar.addAction(self.btnDownload)
        self.tBar.addAction(self.btnUpload)
        self.tBar.addSeparator()
        self.tBar.addAction(self.btnCreateFolder)
        self.tBar.addAction(self.btnRemove)
        self.tBar.addSeparator()
        self.tBar.addAction(self.btnAbout)
        self.tBar.setIconSize(QSize(26, 26))
        self.model = QStandardItemModel()

        self.model.setHorizontalHeaderLabels(["Name", "Size", "Modified"])
        self.listview.header().setDefaultSectionSize(180)
        self.listview.setModel(self.model)
        self.navigate()

        self.listview.header().resizeSection(0, 320)
        self.listview.header().resizeSection(1, 80)
        self.listview.header().resizeSection(2, 80)

        self.listview.doubleClicked.connect(self.list_doubleClicked)
        self.listview.setSortingEnabled(True)
        self.splitter.setSizes([20, 160])
        self.listview.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.listview.setDragDropMode(QAbstractItemView.DragDrop)
        self.listview.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.listview.setIndentation(10)
        self.thread = None
        self.worker = None
        self.map = dict()
        self.setWindowIcon(
            QIcon(os.path.join(self.current_dir, "resources", "ducky.ico"))
        )
        self.listview.installEventFilter(self)
        self.restoreSettings()
        self.select_first()
        self.menu = QMenu()

    def select_first(self):
        if self.listview.model().rowCount() > 0:
            index = self.listview.model().index(0, 0)
            self.listview.setCurrentIndex(index)

    def ix_by_name(self, name):
        for r in range(self.listview.model().rowCount()):
            ix = self.listview.model().index(r, 0)
            if name == self.listview.model().itemFromIndex(ix).text():
                return ix

    def name_by_first_ix(self, ixs):
        if ixs:
            ix = ixs[0]
            if ix.column() == 0:
                m = ix.model().itemFromIndex(ix)
                name = m.text()
                if m.t == FSObjectType.FOLDER:
                    name = "%s/" % name
                return m, name, self.data_model.current_folder + name
        return None, None, None

    def eventFilter(self, obj, event):
        if obj == self.listview:
            if event.type() == QEvent.ContextMenu and obj is self.listview:
                upload_selected_action = (
                    delete_action
                ) = download_action = properties_selected_action = QObject()
                ixs = self.listview.selectedIndexes()
                m, name, upload_path = self.name_by_first_ix(ixs)
                if upload_path is None:
                    upload_path = self.data_model.current_folder
                self.menu.clear()
                if name:
                    if m.t == FSObjectType.FOLDER:
                        upload_selected_action = QAction(
                            QIcon.fromTheme(
                                "network-server",
                                QIcon(
                                    os.path.join(
                                        self.current_dir,
                                        "icons",
                                        "file_upload_24px.svg",
                                    )
                                ),
                            ),
                            "Upload -> %s" % upload_path,
                        )
                        self.menu.addAction(upload_selected_action)
                upload_current_action = QAction(
                    QIcon.fromTheme(
                        "network-server",
                        QIcon(
                            os.path.join(
                                self.current_dir, "icons", "file_upload_24px.svg"
                            )
                        ),
                    ),
                    "Upload -> %s"
                    % (
                        "/"
                        if not self.data_model.current_folder
                        else self.data_model.current_folder
                    ),
                )
                self.menu.addAction(upload_current_action)
                create_folder_action = QAction(
                    QIcon.fromTheme(
                        "folder-new",
                        QIcon(
                            os.path.join(
                                self.current_dir, "icons", "create_new_folder_24px.svg"
                            )
                        ),
                    ),
                    "Create folder",
                )
                self.menu.addAction(create_folder_action)
                if ixs:
                    download_action = QAction(
                        QIcon.fromTheme(
                            "emblem-downloads",
                            QIcon(
                                os.path.join(
                                    self.current_dir, "icons", "download_24px.svg"
                                )
                            ),
                        ),
                        "Download",
                    )
                    self.menu.addAction(download_action)
                    delete_action = QAction(
                        QIcon.fromTheme(
                            "edit-delete",
                            QIcon(
                                os.path.join(
                                    self.current_dir, "icons", "delete_24px.svg"
                                )
                            ),
                        ),
                        "Delete",
                    )
                    self.menu.addAction(delete_action)
                m, name, key = self.name_by_first_ix(ixs)
                if not key:
                    key = self.data_model.current_folder
                if name:
                    properties_selected_action = QAction(
                        QIcon.fromTheme(
                            "document-properties",
                            QIcon(
                                os.path.join(
                                    self.current_dir, "icons", "puzzle_24px.svg"
                                )
                            ),
                        ),
                        "Properties",
                    )
                    self.menu.addAction(properties_selected_action)
                clk = self.menu.exec_(event.globalPos())
                if clk == upload_selected_action:
                    self.upload(upload_path)
                if clk == upload_current_action:
                    self.upload()
                if clk == delete_action:
                    self.delete()
                if clk == download_action:
                    self.download()
                if clk == create_folder_action:
                    self.new_folder()
                if clk == properties_selected_action:
                    self.properties(self.data_model, key)
            if event.type() == QEvent.KeyPress:
                if event.key() == Qt.Key_Return:
                    self.list_doubleClicked()
                if event.key() == Qt.Key_Delete:
                    self.delete()
                if event.key() == Qt.Key_Backspace:
                    self.goUp()
                if event.key() in [Qt.Key_Insert, Qt.Key_C]:
                    self.new_folder()
                if event.key() == Qt.Key_B:
                    self.goBack()
                if event.key() in [Qt.Key_H, Qt.Key_Home]:
                    self.goHome()
                if event.key() == Qt.Key_A:
                    self.about()
                if event.key() == Qt.Key_U:
                    self.upload()
                if event.key() == Qt.Key_D:
                    self.download()
        return super(MainWindow, self).eventFilter(obj, event)

    def simple(self, title, message):
        QMessageBox(
            QMessageBox.Information,
            title,
            message,
            QMessageBox.NoButton,
            self,
            Qt.Dialog | Qt.NoDropShadowWindowHint,
        ).show()

    def about(self):
        sysinfo = QSysInfo()
        sys_info = (
            sysinfo.prettyProductName()
            + "<br>"
            + sysinfo.kernelType()
            + " "
            + sysinfo.kernelVersion()
        )
        title = "S3 Duck 🦆 %s" % __VERSION__
        message = (
            """
                    <span style='color: #3465a4; font-size: 20pt;font-weight: bold;text-align: center;'
                    ></span></p><center><h3>S3 Duck 🦆
                    </h3></center><a title='Vladislav Ananev' href='https://github.com/nexusriot'
                     target='_blank'><br><span style='color: #8743e2; font-size: 10pt;'>
                     ©2022 Vladislav Ananev</a><br><br></strong></span></p>
                     """
            + "version %s" % __VERSION__
            + "<br><br>"
            + sys_info
        )
        self.simple(title, message)

    def properties(self, model, key):
        properties = PropertiesWindow(self, settings=(model, key))
        properties.exec_()

    def modelToListView(self, model_result):
        """
        Converts data mode items to List Items
        :param model_result:
        :return:
        """
        if not model_result:
            self.model.setRowCount(0)
        else:
            self.model.setRowCount(0)
            for i in model_result:
                if i.type_ == FSObjectType.FILE:
                    icon = QIcon().fromTheme(
                        "go-first",
                        QIcon(
                            os.path.join(self.current_dir, "icons", "document_24px.svg")
                        ),
                    )
                    size = str(i.size)
                    modified = str(i.modified)
                else:
                    icon = QIcon().fromTheme(
                        "network-server",
                        QIcon(
                            os.path.join(self.current_dir, "icons", "folder_24px.svg")
                        ),
                    )
                    size = "<DIR>"
                    modified = ""
                self.model.appendRow(
                    [
                        ListItem(size, i.type_, icon, i.name),
                        ListItem(size, i.type_, size),
                        ListItem(size, i.type_, modified),
                    ]
                )

    def change_current_folder(self, new_folder):
        self.data_model.prev_folder = self.data_model.current_folder
        self.data_model.current_folder = new_folder
        return self.data_model.current_folder

    def navigate(self, restore_last_index=False):
        self.modelToListView(self.data_model.list(self.data_model.current_folder))
        self.listview.sortByColumn(0, Qt.AscendingOrder)
        show_folder = (
            self.data_model.current_folder if self.data_model.current_folder else "/"
        )
        self.statusBar().showMessage(
            "[%s][%s] %s" % (self.profile_name, self.data_model.bucket, show_folder), 0
        )
        if restore_last_index and self.data_model.prev_folder:
            name = self.map.get(self.data_model.current_folder)
            if name:
                ix = self.ix_by_name(name)
                if ix:
                    self.listview.setCurrentIndex(ix)

    def get_elem_name(self):
        index = self.listview.selectionModel().currentIndex()
        if index.model():
            i = index.model().itemFromIndex(index)
            return i.text(), i.t

    def list_doubleClicked(self):
        selection = self.listview.selectionModel().selectedIndexes()
        if selection:
            name, t = self.get_elem_name()
            if t == FSObjectType.FOLDER:
                self.map[self.data_model.current_folder] = name
                self.change_current_folder(
                    self.data_model.current_folder + "%s/" % name
                )
                self.navigate()

    def goBack(self):
        self.change_current_folder(self.data_model.prev_folder)
        self.navigate()

    def download(self):
        job = list()
        folder_path = QFileDialog.getExistingDirectory(self, "Select Folder")
        if not folder_path:
            # nothing selected
            return
        for ix in self.listview.selectionModel().selectedIndexes():
            if ix.column() == 0:
                m = ix.model().itemFromIndex(ix)
                name = m.text()
                key = self.data_model.current_folder + name
                if m.t == FSObjectType.FOLDER:
                    job.append((key, None, None, folder_path))
                    # got a folder
                    continue
                local_name = os.path.join(folder_path, name)
                job.append((key, local_name, m.size, folder_path))
        self.assign_thread_operation("download", job, need_refresh=False)

    def assign_thread_operation(self, method, job, need_refresh=True):
        """
        Runs jobs in the separate thread
        :param method:
        :param job:
        :param need_refresh:
        :return:
        """
        if not job:
            return
        self.logview.appendPlainText("starting %s" % method)
        self.thread = QThread()
        self.worker = Worker(self.data_model, job)
        self.worker.moveToThread(self.thread)
        m = getattr(self.worker, method)
        self.thread.started.connect(m)
        self.worker.finished.connect(self.thread.quit)
        self.worker.finished.connect(self.worker.deleteLater)
        self.thread.finished.connect(self.thread.deleteLater)
        self.worker.progress.connect(self.report_logger_progress)
        if need_refresh:
            self.worker.refresh.connect(self.navigate)
        self.thread.start()
        self.disable_action_buttons()
        self.thread.finished.connect(
            lambda: self.logview.appendPlainText("%s completed" % method)
        )
        self.thread.finished.connect(lambda: self.enable_action_buttons())

    def new_folder(self):
        name, ok = QInputDialog.getText(self, "Create folder", "Folder name")
        # TODO: try to make it better
        name.replace("/", "")
        if ok:
            key = self.data_model.current_folder + "%s/" % name
            self.data_model.create_folder(key)
            self.logview.appendPlainText("Created folder %s (%s)" % (name, key))
            self.navigate()
            ix = self.ix_by_name(name)
            if ix:
                self.listview.setCurrentIndex(ix)

    def delete(self):
        names = list()
        job = list()
        for ix in self.listview.selectionModel().selectedIndexes():
            if ix.column() == 0:
                m = ix.model().itemFromIndex(ix)
                name = m.text()
                key = self.data_model.current_folder + name
                if m.t == FSObjectType.FOLDER:  # dir
                    key = key + "/"
                job.append(key)
                names.append(name)
        if names:
            qm = QMessageBox
            ret = qm.question(
                self,
                "",
                "Are you sure to delete objects : %s ?" % ",".join(names),
                qm.Yes | qm.No,
            )
            if ret == qm.Yes:
                self.assign_thread_operation("delete", job)

    def upload(self, folder=None):
        job = list()
        filter = "All files (*)"
        dialog = QFileDialog()
        dialog.setFileMode(QFileDialog.ExistingFiles)
        names = dialog.getOpenFileNames(self, "Open files", "", filter)
        if not all(map(lambda x: x, names)):
            return
        for name in names[0]:
            basename = os.path.basename(name)
            if folder:
                key = folder + "/" + basename
            else:
                key = self.data_model.current_folder + basename
            job.append((key, name))
        self.assign_thread_operation("upload", job)

    def enable_action_buttons(self):
        self.btnCreateFolder.setEnabled(True)
        self.btnUpload.setEnabled(True)
        self.btnDownload.setEnabled(True)
        self.btnRemove.setEnabled(True)
        self.menu.setEnabled(True)

    def disable_action_buttons(self):
        self.menu.setEnabled(False)
        self.btnCreateFolder.setEnabled(False)
        self.btnUpload.setEnabled(False)
        self.btnDownload.setEnabled(False)
        self.btnRemove.setEnabled(False)

    def goUp(self):
        p = self.data_model.current_folder
        new_path_list = p.split("/")[:-2]
        new_path = "/".join(new_path_list)
        if new_path:
            new_path = new_path + "/"
        self.change_current_folder(new_path)
        self.navigate(True)
        self.map.pop(p, None)

    def goHome(self):
        self.change_current_folder("")
        self.navigate()

    def report_logger_progress(self, msg):
        self.logview.appendPlainText(msg)

    def createActions(self):
        self.btnBack = QAction(
            QIcon.fromTheme(
                "go-previous",
                QIcon(os.path.join(self.current_dir, "icons", "arrow_back_24px.svg")),
            ),
            "Back(B)",
            triggered=self.goBack,
        )
        self.btnUp = QAction(
            QIcon.fromTheme(
                "go-up",
                QIcon(os.path.join(self.current_dir, "icons", "arrow_upward_24px.svg")),
            ),
            "Up(Backspace)",
            triggered=self.goUp,
        )
        self.btnHome = QAction(
            QIcon.fromTheme(
                "go-home",
                QIcon(os.path.join(self.current_dir, "icons", "home_24px.svg")),
            ),
            "Home(Home, H)",
            triggered=self.goHome,
        )
        self.btnDownload = QAction(
            QIcon.fromTheme(
                "emblem-downloads",
                QIcon(os.path.join(self.current_dir, "icons", "download_24px.svg")),
            ),
            "Download(D)",
            triggered=self.download,
        )
        self.btnCreateFolder = QAction(
            QIcon.fromTheme(
                "folder-new",
                QIcon(
                    os.path.join(
                        self.current_dir, "icons", "create_new_folder_24px.svg"
                    )
                ),
            ),
            "Create folder(Insert, C)",
            triggered=self.new_folder,
        )
        self.btnRemove = QAction(
            QIcon.fromTheme(
                "edit-delete",
                QIcon(os.path.join(self.current_dir, "icons", "delete_24px.svg")),
            ),
            "Delete(Delete)",
            triggered=self.delete,
        )
        self.btnRefresh = QAction(
            QIcon.fromTheme(
                "view-refresh",
                QIcon(os.path.join(self.current_dir, "icons", "refresh_24px.svg")),
            ),
            "Refresh(R)",
            triggered=self.navigate,
        )
        self.btnUpload = QAction(
            QIcon.fromTheme(
                "network-server",
                QIcon(os.path.join(self.current_dir, "icons", "file_upload_24px.svg")),
            ),
            "Upload(U)",
            triggered=self.upload,
        )
        self.btnAbout = QAction(
            QIcon.fromTheme(
                "help-about",
                QIcon(os.path.join(self.current_dir, "icons", "info_24px.svg")),
            ),
            "About(A)",
            triggered=self.about,
        )

    def restoreSettings(self):
        self.settings.beginGroup("geometry")
        if self.settings.contains("pos"):
            pos = self.settings.value("pos", QPoint(200, 200))
            self.move(pos)
        else:
            self.move(0, 26)
        if self.settings.contains("size"):
            size = self.settings.value("size", QSize(800, 600))
            self.resize(size)
        else:
            self.resize(800, 600)
        self.settings.endGroup()

    def closeEvent(self, e):
        self.writeSettings()

    def writeSettings(self):
        # save only window geometry
        self.settings.beginGroup("geometry")
        self.settings.setValue("pos", self.pos())
        self.settings.setValue("size", self.size())
        self.settings.endGroup()
