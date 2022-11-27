import os
from PyQt5.QtWidgets import *
from PyQt5.QtCore import *
from PyQt5.QtGui import QIcon, QStandardItemModel, QStandardItem
from PyQt5.QtWidgets import QApplication, QWidget, QFileDialog, QPlainTextEdit

from model import Model as DataModel

OS_FAMILY_MAP = {
    "Linux": "🐧",
    "Windows": "⊞ Win",
    "Darwin": "🍎",
}


__VERSION__ = "0.0.2"


class ListItem(QStandardItem):

    def __init__(self, downloadable, size, t, *args, **kwargs):
        self.downloadable = downloadable
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
            key, local_name, size = i
            msg = "Downloading %s -> %s (%s)" % (key, local_name, size)
            self.progress.emit(msg)
            self.data_model.download_file(key, local_name)
        self.finished.emit()

    def delete(self):
        for key in self.job:
            msg = "Moving %s -> /dev/null" % key
            self.progress.emit(msg)
            self.data_model.delete(key)
            self.refresh.emit()
        self.finished.emit()

    def upload(self):
        for i in self.job:
            key, local_name = i
            msg = "Uploading %s -> %s" % (local_name, key)
            self.progress.emit(msg)
            self.data_model.upload_file(local_name, key)
            self.refresh.emit()
        self.finished.emit()


class MainWindow(QMainWindow):
    def __init__(self, *args, **kwargs):
        settings = kwargs.pop('settings')
        super(MainWindow, self).__init__(*args, **kwargs)
        self.setWindowTitle("S3 Duck 🦆 %s PoC" % __VERSION__)
        self.setWindowIcon(QIcon.fromTheme("applications-internet"))
        self.listview = QTreeView()

        settings, profile_name, url, region, bucket, access_key, secret_key = settings
        self.settings = settings

        self.clip = QApplication.clipboard()
        self.splitter = QSplitter()
        self.splitter.setOrientation(Qt.Vertical)
        self.splitter.addWidget(self.listview)
        self.logview = QPlainTextEdit(self)
        self.splitter.addWidget(self.logview)
        self.logview.setReadOnly(True)
        self.logview.appendPlainText(
            "Welcome to S3 Duck 🦆 %s (on %s)" % (
                __VERSION__,
                OS_FAMILY_MAP.get(DataModel.get_os_family(), "❓")))
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
        self.tBar.setMovable(False)
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
        self.model = QStandardItemModel()

        self.model.setHorizontalHeaderLabels(['Name', 'Size', 'Modified'])
        self.listview.header().setDefaultSectionSize(180)
        self.listview.setModel(self.model)

        self.data_model = DataModel(
            url,
            region,
            access_key,
            secret_key,
            bucket
        )
        self.navigate()

        self.listview.header().resizeSection(0, 320)
        self.listview.header().resizeSection(1, 80)
        self.listview.header().resizeSection(2, 80)
        self.listview.doubleClicked.connect(self.list_doubleClicked)
        self.listview.setSortingEnabled(True)
        self.splitter.setSizes([20, 160])
        self.listview.setSelectionMode(QAbstractItemView.ExtendedSelection)
        # self.listview.setDragDropMode(QAbstractItemView.DragDrop)
        self.listview.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.listview.setIndentation(10)
        self.thread = None
        self.worker = None
        self.restoreSettings()

    def simple(self, title, message):
        QMessageBox(QMessageBox.Information,
                    title,
                    message,
                    QMessageBox.NoButton,
                    self,
                    Qt.Dialog | Qt.NoDropShadowWindowHint).show()

    def about(self):
        sysinfo = QSysInfo()
        sys_info = (
                sysinfo.prettyProductName() +
                "<br>" + sysinfo.kernelType() +
                " " + sysinfo.kernelVersion())
        title = "S3 Duck 🦆 %s" % __VERSION__
        message = """
                    <span style='color: #3465a4; font-size: 20pt;font-weight: bold;text-align: center;'
                    ></span></p><center><h3>S3 Duck 🦆
                    </h3></center><a title='Vladislav Ananev' href='https://github.com/nexusriot'
                     target='_blank'><br><span style='color: #8743e2; font-size: 10pt;'>
                     ©2022 Vladislav Ananev</a><br><br></strong></span></p>
                     """ + "version %s" % __VERSION__ + "<br><br>" + sys_info
        self.simple(title, message)

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
                if i.type_ == 1:
                    icon = QIcon().fromTheme("go-first")
                    size = str(i.size)
                    modified = str(i.modified)
                    downloadable = True
                else:
                    icon = QIcon().fromTheme("network-server")
                    size = "<DIR>"
                    modified = ""
                    downloadable = False
                self.model.appendRow([
                    ListItem(downloadable, size,  i.type_, icon, i.name),
                    ListItem(downloadable, size, i.type_, size),
                    ListItem(downloadable, size, i.type_, modified)])

    def change_current_folder(self, new_folder):
        self.data_model.prev_folder = self.data_model.current_folder
        self.data_model.current_folder = new_folder

    def navigate(self):
        self.modelToListView(self.data_model.list(self.data_model.current_folder))
        self.listview.sortByColumn(0, Qt.AscendingOrder)
        show_folder = (self.data_model.current_folder if self.data_model.current_folder else "/")
        self.statusBar().showMessage("[%s][%s] %s" % (
            self.profile_name, self.data_model.bucket, show_folder), 0)

    def get_elem_name(self):
        index = self.listview.selectionModel().currentIndex()
        i = index.model().itemFromIndex(index)
        return i.text(), i.t

    def list_doubleClicked(self):
        name, t = self.get_elem_name()
        if t == 2:
            self.change_current_folder(self.data_model.current_folder + "%s/" % name)
            self.navigate()

    def goBack(self):
        self.change_current_folder(self.data_model.prev_folder)
        self.navigate()

    def download(self):
        job = list()
        folder_path = QFileDialog.getExistingDirectory(self, 'Select Folder')
        if not folder_path:
            return
        for ix in self.listview.selectionModel().selectedIndexes():
            if ix.column() == 0:
                m = ix.model().itemFromIndex(ix)
                if not m.downloadable:
                    self.logview.appendPlainText("Skipping %s, because it's a directory" % m.text())
                    continue
                name = m.text()
                key = self.data_model.current_folder + name
                # todo join path
                local_name = folder_path + "/" + name
                job.append((key, local_name, m.size))
        self.logview.appendPlainText("Starting downloading")
        self.thread = QThread()
        self.worker = Worker(self.data_model, job)
        self.worker.moveToThread(self.thread)
        self.thread.started.connect(self.worker.download)
        self.worker.finished.connect(self.thread.quit)
        self.worker.finished.connect(self.worker.deleteLater)
        self.thread.finished.connect(self.thread.deleteLater)
        self.worker.progress.connect(self.report_logger_progress)
        self.thread.start()
        self.disable_action_buttons()
        self.thread.finished.connect(
            lambda: self.logview.appendPlainText("Download completed")
        )
        self.thread.finished.connect(
            lambda: self.enable_action_buttons()
        )

    def new_folder(self):
        name, ok = QInputDialog.getText(self, 'Create folder', 'Folder name')
        # TODO: try to make it better
        name.replace("/", "")
        if ok:
            key = self.data_model.current_folder + "%s/" % name
            self.data_model.create_folder(key)
            self.logview.appendPlainText("Created folder %s (%s)" %( name, key))
            self.navigate()

    def delete(self):
        names = list()
        job = list()
        for ix in self.listview.selectionModel().selectedIndexes():
            if ix.column() == 0:
                m = ix.model().itemFromIndex(ix)
                name = m.text()
                key = self.data_model.current_folder + name
                if m.t == 2:  # dir
                    key = key + "/"
                job.append(key)
                names.append(name)
        if names:
            qm = QMessageBox
            ret = qm.question(self, '', "Are you sure to delete objects : %s ?" % ",".join(names), qm.Yes | qm.No)
            if ret == qm.Yes:
                self.logview.appendPlainText("Starting deleting")
                # todo: remove duplicate code
                self.thread = QThread()
                self.worker = Worker(self.data_model, job)
                self.worker.moveToThread(self.thread)
                self.thread.started.connect(self.worker.delete)
                self.worker.finished.connect(self.thread.quit)
                self.worker.finished.connect(self.worker.deleteLater)
                self.thread.finished.connect(self.thread.deleteLater)
                self.worker.progress.connect(self.report_logger_progress)
                self.worker.refresh.connect(self.navigate)
                self.thread.start()
                self.disable_action_buttons()
                self.thread.finished.connect(
                    lambda: self.logview.appendPlainText("Deleting completed")
                )
                self.thread.finished.connect(
                    lambda: self.enable_action_buttons()
                )

    def upload(self):
        job = list()
        filter = "All files (*.*)"
        dialog = QFileDialog()
        dialog.setFileMode(QFileDialog.ExistingFiles)
        names = dialog.getOpenFileNames(self, "Open files", "", filter)
        if not all(map(lambda x: x, names)):
            return
        for name in names[0]:
            basename = os.path.basename(name)
            key = self.data_model.current_folder + basename
            job.append((key, name))
        self.logview.appendPlainText("Starting uploading")
        # TODO: common code?
        self.thread = QThread()
        self.worker = Worker(self.data_model, job)
        self.worker.moveToThread(self.thread)
        self.thread.started.connect(self.worker.upload)
        self.worker.finished.connect(self.thread.quit)
        self.worker.finished.connect(self.worker.deleteLater)
        self.thread.finished.connect(self.thread.deleteLater)
        self.worker.progress.connect(self.report_logger_progress)
        self.worker.refresh.connect(self.navigate)
        self.thread.start()
        self.disable_action_buttons()
        self.thread.finished.connect(
            lambda: self.logview.appendPlainText("Uploading completed")
        )
        self.thread.finished.connect(
            lambda: self.enable_action_buttons()
        )

    def enable_action_buttons(self):
        self.btnUpload.setEnabled(True)
        self.btnDownload.setEnabled(True)
        self.btnRemove.setEnabled(True)

    def disable_action_buttons(self):
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
        self.navigate()

    def goHome(self):
        self.change_current_folder("")
        self.navigate()

    def report_logger_progress(self, msg):
        self.logview.appendPlainText(msg)

    def createActions(self):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        self.btnBack = QAction(QIcon.fromTheme("go-previous", QIcon(os.path.join(
            current_dir, "icons/arrow_back_24px.svg"))), "back", triggered=self.goBack)
        self.btnUp = QAction(QIcon.fromTheme("go-up", QIcon(os.path.join(
            current_dir, "icons/arrow_upward_24px.svg"))), "up", triggered=self.goUp)
        self.btnHome = QAction(QIcon.fromTheme("go-home", QIcon(os.path.join(
            current_dir, "icons/home_24px.svg"))), "home", triggered=self.goHome)
        self.btnDownload = QAction(QIcon.fromTheme("emblem-downloads", QIcon(os.path.join(
            current_dir, "icons/download_24px.svg"))), "download", triggered=self.download)
        self.btnCreateFolder = QAction(QIcon.fromTheme("folder-new", QIcon(os.path.join(
            current_dir, "icons/create_new_folder_24px.svg"))), "new folder", triggered=self.new_folder)
        self.btnRemove = QAction(QIcon.fromTheme("edit-delete", QIcon(os.path.join(
            current_dir, "icons/delete_24px.svg"))), "delete", triggered=self.delete)
        self.btnRefresh = QAction(QIcon.fromTheme("view-refresh", QIcon(os.path.join(
            current_dir, "icons/refresh_24px.svg"))), "refresh", triggered=self.navigate)
        self.btnUpload = QAction(QIcon.fromTheme("network-server", QIcon(os.path.join(
            current_dir, "icons/file_upload_24px.svg"))), "upload", triggered=self.upload)
        self.btnAbout = QAction(QIcon.fromTheme("help-about", QIcon(os.path.join(
            current_dir, "icons/info_24px.svg"))), "about", triggered=self.about)

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
