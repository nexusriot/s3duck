import sys

from PyQt5.QtWidgets import *
from PyQt5.QtCore import *
from PyQt5.QtGui import QIcon, QPixmap, QStandardItemModel, QStandardItem
from PyQt5.QtWidgets import QTreeWidget, QTreeWidgetItem, QApplication, QWidget

from model import Model as DataModel


class MyWindow(QMainWindow):
    def __init__(self):
        super(MyWindow, self).__init__()
        self.setWindowTitle("S3 Duck 🦆  for 🐧 0.0.1 PoC")
        self.setWindowIcon(QIcon.fromTheme("applications-internet"))
        self.listview = QTreeView()

        self.settings = QSettings("s3duck", "s3duck")
        self.clip = QApplication.clipboard()

        self.splitter = QSplitter()
        self.splitter.setOrientation(Qt.Horizontal)
        self.splitter.addWidget(self.listview)

        hlay = QHBoxLayout()
        hlay.addWidget(self.splitter)

        wid = QWidget()
        wid.setLayout(hlay)
        self.setCentralWidget(wid)
        self.setGeometry(0, 26, 900, 500)

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
        self.tBar.addAction(self.btnDownload)
        self.tBar.addSeparator()
        self.model = QStandardItemModel()

        self.model.setHorizontalHeaderLabels(['name', 'size', 'modified'])
        self.listview.header().setDefaultSectionSize(180)
        self.listview.setModel(self.model)

        settings = self.get_connection_settings()
        uri, region, bucket, key, access_key, secret_key = settings
        self.data_model = DataModel(
            uri,
            region,
            access_key,
            secret_key,
            key,
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
        self.listview.setDragDropMode(QAbstractItemView.DragDrop)

        self.listview.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.listview.setIndentation(10)
        self.restoreSettings()

    def modelToListView(self, model_result):
        if not model_result:
            self.model.setRowCount(0)
        else:
            self.model.setRowCount(0)
            for i in model_result:
                if i.type_ == 1:
                    icon = QIcon().fromTheme("go-first")
                    size = str(i.size)
                    modified = str(i.modified)
                else:
                    icon = QIcon().fromTheme("folder-remote")
                    size = "<DIR>"
                    modified = ""
                self.model.appendRow([
                    QStandardItem(icon, i.name),
                    QStandardItem(size),
                    QStandardItem(modified)])

    def change_current_folder(self, new_folder):
        self.data_model.prev_folder = self.data_model.current_folder
        self.data_model.current_folder = new_folder

    def navigate(self):
        self.modelToListView(self.data_model.list(self.data_model.current_folder))
        self.listview.sortByColumn(0, Qt.AscendingOrder)
        show_folder = (self.data_model.current_folder if self.data_model.current_folder else "/")
        self.statusBar().showMessage("path: %s" % show_folder, 0)

    def list_doubleClicked(self):
        index = self.listview.selectionModel().currentIndex()
        i = index.model().itemFromIndex(index)
        name = i.text()
        self.change_current_folder(self.data_model.current_folder + "%s/" % name)
        self.navigate()

    def goBack(self):
        self.change_current_folder(self.data_model.prev_folder)
        self.navigate()

    def download(self):
        pass

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

    def createActions(self):
        self.btnBack = QAction(QIcon.fromTheme("go-previous"), "go back", triggered=self.goBack)
        self.btnUp = QAction(QIcon.fromTheme("go-up"), "go up", triggered=self.goUp)
        self.btnHome = QAction(QIcon.fromTheme("go-home"), "home folder", triggered=self.goHome)
        self.btnDownload = QAction(QIcon.fromTheme("emblem-downloads"), "emblem-downloads", triggered=self.download)

    def restoreSettings(self):
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

    def get_connection_settings(self):
        uri = self.settings.value("uri")
        region = self.settings.value("region")
        bucket = self.settings.value("bucket")
        key = self.settings.value("key")
        access_key = self.settings.value("access_key")
        secret_key = self.settings.value("secret_key")
        return uri, region, bucket, key, access_key, secret_key

    def closeEvent(self, e):
        print("writing settings ...")
        self.writeSettings()

    def writeSettings(self):
        self.settings.setValue("pos", self.pos())
        self.settings.setValue("size", self.size())


if __name__ == '__main__':
    app = QApplication(sys.argv)
    w = MyWindow()
    w.show()
    if len(sys.argv) > 1:
        path = sys.argv[1]
        print(path)
        w.setWindowTitle(path)
    sys.exit(app.exec_())
