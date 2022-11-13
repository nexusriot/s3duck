import datetime
import sys

from PyQt5.QtWidgets import *
from PyQt5.QtCore import *
from PyQt5.QtGui import QIcon, QPixmap, QStandardItemModel, QStandardItem
from PyQt5.QtWidgets import QTreeWidget, QTreeWidgetItem, QApplication, QWidget

from qjsonmodel import QJsonModel, QJsonTreeItem
# from collections import deque


class MyWindow(QMainWindow):
    def __init__(self):
        super(MyWindow, self).__init__()
        self.setWindowTitle("S3 Duck 🦆  for 🐧 0.0.1")
        self.setWindowIcon(QIcon.fromTheme("document-new"))
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
        self.createStatusBar()
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
        self.tBar.addSeparator()

        self.model = QStandardItemModel()

        self.model.setHorizontalHeaderLabels(['name', 'size', 'modified'])
        self.listview.header().setDefaultSectionSize(180)
        self.listview.setModel(self.model)
        # demo set
        self.model.appendRow([
            QStandardItem(QIcon().fromTheme("folder-remote"), 'docs'),
            QStandardItem("<DIR>"),
            QStandardItem("")])
        self.model.appendRow([
            QStandardItem(QIcon().fromTheme("folder-remote"), 'video'),
            QStandardItem("<DIR>"),
            QStandardItem("")])
        self.model.appendRow([
            QStandardItem(QIcon().fromTheme("document-new"), 'file.day'),
            QStandardItem("688374411"),
            QStandardItem(datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))])
        self.listview.header().resizeSection(0, 320)
        self.listview.header().resizeSection(1, 80)
        self.listview.header().resizeSection(2, 80)
        self.listview.doubleClicked.connect(self.list_doubleClicked)
        self.listview.setSortingEnabled(True)
        # docs = QStandardPaths.standardLocations(QStandardPaths.DocumentsLocation)[0]
        self.splitter.setSizes([20, 160])
        self.listview.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.listview.setDragDropMode(QAbstractItemView.DragDrop)
        # self.listview.setDragEnabled(True)
        # self.listview.setAcceptDrops(True)
        # self.listview.setDropIndicatorShown(True)

        self.listview.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.listview.setIndentation(10)
        self.listview.sortByColumn(0, Qt.AscendingOrder)
        self.restoreSettings()

    def createStatusBar(self):
        welcome = "Welcome to S3 Duck🦆 for Linux"
        self.statusBar().showMessage(welcome, 0)

    def list_doubleClicked(self):
        index = self.listview.selectionModel().currentIndex()

    def goBack(self):
        index = self.listview.selectionModel().currentIndex()
        # path = self.fileModel.fileInfo(index).path()

    def goUp(self):
        pass

    def goHome(self):
        docs = QStandardPaths.standardLocations(QStandardPaths.HomeLocation)[0]

    def createActions(self):
        self.btnBack = QAction(QIcon.fromTheme("go-previous"), "go back", triggered=self.goBack)
        self.btnUp = QAction(QIcon.fromTheme("go-up"), "go up", triggered=self.goUp)
        self.btnHome = QAction(QIcon.fromTheme("go-home"), "home folder", triggered=self.goHome)

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
