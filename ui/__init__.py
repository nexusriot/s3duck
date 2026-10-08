"""The Qt layer: the window, its widgets and its dialogs.

Everything here may import core/, workers/, model and utils. Nothing here is
imported by core/ -- that is what keeps the domain logic testable without a
QApplication, and CorePurityTests enforces it.
"""
