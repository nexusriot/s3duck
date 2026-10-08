"""QObjects that run on a QThread.

They talk to the window only through signals, and never touch a widget: a Qt
widget touched from a non-GUI thread is undefined behaviour rather than an
error anything tells you about.
"""
