from PyQt6.QtCore import Qt
from PyQt6.QtGui import QColor, QPalette
from PyQt6.QtWidgets import QApplication

THEMES = ("system", "light", "dark")

# The pristine style + palette captured on the first apply, so "system" can
# restore whatever the platform gave us at startup.
_default_palette = None
_default_style_name = None


def _remember_defaults(app):
    global _default_palette, _default_style_name
    if _default_palette is None:
        _default_palette = QPalette(app.palette())
        _default_style_name = app.style().objectName()


def _dark_palette():
    p = QPalette()
    window = QColor(53, 53, 53)
    base = QColor(42, 42, 42)
    text = QColor(221, 221, 221)
    disabled = QColor(127, 127, 127)
    highlight = QColor(38, 110, 183)

    p.setColor(QPalette.ColorRole.Window, window)
    p.setColor(QPalette.ColorRole.WindowText, text)
    p.setColor(QPalette.ColorRole.Base, base)
    p.setColor(QPalette.ColorRole.AlternateBase, window)
    p.setColor(QPalette.ColorRole.ToolTipBase, window)
    p.setColor(QPalette.ColorRole.ToolTipText, text)
    p.setColor(QPalette.ColorRole.Text, text)
    p.setColor(QPalette.ColorRole.Button, window)
    p.setColor(QPalette.ColorRole.ButtonText, text)
    p.setColor(QPalette.ColorRole.BrightText, QColor(255, 80, 80))
    p.setColor(QPalette.ColorRole.Link, QColor(90, 160, 240))
    p.setColor(QPalette.ColorRole.Highlight, highlight)
    p.setColor(QPalette.ColorRole.HighlightedText, QColor(Qt.GlobalColor.white))
    p.setColor(QPalette.ColorRole.PlaceholderText, disabled)

    for role in (QPalette.ColorRole.WindowText, QPalette.ColorRole.Text,
                 QPalette.ColorRole.ButtonText):
        p.setColor(QPalette.ColorGroup.Disabled, role, disabled)
    p.setColor(QPalette.ColorGroup.Disabled, QPalette.ColorRole.Base,
               QColor(50, 50, 50))
    p.setColor(QPalette.ColorGroup.Disabled, QPalette.ColorRole.Highlight,
               QColor(70, 70, 70))
    return p


def apply_theme(app, name: str) -> str:
    """Apply a theme to the whole application. Returns the name actually used."""
    _remember_defaults(app)
    name = (name or "system").lower()
    if name not in THEMES:
        name = "system"

    if name == "dark":
        app.setStyle("Fusion")
        app.setPalette(_dark_palette())
    elif name == "light":
        app.setStyle("Fusion")
        app.setPalette(app.style().standardPalette())
    else:  # system: restore the captured startup defaults
        if _default_style_name:
            app.setStyle(_default_style_name)
        if _default_palette is not None:
            app.setPalette(_default_palette)
    return name
