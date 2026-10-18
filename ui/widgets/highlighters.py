"""Syntax and diff highlighting for the preview and version views.
"""

import re
from PyQt6.QtGui import QColor, QFont, QSyntaxHighlighter, QTextCharFormat


class CodeHighlighter(QSyntaxHighlighter):
    """
    Deliberately language-agnostic: strings, comments, numbers and a common
    keyword set. Enough to make code readable without shipping a grammar per
    language.
    """

    KEYWORDS = (
        "and as assert async await break case class const continue def default"
        " del elif else except export extends finally for from func function"
        " global if impl import in interface is lambda let match mod new nil"
        " none not null or pass private protected public raise return select"
        " self static struct switch this throw try type var void while with"
        " yield true false"
    ).split()

    def __init__(self, document):
        super().__init__(document)
        self._rules = []

        keyword_fmt = QTextCharFormat()
        keyword_fmt.setForeground(QColor(86, 156, 214))
        keyword_fmt.setFontWeight(QFont.Weight.Bold)
        self._rules.append((
            re.compile(r"\b(" + "|".join(self.KEYWORDS) + r")\b"), keyword_fmt))

        number_fmt = QTextCharFormat()
        number_fmt.setForeground(QColor(181, 206, 168))
        self._rules.append((re.compile(r"\b\d[\d_.xXa-fA-F]*\b"), number_fmt))

        string_fmt = QTextCharFormat()
        string_fmt.setForeground(QColor(206, 145, 120))
        self._rules.append((
            re.compile(r"'[^'\\]*(?:\\.[^'\\]*)*'"
                       r'|"[^"\\]*(?:\\.[^"\\]*)*"'), string_fmt))

        comment_fmt = QTextCharFormat()
        comment_fmt.setForeground(QColor(106, 153, 85))
        comment_fmt.setFontItalic(True)
        # Applied last so a trailing comment wins over anything inside it.
        self._comment_rule = (re.compile(r"(#|//).*$"), comment_fmt)

    def highlightBlock(self, text):
        for pattern, fmt in self._rules:
            for match in pattern.finditer(text):
                self.setFormat(match.start(), match.end() - match.start(), fmt)
        pattern, fmt = self._comment_rule
        match = pattern.search(text)
        if match:
            self.setFormat(match.start(), len(text) - match.start(), fmt)


class DiffHighlighter(QSyntaxHighlighter):
    """Colours a unified diff: added, removed and hunk lines."""

    def __init__(self, document):
        super().__init__(document)
        self._added = QTextCharFormat()
        self._added.setForeground(QColor("#2e7d32"))
        self._removed = QTextCharFormat()
        self._removed.setForeground(QColor("#c62828"))
        self._hunk = QTextCharFormat()
        self._hunk.setForeground(QColor("#1565c0"))

    def highlightBlock(self, text):
        if text.startswith("@@"):
            self.setFormat(0, len(text), self._hunk)
        elif text.startswith("+") and not text.startswith("+++"):
            self.setFormat(0, len(text), self._added)
        elif text.startswith("-") and not text.startswith("---"):
            self.setFormat(0, len(text), self._removed)
