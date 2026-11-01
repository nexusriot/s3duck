"""The media preview streams a real object through a presigned link.

Offline the player is handed a URL and nothing more. Whether that URL is one
a media backend can actually open -- signed for GET, reachable, answering
range requests -- is a question only the server can answer.
"""

import io
import struct
import unittest
import wave

from PyQt6.QtWidgets import QDialog

import ui.dialogs.preview as preview
from tests.e2e_gui.harness import GuiServerCase


def _wav(seconds=1, rate=8000):
    buffer = io.BytesIO()
    with wave.open(buffer, "wb") as out:
        out.setnchannels(1)
        out.setsampwidth(2)
        out.setframerate(rate)
        out.writeframes(b"".join(
            struct.pack("<h", (i % 64) * 256) for i in range(rate * seconds)))
    return buffer.getvalue()


@unittest.skipIf(preview.QMediaPlayer is None, "QtMultimedia not installed")
class MediaPreviewTests(GuiServerCase):

    def test_a_wav_object_loads_in_the_player(self):
        key = self.put("media/tone.wav", _wav(), ContentType="audio/wav")
        dlg = preview.PreviewDialog(self.window, self.window.data_model, key)
        self.addCleanup(QDialog.close, dlg)
        self.assertEqual(dlg._stack.currentIndex(), dlg._media_page)
        player = dlg._player
        loaded = (preview.QMediaPlayer.MediaStatus.LoadedMedia,
                  preview.QMediaPlayer.MediaStatus.BufferedMedia,
                  preview.QMediaPlayer.MediaStatus.BufferingMedia)
        self.pump(lambda: player.mediaStatus() in loaded
                  or player.error() != preview.QMediaPlayer.Error.NoError,
                  what="the player to open the stream")
        self.assertEqual(player.error(), preview.QMediaPlayer.Error.NoError,
                         player.errorString())
        self.pump(lambda: player.duration() > 0, what="a duration")
        self.assertAlmostEqual(player.duration(), 1000, delta=150)
        dlg.stop_threads()
