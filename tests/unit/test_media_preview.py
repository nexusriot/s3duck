"""Audio and video in the preview: streamed from a presigned link."""

import unittest
import ui.dialogs.preview as preview
from core.naming import format_media_time, media_kind
from tests.support import _ensure_qapp


class MediaKindTests(unittest.TestCase):

    def test_extension_first(self):
        self.assertEqual(media_kind("a/b/song.MP3"), "audio")
        self.assertEqual(media_kind("clip.webm"), "video")
        self.assertEqual(media_kind("clip.webm", "audio/ogg"), "video")
        self.assertEqual(media_kind("notes.txt"), "")

    def test_content_type_fills_in(self):
        self.assertEqual(media_kind("blob", "audio/mpeg"), "audio")
        self.assertEqual(media_kind("blob", "VIDEO/mp4"), "video")
        self.assertEqual(media_kind("blob", "binary/octet-stream"), "")
        # The ".ts" ambiguity stays deliberate: no extension match.
        self.assertEqual(media_kind("segment.ts"), "")

    def test_time(self):
        self.assertEqual(format_media_time(0), "0:00")
        self.assertEqual(format_media_time(61_500), "1:01")
        self.assertEqual(format_media_time(3_725_000), "1:02:05")
        self.assertEqual(format_media_time(None), "0:00")


class _FakeModel:
    bucket = "b"
    no_ssl_check = False
    requester_pays = False

    def __init__(self, fail=False):
        self.fail = fail
        self.previewed = []
        self.presigned = []

    def clone_for_worker(self):
        return self

    def get_object_preview(self, key, max_bytes):
        self.previewed.append(key)
        return {"data": b"\x00\x01", "content_type": "audio/mpeg",
                "size": 2, "truncated": False}

    def presigned_get_url(self, key, expires_sec=3600):
        if self.fail:
            raise ValueError("no bucket")
        self.presigned.append((key, expires_sec))
        # A port nothing listens on: the player fails fast and quietly.
        return "http://127.0.0.1:9/" + key


@unittest.skipIf(preview.QMediaPlayer is None, "QtMultimedia not installed")
class MediaPreviewTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._app = _ensure_qapp()

    def _dlg(self, key, model=None):
        model = model or _FakeModel()
        dlg = preview.PreviewDialog(None, model, key)
        self.addCleanup(dlg.close)
        return dlg, model

    def test_a_video_streams_instead_of_downloading(self):
        dlg, model = self._dlg("movies/clip.mp4")
        self.assertEqual(model.previewed, [], "the body was downloaded")
        self.assertEqual(model.presigned, [("movies/clip.mp4", 3600)])
        self.assertEqual(dlg._stack.currentIndex(), dlg._media_page)
        self.assertEqual(dlg._player.source().toString(),
                         "http://127.0.0.1:9/movies/clip.mp4")
        self.assertFalse(dlg._video.isHidden())

    def test_audio_hides_the_video_surface(self):
        dlg, _ = self._dlg("song.flac")
        self.assertTrue(dlg._video.isHidden())
        self.assertEqual(dlg._play_btn.text(), "Play")

    def test_content_type_alone_switches_to_the_player(self):
        dlg, model = self._dlg("blob")
        dlg._on_loaded(model.get_object_preview("blob", 1), None)
        self.assertEqual(dlg._stack.currentIndex(), dlg._media_page)

    def test_a_presign_failure_is_shown(self):
        dlg, _ = self._dlg("clip.mp4", _FakeModel(fail=True))
        self.assertEqual(dlg._stack.currentIndex(), 0)
        self.assertIn("no bucket", dlg._status.text())

    def test_profile_limits_are_explained(self):
        model = _FakeModel()
        model.no_ssl_check = True
        model.requester_pays = True
        dlg, _ = self._dlg("clip.mp4", model)
        self.assertIn("TLS", dlg._media_note.text())
        self.assertIn("requester-pays", dlg._media_note.text())

    def test_closing_stops_the_player(self):
        dlg, _ = self._dlg("clip.mp4")
        dlg.stop_threads()
        self.assertTrue(dlg._player.source().isEmpty())

    def test_time_label_tracks_the_player(self):
        dlg, _ = self._dlg("clip.mp4")
        dlg._on_duration(90_000)
        self.assertEqual(dlg._seek.maximum(), 90_000)
        self.assertIn("1:30", dlg._time.text())


if __name__ == "__main__":
    unittest.main()
