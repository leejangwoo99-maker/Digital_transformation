import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

from inspection.special_checks import load_time, save_time, time_path
from label_images import YoloBoxLabeler


class FrameJumpOCRTimeTest(unittest.TestCase):
    def test_ocr_time_is_saved_per_group(self):
        with tempfile.TemporaryDirectory() as directory:
            info = Path(directory)
            save_time(info, "total", "OCR", 2.5, 7)
            self.assertEqual(time_path(info, "total", "OCR").name, "total_OCR_test_condition.txt")
            self.assertEqual(load_time(info, "total", "OCR"), (2.5, 7))
            self.assertEqual(load_time(info, "other", "OCR"), (0, None))

    def test_jump_saves_before_loading_and_uses_one_based_number(self):
        labeler = YoloBoxLabeler(Path("images"), Path("labels"), {0: "ok"},
                                 image_paths=[Path("a.jpg"), Path("b.jpg")])
        actions = Mock()
        labeler._save_label = actions.save
        labeler._load_current_image = actions.load
        labeler.playing = True
        self.assertTrue(labeler.jump_to_frame("2"))
        self.assertEqual(labeler.index, 1)
        self.assertFalse(labeler.playing)
        self.assertEqual([call[0] for call in actions.mock_calls], ["save", "load"])
        for value in ("", "abc", "0", "3"):
            self.assertFalse(labeler.jump_to_frame(value))
        self.assertEqual(labeler.index, 1)
