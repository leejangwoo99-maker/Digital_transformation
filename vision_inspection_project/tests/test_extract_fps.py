import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch, Mock

import cv2
import numpy as np

from extract_frames import extract_frames


class ExtractFpsTest(unittest.TestCase):
    def make_video(self, directory):
        source = directory / "sample.avi"
        writer = cv2.VideoWriter(str(source), cv2.VideoWriter_fourcc(*"MJPG"), 30, (64, 48))
        self.assertTrue(writer.isOpened())
        for index in range(60):
            writer.write(np.full((48, 64, 3), index, dtype=np.uint8))
        writer.release()
        return source

    def args(self, source, output, fps, start=0):
        return SimpleNamespace(video=source, output_dir=output, split="train", prefix=None,
                               every_n_frames=1, every_seconds=None, target_fps=fps,
                               start_frame=start, max_frames=None, resize=None, quality=100)

    def test_requested_fps_changes_saved_count(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            source = self.make_video(directory)
            for fps, expected in ((5, 10), (10, 20), (23, 46), (60, 60)):
                with self.subTest(fps=fps):
                    output = directory / str(fps)
                    logs = []
                    self.assertEqual(extract_frames(self.args(source, output, fps), logs.append), expected)
                    self.assertEqual(len(list(output.glob("*.jpg"))), expected)
                    self.assertTrue(any("Requested FPS:" in line for line in logs))
            self.assertEqual(extract_frames(self.args(source, directory / "offset", 5, start=30), lambda text: None), 5)

    def test_gui_reextraction_selects_only_new_group(self):
        import video_inspection_gui as gui
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            source = self.make_video(directory)
            app = gui.VideoInspectionApp.__new__(gui.VideoInspectionApp)
            app.output_dir = directory / "test"
            app.min_confidence_var = Mock()
            app.iou_var = Mock()
            app.root = SimpleNamespace(after=lambda delay, action: action())
            app.status_var = SimpleNamespace(set=lambda text: None)
            app.video_var = SimpleNamespace(set=lambda text: None)
            app.extract_button = SimpleNamespace(config=lambda **kwargs: None)
            app.append_train_log = lambda text: None
            with patch.object(gui, "ROOT_DIR", directory), patch.object(gui.messagebox,"showerror") as error:
                app._extract_worker(source, 10)
                first_paths = list(app.active_image_paths)
                self.assertEqual(len(first_paths), 20)
                app._extract_worker(source, 5)
                self.assertEqual(len(app.active_image_paths), 10)
                self.assertEqual(app.active_group_name, "sample_5fps")
                self.assertTrue(all(path.exists() for path in first_paths))
                self.assertFalse(set(first_paths) & set(app.active_image_paths))
                error.assert_not_called()
                app.min_confidence_var.set.assert_called_with(50)

    def test_invalid_fps_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            source = self.make_video(directory)
            for fps in (0, -1, float("nan"), float("inf")):
                with self.assertRaises(ValueError):
                    extract_frames(self.args(source, directory / "frames", fps))


if __name__ == "__main__":
    unittest.main()
