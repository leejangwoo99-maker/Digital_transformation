import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import cv2
import numpy as np

from inspection.video_export import finalize_result_video


class VideoExportTest(unittest.TestCase):
    def test_encoding_failure_preserves_original_and_cleans_temporary_files(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "result.mp4"
            source.write_bytes(b"original")
            with patch("inspection.video_export.subprocess.run", return_value=SimpleNamespace(returncode=1, stderr="encoder error")):
                with self.assertRaisesRegex(RuntimeError, "original result preserved"):
                    finalize_result_video(source)
            self.assertEqual(source.read_bytes(), b"original")
            self.assertEqual(list(Path(directory).iterdir()), [source])

    def test_single_result_is_h264_preserves_fps_and_resolution(self):
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary) / "result.mp4"
            writer = cv2.VideoWriter(str(source), cv2.VideoWriter_fourcc(*"mp4v"), 3, (320, 240))
            for _ in range(6):
                writer.write(np.full((240, 320, 3), 90, dtype=np.uint8))
            writer.release()
            output = finalize_result_video(source)
            self.assertEqual(output, source)
            self.assertEqual(list(Path(temporary).iterdir()), [source])
            capture = cv2.VideoCapture(str(output))
            self.assertTrue(capture.read()[0])
            self.assertEqual(int(capture.get(cv2.CAP_PROP_FOURCC)).to_bytes(4, "little"), b"h264")
            self.assertEqual(capture.get(cv2.CAP_PROP_FPS), 3)
            self.assertEqual(capture.get(cv2.CAP_PROP_FRAME_COUNT), 6)
            self.assertEqual(capture.get(cv2.CAP_PROP_FRAME_WIDTH), 320)
            self.assertEqual(capture.get(cv2.CAP_PROP_FRAME_HEIGHT), 240)
            capture.release()
