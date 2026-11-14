import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import cv2
import numpy as np

from extract_frames import extract_frames
from inspection.frame_conditions import (WindowResults, frame_group_name, group_fps,
    load_frame_range, save_frame_range, sampled_frame_count)
from inspection.special_checks import SpecialInspector
from main import inspect_video


class GroupFPSWindowsTest(unittest.TestCase):
    def test_names_support_user_selected_fps_not_fixed_ten(self):
        for fps in (5, 10, 15, 23, 7.5):
            name = frame_group_name("total-1", fps)
            self.assertEqual(name, f"total-1_{fps:g}fps")
            self.assertEqual(group_fps(name), fps)
        with self.assertRaises(ValueError):
            group_fps("old_group")

    def test_frame_ranges_persist_and_allow_single_frame(self):
        with tempfile.TemporaryDirectory() as temporary:
            info = Path(temporary)
            for kind in ("OCR", "conn", "label"):
                self.assertEqual(load_frame_range(info, "A_15fps", kind), (1, None))
                save_frame_range(info, "A_15fps", kind, 3, 3)
                self.assertEqual(load_frame_range(info, "A_15fps", kind), (3, 3))
            with self.assertRaises(ValueError):
                save_frame_range(info, "A_15fps", "OCR", 0, 5)

    def test_individual_successes_can_occur_on_different_frames(self):
        for kind in ("OCR", "label"):
            tracker = WindowResults()
            items = ["first", "second"]
            failed = {"item_key": "second", "class_name": "mismatch"}
            self.assertEqual(tracker.update(kind, items, [failed], 1, 1, 3, 5), [])
            self.assertIn("PENDING", tracker.status_lines[-1])
            first_bad = {"item_key": "first", "class_name": "mismatch"}
            self.assertEqual(tracker.update(kind, items, [first_bad], 2, 1, 3, 5), [])
            self.assertIn("OK", tracker.status_lines[-1])
            self.assertEqual(tracker.update(kind, items, [first_bad, failed], 3, 1, 3, 5), [])

    def test_failure_only_at_end_and_stays_ng_after_window(self):
        tracker = WindowResults()
        bad = {"item_key": "first", "class_name": "OCR_mismatch"}
        self.assertEqual(tracker.update("OCR", ["first"], [], 1, 2, 3, 5), [])
        self.assertEqual(tracker.update("OCR", ["first"], [bad], 2, 2, 3, 5), [])
        self.assertEqual(tracker.update("OCR", ["first"], [bad], 3, 2, 3, 5), [bad])
        self.assertEqual(tracker.update("OCR", ["first"], [], 4, 2, 3, 5), [bad])

    def test_real_ocr_checker_latches_success_and_skips_already_passed_items(self):
        inspector = SpecialInspector.__new__(SpecialInspector)
        inspector.references = {"OCR": {"regions": [
            {"role": "text", "box": [0, 0, .5, 1], "text": "A"},
            {"role": "text", "box": [.5, 0, 1, 1], "text": "B"}]}}
        inspector.times = {"OCR": (1, 3)}
        reader = Mock(side_effect=[["A"], ["bad"], ["B"]])
        inspector.ocr = SimpleNamespace(candidates=reader)
        image = np.zeros((80, 80, 3), dtype=np.uint8)
        self.assertEqual(inspector.check_frame(image, 1, 4), [])
        self.assertIn("PENDING", inspector.status_lines[0])
        self.assertEqual(inspector.check_frame(image, 2, 4), [])
        self.assertIn("OK", inspector.status_lines[0])
        self.assertEqual(inspector.check_frame(image, 3, 4), [])
        self.assertEqual(reader.call_count, 3)

    def test_test_and_extract_sample_same_frames_and_save_side_panel(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source.avi"
            writer = cv2.VideoWriter(str(source), cv2.VideoWriter_fourcc(*"MJPG"), 30, (64, 48))
            for index in range(60):
                writer.write(np.full((48, 64, 3), index * 3, dtype=np.uint8))
            writer.release()
            for fps in (10, 23):
                frames = root / str(fps)
                extract_frames(SimpleNamespace(video=source, output_dir=frames, split="train", prefix="A",
                    every_n_frames=1, every_seconds=None, target_fps=fps, start_frame=0,
                    max_frames=None, resize=None, quality=100), log=lambda text: None)
                seen = []
                detector = SimpleNamespace(model=object(), predict=lambda frame: (seen.append(frame.copy()) or [], frame))
                special = SimpleNamespace(references={}, times={}, status_lines=[],
                    check_frame=lambda frame, number, total, valid_mask=None: [])
                output = root / f"result_{fps}.mp4"
                settings = {"test_fps": fps, "special_frame_ranges": True, "save_test_details": True}
                with patch("model.yolo_inference.YoloInference", return_value=detector), \
                     patch("inspection.special_checks.SpecialInspector", return_value=special):
                    inspect_video(source, settings, output, show_window=False, display_height=96)
                images = [cv2.imread(str(path)) for path in sorted(frames.glob("*.jpg"))]
                self.assertEqual(len(seen), len(images))
                for actual, extracted in zip(seen, images):
                    self.assertLess(abs(float(actual.mean()) - float(extracted.mean())), 2)
                capture = cv2.VideoCapture(str(output))
                self.assertAlmostEqual(capture.get(cv2.CAP_PROP_FPS), fps, places=2)
                self.assertEqual(capture.get(cv2.CAP_PROP_FRAME_COUNT), len(images))
                self.assertEqual(capture.get(cv2.CAP_PROP_FRAME_WIDTH), 304)
                self.assertEqual(capture.get(cv2.CAP_PROP_FRAME_HEIGHT), 48)
                capture.release()
            self.assertEqual(sampled_frame_count(2, 30, 23), 1)
