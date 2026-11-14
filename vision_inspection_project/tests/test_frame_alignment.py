import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import cv2
import numpy as np

from inspection.frame_alignment import FrameAligner
from inspection.special_checks import SpecialInspector


class FrameAlignmentTest(unittest.TestCase):
    def test_preview_is_centered_and_keeps_original_orientation(self):
        from main import centered_preview_geometry
        for original_width, original_height in ((2160, 3840), (3840, 2160), (640, 480)):
            width, height, x, y = centered_preview_geometry(original_width, original_height, 1920, 1080)
            self.assertAlmostEqual(width / height, original_width / original_height, places=2)
            self.assertLessEqual(width, 1840)
            self.assertLessEqual(height, 980)
            self.assertLessEqual(abs(x + width / 2 - 960), 1)
            self.assertLessEqual(abs(y + (height + 40) / 2 - 540), 1)

    def reference(self):
        image = np.full((480, 400, 3), 255, dtype=np.uint8)
        noise = np.random.default_rng(42).integers(15, 125, (300, 240, 3), dtype=np.uint8)
        image[80:380, 80:320] = noise
        for index in range(8):
            cv2.putText(image, f"PIN{index}", (90, 110 + index * 32), cv2.FONT_HERSHEY_SIMPLEX,
                        0.6, (220, 220, 220), 2, cv2.LINE_AA)
        return image

    def transformed(self, reference):
        affine = cv2.getRotationMatrix2D((200, 240), 14, 0.9)
        affine[:, 2] += (100, 100)
        matrix = np.vstack((affine, [0, 0, 1])).astype(np.float64)
        matrix[2, 0] = 0.00015
        frame = cv2.warpPerspective(reference, matrix, (640, 720), borderValue=(255, 255, 255))
        return frame, matrix

    def test_rotation_scale_translation_and_perspective(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "reference.png"
            reference = self.reference()
            cv2.imwrite(str(path), reference)
            frame, known_transform = self.transformed(reference)
            result = FrameAligner(path).align(frame)
            self.assertTrue(result.ok, result.message)
            self.assertEqual(result.image.shape, reference.shape)
            points = np.float32([[100, 100], [300, 100], [300, 350], [100, 350]]).reshape(-1, 1, 2)
            camera_points = cv2.perspectiveTransform(points, known_transform)
            recovered = cv2.perspectiveTransform(camera_points, result.matrix)
            self.assertLess(float(np.max(np.abs(recovered - points))), 3)

    def test_missing_and_cut_off_product_do_not_pass(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "reference.png"
            reference = self.reference()
            cv2.imwrite(str(path), reference)
            aligner = FrameAligner(path)
            self.assertFalse(aligner.align(np.full_like(reference, 255)).ok)
            result = aligner.align(reference[:250])
            self.assertFalse(result.ok)

    def test_missing_ocr_region_skips_ocr_and_reports_failure(self):
        inspector = SpecialInspector.__new__(SpecialInspector)
        inspector.references = {"OCR": {"regions": [{"role": "text", "box": [0.1, 0.1, 0.5, 0.5], "text": "ABC"}]}}
        inspector.times = {"OCR": (0, None)}
        inspector.reference_transforms = {}
        inspector.ocr = SimpleNamespace(candidates=lambda crop: self.fail("OCR must not run on missing image data"))
        frame = np.zeros((100, 100, 3), dtype=np.uint8)
        mask = np.zeros((100, 100), dtype=np.uint8)
        failures = inspector.check(frame, 0, valid_mask=mask)
        self.assertEqual(failures[0]["class_name"], "OCR_region_not_visible")

    def test_alignment_failure_is_saved_as_fail_not_pass(self):
        from main import inspect_video
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            reference_path = directory / "reference.png"
            cv2.imwrite(str(reference_path), self.reference())
            source = directory / "source.avi"
            writer = cv2.VideoWriter(str(source), cv2.VideoWriter_fourcc(*"MJPG"), 10, (640, 480))
            writer.write(np.full((480, 640, 3), 255, dtype=np.uint8))
            writer.release()
            detector = SimpleNamespace(model=True, predict=lambda frame: self.fail("Detection must not run after alignment failure"))
            special = SimpleNamespace(references={}, check=lambda *args, **kwargs: self.fail("Special checks must not run"))
            settings = {"root_dir": str(directory), "alignment_enabled": True, "alignment_reference": str(reference_path)}
            with patch("model.yolo_inference.YoloInference", return_value=detector), \
                    patch("inspection.special_checks.SpecialInspector", return_value=special):
                output = inspect_video(source, settings, directory / "result.mp4", show_window=False,
                                       status_suffix=True, log_dir=directory / "logs")
            self.assertTrue(output.name.endswith("_f.mp4"))
            self.assertIn("alignment_failed", (directory / "logs" / f"{output.stem}.txt").read_text())
            cap = cv2.VideoCapture(str(output))
            ok, image = cap.read()
            cap.release()
            self.assertTrue(ok)
            self.assertEqual(image.shape[:2], (480, 400))

    def test_successful_alignment_runs_checks_and_saves_reference_size(self):
        from main import inspect_video
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            reference = self.reference()
            reference_path = directory / "reference.png"
            cv2.imwrite(str(reference_path), reference)
            frame, _ = self.transformed(reference)
            source = directory / "source.avi"
            writer = cv2.VideoWriter(str(source), cv2.VideoWriter_fourcc(*"MJPG"), 10, (640, 720))
            writer.write(frame)
            writer.release()
            seen_shapes = []
            detector = SimpleNamespace(model=True, predict=lambda image: (seen_shapes.append(image.shape) or [], image))
            def check(image, seconds, valid_mask=None):
                self.assertEqual(image.shape, reference.shape)
                self.assertIsNotNone(valid_mask)
                return []
            special = SimpleNamespace(references={}, check=check)
            settings = {"root_dir": str(directory), "alignment_enabled": True, "alignment_reference": str(reference_path)}
            with patch("model.yolo_inference.YoloInference", return_value=detector), \
                    patch("inspection.special_checks.SpecialInspector", return_value=special):
                output = inspect_video(source, settings, directory / "result.mp4", show_window=False, status_suffix=True)
            self.assertTrue(output.name.endswith("_p.mp4"))
            self.assertEqual(seen_shapes, [reference.shape])
            cap = cv2.VideoCapture(str(output))
            ok, image = cap.read()
            cap.release()
            self.assertTrue(ok)
            self.assertEqual(image.shape, reference.shape)

    def test_special_reference_coordinates_are_mapped_to_preview(self):
        inspector = SpecialInspector.__new__(SpecialInspector)
        inspector.references = {"OCR": {"regions": [{"role": "text", "box": [0.4, 0.4, 0.6, 0.6], "text": "ABC"}]}}
        inspector.times = {"OCR": (0, None)}
        matrix = np.array([[1, 0, 20], [0, 1, 0], [0, 0, 1]], dtype=np.float64)
        inspector.reference_transforms = {"OCR": (matrix, (100, 100))}
        inspector.ocr = SimpleNamespace(candidates=lambda crop: ["wrong"])
        failures = inspector.check(np.zeros((100, 100, 3), dtype=np.uint8), 0)
        self.assertEqual(failures[0]["xyxy"], (20, 40, 40, 60))


if __name__ == "__main__":
    unittest.main()
