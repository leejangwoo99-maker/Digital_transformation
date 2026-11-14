import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import cv2
import numpy as np

from main import inspect_video
from video_inspection_gui import VideoInspectionApp
from model.yolo_inference import YoloInference


class FileInspectionTest(unittest.TestCase):
    def test_original_resolution_inference_pads_without_scaling(self):
        with patch.object(YoloInference, "_load_model"):
            detector = YoloInference({"root_dir": ".", "model": {"original_resolution": True}})
        result = SimpleNamespace(names={}, boxes=None, plot=lambda: np.zeros((96, 128, 3), dtype=np.uint8))
        detector.model = Mock()
        detector.model.predict.return_value = [result]
        image = np.arange(65 * 97 * 3, dtype=np.uint8).reshape(65, 97, 3)
        boxes, annotated = detector.predict(image)
        args = detector.model.predict.call_args.kwargs
        self.assertEqual(args["imgsz"], [96, 128])
        np.testing.assert_array_equal(args["source"][:65, :97], image)
        self.assertEqual(annotated.shape, image.shape)
        self.assertEqual(boxes, [])

    def test_gui_removes_legacy_fields_and_saves_only_current_conditions(self):
        import tkinter as tk
        from video_inspection_gui import TEST_CONDITION_FILE
        root = tk.Tk()
        root.withdraw()
        try:
            app = VideoInspectionApp(root)
            widgets = []
            def collect(parent):
                for widget in parent.winfo_children():
                    widgets.append(widget)
                    collect(widget)
            collect(root)
            texts = [widget.cget("text") for widget in widgets if "text" in widget.keys()]
            self.assertNotIn("confidence %", texts)
            self.assertNotIn("iou %", texts)
            self.assertNotIn("overlap label box %", texts)
            self.assertNotIn('worker',texts)
            self.assertNotIn('spread',texts)
            self.assertIn('Test fps',texts)
            self.assertIn("threshold %", texts)
            for removed in ("phone stream URL", "live second", "align to training", "image size", "display width", "display height", "OCR time", "conn time", "label time"):
                self.assertNotIn(removed, texts)
            app.active_group_name = "A_10fps"
            app.min_confidence_var.set(37)
            settings = app._settings_from_ui()
            self.assertEqual(settings["model"]["confidence"], .37)
            self.assertEqual(settings["judge"]["min_confidence"], .37)
            self.assertEqual(settings["test_fps"], 10)
            self.assertFalse(settings["alignment_enabled"])
            self.assertIsNone(settings["alignment_reference"])
            self.assertFalse(settings["match_original_format"])
            self.assertFalse(settings["model"]["original_resolution"])
            self.assertEqual(settings["model"]["image_size"], 640)
            with tempfile.TemporaryDirectory() as temporary:
                app.output_dir = Path(temporary)
                app.save_test_conditions()
                text = (app.output_dir / "info" / "A_10fps_test_condition.txt").read_text()
                self.assertEqual(len(text.splitlines()), 4)
                self.assertIn("confidence_percent: 37", text)
                self.assertIn("threshold_percent: 37", text)
                self.assertNotIn("image_size", text)
                self.assertNotIn("phone_stream_url", text)
        finally:
            root.destroy()

    def run_video(self, directory, defects):
        source = directory / "phone_stream_sample.avi"
        writer = cv2.VideoWriter(str(source), cv2.VideoWriter_fourcc(*"MJPG"), 10, (96, 64))
        self.assertTrue(writer.isOpened())
        for _ in range(12):
            writer.write(np.zeros((64, 96, 3), dtype=np.uint8))
        writer.release()
        detector = SimpleNamespace(model=True, predict=lambda frame: (list(defects), frame))
        special = SimpleNamespace(references={}, check=lambda *args: [])
        with patch("model.yolo_inference.YoloInference", return_value=detector), \
                patch("inspection.special_checks.SpecialInspector", return_value=special):
            output = inspect_video(source, {}, directory / "phone_stream_sample_result.mp4",
                                   show_window=False, log_dir=directory / "logs", file_test_log=True)
        capture = cv2.VideoCapture(str(output))
        count = int(capture.get(cv2.CAP_PROP_FRAME_COUNT))
        capture.release()
        self.assertEqual(count, 12)
        self.assertTrue(source.exists())
        marker = "f" if defects else "p"
        self.assertEqual(output.name, f"phone_stream_sample_result_{marker}.mp4")
        return (directory / "logs" / f"sample_result_{marker}.txt").read_text(encoding="utf-8")

    def test_pass_logs_and_complete_video(self):
        with tempfile.TemporaryDirectory() as temporary:
            text = self.run_video(Path(temporary), [])
            self.assertTrue(text.startswith("result_PASS\n"))
            self.assertIn("inspection_status_completed", text)
            self.assertIn("processed_frames_12_of_12", text)
            self.assertIn("appearance_checked_frames_12", text)
            self.assertIn("appearance_detected_defects_0", text)

    def test_ng_logs_use_video_timestamp_and_no_phone_prefix(self):
        with tempfile.TemporaryDirectory() as temporary:
            text = self.run_video(Path(temporary), [{"class_name": "defect", "confidence": 0.57,
                                                     "xyxy": [10, 10, 30, 30], "special_failure": True}])
            self.assertIn("0:00:01_defect_0.57", text)

    def test_stopped_video_marked_partial_and_counts_written_frame(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            source = directory / "sample.avi"
            writer = cv2.VideoWriter(str(source), cv2.VideoWriter_fourcc(*"MJPG"), 10, (96, 64))
            for _ in range(3):
                writer.write(np.zeros((64, 96, 3), dtype=np.uint8))
            writer.release()
            detector = SimpleNamespace(model=True, predict=lambda image: ([], image))
            special = SimpleNamespace(references={}, check=lambda *args: [])
            progress = Mock()
            with patch("model.yolo_inference.YoloInference", return_value=detector), \
                    patch("inspection.special_checks.SpecialInspector", return_value=special), \
                    patch("cv2.namedWindow"), patch("cv2.resizeWindow"), patch("cv2.imshow"), \
                    patch("cv2.waitKey", return_value=27):
                output = inspect_video(source, {}, directory / "sample_result.mp4", file_test_log=True,
                                       log_dir=directory / "logs", progress_callback=progress)
            self.assertEqual(output.name, "sample_result_partial_f.mp4")
            text = (directory / "logs" / "sample_result_partial_f.txt").read_text()
            self.assertIn("inspection_status_user_stopped", text)
            self.assertIn("processed_frames_1_of_3", text)
            self.assertNotIn("result_PASS", text)
            self.assertEqual(progress.call_args.args[:2], (1, 3))

    def test_worker_uses_result_name_no_live_duration_and_keeps_existing_file(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            previous = directory / "sample_result.mp4"
            previous.write_bytes(b"existing result")
            app = VideoInspectionApp.__new__(VideoInspectionApp)
            app.output_dir = directory
            app.log_dir = directory / "logs"
            app.root = SimpleNamespace(after=lambda delay, callback: callback())
            app.status_var = Mock()
            app.live_test_button = Mock()
            app.live_test_running = True
            app.display_width_var = SimpleNamespace(get=lambda: 640)
            app.display_height_var = SimpleNamespace(get=lambda: 480)
            app._settings_from_ui = lambda: {"root_dir": str(directory), "require_regular_model": False}
            app.append_train_log = Mock()
            app.write_operation_log = Mock()
            app._begin_pipeline_clock = Mock()
            app._end_pipeline_clock = Mock()
            with patch("video_inspection_gui.DEFAULT_TEST_VIDEO_DIR", directory), \
                    patch("video_inspection_gui.TEST_LOG_DIR", app.log_dir), \
                    patch("video_inspection_gui.inspect_video", return_value=directory / "sample_result_1.mp4") as inspect:
                app._inspect_worker(directory / "sample.mp4", file_test=True)
            args = inspect.call_args.kwargs
            self.assertEqual(args["output_path"].name, "sample_result_1.mp4")
            self.assertIsNone(args["max_seconds"])
            self.assertFalse(args["status_suffix"])
            self.assertTrue(args["file_test_log"])
            self.assertEqual(args["log_dir"], app.log_dir)
            self.assertFalse(app.live_test_running)
            app.write_operation_log.assert_not_called()
            self.assertEqual(previous.read_bytes(), b"existing result")


if __name__ == "__main__":
    unittest.main()
