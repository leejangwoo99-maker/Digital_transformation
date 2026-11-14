import tempfile
import io
from urllib.error import HTTPError
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np
import cv2

from camera.phone_usb import PhoneCapture, PhoneUSB, load_actions, save_actions
from inspection.rotated_regions import (
    appearance_obb_labels, crop_rotated_region, obb_row,
    point_in_region, polygon_inside, region_polygon,
)


class RotationTests(unittest.TestCase):
    def test_rotation_uses_pixel_aspect_ratio(self):
        region = {"box": [.4, .4, .6, .6], "angle": 90}
        polygon = region_polygon(region, (100, 200, 3))
        np.testing.assert_allclose(polygon[0], [110, 30], atol=1e-5)
        self.assertTrue(point_in_region((100, 50), polygon))
        self.assertFalse(point_in_region((115, 50), polygon))

    def test_deskew_preserves_region_dimensions(self):
        image = np.full((100, 200, 3), 123, np.uint8)
        crop = crop_rotated_region(image, {"box": [.4, .4, .6, .6], "angle": 45})
        self.assertEqual(crop.shape, (20, 40, 3))
        self.assertTrue(np.all(crop == 123))

    def test_obb_training_row_and_bounds(self):
        text = appearance_obb_labels("2 .5 .5 .2 .2\n", [45], (100, 200, 3))
        self.assertEqual(len(text.split()), 9)
        with self.assertRaises(ValueError):
            obb_row(2, {"box": [0, 0, .4, .4], "angle": 45}, (100, 200, 3))
        with self.assertRaises(ValueError):
            appearance_obb_labels("2 .5 .5 .2 .2", [], (100, 200, 3))

    def test_partial_overlap_is_not_containment(self):
        area = region_polygon({"box": [.4, .4, .6, .6], "angle": 30}, (100, 100, 3))
        self.assertTrue(polygon_inside(area, area))
        self.assertFalse(polygon_inside(area + [3, 0], area))


class USBSettingsTests(unittest.TestCase):
    def test_previous_error_can_be_read_before_restart(self):
        client = PhoneUSB()
        client.port = 12345
        body = b'{"ready":true,"recording":false,"error":"Camera dropped a sampled frame; lower FPS"}'
        with patch("camera.phone_usb.urlopen", return_value=io.BytesIO(body)):
            state = client.request("status", allow_error=True)
        self.assertTrue(state["ready"])
        self.assertIn("dropped", state["error"])
        with patch("camera.phone_usb.urlopen", return_value=io.BytesIO(body)):
            with self.assertRaisesRegex(RuntimeError, "dropped"):
                client.request("status")

    def test_http_error_displays_camera_reason(self):
        client = PhoneUSB()
        client.port = 12345
        error = HTTPError("http://localhost", 400, "OK", {}, io.BytesIO(b'{"error":"Unsupported zoom/focus"}'))
        with patch("camera.phone_usb.urlopen", side_effect=error):
            with self.assertRaisesRegex(RuntimeError, "Unsupported zoom/focus.*HTTP 400"):
                client.configure({"zoom": 1, "focus": 0, "auto_focus": True})

    def test_capture_preserves_frame_order_and_original(self):
        class Client:
            def __init__(self):
                self.number = 0
                self.recording = True

            def configure(self, values):
                return {}

            def request(self, endpoint, **kwargs):
                if endpoint == "status":
                    return {"width": 32, "height": 24, "fps_max": 30, "recording": self.recording}
                if endpoint == "stop":
                    self.recording = False
                    return {}
                if endpoint == "frame":
                    if self.number == 3:
                        self.recording = False
                        return b"", {}
                    self.number += 1
                    image = np.full((24, 32, 3), self.number * 50, np.uint8)
                    return cv2.imencode(".jpg", image)[1].tobytes(), {"X-Frame-Number": str(self.number)}
                return {}

        with tempfile.TemporaryDirectory() as directory, patch("camera.phone_usb.save_actions") as save:
            path = Path(directory) / "raw.mp4"
            capture = PhoneCapture(Client(), 10, path, load_actions(Path(directory) / "missing.json"), True)
            frames = []
            while True:
                success, frame = capture.read()
                if not success:
                    break
                frames.append(round(float(frame.mean())))
            capture.release()
            self.assertEqual(frames, [50, 100, 150])
            self.assertTrue(path.is_file())
            self.assertEqual(save.call_args.args[0]["duration"], .3)

    def test_slow_inspection_drains_all_161_frames_after_capture(self):
        class Client:
            number = 0
            recording = True

            def configure(self, values):
                return {}

            def request(self, endpoint, **kwargs):
                if endpoint == "status":
                    return {"width": 32, "height": 24, "fps_max": 30, "recording": self.recording}
                if endpoint == "stop":
                    self.recording = False
                if endpoint == "frame":
                    if self.number == 161:
                        self.recording = False
                        return b"", {}
                    self.number += 1
                    image = np.full((24, 32, 3), self.number, np.uint8)
                    return cv2.imencode(".jpg", image)[1].tobytes(), {"X-Frame-Number": str(self.number)}
                return {}

        with tempfile.TemporaryDirectory() as directory:
            capture = PhoneCapture(Client(), 23, None, load_actions(Path(directory) / "missing.json"), False)
            buffer_path = Path(capture.frame_buffer.name)
            try:
                self.assertTrue(capture.finished.wait(5))
                self.assertIsNone(capture.error)
                self.assertEqual(capture.frames.qsize(), 161)
                means = []
                while True:
                    ok, frame = capture.read()
                    if not ok:
                        break
                    means.append(round(float(frame.mean())))
                self.assertEqual(means, list(range(1, 162)))
                self.assertEqual(list(buffer_path.iterdir()), [])
            finally:
                capture.release()
            self.assertFalse(buffer_path.exists())

    def test_save_failure_still_finishes_capture(self):
        class Client:
            number = 0
            def configure(self, values):
                return {}
            def request(self, endpoint, **kwargs):
                if endpoint == "status":
                    return {"width": 32, "height": 24, "fps_max": 30, "recording": False}
                if endpoint == "frame":
                    if self.number:
                        return b"", {}
                    self.number += 1
                    return cv2.imencode(".jpg", np.zeros((24, 32, 3), np.uint8))[1].tobytes(), {"X-Frame-Number": "1"}
                return {}
        with tempfile.TemporaryDirectory() as directory, patch("camera.phone_usb.save_actions", side_effect=OSError("disk full")):
            capture = PhoneCapture(Client(), 10, Path(directory) / "raw.mp4", load_actions(Path(directory) / "missing.json"), True)
            self.assertTrue(capture.finished.wait(3))
            with self.assertRaisesRegex(RuntimeError, "disk full"):
                capture.read()
            capture.release()

    def test_action_round_trip(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "actions.json"
            self.assertFalse(load_actions(path)["enabled"])
            data = {"enabled": True, "initial": {"zoom": 1, "focus": 0, "auto_focus": True},
                    "events": [{"seconds": .5, "values": {"zoom": 2, "focus": 1, "auto_focus": False}}],
                    "duration": 2}
            save_actions(data, path)
            self.assertEqual(load_actions(path), data)
            data["duration"] = -1
            save_actions(data, path)
            with self.assertRaises(ValueError):
                load_actions(path)

    def test_only_one_authorized_usb_phone(self):
        for rows in ("List of devices attached\nphone unauthorized\n",
                     "List of devices attached\na device\nb device\n",
                     "List of devices attached\n192.168.1.2:5555 device\n"):
            with self.subTest(rows=rows), patch("camera.phone_usb.adb", return_value=rows):
                with self.assertRaises(RuntimeError):
                    PhoneUSB().connect()


if __name__ == "__main__":
    unittest.main()
