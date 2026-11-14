import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import cv2
import numpy as np

from camera.record_conditions import load_conditions, save_conditions, recording_actions, validate_conditions
from camera.phone_usb import PhoneCapture, PhoneUSB


class RecordingConditionsTests(unittest.TestCase):
    def test_defaults_and_roundtrip(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "info/record_condition.txt"
            data = load_conditions(path)
            self.assertEqual(data["total_time"], 10)
            data["zoom_intervals"] = [{"zoom": 2, "start": 3, "end": 5}]
            save_conditions(data, path)
            self.assertEqual(load_conditions(path), data)
            actions = recording_actions(data)
            self.assertEqual([event["seconds"] for event in actions["events"]], [3, 5])
            self.assertEqual([event["values"]["zoom"] for event in actions["events"]], [2, 1])

    def test_invalid_intervals(self):
        for intervals in ([{"zoom": 2, "start": 4, "end": 3}],
                          [{"zoom": 2, "start": 3, "end": 11}],
                          [{"zoom": 2, "start": 1, "end": 5}, {"zoom": 3, "start": 4, "end": 6}]):
            data = {"fps": 23, "total_time": 10, "initial": {"zoom": 1, "focus": 0, "auto_focus": True}, "zoom_intervals": intervals}
            with self.subTest(intervals=intervals), self.assertRaises(ValueError):
                validate_conditions(data)

    def test_adjacent_intervals_restore_then_apply(self):
        data = {"fps": 10, "total_time": 10, "initial": {"zoom": 1, "focus": 0, "auto_focus": True},
                "zoom_intervals": [{"zoom": 2, "start": 0, "end": 3}, {"zoom": 3, "start": 3, "end": 6}]}
        events = recording_actions(data)["events"]
        self.assertEqual([(event["seconds"], event["values"]["zoom"]) for event in events], [(0, 2), (3, 1), (3, 3), (6, 1)])

    def test_preview_rotates_clockwise(self):
        frame = np.zeros((24, 32, 3), np.uint8)
        frame[:, :16] = 255
        body = cv2.imencode(".jpg", frame)[1].tobytes()
        phone = PhoneUSB()
        with patch.object(phone, "request", return_value=(body, {})):
            rotated = phone.preview()
        self.assertEqual(rotated.shape, (32, 24, 3))
        self.assertGreater(rotated[:16].mean(), rotated[16:].mean())

    def test_capture_rotates_and_limits_saved_frames(self):
        class Client:
            count = 0
            stopped = False
            def request(self, endpoint, **values):
                if endpoint == "status":
                    return {"width": 32, "height": 24, "fps_max": 30, "recording": not self.stopped}
                if endpoint == "frame":
                    self.count += 1
                    return cv2.imencode(".jpg", np.zeros((24, 32, 3), np.uint8))[1].tobytes(), {"X-Frame-Number": str(self.count)}
                if endpoint == "stop":
                    self.stopped = True
                return {}
            def configure(self, values):
                return {}
        with tempfile.TemporaryDirectory() as directory, patch("camera.phone_usb.save_actions") as save:
            client = Client()
            actions = {"initial": {"zoom": 1, "focus": 0, "auto_focus": True}, "events": [], "duration": .2}
            path = Path(directory) / "original.mp4"
            capture = PhoneCapture(client, 10, path, actions, True, save_recorded_actions=False)
            frames = []
            while True:
                ok, frame = capture.read()
                if not ok:
                    break
                frames.append(frame)
            capture.release()
            self.assertEqual(len(frames), 2)
            self.assertEqual(frames[0].shape, (32, 24, 3))
            self.assertEqual(capture.get(cv2.CAP_PROP_FRAME_WIDTH), 24)
            video = cv2.VideoCapture(str(path))
            self.assertEqual(video.get(cv2.CAP_PROP_FRAME_HEIGHT), 32)
            self.assertEqual(video.get(cv2.CAP_PROP_FRAME_COUNT), 2)
            video.release()
            self.assertTrue(client.stopped)
            save.assert_not_called()


if __name__ == "__main__":
    unittest.main()
