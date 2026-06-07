from __future__ import annotations

from typing import Any

import cv2


class USBCamera:
    def __init__(self, settings: dict[str, Any]) -> None:
        camera_settings = settings.get("camera", {})
        self.index = int(camera_settings.get("index", 0))
        self.width = int(camera_settings.get("width", 1280))
        self.height = int(camera_settings.get("height", 720))
        self.fps = int(camera_settings.get("fps", 30))
        self.backend = camera_settings.get("backend", "dshow")
        self.fourcc = camera_settings.get("fourcc", "MJPG")
        self.brightness = int(camera_settings.get("brightness", 128))
        self.contrast = int(camera_settings.get("contrast", 128))
        self.sharpness = int(camera_settings.get("sharpness", 128))
        self.capture: cv2.VideoCapture | None = None

    def open(self) -> None:
        backend_code = cv2.CAP_DSHOW if self.backend == "dshow" else cv2.CAP_ANY
        self.capture = cv2.VideoCapture(self.index, backend_code)
        if self.fourcc:
            self.capture.set(cv2.CAP_PROP_FOURCC, cv2.VideoWriter_fourcc(*self.fourcc[:4]))
        self.capture.set(cv2.CAP_PROP_FRAME_WIDTH, self.width)
        self.capture.set(cv2.CAP_PROP_FRAME_HEIGHT, self.height)
        self.capture.set(cv2.CAP_PROP_FPS, self.fps)
        self.set_brightness(self.brightness)
        self.set_contrast(self.contrast)
        self.set_sharpness(self.sharpness)

        if not self.capture.isOpened():
            raise RuntimeError(f"USB camera open failed. camera index={self.index}")

    def read(self):
        if self.capture is None:
            raise RuntimeError("Camera is not opened.")

        ok, frame = self.capture.read()
        if not ok or frame is None:
            raise RuntimeError("Camera frame grab failed.")
        return frame

    def release(self) -> None:
        if self.capture is not None:
            self.capture.release()
            self.capture = None

    def set_brightness(self, value: int) -> bool:
        if self.capture is None:
            return False
        return bool(self.capture.set(cv2.CAP_PROP_BRIGHTNESS, int(value)))

    def set_contrast(self, value: int) -> bool:
        if self.capture is None:
            return False
        return bool(self.capture.set(cv2.CAP_PROP_CONTRAST, int(value)))

    def set_sharpness(self, value: int) -> bool:
        if self.capture is None:
            return False
        return bool(self.capture.set(cv2.CAP_PROP_SHARPNESS, int(value)))
