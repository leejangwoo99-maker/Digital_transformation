from __future__ import annotations

from pathlib import Path
from typing import Any

import cv2


class YoloInference:
    def __init__(self, settings: dict[str, Any]) -> None:
        model_settings = settings.get("model", {})
        root_dir = Path(settings["root_dir"])
        self.weights_path = root_dir / model_settings.get("weights", "weights/best.pt")
        self.confidence = float(model_settings.get("confidence", 0.35))
        self.iou = float(model_settings.get("iou", 0.45))
        self.image_size = int(model_settings.get("image_size", 640))
        self.model = None
        self.model_error = ""
        self._load_model()

    def _load_model(self) -> None:
        if not self.weights_path.exists() or self.weights_path.stat().st_size == 0:
            self.model_error = f"YOLO weights not found: {self.weights_path}"
            return

        try:
            from ultralytics import YOLO

            self.model = YOLO(str(self.weights_path))
        except Exception as exc:
            self.model_error = f"YOLO load failed: {exc}"

    def predict(self, image):
        if self.model is None:
            annotated = image.copy()
            cv2.putText(
                annotated,
                self.model_error or "YOLO model is not loaded",
                (20, 40),
                cv2.FONT_HERSHEY_SIMPLEX,
                0.8,
                (0, 0, 255),
                2,
                cv2.LINE_AA,
            )
            return [], annotated

        results = self.model.predict(
            source=image,
            conf=self.confidence,
            iou=self.iou,
            imgsz=self.image_size,
            verbose=False,
        )

        result = results[0]
        detections = self._parse_detections(result)
        annotated = result.plot()
        return detections, annotated

    def _parse_detections(self, result) -> list[dict[str, Any]]:
        detections: list[dict[str, Any]] = []
        names = result.names

        if result.boxes is None:
            return detections

        for box in result.boxes:
            class_id = int(box.cls[0].item())
            confidence = float(box.conf[0].item())
            xyxy = [float(value) for value in box.xyxy[0].tolist()]
            detections.append(
                {
                    "class_id": class_id,
                    "class_name": names.get(class_id, str(class_id)),
                    "confidence": confidence,
                    "xyxy": xyxy,
                }
            )

        return detections
