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
        self.recording_confidence = .01 if settings.get("record_confidence_points") else self.confidence
        self.iou = float(model_settings.get("iou", 0.45))
        self.image_size = int(model_settings.get("image_size", 640))
        self.original_resolution = bool(model_settings.get("original_resolution", False))
        self.render_predictions = bool(model_settings.get("render_predictions", True))
        self.backend = model_settings.get('appearance_backend','pytorch')
        if self.backend not in ('pytorch','openvino'):
            raise ValueError(f'Unknown appearance backend: {self.backend}')
        self.model_path = self.weights_path
        self.model = None
        self.model_error = ""
        self._load_model()

    def _load_model(self) -> None:
        if not self.weights_path.exists() or self.weights_path.stat().st_size == 0:
            self.model_error = f"YOLO weights not found: {self.weights_path}"
            return

        try:
            from ultralytics import YOLO

            if self.backend == 'openvino':
                import yaml
                from model.appearance_openvino import openvino_artifact
                self.model_path = openvino_artifact(self.weights_path,self.image_size)
                metadata = yaml.safe_load((self.model_path/'metadata.yaml').read_text(encoding='utf-8'))
                self.model = YOLO(str(self.model_path),task=metadata['task'])
            else:
                self.model = YOLO(str(self.model_path))
        except Exception as exc:
            self.model_error = f"YOLO load failed: {exc}"
            if self.backend == 'openvino':
                raise RuntimeError(f'Appearance OpenVINO preparation failed: {exc}') from exc

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

        inference_image = image
        if self.original_resolution:
            height, width = image.shape[:2]
            inference_image = cv2.copyMakeBorder(image, 0, (-height) % 32, 0, (-width) % 32,
                                                 cv2.BORDER_CONSTANT, value=(114, 114, 114))
        results = self.model.predict(
            source=inference_image,
            conf=self.recording_confidence,
            iou=self.iou,
            imgsz=(list(inference_image.shape[:2])
                   if self.original_resolution else self.image_size),
            rect=True,
            verbose=False,
            **({'device':'intel:cpu'} if self.backend == 'openvino' else {}),
        )

        result = results[0]
        detections = self._parse_detections(result)
        annotated = result.plot() if self.render_predictions else image
        if self.original_resolution:
            height, width = image.shape[:2]
            for detection in detections:
                x1, y1, x2, y2 = detection["xyxy"]
                detection["xyxy"] = [max(0, min(width, x1)), max(0, min(height, y1)),
                                     max(0, min(width, x2)), max(0, min(height, y2))]
            detections = [box for box in detections if box["xyxy"][2] > box["xyxy"][0]
                          and box["xyxy"][3] > box["xyxy"][1]]
            annotated = annotated[:height, :width].copy()
        return detections, annotated

    def _parse_detections(self, result) -> list[dict[str, Any]]:
        detections: list[dict[str, Any]] = []
        names = result.names

        if getattr(result, "obb", None) is not None:
            for box in result.obb:
                class_id = int(box.cls[0].item())
                detections.append({"class_id": class_id, "class_name": names.get(class_id, str(class_id)),
                    "confidence": float(box.conf[0].item()), "xyxy": box.xyxy[0].tolist(),
                    "polygon": box.xyxyxyxy[0].tolist()})
            return detections

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
