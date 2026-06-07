from __future__ import annotations

from typing import Any


class InspectionJudge:
    def __init__(self, settings: dict[str, Any]) -> None:
        judge_settings = settings.get("judge", {})
        self.ng_classes = set(judge_settings.get("ng_classes", ["defect", "ng"]))
        self.min_confidence = float(judge_settings.get("min_confidence", 0.35))
        self.no_detection_judge = judge_settings.get("no_detection_judge", "OK")

    def judge(self, detections: list[dict[str, Any]]) -> dict[str, Any]:
        valid_detections = [
            detection
            for detection in detections
            if detection.get("confidence", 0.0) >= self.min_confidence
        ]

        if not valid_detections:
            return {
                "judge": self.no_detection_judge,
                "reason": "No valid detections",
                "ng_count": 0,
                "total_count": 0,
            }

        ng_detections = [
            detection
            for detection in valid_detections
            if str(detection.get("class_name", "")).lower() in self.ng_classes
        ]

        if ng_detections:
            return {
                "judge": "NG",
                "reason": f"Detected NG class: {ng_detections[0]['class_name']}",
                "ng_count": len(ng_detections),
                "total_count": len(valid_detections),
            }

        return {
            "judge": "OK",
            "reason": "Only allowed classes detected",
            "ng_count": 0,
            "total_count": len(valid_detections),
        }
