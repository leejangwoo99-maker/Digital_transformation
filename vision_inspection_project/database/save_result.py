from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import cv2


class ResultSaver:
    def __init__(self, settings: dict[str, Any]) -> None:
        root_dir = Path(settings["root_dir"])
        save_settings = settings.get("save", {})
        self.image_dir = root_dir / save_settings.get("image_dir", "data/results")
        self.csv_path = root_dir / save_settings.get("csv_path", "data/results/inspection_results.csv")
        self.image_dir.mkdir(parents=True, exist_ok=True)
        self.csv_path.parent.mkdir(parents=True, exist_ok=True)

    def save(
        self,
        image,
        source: str,
        result: dict[str, Any],
        detections: list[dict[str, Any]],
    ) -> dict[str, str]:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        image_path = self.image_dir / f"{timestamp}_{result['judge']}.jpg"
        cv2.imwrite(str(image_path), image)

        row = {
            "timestamp": timestamp,
            "source": source,
            "judge": result["judge"],
            "reason": result["reason"],
            "ng_count": result["ng_count"],
            "total_count": result["total_count"],
            "image_path": str(image_path),
            "detections_json": json.dumps(detections, ensure_ascii=False),
        }
        self._append_csv(row)

        return {"image_path": str(image_path), "csv_path": str(self.csv_path)}

    def _append_csv(self, row: dict[str, Any]) -> None:
        exists = self.csv_path.exists()
        with self.csv_path.open("a", newline="", encoding="utf-8-sig") as file:
            writer = csv.DictWriter(file, fieldnames=list(row.keys()))
            if not exists:
                writer.writeheader()
            writer.writerow(row)
