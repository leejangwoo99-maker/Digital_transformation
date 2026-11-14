from __future__ import annotations

import cv2
import numpy as np


def auto_exclude_regions(image) -> list[dict]:
    # Retained for compatibility with older callers; exclusion is manual only.
    return []


def mask_regions(frame, regions):
    output = frame.copy()
    height, width = frame.shape[:2]
    for region in regions:
        if region["role"] != "exclude":
            continue
        if region.get("angle", 0):
            from inspection.rotated_regions import region_polygon
            cv2.fillConvexPoly(output, np.round(region_polygon(region, frame.shape)).astype(np.int32), (0, 0, 0))
            continue
        x1, y1, x2, y2 = region["box"]
        output[max(0, round(y1 * height)):min(height, round(y2 * height)),
               max(0, round(x1 * width)):min(width, round(x2 * width))] = 0
    return output


def is_contained(box, area) -> bool:
    if box[2] <= box[0] or box[3] <= box[1] or area[2] <= area[0] or area[3] <= area[1]:
        return False
    return area[0] <= box[0] and area[1] <= box[1] and box[2] <= area[2] and box[3] <= area[3]


def filter_training_scratches(text, classes):
    from inspection.rotated_regions import row_polygon, polygon_inside
    names = {str(key): str(value).lower() for key, value in classes.items()}
    rows = []
    for line in text.splitlines():
        if not line.strip():
            continue
        parts = line.split()
        if len(parts) != 5 and not (len(parts) >= 7 and len(parts) % 2 == 1):
            raise ValueError(f"Invalid YOLO label row: {line}")
        rows.append((line, names.get(parts[0], ""), row_polygon(parts)))
    areas = [box for _, name, box in rows if name == "except"]
    kept = [line for line, name, box in rows
            if name != "scratch" or not any(polygon_inside(box, area) for area in areas)]
    return "\n".join(kept) + ("\n" if kept else ""), len(rows) - len(kept)
