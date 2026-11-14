"""Geometry-preserving lighting samples and Conn temporal confirmation."""

import numpy as np


def reflection_samples(image):
    height, width = image.shape[:2]
    y, x = np.mgrid[:height, :width].astype(np.float32)
    for index, (cx, cy, gain, contrast) in enumerate((
        (.25, .35, 65, .75), (.7, .65, 95, .6), (.5, .5, -45, 1.1)
    )):
        glow = np.exp(-(((x / max(width, 1) - cx) / .3) ** 2
                        + ((y / max(height, 1) - cy) / .4) ** 2) / 2)
        sample = (image.astype(np.float32) - 128) * contrast + 128 + gain * glow[..., None]
        yield f"reflection_{index + 1}", np.clip(sample, 0, 255).astype(np.uint8)


class ConnConfirmation:
    def __init__(self):
        self.previous_number = None
        self.previous = []
        self.pending = []

    def update(self, failures, number):
        previous = self.previous if self.previous_number == number - 1 else []
        current = [item for item in failures if item.get("inspection_kind") == "conn"]
        confirmed = []
        self.pending = []
        for item in current:
            matches = [old for old in previous
                       if old["class_name"] == item["class_name"]
                       and old.get("pin_number") == item.get("pin_number")]
            centers = item.get("evidence_centers")
            if centers:
                matches = [old for old in matches if any(
                    np.linalg.norm(np.asarray(a) - b) <= 20
                    for a in centers for b in old.get("evidence_centers", []))]
            if matches:
                confirmed.append(dict(item, consecutive_frames=2))
            else:
                pending = dict(item, consecutive_frames=1, confirmation_state="pending")
                pending["display_label"] = item.get("display_label", item["class_name"]).replace("NG", "PENDING") + " (1/2)"
                self.pending.append(pending)
        self.previous_number = number
        self.previous = current
        return [item for item in failures if item.get("inspection_kind") != "conn"] + confirmed
