import math
import re

from inspection.special_checks import time_path


def group_fps(group):
    match = re.search(r"_(\d+(?:\.\d+)?)fps$", group or "")
    if not match or float(match[1]) <= 0:
        raise ValueError("Select a new frame group ending in _<number>fps. Extract frames again first.")
    return float(match[1])


def frame_group_name(stem, fps):
    if not math.isfinite(fps) or fps <= 0:
        raise ValueError("FPS must be greater than zero.")
    return f"{stem}_{fps:g}fps"


def sampled_frame_count(source_count, source_fps, target_fps):
    return math.floor((source_count - 1) * target_fps / source_fps + 1e-9) + 1 if source_count > 0 else 0


def load_frame_range(info, group, kind):
    path = time_path(info, group, kind)
    if not path.exists():
        return 1, None
    values = dict(line.split("=", 1) for line in path.read_text(encoding="utf-8").splitlines() if "=" in line)
    if "start_frame" not in values:
        raise ValueError(f"Old seconds setting: set the frame range again: {path}")
    start = int(values["start_frame"])
    end = int(values["end_frame"]) if values.get("end_frame", "").strip() else None
    validate_frame_range(start, end)
    return start, end


def validate_frame_range(start, end):
    if start < 1 or (end is not None and end < start):
        raise ValueError("Frame numbers start at 1. End must be >= start. Blank end = last frame.")


def save_frame_range(info, group, kind, start, end):
    validate_frame_range(start, end)
    info.mkdir(parents=True, exist_ok=True)
    time_path(info, group, kind).write_text(
        f"start_frame={start}\nend_frame={'' if end is None else end}\n", encoding="utf-8")


class WindowResults:
    def __init__(self):
        self.successes = set()
        self.last_failures = {}
        self.status_lines = []

    def update(self, kind, items, failures, number, start, end, total):
        deadline = end if end is not None else total
        if number < start:
            self.status_lines.append(f"{kind}: WAIT (frames {start}-{deadline})")
            return []
        if number <= deadline:
            generic = [failure for failure in failures if failure.get("item_key") not in items]
            for item in items:
                key = (kind, item)
                bad = [failure for failure in failures if failure.get("item_key") == item] or generic
                if bad:
                    self.last_failures[key] = bad
                else:
                    self.successes.add(key)
        remaining = [item for item in items if (kind, item) not in self.successes]
        state = "OK" if not remaining else "NG" if number >= deadline else "PENDING"
        self.status_lines.append(f"{kind}: {state} ({len(items)-len(remaining)}/{len(items)} passed)")
        if state != "NG":
            return []
        return [failure for item in remaining for failure in self.last_failures.get((kind, item), [])]
