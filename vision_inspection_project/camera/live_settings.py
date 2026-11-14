"""Resolve live inspection strictly from the main window's selected group."""
import math
import re
from pathlib import Path

from camera.record_conditions import load_conditions


def resolve_live_settings(app):
    group = app.active_group_name
    if not group or not app.active_image_paths:
        raise ValueError("Select a frame group in the main window first.")
    settings = app.settings_for_frame_group(group, app.active_image_paths)
    info = Path(settings["special_info_dir"])
    base = re.sub(r"_\d+(?:\.\d+)?fps$", "", group)
    path = info / f"{base}_record_condition.txt"
    if not path.is_file():
        raise ValueError(f"Record condition missing: {path}")
    conditions = load_conditions(path)
    group_fps = settings.get('group_fps',settings['test_fps'])
    if not math.isclose(group_fps,conditions["fps"],abs_tol=1e-6):
        raise ValueError(f"Record condition FPS ({conditions['fps']:g}) must match group FPS ({group_fps:g}).")
    settings.update(record_condition=str(path),live_expected_frames=math.ceil(conditions["fps"]*conditions["total_time"]),
                    special_photo_inspection=True,photo_frame_selection=True,live_photo_inspection=True,
                    record_confidence_points=False)
    from inspection.pipeline_audit import audit_pipeline
    audit = audit_pipeline(settings)
    if audit["errors"]:
        raise ValueError("\n".join(audit["errors"]))
    from inspection.special_checks import load_reference
    from inspection.photo_inspection import load_photo_frame_ranges
    for kind in ("OCR","label","conn"):
        if load_reference(Path(settings["root_dir"]),group,kind):
            end = max(end for _,end in load_photo_frame_ranges(info,group,kind))
            if end > settings["live_expected_frames"]:
                raise ValueError(f"{kind}: inspection frame {end} exceeds capture length ({settings['live_expected_frames']} frames).")
    from inspection.frame_conditions import sampled_frame_count
    settings['live_expected_frames'] = sampled_frame_count(settings['live_expected_frames'],conditions['fps'],settings['test_fps'])
    return settings,conditions
