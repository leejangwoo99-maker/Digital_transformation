from pathlib import Path

from inspection.special_checks import group_key, load_reference, load_time


def audit_pipeline(settings):
    root = Path(settings["root_dir"])
    group = settings.get("special_group")
    info = Path(settings.get("special_info_dir", root / "test" / "info"))
    messages, errors = [], []
    sampling = None
    if settings.get('group_fps') and settings.get('test_fps'):
        from inspection.test_sampling import TestSampling
        try:
            sampling = TestSampling(settings['group_fps'],settings['test_fps'])
            messages.append(f"Test sampling: {sampling.test_fps:g} FPS; annotations from {sampling.group_fps:g} FPS group")
        except ValueError as exc:
            errors.append(str(exc))
    weights = root / settings.get("model", {}).get("weights", "weights/best.pt")
    required = settings.get("require_regular_model", True)
    if required:
        backend = settings.get('model',{}).get('appearance_backend','pytorch')
        messages.append(f"Appearance YOLO ({backend}): {weights}; analysis 640; output original resolution")
        if not weights.is_file() or not weights.stat().st_size:
            errors.append(f"Appearance model missing: {weights}. Train the selected group first.")
    else:
        messages.append("Appearance: no regular labels in selected group")
    if settings.get('essential_shapes'):
        messages.append('Essential: each group must have one labeled sample where all of its boxes pass; ungrouped boxes are independent.')
    for kind in ("OCR", "conn", "label"):
        reference = load_reference(root, group, kind) if group else None
        if not reference:
            messages.append(f"{kind}: disabled (no labels for this group)")
            continue
        if settings.get("special_labeled_frames"):
            from inspection.special_checks import labeled_frame_schedule
            schedule = labeled_frame_schedule(root, group, kind, settings.get("special_frame_paths", []))
            if sampling:
                schedule = sampling.schedule(schedule)
            if kind == "conn" and settings.get("conn_inspection_frame"):
                from inspection.photo_inspection import load_photo_frame_range
                try:
                    start,end = load_photo_frame_range(info,group,kind)
                    if sampling:
                        start,end = sampling.interval((start,end))
                    if not schedule:
                        raise ValueError("Conn requires labeled training frames.")
                    messages.append(f"Conn: EVERY frame {start}~{end}; nearest labeled position conditions; training unchanged")
                except ValueError as exc:
                    errors.append(str(exc))
            elif settings.get("special_photo_inspection") and kind in ("OCR","label"):
                from inspection.photo_inspection import load_time as load_photo_time
                try:
                    if settings.get("photo_frame_selection"):
                        from inspection.photo_inspection import load_photo_frame_ranges
                        ranges = load_photo_frame_ranges(info,group,kind)
                        if sampling:
                            ranges = [sampling.interval(bounds) for bounds in ranges]
                        for start,end in ranges:
                            if not any(start <= number <= end for number in schedule):
                                raise ValueError(f"{kind}: no training labels in frames {start}~{end}.")
                            messages.append(f"{kind}: labeled candidates in frames {start}~{end}; each item latches OK and is not rechecked; ALL items required in EVERY range; training uses ALL labeled frames")
                    else:
                        start,end = load_photo_time(info,group,kind)
                        messages.append(f"{kind}: labeled candidates in {start:g}~{end:g} sec; first whole-frame OK wins; no cross-frame partial merging")
                except ValueError as exc:
                    errors.append(str(exc))
            else:
                messages.append(f"{kind}: only labeled frame numbers {', '.join(map(str, sorted(schedule)))}; per-frame regions/conditions")
        elif settings.get("special_frame_ranges"):
            from inspection.frame_conditions import load_frame_range
            start, end = load_frame_range(info, group, kind)
            unit = "frames"
        else:
            start, end = load_time(info, group, kind)
            unit = "seconds"
        if not settings.get("special_labeled_frames"):
            messages.append(f"{kind}: {len(reference['regions'])} regions; {unit} {start:g} to {end if end is not None else 'end'}; {reference['image']}")
        if kind == "conn":
            path = root / "weights" / f"{group_key(group)}_conn_algorith.pt"
            messages.append(f"Conn YOLO: {path}")
            if not path.is_file() or not path.stat().st_size:
                errors.append(f"Conn model missing: {path}. Train the selected group first.")
            from inspection.special_checks import read_json
            metadata = read_json(path.with_suffix(".json"), {})
            messages.append("Conn analysis: connector crop + full-image candidates rechecked at connector training scale" if metadata.get("conn_crop")
                            else "WARNING: legacy whole-image Conn model. Train again to enable connector-crop training.")
            pins = sum(region['role'] == 'pin' for region in reference['regions'])
            messages.append(f"Conn position check: {pins} pin areas; one detected pin center per area (no slot required)")
            if not pins:
                errors.append("Conn requires pin areas for the expected pin centers.")
            messages.append("Conn M boundaries: >=2 detected pin centers outside all red boxes fails; numeric gap checks removed, pin-area checks remain active")
        else:
            if kind == "OCR" and any(region["role"] == "mark" for region in reference["regions"]):
                from inspection.mark_model import model_path as mark_model_path
                mark_path = mark_model_path(root,group)
                messages.append(f"Mark: learned normal image templates, bounded position/scale/rotation tolerance: {mark_path}")
                if not mark_path.exists():
                    errors.append(f"Mark model missing. Train the selected group first: {mark_path}")
                if not any(region["role"] == "text" for region in reference["regions"]):
                    continue
            from inspection.text_image_model import model_path
            path = model_path(root, group, kind)
            if path.exists():
                messages.append(f"{kind}: supervised Text PASS/FAIL image model: {path}; UNKNOWN defers to exact OCR; confident FAIL vetoes; fine-tuned CTC character recognition; QR comparison retained")
            else:
                messages.append(f"{kind}: pretrained multilingual OCR" + (" + QR decoder" if kind == "label" else ""))
    return {"group": group, "messages": messages, "errors": errors}
