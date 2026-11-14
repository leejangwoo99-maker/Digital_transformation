"""Group-time persistence and deterministic sharpest sampled-frame selection."""
import json
import math
from pathlib import Path

import cv2


def inline_inspectors(special, tasks, total_frames):
    import copy
    from inspection.conn_robustness import ConnConfirmation
    inspectors,status = {},{}
    for task,(start,end) in tasks.items():
        kind = photo_kind(task)
        if end > total_frames:
            raise ValueError(f'{kind}: inspection range exceeds {total_frames} sampled frames.')
        if not any(start <= number <= end for number in special.annotation_frames[kind]):
            raise ValueError(f'{kind}: no matching training labels in inspection range.')
        inspector = copy.copy(special)
        from inspection.special_checks import CodeReader
        from inspection.polygon_roi import PolygonAligner
        original_reader = getattr(special,'qr',None)
        inspector.qr = CodeReader() if original_reader is None or isinstance(original_reader,CodeReader) else copy.copy(original_reader)
        inspector.polygon_aligner = PolygonAligner()
        inspector.ocr = None if getattr(special,'ocr',None) is None else copy.copy(special.ocr)
        if inspector.ocr is not None and hasattr(inspector.ocr,'cache'):
            inspector.ocr.cache = {}
        inspector.conn_confirmation = ConnConfirmation()
        inspector.references = {kind:special.references[kind]}
        inspector.annotation_frames = {kind:special.annotation_frames[kind]}
        inspector.labeled_frames_only = True
        inspector.conn_inspection_range = None
        inspector.times = {kind:(0,None)}
        inspector.begin_item_range(start,end)
        inspectors[task] = inspector
        status[task] = f'{task}: WAIT frames {start}~{end}'
    special.annotation_frames = {kind:schedule for kind,schedule in special.annotation_frames.items()
                                 if kind not in {photo_kind(task) for task in tasks}}
    return inspectors,status


def time_path(info, group, kind):
    from inspection.special_checks import group_key
    if kind not in ("label", "OCR", "conn"):
        raise ValueError("Unsupported inspection kind")
    return Path(info) / f"{group_key(group)}_{kind}_inspection_time.txt"


def save_time(info, group, kind, start, end):
    start, end = float(start), float(end)
    if not (math.isfinite(start) and math.isfinite(end) and 0 <= start < end):
        raise ValueError("Seconds must be finite: 0 <= start < end.")
    path = time_path(info, group, kind)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps({"start_seconds":start,"end_seconds":end},indent=2),encoding="utf-8")
    temporary.replace(path)


def load_time(info, group, kind):
    path = time_path(info, group, kind)
    if not path.exists():
        raise ValueError(f"Save {kind} photo inspection seconds first: {path}")
    data = json.loads(path.read_text(encoding="utf-8"))
    start,end = float(data["start_seconds"]),float(data["end_seconds"])
    if not (math.isfinite(start) and math.isfinite(end) and 0 <= start < end):
        raise ValueError(f"Invalid inspection seconds: {path}")
    return start,end


def sharpness(frame):
    gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
    if max(gray.shape) > 1200:
        scale = 1200/max(gray.shape)
        gray = cv2.resize(gray,None,fx=scale,fy=scale,interpolation=cv2.INTER_AREA)
    return float(cv2.Laplacian(gray,cv2.CV_64F).var())


def frame_path(info, group, kind):
    return time_path(info,group,kind).with_name(time_path(info,group,kind).name.replace("_inspection_time", "_inspection_frame"))


def save_frame(info, group, kind, number):
    value = float(number)
    if not math.isfinite(value) or not value.is_integer() or value < 1:
        raise ValueError("Inspection frame must be a positive integer (1-based).")
    path = frame_path(info,group,kind)
    path.parent.mkdir(parents=True,exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps({"frame_number":int(value)},indent=2),encoding="utf-8")
    temporary.replace(path)


def load_frame(info, group, kind):
    path = frame_path(info,group,kind)
    if not path.exists():
        raise ValueError(f"Save {kind} inspection frame first: {path}")
    value = json.loads(path.read_text(encoding="utf-8"))["frame_number"]
    if isinstance(value,bool) or not isinstance(value,int) or value < 1:
        raise ValueError(f"Invalid inspection frame: {path}")
    return value


def save_photo_frame_range(info, group, kind, start, end):
    values = [float(start),float(end)]
    if any(not math.isfinite(value) or not value.is_integer() or value < 1 for value in values) or values[0] > values[1]:
        raise ValueError("Frame range must be integers: 1 <= start <= end.")
    path = frame_path(info,group,kind)
    path.parent.mkdir(parents=True,exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps({"start_frame":int(values[0]),"end_frame":int(values[1])},indent=2),encoding="utf-8")
    temporary.replace(path)


def load_photo_frame_range(info, group, kind):
    ranges = load_photo_frame_ranges(info, group, kind)
    if len(ranges) != 1:
        raise ValueError(f"{kind}: multiple ranges require independent inspection.")
    return ranges[0]


def save_photo_frame_ranges(info, group, kind, ranges):
    normalized = []
    for start,end in ranges:
        values = [float(start),float(end)]
        if any(not math.isfinite(v) or not v.is_integer() or v < 1 for v in values) or values[0] > values[1]:
            raise ValueError("Frame range must be integers: 1 <= start <= end.")
        normalized.append(tuple(map(int,values)))
    if not normalized or (kind == "conn" and len(normalized) != 1):
        raise ValueError("At least one range is required; Conn supports one range only.")
    if normalized != sorted(normalized) or any(b >= c for (_,b),(c,_) in zip(normalized,normalized[1:])):
        raise ValueError("Ranges must be ordered and must not overlap.")
    path = frame_path(info,group,kind)
    path.parent.mkdir(parents=True,exist_ok=True)
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps({'ranges':[{'start_frame':a,'end_frame':b} for a,b in normalized]},indent=2),encoding='utf-8')
    temporary.replace(path)


def photo_kind(task):
    return task.split('__range_',1)[0]


def range_statuses(states):
    grouped = {}
    for task,state in states.items():
        grouped.setdefault(photo_kind(task),[]).append(state)
    return {kind:('NG' if 'NG' in values else 'OK' if all(v == 'OK' for v in values) else 'PENDING')
            for kind,values in grouped.items()}


def inspection_tasks(info, group, kinds):
    tasks = {}
    for kind in kinds:
        ranges = load_photo_frame_ranges(info,group,kind)
        for index,bounds in enumerate(ranges,1):
            tasks[kind if len(ranges) == 1 else f'{kind}__range_{index}'] = bounds
    return tasks


def load_photo_frame_ranges(info, group, kind):
    path = frame_path(info,group,kind)
    if not path.exists():
        raise ValueError(f"Save {kind} inspection frame range first: {path}")
    data = json.loads(path.read_text(encoding="utf-8"))
    if "frame_number" in data:
        number = load_frame(info,group,kind)
        return [(number,number)]
    rows = data.get('ranges',[data])
    result = []
    if not isinstance(rows,list) or not rows or (kind == 'conn' and len(rows) != 1):
        raise ValueError(f"Invalid inspection frame ranges: {path}")
    for row in rows:
        if not isinstance(row,dict):
            raise ValueError(f"Invalid inspection frame range: {path}")
        start,end = row.get('start_frame'),row.get('end_frame')
        if any(isinstance(v,bool) or not isinstance(v,int) or v < 1 for v in (start,end)) or start > end:
            raise ValueError(f"Invalid inspection frame range: {path}")
        result.append((start,end))
    if result != sorted(result) or any(b >= c for (_,b),(c,_) in zip(result,result[1:])):
        raise ValueError(f"Inspection ranges overlap or are unordered: {path}")
    return result


def select_frame_range_photos(source, fps, schedules, ranges, evaluate=None, performance=None):
    from inspection.frame_conditions import sampled_frame_count
    capture = cv2.VideoCapture(str(source))
    try:
        if not capture.isOpened():
            raise ValueError(f"Cannot open photo inspection source: {source}")
        source_fps = float(capture.get(cv2.CAP_PROP_FPS))
        count = int(capture.get(cv2.CAP_PROP_FRAME_COUNT))
        if source_fps <= 0 or not math.isfinite(fps) or not 0 < fps <= source_fps+1e-6:
            raise ValueError("Invalid source/group FPS")
        total = sampled_frame_count(count,source_fps,fps)
        for kind,(start,end) in ranges.items():
            if any(isinstance(n,bool) or not isinstance(n,int) for n in (start,end)) or not 1 <= start <= end <= total:
                raise ValueError(f"{kind}: frame range {start}~{end} exceeds/invalid for {total} group frames.")
    finally:
        capture.release()
    seconds = {kind:((start-1)/fps,(end-1)/fps) for kind,(start,end) in ranges.items()}
    selected = select_photos(source,fps,schedules,seconds,evaluate=evaluate,performance=performance)
    for kind,selection in selected.items():
        selection.update(selection="first_complete_pass_in_range" if evaluate else "sharpest_frame_range",start_frame=ranges[kind][0],end_frame=ranges[kind][1],
                         source_frame_index=math.ceil((selection["frame_number"]-1)*source_fps/fps-1e-9))
    return selected


def select_numbered_photos(source, fps, schedules, numbers):
    from inspection.frame_conditions import sampled_frame_count
    capture = cv2.VideoCapture(str(source))
    try:
        if not capture.isOpened():
            raise ValueError(f"Cannot open photo inspection source: {source}")
        source_fps = float(capture.get(cv2.CAP_PROP_FPS))
        count = int(capture.get(cv2.CAP_PROP_FRAME_COUNT))
        if not math.isfinite(fps) or source_fps <= 0 or not 0 < fps <= source_fps+1e-6:
            raise ValueError("Invalid source/group FPS")
        total = sampled_frame_count(count,source_fps,fps)
        for kind,number in numbers.items():
            if not isinstance(number,int) or not 1 <= number <= total:
                raise ValueError(f"{kind}: frame {number} exceeds this video's {total} group frames.")
            if number not in schedules[kind]:
                raise ValueError(f"{kind}: frame {number} has no matching training label. Inspection stopped.")
        cache,selected = {},{}
        for kind,number in numbers.items():
            source_index = math.ceil((number-1)*source_fps/fps-1e-9)
            if source_index not in cache:
                capture.set(cv2.CAP_PROP_POS_FRAMES,source_index)
                ok,image = capture.read()
                if not ok or abs(capture.get(cv2.CAP_PROP_POS_FRAMES)-(source_index+1)) > .5:
                    capture.set(cv2.CAP_PROP_POS_FRAMES,0)
                    for _ in range(source_index+1):
                        ok,image = capture.read()
                        if not ok:
                            break
                if not ok:
                    raise ValueError(f"Cannot decode exact frame {number} for {kind}.")
                cache[source_index] = image
            selected[kind] = {"frame_number":number,"seconds":(number-1)/fps,
                              "source_frame_index":source_index,"selection":"fixed_frame",
                              "image":cache[source_index]}
        return selected
    finally:
        capture.release()


def select_photos(source, fps, schedules, ranges, progress=None, evaluate=None, performance=None):
    capture = cv2.VideoCapture(str(source))
    selected = {}
    completed = set()
    attempts = {kind: 0 for kind in ranges}
    try:
        if not capture.isOpened():
            raise ValueError(f"Cannot open photo inspection source: {source}")
        source_fps = float(capture.get(cv2.CAP_PROP_FPS))
        count = int(capture.get(cv2.CAP_PROP_FRAME_COUNT))
        if source_fps <= 0 or fps <= 0 or fps > source_fps+1e-6:
            raise ValueError("Invalid source/group FPS for photo inspection")
        duration = count/source_fps
        for kind,(start,end) in ranges.items():
            if end > duration+1e-6:
                raise ValueError(f"{kind} seconds {start:g}~{end:g} exceed video duration {duration:g}.")
        index,number = 0,0
        while True:
            ok,frame = performance.call("photo_frame_read_decode",capture.read) if performance else capture.read()
            if not ok:
                if index < count and index/source_fps <= max(end for _,end in ranges.values()):
                    raise ValueError("Video decoding ended during photo selection")
                break
            source_index = index
            index += 1
            if source_index/source_fps+1e-9 < number/fps:
                continue
            seconds = number/fps
            number += 1
            if seconds > max(end for _,end in ranges.values())+1e-9:
                break
            active = [kind for kind,(start,end) in ranges.items() if start-1e-9 <= seconds <= end+1e-9]
            if not active:
                continue
            score = sharpness(frame)
            for kind in active:
                if kind in completed or number not in schedules[kind]:
                    continue
                attempts[kind] += 1
                candidate = {"frame_number":number,"seconds":seconds,"sharpness":score,"image":frame.copy()}
                if evaluate is not None:
                    passed = evaluate(kind, frame, number)
                    candidate.update(attempted_frames=attempts[kind], result="OK" if passed else "NG",
                                     selection="first_complete_pass_in_range")
                    selected[kind] = candidate
                    if passed:
                        completed.add(kind)
                elif kind not in selected or score > selected[kind]["sharpness"]:
                    selected[kind] = candidate
            if evaluate is not None and len(completed) == len(ranges):
                break
            if progress:
                progress(number, count, seconds, 0)
    finally:
        capture.release()
    for kind in ranges:
        if kind not in selected:
            raise ValueError(f"{kind}: no matching training labels in inspection range. Inspection stopped.")
        if evaluate is not None:
            selected[kind]["decision_frame"] = (selected[kind]["frame_number"] if kind in completed
                                                 else math.floor(ranges[kind][1]*fps+1e-9)+1)
        number = selected[kind]["frame_number"]
        if number not in schedules[kind]:
            raise ValueError(f"{kind}: sharpest frame {number} has no matching training label. Inspection stopped.")
    return selected
