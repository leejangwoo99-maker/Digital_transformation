from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
from time import monotonic, perf_counter
from typing import Any

import numpy as np
import yaml
from inspection.automatic_runtime import serialized as serialized_runtime


ROOT_DIR = Path(__file__).resolve().parent
CONFIG_PATH = ROOT_DIR / "config" / "settings.yaml"
VISION_BASE_DIR = Path(r"C:\vision_inspection")
OPERATION_DIR = VISION_BASE_DIR / "operation"
ORIGINAL_VIDEO_DIR = VISION_BASE_DIR / "original"
DEFAULT_TEST_VIDEO_DIR = VISION_BASE_DIR / "test"
TEST_LOG_DIR = VISION_BASE_DIR / "testlog"


def ensure_vision_dirs() -> None:
    for directory in (VISION_BASE_DIR, OPERATION_DIR, ORIGINAL_VIDEO_DIR, DEFAULT_TEST_VIDEO_DIR, TEST_LOG_DIR):
        directory.mkdir(parents=True, exist_ok=True)
    (DEFAULT_TEST_VIDEO_DIR / "info").mkdir(parents=True, exist_ok=True)


ensure_vision_dirs()


def load_settings(path: Path = CONFIG_PATH) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as file:
        settings = yaml.safe_load(file) or {}

    settings["root_dir"] = str(ROOT_DIR)
    return settings


def inspect_image(image_path: Path, settings: dict[str, Any]) -> dict[str, Any]:
    import cv2

    from database.save_result import ResultSaver
    from inspection.judge_logic import InspectionJudge
    from model.yolo_inference import YoloInference

    image = cv2.imread(str(image_path))
    if image is None:
        raise FileNotFoundError(f"Cannot read image: {image_path}")

    detector = YoloInference(settings)
    judge = InspectionJudge(settings)
    saver = ResultSaver(settings)

    detections, annotated = detector.predict(image)
    result = judge.judge(detections)
    saved = saver.save(
        image=annotated,
        source=str(image_path),
        result=result,
        detections=detections,
    )

    print(f"source: {image_path}")
    print(f"judge: {result['judge']}")
    print(f"reason: {result['reason']}")
    print(f"image: {saved['image_path']}")
    print(f"csv: {saved['csv_path']}")
    return result


def find_images(path: Path) -> list[Path]:
    if path.is_file():
        return [path]

    extensions = ("*.jpg", "*.jpeg", "*.png", "*.bmp")
    images: list[Path] = []
    for extension in extensions:
        images.extend(path.glob(extension))
    return sorted(images)


def inspect_images(path: Path, settings: dict[str, Any]) -> None:
    import cv2

    from database.save_result import ResultSaver
    from inspection.judge_logic import InspectionJudge
    from model.yolo_inference import YoloInference

    image_paths = find_images(path)
    if not image_paths:
        raise FileNotFoundError(f"No images found: {path}")

    detector = YoloInference(settings)
    judge = InspectionJudge(settings)
    saver = ResultSaver(settings)

    ok_count = 0
    ng_count = 0
    for image_path in image_paths:
        image = cv2.imread(str(image_path))
        if image is None:
            print(f"skip unreadable image: {image_path}")
            continue

        detections, annotated = detector.predict(image)
        result = judge.judge(detections)
        saved = saver.save(
            image=annotated,
            source=str(image_path),
            result=result,
            detections=detections,
        )
        print(f"{image_path.name}: {result['judge']} - {result['reason']} - {saved['image_path']}")
        if result["judge"] == "NG":
            ng_count += 1
        else:
            ok_count += 1

    print(f"total: {len(image_paths)}")
    print(f"OK: {ok_count}")
    print(f"NG: {ng_count}")


def _ng_detections(frame, detections, settings):
    from inspection.region_masks import is_contained

    judge_settings = settings.get("judge", {})
    ng_classes = {class_name.lower() for class_name in judge_settings.get("ng_classes", [])}
    min_confidence = float(judge_settings.get("min_confidence", 0.35))

    ng_detections = []
    height, width = frame.shape[:2]
    exception_boxes = [[x1 * width, y1 * height, x2 * width, y2 * height]
                       for region in settings.get("except_regions", [])
                       for x1, y1, x2, y2 in [region["box"]]]
    from inspection.rotated_regions import region_polygon, polygon_inside
    exception_polygons = [region_polygon(region, frame.shape) for region in settings.get("except_regions", [])]
    if not exception_boxes and settings.get("allow_predicted_except", False):
        exception_boxes.extend(detection["xyxy"] for detection in detections
                               if str(detection.get("class_name", "")).lower() == "except")
    for detection in detections:
        class_name = str(detection.get("class_name", ""))
        confidence = float(detection.get("confidence", 0.0))
        if not detection.get("special_failure") and (class_name.lower() not in ng_classes or confidence < min_confidence):
            continue
        if not detection.get("special_failure"):
            box = detection["xyxy"]
            polygon = detection.get("polygon") or [[box[0], box[1]], [box[2], box[1]], [box[2], box[3]], [box[0], box[3]]]
            if (exception_polygons and any(polygon_inside(polygon, area) for area in exception_polygons)) or (
                    not exception_polygons and any(is_contained(box, area) for area in exception_boxes)):
                continue

        ng_detections.append(detection)
    return ng_detections


def draw_ng_detections(frame, detections: list[dict[str, Any]], settings: dict[str, Any], *, hidden_kinds=()):
    import cv2

    ng_detections = _ng_detections(frame,detections,settings)
    visible = ng_detections
    if hidden_kinds:
        def is_visible(detection):
            return detection.get('inspection_kind') not in hidden_kinds

        # A hidden predicted exception can affect verdict filtering but not the display.
        if (settings.get('allow_predicted_except') and not settings.get('except_regions')
                and any(not is_visible(d) and str(d.get('class_name','')).lower() == 'except' for d in detections)):
            visible = _ng_detections(frame,[d for d in detections if is_visible(d)],settings)
        else:
            visible = [d for d in ng_detections if is_visible(d)]
    annotated = frame.copy()
    for detection in visible:
        class_name = str(detection.get('class_name',''))
        confidence = float(detection.get('confidence',0.))
        x1, y1, x2, y2 = [int(value) for value in detection["xyxy"]]
        if detection.get("polygon") is not None:
            import numpy as np
            cv2.polylines(annotated, [np.round(detection["polygon"]).astype(np.int32)], True, (0, 0, 255), 3)
        else:
            cv2.rectangle(annotated, (x1, y1), (x2, y2), (0, 0, 255), 3)
        label = detection.get("display_label") or f"{class_name} {confidence:.2f}"
        cv2.putText(
            annotated,
            label,
            (x1, max(24, y1 - 8)),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.8,
            (0, 0, 255),
            2,
            cv2.LINE_AA,
        )
    status = "NG" if visible else "OK"
    color = (0, 0, 255) if visible else (0, 180, 0)
    cv2.putText(annotated, status, (20, 45), cv2.FONT_HERSHEY_SIMPLEX, 1.4, color, 3, cv2.LINE_AA)
    return annotated, ng_detections


def format_video_time(seconds: float) -> str:
    total_seconds = max(0, int(seconds))
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    second = total_seconds % 60
    return f"{hours}:{minutes:02d}:{second:02d}"


def unique_path(path: Path) -> Path:
    if not path.exists():
        return path

    index = 1
    while True:
        import re
        suffix = re.search(r'_\[\d+(?:\.\d+)?fps\]$',path.stem)
        fps_suffix = suffix.group() if suffix else ''
        base = path.stem[:-len(fps_suffix)] if fps_suffix else path.stem
        marker = base[-2:] if base.endswith(("_p","_f")) else ""
        stem = base[:-2] if marker else base
        candidate = path.with_name(f"{stem}_{index}{marker}{fps_suffix}{path.suffix}")
        if not candidate.exists():
            return candidate
        index += 1


def result_file_path(path, passed):
    import re
    token = re.search(r'_\[\d+(?:\.\d+)?fps\]',path.stem)
    fps_suffix = token.group() if token else ''
    if token:
        path = path.with_name(path.stem[:token.start()]+path.stem[token.end():]+path.suffix)
    stem = path.stem[:-2] if path.stem.endswith(("_p","_f")) else path.stem
    return unique_path(path.with_name(f"{stem}_{'p' if passed else 'f'}{fps_suffix}{path.suffix}"))


def result_hold_frames(fps, successful_kinds):
    import math
    return max(1, math.ceil(float(fps)*.5)) if successful_kinds else 1


def letterbox_frame(frame, canvas_width: int, canvas_height: int):
    import cv2

    height, width = frame.shape[:2]
    scale = min(canvas_width / width, canvas_height / height)
    display_width = int(width * scale)
    display_height = int(height * scale)
    resized = cv2.resize(frame, (display_width, display_height), interpolation=cv2.INTER_AREA)
    canvas = np.zeros((canvas_height, canvas_width, 3), dtype=np.uint8)
    offset_x = (canvas_width - display_width) // 2
    offset_y = (canvas_height - display_height) // 2
    canvas[offset_y : offset_y + display_height, offset_x : offset_x + display_width] = resized
    return canvas


def centered_preview_geometry(width: int, height: int, screen_width: int, screen_height: int):
    available_width = max(1, screen_width - 80)
    available_height = max(1, screen_height - 100)
    scale = min(1.0, available_width / max(1, width), available_height / max(1, height))
    preview_width = max(1, round(width * scale))
    preview_height = max(1, round(height * scale))
    return (preview_width, preview_height,
            max(0, (screen_width - preview_width) // 2),
            max(0, (screen_height - preview_height - 40) // 2))


def is_stream_source(source: str) -> bool:
    return source.lower().startswith(("http://", "https://", "rtsp://", "rtmp://"))


def source_stem(source: str) -> str:
    if is_stream_source(source):
        return f"phone_stream_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    return Path(source).stem


def open_video_capture(source: str):
    import os

    import cv2

    if not is_stream_source(source):
        return cv2.VideoCapture(source)

    os.environ.setdefault(
        "OPENCV_FFMPEG_CAPTURE_OPTIONS",
        "rw_timeout;5000000|stimeout;5000000|timeout;5000000",
    )
    params = []
    if hasattr(cv2, "CAP_PROP_OPEN_TIMEOUT_MSEC"):
        params.extend([cv2.CAP_PROP_OPEN_TIMEOUT_MSEC, 5000])
    if hasattr(cv2, "CAP_PROP_READ_TIMEOUT_MSEC"):
        params.extend([cv2.CAP_PROP_READ_TIMEOUT_MSEC, 5000])

    try:
        capture = cv2.VideoCapture(source, cv2.CAP_FFMPEG, params) if params else cv2.VideoCapture(source, cv2.CAP_FFMPEG)
    except Exception:
        capture = cv2.VideoCapture(source)

    if hasattr(cv2, "CAP_PROP_BUFFERSIZE"):
        capture.set(cv2.CAP_PROP_BUFFERSIZE, 1)
    return capture


@serialized_runtime
def inspect_video(
    video_path: Path | str,
    settings: dict[str, Any],
    output_path: Path | None = None,
    display_width: int = 1600,
    display_height: int = 900,
    show_window: bool = True,
    window_position: tuple[int, int] | None = None,
    max_seconds: float | None = None,
    status_suffix: bool = False,
    log_dir: Path | None = None,
    file_test_log: bool = False,
    progress_callback=None,
    capture_override=None,
    capture_factory=None,
    result_callback=None,
) -> Path:
    import cv2

    from model.yolo_inference import YoloInference
    from inspection.openvino_ocr import cache_snapshot,cache_report,learn_cache_usage,prepare_profiled_models

    source = str(video_path)
    if not settings.get('live_photo_inspection') and not is_stream_source(source):
        settings['test_source_name'] = Path(source).stem
    from inspection.performance import Performance, result_directory
    performance = Performance()
    performance.mark('pipeline_entered')
    from inspection.resource_policy import ResourceGate
    resource_gate = ResourceGate(enabled=settings.get('resource_limits',False),status_callback=
        None if settings.get('live_photo_inspection') else settings.get('resource_status_callback'))
    settings['_resource_gate'] = resource_gate
    if not is_stream_source(source):
        path = Path(video_path)
        if not path.exists():
            raise FileNotFoundError(f"Video file not found: {path}")
        source = str(path)

    if output_path is None:
        output_path = DEFAULT_TEST_VIDEO_DIR / f"{source_stem(source)}_test.mp4"
    if settings.get("result_directory"):
        destination = result_directory(Path(settings["result_directory"]).parent, source_stem(source),
            settings.get("special_group","group"),settings.get("test_fps",30),
            float(settings.get("judge",{}).get("min_confidence",.5))*100,
            fraction=bool(settings.get('confidence_history') and not settings.get('live_photo_inspection')))
        if settings.get('serial_naming'):
            import tempfile
            destination.parent.mkdir(parents=True,exist_ok=True)
            destination = Path(tempfile.mkdtemp(prefix='.inspection_',dir=destination.parent))
            settings['_serial_stage'] = destination
        output_path = unique_path(destination / Path(output_path).name)
        output_path = output_path.with_name(f'{output_path.stem}_[{float(settings.get("test_fps",30)):g}fps]{output_path.suffix}')

    if settings.get("cpu_threads"):
        import torch
        torch.set_num_threads(max(1, int(settings["cpu_threads"])))
    model_load_started = monotonic()
    from inspection.special_checks import SpecialInspector
    from inspection.frame_alignment import FrameAligner
    reference_path = settings.get("alignment_reference")
    aligner = FrameAligner(Path(reference_path)) if settings.get("alignment_enabled") and reference_path else None
    if settings.get('auto_runtime'):
        from inspection.automatic_runtime import prepare
        detector,special = prepare(settings,YoloInference,SpecialInspector)
    else:
        detector = YoloInference(dict(settings,model=dict(settings.get("model",{}),render_predictions=False)))
        special = SpecialInspector(settings)
    special.performance = performance
    from inspection.inference_telemetry import InferenceTelemetry
    inference_telemetry = InferenceTelemetry()
    readers = getattr(special,'trained_text_ocr',{})
    cache_scope = (settings.get('root_dir'), settings.get('special_group'), settings.get('test_fps'),
                   tuple(sorted((kind,getattr(model,'job_workers',1),getattr(model,'inference_threads',4))
                                for kind,model in readers.items())))
    cache_preparation = prepare_profiled_models(cache_scope) if readers else None
    if cache_preparation is not None:
        performance.add('ocr_profile_preparation',cache_preparation['seconds'])
    ocr_cache_start = cache_snapshot()
    for kind,model in readers.items():
        model.telemetry,model.telemetry_kind = inference_telemetry,kind
    performance.mark('models_ready')
    live_photo_ranges = {}
    live_photo_best = {}
    live_photo_inspectors = {}
    live_photo_status = {}
    live_photo_completed = set()
    from inspection.photo_inspection import inspection_tasks, photo_kind
    from inspection.test_sampling import TestSampling
    sampling = TestSampling(settings.get('group_fps') or settings.get('test_fps') or 30,
                            settings.get('test_fps') or 30)
    essential = None
    if settings.get('essential_shapes'):
        from inspection.essential_shapes import EssentialShapes
        essential = EssentialShapes(settings['essential_shapes'],sampling,
                                    settings.get('essential_score_threshold',.60))
    if sampling.group_fps != sampling.test_fps:
        special.annotation_frames = {kind:sampling.schedule(schedule)
                                     for kind,schedule in special.annotation_frames.items()}
        if getattr(special,'conn_inspection_range',None) is not None:
            special.conn_inspection_range = sampling.interval(special.conn_inspection_range)
    if settings.get("live_photo_inspection"):
        import copy
        from inspection.photo_inspection import load_photo_frame_range
        tasks = sampling.tasks(inspection_tasks(Path(settings['special_info_dir']),settings['special_group'],
                                 [kind for kind in ('OCR','label') if kind in special.references]))
        for task,(start,end) in tasks.items():
            kind = photo_kind(task)
            if end > settings["live_expected_frames"]:
                raise ValueError(f"{kind}: inspection range exceeds live capture length.")
            if not any(start <= number <= end for number in special.annotation_frames[kind]):
                raise ValueError(f"{kind}: no matching training labels in inspection range.")
            live_photo_ranges[task] = start,end
            inspector = copy.copy(special)
            from inspection.conn_robustness import ConnConfirmation
            inspector.conn_confirmation = ConnConfirmation()
            inspector.references = {kind:special.references[kind]}
            inspector.annotation_frames = {kind:special.annotation_frames[kind]}
            inspector.labeled_frames_only = True
            inspector.conn_inspection_range = None
            inspector.times = {kind:(0,None)}
            inspector.begin_item_range(start,end)
            live_photo_inspectors[task] = inspector
            live_photo_status[task] = f"{task}: WAIT frames {start}~{end}"
        special.annotation_frames = {kind:schedule for kind,schedule in special.annotation_frames.items()
                                     if kind not in {photo_kind(task) for task in live_photo_ranges}}
    model_load_seconds = monotonic()-model_load_started
    performance.add("model_initialization", model_load_seconds)
    if detector.model is None and (settings.get("require_regular_model") or not (special.references or essential)):
        raise RuntimeError(detector.model_error)
    if capture_factory is not None:
        capture_override = capture_factory()
    capture = capture_override if capture_override is not None else open_video_capture(source)
    if settings.get('auto_runtime'):
        from types import SimpleNamespace
        from inspection.automatic_runtime import retain
        from inspection.runtime_pipeline import ReleaseOnce
        capture = ReleaseOnce(capture)
        retain(settings,SimpleNamespace(close=capture.release))
    if not capture.isOpened():
        if not is_stream_source(source):
            capture.release()
            raise RuntimeError(f"Video open failed: {source}\nCheck that the selected file exists and can be decoded.")
        raise RuntimeError(
            f"Video open failed: {source}\n\n"
            "Check that the phone camera app is running, the PC and phone are on the same network, "
            "and the URL looks like http://192.168.x.x:8080/video."
        )

    fps = capture.get(cv2.CAP_PROP_FPS) or 30
    source_fps = fps
    test_fps = settings.get("test_fps")
    if test_fps is not None:
        import math
        fps = float(test_fps)
        if not math.isfinite(fps) or fps <= 0 or fps > source_fps + 1e-6:
            capture.release()
            raise ValueError(f"Test FPS must be > 0 and <= source FPS ({source_fps:g}).")
    source_total = int(capture.get(cv2.CAP_PROP_FRAME_COUNT) or 0)
    if test_fps is not None:
        from inspection.frame_conditions import sampled_frame_count
        total_frames = sampled_frame_count(source_total, source_fps, fps)
    else:
        total_frames = source_total
    if capture_override is not None:
        total_frames = int(settings.get("live_expected_frames", 0))
    if settings.get("special_frame_ranges"):
        for kind, (start, end) in special.times.items():
            if start > total_frames or (end is not None and end > total_frames):
                capture.release()
                raise ValueError(f"{kind} frame range exceeds this test video's {total_frames} sampled frames.")
    if settings.get("special_labeled_frames"):
        for kind, schedule in special.annotation_frames.items():
            if kind == "conn" and getattr(special,"conn_inspection_range",None) is not None:
                if special.conn_inspection_range[1] > total_frames:
                    capture.release()
                    raise ValueError(f"Conn inspection range exceeds {total_frames} sampled frames.")
                continue
            if settings.get("special_photo_inspection") and kind in ("OCR","label"):
                continue
            if any(number > total_frames for number in schedule):
                capture.release()
                raise ValueError(f"{kind} labels exceed this test video's {total_frames} sampled frames.")
    source_width = int(capture.get(cv2.CAP_PROP_FRAME_WIDTH) or 0)
    source_height = int(capture.get(cv2.CAP_PROP_FRAME_HEIGHT) or 0)
    inline_file_photos = bool(settings.get('special_photo_inspection') and settings.get('photo_frame_selection')
                              and not settings.get('live_photo_inspection'))
    if inline_file_photos:
        from inspection.photo_inspection import inline_inspectors
        try:
            live_photo_ranges = sampling.tasks(inspection_tasks(Path(settings['special_info_dir']),settings['special_group'],
                                    [kind for kind in ('OCR','label') if kind in special.references]))
            live_photo_inspectors,live_photo_status = inline_inspectors(special,live_photo_ranges,total_frames)
        except Exception:
            capture.release()
            raise
    photo_selections = {}
    photo_paths = []
    photo_reports = []
    confidence_run = None
    if settings.get('confidence_history') and not settings.get('serial_naming') and not settings.get('live_photo_inspection') and detector.model is not None and settings.get('result_directory'):
        from inspection.confidence_runs import reserve_run
        confidence_run = reserve_run(output_path.parent)
        output_path = output_path.with_name(f'{confidence_run[1]}st_{output_path.parent.name}_result{output_path.suffix}')
    log_base_stem = output_path.stem if output_path else None
    photo_selection_seconds = 0.
    file_photo_results = {}
    file_photo_attempts = {}
    appearance_stream = None
    inference_pool = None
    special_pool = None
    saver = None
    automatic = settings.get('_automatic_runtime',{})
    if settings.get("special_photo_inspection") and not settings.get("live_photo_inspection") and not inline_file_photos:
        if capture_override is not None or is_stream_source(source):
            capture.release()
            raise ValueError("Photo time selection requires a saved video file.")
        from inspection.photo_inspection import load_time, select_photos
        photo_kinds = [kind for kind in ("label","OCR") if kind in special.references]
        if photo_kinds:
            try:
                if automatic.get('parallel') and detector.model is not None and aligner is None:
                    from inspection.runtime_pipeline import AppearanceStream
                    from inspection.automatic_runtime import retain
                    appearance_stream = retain(settings,AppearanceStream(source,fps,detector,performance,gate=resource_gate))
                photo_selection_started = perf_counter()
                import copy
                candidate_inspectors = {}
                for kind in photo_kinds:
                    inspector = copy.copy(special)
                    inspector.references = {kind:special.references[kind]}
                    inspector.annotation_frames = {kind:special.annotation_frames[kind]}
                    inspector.labeled_frames_only = True
                    inspector.conn_inspection_range = None
                    inspector.times = {kind:(0,None)}
                    from inspection.conn_robustness import ConnConfirmation
                    inspector.conn_confirmation = ConnConfirmation()
                    candidate_inspectors[kind] = inspector
                task_inspectors = {}
                def evaluate_photo(task, image, number):
                    performance.mark('first_special_inspection_started')
                    kind = photo_kind(task)
                    if task not in task_inspectors:
                        inspector = copy.copy(candidate_inspectors[kind])
                        if settings.get('photo_frame_selection'):
                            inspector.begin_item_range(*ranges[task])
                        task_inspectors[task] = inspector
                    inspector = task_inspectors[task]
                    failures = inspector.check_frame(image,number,total_frames)
                    file_photo_results[task] = (copy.copy(inspector),list(failures),list(inspector.detail_detections))
                    file_photo_attempts.setdefault(number,{})[task] = copy.deepcopy(inspector.detail_detections)
                    return not failures
                if settings.get("photo_frame_selection"):
                    from inspection.photo_inspection import load_photo_frame_range, select_frame_range_photos
                    ranges = sampling.tasks(inspection_tasks(Path(settings['special_info_dir']),settings['special_group'],photo_kinds))
                    schedules = {task:special.annotation_frames[photo_kind(task)] for task in ranges}
                    photo_selections = select_frame_range_photos(source,fps,schedules,ranges,evaluate=evaluate_photo,performance=performance)
                else:
                    ranges = {kind:load_time(Path(settings["special_info_dir"]),settings["special_group"],kind)
                              for kind in photo_kinds}
                    photo_selections = select_photos(source,fps,special.annotation_frames,ranges,evaluate=evaluate_photo,performance=performance)
                for kind,selection in photo_selections.items():
                    special.annotation_frames.pop(photo_kind(kind),None)
                photo_selection_seconds = perf_counter()-photo_selection_started
                performance.add_since("photo_selection_including_inspection",photo_selection_started)
                performance.mark('file_photo_inspections_ready')
            except Exception:
                capture.release()
                if hasattr(getattr(special,"mark_model",None),"close"):
                    special.mark_model.close()
                raise
    video_output = (not (photo_selections or (live_photo_ranges if inline_file_photos else {}))
                    or detector.model is not None or "conn" in special.references or essential is not None)
    photo_only_iterator = (iter(sorted({selection.get("decision_frame",selection["frame_number"]):selection["image"]
                                       for selection in photo_selections.values()}.items()))
                           if photo_selections and not video_output else None)
    writer = None
    if output_path is not None and video_output:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        fourcc = cv2.VideoWriter_fourcc(*"mp4v")
        if source_width <= 0 or source_height <= 0:
            ok, probe_frame = capture.read()
            if not ok:
                raise RuntimeError(f"Cannot read first frame from: {source}")
            source_height, source_width = probe_frame.shape[:2]
            capture.set(cv2.CAP_PROP_POS_FRAMES, 0)
        saved_width, saved_height = source_width, source_height
        if aligner:
            saved_height, saved_width = aligner.reference.shape[:2]
        elif settings.get("match_original_format") and reference_path:
            reference = cv2.imread(str(reference_path))
            if reference is None:
                raise RuntimeError(f"Original frame cannot be read: {reference_path}")
            saved_height, saved_width = reference.shape[:2]
        saved_panel_width = (max(2, round(480 * saved_height / display_height / 2) * 2)
                             if settings.get("save_test_details") else 0)
        writer = cv2.VideoWriter(str(output_path), fourcc, fps, (saved_width + saved_panel_width, saved_height))
        if not writer.isOpened():
            writer.release()
            raise RuntimeError(f"Output video writer open failed: {output_path}")

    frame_count = 0
    from inspection.conn_outcome import ConnOutcome
    conn_outcome = ConnOutcome()
    confidence_recorder = None
    conn_event_keys = set()
    other_event_keys = set()
    source_frame_count = 0
    stop_reason = "completed"
    diagnostics = {}
    appearance_checked_frames = 0
    appearance_defects = 0
    ng_frame_count = 0
    ng_events: dict[tuple[int, str], float] = {}
    started_at = monotonic()
    if settings.get('auto_runtime'):
        from inspection.runtime_pipeline import OrderedSaver,LiveMediaSaver
        from inspection.automatic_runtime import retain
        saver_type = LiveMediaSaver if settings.get('live_async_media') else OrderedSaver
        saver = retain(settings,saver_type(performance,int(automatic.get('save_queue_capacity',2))))
        if settings.get('live_async_media'):
            settings['_live_performance'] = performance
        if automatic.get('parallel') and appearance_stream is None and aligner is None and detector.model is not None:
            from concurrent.futures import ThreadPoolExecutor
            inference_pool = retain(settings,ThreadPoolExecutor(max_workers=1,thread_name_prefix='appearance'))
        if int(automatic.get('special_workers',1)) > 1 and live_photo_ranges and aligner is None:
            from inspection.parallel_checks import ParallelChecks
            special_pool = retain(settings,ParallelChecks(int(automatic['special_workers']),performance))
    def predict_appearance(image):
        performance.mark('first_appearance_inference_started')
        return performance.call('appearance_inference',detector.predict,image)

    def read_sample(number):
        nonlocal source_frame_count
        while True:
            performance.call('resource_admission_wait',resource_gate.before_submit)
            if max_seconds is not None and monotonic() - started_at >= max_seconds:
                return None
            ok, image = performance.call('frame_read_decode_wait',capture.read)
            if not ok:
                return None
            index = source_frame_count
            source_frame_count += 1
            if test_fps is None or index / source_fps + 1e-9 >= number / fps:
                return index,image

    def predict_sample(read_future):
        sample = read_future.result()
        return predict_appearance(sample[1]) if sample is not None else None

    pending_sample = None
    lookahead_frames = 0
    lookahead_enabled = (inference_pool is not None and video_output and bool(live_photo_ranges)
                         and photo_only_iterator is None and max_seconds is None)
    try:
        if (settings.get("record_confidence_points") and settings.get("save_scatter_plot", True)
                and not settings.get("live_photo_inspection") and settings.get("result_directory")
                and detector.model is not None):
            from inspection.threshold_review import ConfidenceRecorder
            confidence_recorder = ConfidenceRecorder(output_path.parent,settings.get("special_group"),
                float(settings.get("judge",{}).get("min_confidence",.5))*100,source,fps,run=confidence_run,
                review_thresholds=settings.get('scatter_thresholds'))
        if show_window:
            cv2.namedWindow("Video Inspection - NG labels only", cv2.WINDOW_NORMAL)
            cv2.resizeWindow("Video Inspection - NG labels only", display_width + 480, display_height)
            if window_position is not None:
                cv2.moveWindow("Video Inspection - NG labels only", max(0, window_position[0] - 240), window_position[1])

        while True:
            if max_seconds is not None and monotonic() - started_at >= max_seconds:
                break

            appearance_future = None
            if photo_only_iterator is not None:
                performance.call('resource_admission_wait',resource_gate.before_submit)
                try:
                    number,frame = next(photo_only_iterator)
                except StopIteration:
                    frame_count = total_frames
                    break
                frame_count = number-1
            else:
                if pending_sample is not None:
                    read_future,appearance_future = pending_sample
                    pending_sample = None
                    sample = performance.call('appearance_lookahead_input_wait',read_future.result)
                else:
                    sample = read_sample(frame_count)
                if sample is None:
                    timed_out = max_seconds is not None and monotonic() - started_at >= max_seconds
                    if not timed_out and source_total and source_frame_count < source_total and not is_stream_source(source):
                        stop_reason = "decode_incomplete"
                    break

                source_index,frame = sample

            elapsed_seconds = (monotonic() - started_at if is_stream_source(source) and capture_override is None else frame_count / fps)
            if appearance_future is not None:
                lookahead_frames += 1
            elif inference_pool is not None and video_output:
                appearance_future = inference_pool.submit(predict_appearance,frame)
            parallel_results = {}
            if special_pool is not None:
                number = frame_count+1
                checks = {task:live_photo_inspectors[task] for task,(start,end) in live_photo_ranges.items()
                          if task not in live_photo_completed and start <= number <= end
                          and number in live_photo_inspectors[task].annotation_frames[photo_kind(task)]}
                if getattr(special,'annotation_frames',{}) and (settings.get('special_frame_ranges') or settings.get('special_labeled_frames')):
                    checks['_other'] = special
                if checks:
                    performance.mark('first_special_inspection_started')
                    parallel_results = performance.call('special_frame_parallel_wall',special_pool.run,
                        frame,number,total_frames,checks,getattr(special,'shape_registration',None))
            selected_kinds = [kind for kind,selection in photo_selections.items()
                              if selection.get("decision_frame",selection["frame_number"]) == frame_count+1]
            live_photo_results = {kind:file_photo_results[kind] for kind in selected_kinds if kind in file_photo_results}
            frame_photo_details = dict(file_photo_attempts.get(frame_count+1,{}))
            for kind,(start,end) in live_photo_ranges.items():
                if kind in live_photo_completed:
                    continue
                number = frame_count+1
                inspector = live_photo_inspectors[kind]
                if start <= number <= end and number in inspector.annotation_frames[photo_kind(kind)]:
                    from inspection.photo_inspection import sharpness
                    score = sharpness(frame)
                    performance.mark('first_special_inspection_started')
                    failures = (parallel_results[kind] if kind in parallel_results else
                                performance.call('inline_photo_inspection.'+photo_kind(kind),inspector.check_frame,frame,number,total_frames))
                    frame_photo_details[kind] = list(inspector.detail_detections)
                    attempts = live_photo_best.get(kind,{}).get("attempted_frames",0)+1
                    live_photo_best[kind] = dict(image=frame.copy(),frame_number=number,
                        seconds=(number-1)/fps,sharpness=score,start_frame=start,end_frame=end,
                        source_frame_index=source_index if inline_file_photos else None,
                        attempted_frames=attempts,selection="first_complete_pass_in_range",
                        failures=list(failures),details=list(inspector.detail_detections))
                    live_photo_status[kind] = f"{kind}: PENDING | checked frame {number}"
                selection = live_photo_best.get(kind)
                if selection is not None and (not selection["failures"] or number == end):
                    selection = live_photo_best.pop(kind)
                    failures = selection.pop("failures")
                    details = selection.pop("details")
                    selection['result'] = 'NG' if failures else 'OK'
                    selection['decision_frame'] = number
                    live_photo_completed.add(kind)
                    live_photo_results[kind] = (inspector,failures,details)
                    photo_selections[kind] = selection
                    selected_kinds.append(kind)
                    live_photo_status[kind] = f"{kind}: {'NG' if failures else 'OK'} | selected frame {selection['frame_number']}"
                    if set(live_photo_ranges) == live_photo_completed:
                        performance.mark('inline_photo_inspections_ready')
            from inspection.photo_inspection import range_statuses
            task_states = {task:(photo_selections[task].get('result','NG')
                                if frame_count+1 >= photo_selections[task].get('decision_frame',photo_selections[task]['frame_number'])
                                else 'PENDING') for task in file_photo_results}
            task_states.update({task:('NG' if live_photo_results[task][1] else 'OK')
                                for task in live_photo_results if task in live_photo_ranges})
            for task in live_photo_ranges:
                if task not in task_states:
                    task_states[task] = (photo_selections[task].get('result','NG')
                                        if task in live_photo_completed else 'PENDING')
            if photo_selections and not video_output and not selected_kinds:
                frame_count += 1
                continue
            alignment_note = ""
            inspection_frame = frame
            aligned = performance.call("frame_alignment",aligner.align,frame) if aligner else None
            if aligned is not None:
                alignment_note = aligned.message
            if aligned is not None and not aligned.ok:
                detections = [{"class_name": "alignment_failed", "confidence": 1.0,
                               "xyxy": [0, 0, frame.shape[1], frame.shape[0]], "special_failure": True}]
            else:
                if aligned is not None:
                    inspection_frame = aligned.image
                if appearance_stream is not None:
                    detections,_ = performance.call('appearance_result_wait',appearance_stream.take,frame_count+1)
                elif appearance_future is not None:
                    detections,_ = performance.call('appearance_result_wait',appearance_future.result)
                else:
                    detections, _ = predict_appearance(inspection_frame)
                if confidence_recorder is not None:
                    record_settings = dict(settings,judge=dict(settings.get("judge",{}),min_confidence=.01))
                    appearance_candidates = [item for item in detections if not item.get("special_failure") and not item.get("inspection_kind")]
                    _,eligible = draw_ng_detections(inspection_frame,appearance_candidates,record_settings)
                    if saver is not None:
                        import copy
                        saver.submit('confidence_frame_save',confidence_recorder.add,frame_count+1,inspection_frame.copy(),copy.deepcopy(eligible))
                    else:
                        performance.call("confidence_frame_save",confidence_recorder.add,frame_count+1,inspection_frame,eligible)
                if detector.model is not None:
                    appearance_checked_frames += 1
                    appearance_defects += sum(float(detection.get("confidence",0)) >= float(settings.get("judge",{}).get("min_confidence",.35)) and detection.get("class_name") in
                                              settings.get("judge", {}).get("ng_classes", [])
                                              for detection in detections)
                if (settings.get("special_frame_ranges") or settings.get("special_labeled_frames")) and (not (file_photo_results or live_photo_ranges) or getattr(special,"annotation_frames",{})):
                    detections.extend(parallel_results['_other'] if '_other' in parallel_results else
                                      special.check_frame(inspection_frame, frame_count + 1, total_frames,
                                                          valid_mask=aligned.valid_mask if aligned is not None else None))
                elif file_photo_results or live_photo_ranges:
                    pass
                elif aligned is not None:
                    detections.extend(special.check(inspection_frame, elapsed_seconds, valid_mask=aligned.valid_mask))
                else:
                    detections.extend(special.check(inspection_frame, elapsed_seconds))
            if lookahead_enabled and live_photo_completed == set(live_photo_ranges):
                read_future = inference_pool.submit(read_sample,frame_count+1)
                pending_sample = (read_future,inference_pool.submit(predict_sample,read_future))
            if essential is not None and frame_count+1 in essential.schedule:
                performance.call('essential_shape_check',essential.check,frame,frame_count+1)
            for task,(_,failures,_) in live_photo_results.items():
                if photo_selections[task]['frame_number'] == frame_count+1:
                    detections.extend(failures)
            annotation_started = perf_counter()
            guided_frame = special.draw_conn_guides(inspection_frame, frame_count + 1) if hasattr(special, "draw_conn_guides") else inspection_frame
            if hasattr(special,"draw_mark_guides") and not (file_photo_results or live_photo_ranges):
                guided_frame = special.draw_mark_guides(guided_frame)
            photo_kinds = {photo_kind(task) for task in set(file_photo_results) | set(live_photo_ranges)}
            annotated, ng_detections = draw_ng_detections(guided_frame,detections,settings,hidden_kinds=photo_kinds)
            if photo_kinds:
                from inspection.annotation_details import draw_inspection_labels
                draw_inspection_labels(annotated,[item for values in frame_photo_details.values() for item in values],
                                       copy_frame=False)
            statuses = [f"Frame {frame_count + 1}/{total_frames} | {fps:g} fps"] + list(getattr(special, "status_lines", [])) + list(live_photo_status.values())
            essential_failed = False
            if essential is not None:
                remaining = len(essential.failures())
                essential_failed = bool(remaining and total_frames and frame_count+1 >= total_frames)
                state = 'OK' if not remaining else 'NG' if essential_failed else 'PENDING'
                statuses.append(f'Essential: {state} | {essential.required_count-remaining}/{essential.required_count} OK')
            for kind in file_photo_results:
                selection = photo_selections[kind]
                state = selection.get("result","NG") if frame_count+1 >= selection.get("decision_frame",selection["frame_number"]) else "PENDING"
                statuses.append(f"{kind}: {state} | inspection frame range")
            statuses.extend(f'{kind}: {state} | ALL inspection ranges' for kind,state in range_statuses(task_states).items())
            if resource_gate.message and not settings.get('live_photo_inspection'):
                statuses.append(resource_gate.message)
            pending = any("PENDING" in line or "WAIT" in line for line in statuses)
            if (pending and not ng_detections) or file_photo_results or live_photo_ranges or essential is not None:
                cv2.rectangle(annotated, (0, 0), (300, 65), (0, 0, 0), -1)
                failed_range = essential_failed or 'NG' in range_statuses(task_states).values()
                state = "NG" if ng_detections or failed_range else "PENDING" if pending else "OK"
                color = (0,0,255) if ng_detections or failed_range else (0,200,255) if pending else (0,180,0)
                cv2.putText(annotated, state, (20, 45), cv2.FONT_HERSHEY_SIMPLEX, 1.4, color, 3, cv2.LINE_AA)
            detail_detections = getattr(special, "detail_detections", ng_detections)
            detail_detections = list(detail_detections) + [item for values in frame_photo_details.values() for item in values]
            conn_schedule = getattr(special,"annotation_frames",{}).get("conn",{})
            conn_active = (special.conn_reference(frame_count+1) is not None if hasattr(special,"conn_reference")
                           else frame_count+1 in conn_schedule)
            if not getattr(special,"labeled_frames_only",True) and "conn" in special.references:
                start,end = special.times["conn"]
                conn_active = elapsed_seconds >= start and (end is None or elapsed_seconds <= end)
            conn_current_failures = [item for item in detections if item.get("inspection_kind") == "conn"]
            conn_current_pending = list(getattr(getattr(special,"conn_confirmation",None),"pending",[]))
            performance.add_since('frame_annotation',annotation_started)
            if conn_active and output_path is not None and (conn_current_failures or conn_current_pending or conn_outcome.first_ok is None):
                photo_started = perf_counter()
                from inspection.annotation_details import preview_with_details
                conn_failures = conn_current_failures
                conn_pending = conn_current_pending
                conn_details = [item for item in detail_detections if item.get("inspection_kind") == "conn"]
                conn_source = (special.draw_conn_guides(inspection_frame,frame_count+1)
                               if hasattr(special,"draw_conn_guides") else inspection_frame.copy())
                conn_photo,_ = draw_ng_detections(conn_source,conn_failures+conn_pending,settings)
                state = "NG" if conn_failures or conn_pending else "OK"
                conn_photo = preview_with_details(conn_photo,conn_details,status_lines=[
                    f"conn: {state} | Frame {frame_count+1} | {frame_count/fps:.3f} sec"],detail_height=display_height)
                conn_outcome.observe(frame_count+1,conn_failures,conn_pending,conn_photo)
                performance.add_since('inspection_photo_composition',photo_started)
            if selected_kinds and output_path is not None:
                for kind in selected_kinds:
                    photo_started = perf_counter()
                    from inspection.annotation_details import preview_with_details
                    selection = photo_selections[kind]
                    kind_details = [item for item in detail_detections if item.get("inspection_kind") == kind]
                    kind_failures = [item for item in ng_detections if item.get("inspection_kind") == kind]
                    photo_frame = inspection_frame
                    photo_inspector = special
                    if kind in live_photo_results:
                        photo_inspector,kind_failures,kind_details = live_photo_results[kind]
                        photo_frame = selection["image"]
                    photo_path = result_file_path(output_path.with_name(f"{log_base_stem}_{kind}.png"),not kind_failures)
                    photo_path.parent.mkdir(parents=True,exist_ok=True)
                    photo_source = photo_inspector.draw_mark_guides(photo_frame) if photo_kind(kind) == "OCR" and hasattr(photo_inspector,"draw_mark_guides") else photo_frame.copy()
                    from inspection.annotation_details import draw_inspection_labels, draw_pass_labels
                    boxes = (photo_inspector.result_photo_boxes(photo_kind(kind),selection['frame_number'],photo_frame.shape)
                             if hasattr(photo_inspector,'result_photo_boxes') else None)
                    if boxes:
                        photo = draw_inspection_labels(photo_frame,boxes)
                        cv2.putText(photo, "NG" if kind_failures else "OK", (20,45),
                                    cv2.FONT_HERSHEY_SIMPLEX,1.4,
                                    (0,0,255) if kind_failures else (0,180,0),3,cv2.LINE_AA)
                    else:
                        photo, _ = draw_ng_detections(photo_source,kind_failures,settings)
                        photo = draw_pass_labels(photo,kind_details)
                    photo = preview_with_details(photo,kind_details,status_lines=[
                        f"{kind}: {'NG' if kind_failures else 'OK'} | Frame {selection['frame_number']} | {selection['seconds']:.3f} sec"],
                        detail_height=display_height)
                    performance.add_since('inspection_photo_composition',photo_started)
                    if saver is not None:
                        from inspection.runtime_pipeline import write_image
                        saver.submit('inspection_photo_save',write_image,photo_path,photo.copy())
                    elif not performance.call("inspection_photo_save",cv2.imwrite,str(photo_path),photo):
                        raise RuntimeError(f"Could not save inspection photo: {photo_path}")
                    photo_paths.append(photo_path)
                    photo_reports.append({key:value for key,value in selection.items() if key != "image"} | {
                        "kind":photo_kind(kind),"inspection_task":kind,"output":str(photo_path),"result":"NG" if kind_failures else "OK",
                        "annotation_frame_number":sampling.annotation_number(selection['frame_number']),
                        "item_passes":[dict(item_key=key[1],test_frame=records[0]['source_frame_number'],
                                            annotation_frame=sampling.annotation_number(records[0]['source_frame_number']))
                                       for key,records in getattr(photo_inspector,'item_pass_records',{}).items()]})
                    if kind in live_photo_results:
                        selection.pop("image",None)
            annotation_started = perf_counter()
            if alignment_note:
                cv2.putText(annotated, alignment_note, (20, 80), cv2.FONT_HERSHEY_SIMPLEX, 0.7,
                            (0, 180, 0) if aligned.ok else (0, 0, 255), 2, cv2.LINE_AA)
            successful_photo_kinds = [kind for kind in selected_kinds if photo_kind(kind) in ("OCR","label")
                                      and kind in live_photo_results and not live_photo_results[kind][1]]
            if successful_photo_kinds:
                title = " / ".join(f"{kind}: OK" for kind in successful_photo_kinds)
                cv2.rectangle(annotated,(0,0),(min(annotated.shape[1],620),65),(0,0,0),-1)
                cv2.putText(annotated,title,(20,45),cv2.FONT_HERSHEY_SIMPLEX,1.2,(0,210,0),3,cv2.LINE_AA)
            display_frame = letterbox_frame(annotated, display_width, display_height) if show_window else None
            if ng_detections:
                ng_frame_count += 1
                elapsed_seconds = (monotonic() - started_at if is_stream_source(source) and capture_override is None else frame_count / fps)
                event_second = int(elapsed_seconds)
                for detection in ng_detections:
                    class_name = str(detection.get("class_name", ""))
                    confidence = float(detection.get("confidence", 0.0))
                    key = (event_second, class_name)
                    if detection.get("inspection_kind") == "conn":
                        conn_event_keys.add(key)
                    else:
                        other_event_keys.add(key)
                    ng_events[key] = max(confidence, ng_events.get(key, 0.0))
                    if "recognized_text" in detection:
                        diagnostics.setdefault((event_second, class_name), {
                            "expected": detection.get("expected_format", detection["expected_text"]),
                            "recognized": detection["recognized_text"]})

            performance.add_since('frame_annotation',annotation_started)
            if writer is not None:
                composition_started = perf_counter()
                saved_frame = (letterbox_frame(annotated, saved_width, saved_height)
                               if annotated.shape[:2] != (saved_height, saved_width) else annotated)
                if settings.get("save_test_details"):
                    from inspection.annotation_details import preview_with_details
                    saved_frame = preview_with_details(saved_frame, detail_detections, status_lines=statuses,
                                                       detail_height=display_height)
                performance.add_since("result_video_composition",composition_started)
                for _ in range(result_hold_frames(fps,successful_photo_kinds)):
                    if saver is not None:
                        saver.submit('result_video_write',writer.write,saved_frame.copy())
                    else:
                        performance.call("result_video_write",writer.write,saved_frame)

            frame_count += 1
            performance.mark('first_result_frame_ready')
            if result_callback is not None:
                with performance.measure("embedded_result_prepare"):
                    result_callback(annotated,frame_count,total_frames,statuses,detail_detections)
            if progress_callback is not None:
                performance.call('progress_callback',progress_callback,frame_count,total_frames,
                                 frame_count/fps,monotonic()-started_at)

            if show_window:
                preview_started = perf_counter()
                from inspection.annotation_details import preview_with_details
                cv2.imshow("Video Inspection - NG labels only", preview_with_details(display_frame, detail_detections, status_lines=statuses))
                key = cv2.waitKey(1) & 0xFF
                performance.add_since("preview_display",preview_started)
                if key in (27, ord("q"), ord("Q")):
                    stop_reason = "user_stopped"
                    break
    except Exception:
        if saver is not None:
            try:
                saver.abort() if hasattr(saver,'abort') else saver.close()
            except Exception:
                pass
        if writer is not None:
            writer.release()
            writer = None
        if output_path and output_path.exists():
            partial = unique_path(output_path.with_name(f"{output_path.stem}_partial{output_path.suffix}"))
            output_path.replace(partial)
        raise
    finally:
        close_errors = []
        for resource in (appearance_stream,inference_pool,special_pool,saver):
            if resource is not None:
                try:
                    stage = 'media_cleanup_wait' if resource is saver else 'pipeline_cleanup'
                    if hasattr(resource,'close'):
                        performance.call(stage,resource.close)
                    else:
                        performance.call(stage,resource.shutdown,wait=True,cancel_futures=True)
                except Exception as exc:
                    close_errors.append(exc)
        performance.call('pipeline_cleanup',capture.release)
        if hasattr(getattr(special,"mark_model",None),"close") and not getattr(special,'cached_mark_runtime',False):
            performance.call('pipeline_cleanup',special.mark_model.close)
        if writer is not None:
            if settings.get('live_async_media') and saver is not None:
                saver.submit('result_video_release',writer.release)
            else:
                performance.call('result_video_release',writer.release)
        if show_window:
            performance.call('pipeline_cleanup',cv2.destroyAllWindows)
        if confidence_recorder is not None:
            import sys
            performance.call('confidence_flush',confidence_recorder.flush,
                             "failed" if sys.exc_info()[0] is not None or close_errors else stop_reason)
        if close_errors:
            if output_path and output_path.exists():
                output_path.replace(unique_path(output_path.with_name(f'{output_path.stem}_partial{output_path.suffix}')))
            raise close_errors[0]

    if output_path is not None:
        conn_finalize_started = perf_counter()
        conn_state,conn_snapshot = conn_outcome.result()
        if conn_snapshot is not None and conn_state == "OK":
            for key in conn_event_keys-other_event_keys:
                ng_events.pop(key,None)
                diagnostics.pop(key,None)
        performance.add_since('conn_result_finalize',conn_finalize_started)
        if conn_snapshot is not None:
            conn_number,conn_photo = conn_snapshot
            if conn_state == "NG":
                ng_events[(int((conn_number-1)/fps),"conn_range_ng")] = 1.
                if ng_frame_count == 0:
                    ng_frame_count = 1
            conn_path = result_file_path(output_path.with_name(f"{log_base_stem}_conn.png"),conn_state == "OK")
            if settings.get('live_async_media') and saver is not None:
                from inspection.runtime_pipeline import write_image
                saver.submit('inspection_photo_save',write_image,conn_path,conn_photo.copy())
            elif not performance.call("inspection_photo_save",cv2.imwrite,str(conn_path),conn_photo):
                raise RuntimeError(f"Could not save Conn inspection photo: {conn_path}")
            photo_paths.append(conn_path)
            photo_reports.append({"kind":"conn","frame_number":conn_number,"seconds":(conn_number-1)/fps,
                "result":conn_state,"output":str(conn_path),"confirmed_two_frame_failure":conn_outcome.confirmed})
    if live_photo_ranges and set(live_photo_ranges) != {report.get("inspection_task",report["kind"]) for report in photo_reports if report["kind"] in ('OCR','label')}:
        raise RuntimeError("Capture ended before all OCR/Label inspection ranges completed. Not a completed PASS test.")
    if capture_override is not None and settings.get("special_labeled_frames"):
        missing = [number for kind,schedule in special.annotation_frames.items()
                   if not (kind == "conn" and getattr(special,"conn_inspection_range",None) is not None)
                   for number in schedule if number > frame_count]
        if getattr(special,"conn_inspection_range",None) is not None and special.conn_inspection_range[1] > frame_count:
            missing.append(special.conn_inspection_range[1])
        if missing:
            if output_path and output_path.exists():
                partial = unique_path(output_path.with_name(f"{output_path.stem}_partial{output_path.suffix}"))
                output_path.replace(partial)
            raise RuntimeError(f"Live capture ended before labeled frame {min(missing)}. Not a completed PASS test.")

    if essential is not None:
        failures = essential.failures()
        for name in failures:
            ng_events[(int(max(0,frame_count-1)/fps),name)] = 1.
        if failures:
            ng_frame_count = max(1,ng_frame_count)
    print(f"video: {source}")
    print(f"frames: {frame_count}")
    print(f"NG frames: {ng_frame_count}")
    if output_path is not None:
        if not video_output:
            if not photo_paths:
                raise RuntimeError("No inspection photo was produced. Not a completed inspection.")
            output_path = photo_paths[0]
        if file_test_log and stop_reason != "completed" and video_output:
            partial_path = unique_path(output_path.with_name(f"{output_path.stem}_partial{output_path.suffix}"))
            if settings.get('live_async_media') and saver is not None:
                saver.submit('result_video_rename',output_path.replace,partial_path)
            else:
                performance.call('result_video_rename',output_path.replace,partial_path)
            output_path = partial_path

        if video_output and (status_suffix or file_test_log):
            final_path = result_file_path(output_path,not ng_events and stop_reason == "completed")
            if settings.get('live_async_media') and saver is not None:
                saver.submit('result_video_rename',output_path.replace,final_path)
            else:
                performance.call('result_video_rename',output_path.replace,final_path)
            output_path = final_path

        if frame_count and video_output:
            from inspection.video_export import finalize_result_video
            if settings.get('live_async_media') and saver is not None:
                saver.submit('result_video_encoding',finalize_result_video,output_path)
            else:
                performance.call("result_video_encoding",finalize_result_video,output_path)

        serial_finalize = None
        serial_log_name = None
        if settings.get('_serial_stage'):
            serial_started = perf_counter()
            from inspection.serial_results import recognized_serial,plan_results
            inspectors = [special,*live_photo_inspectors.values(),*(row[0] for row in file_photo_results.values())]
            serial = recognized_serial(inspectors)
            settings['_serial_number'] = serial
            output_path,serial_log_name,serial_finalize = plan_results(settings,settings['_serial_stage'],
                output_path,photo_reports,serial,fps,not ng_events and stop_reason=='completed',confidence_recorder,stop_reason)
            performance.add_since('serial_result_planning',serial_started)
        cache_profile = (performance.call('ocr_cache_profile_finalize',learn_cache_usage,inference_telemetry,cache_scope)
                         if stop_reason == 'completed' else None)
        if log_dir is not None and (ng_events or file_test_log):
            txt_prepare_started = perf_counter()
            log_dir.mkdir(parents=True, exist_ok=True)
            log_stem = log_base_stem if photo_selections else output_path.stem
            if file_test_log:
                log_stem = log_stem.removeprefix("phone_stream_")
            log_path = (result_file_path(log_dir / f"{log_stem}.txt",not ng_events and stop_reason == "completed")
                        if status_suffix or file_test_log else unique_path(log_dir / f"{log_stem}.txt"))
            if settings.get('live_log_timestamp'):
                group = settings['special_group']
                threshold = float(settings.get('judge',{}).get('min_confidence',.5))*100
                verdict = 'p' if not ng_events and stop_reason == 'completed' else 'f'
                log_path = log_dir/f"{group}_live_test_threshold_{threshold:g}_result_{verdict}_[{fps:g}fps]_{settings['live_log_timestamp']}.txt"
            if serial_log_name:
                log_path = log_dir/f'{serial_log_name}.txt'
            lines = [
                f"{format_video_time(second)}_{class_name}_{confidence:.2f}"
                for (second, class_name), confidence in sorted(ng_events.items())
            ]
            if not lines and stop_reason == "completed":
                lines = ["result_PASS"]
            if file_test_log:
                lines.extend([f"inspection_status_{stop_reason}",
                              f"processed_frames_{frame_count}_of_{total_frames}",
                              f"processed_video_seconds_{frame_count / fps:.3f}",
                              f"inspection_fps_{fps:g}",
                              f"appearance_checked_frames_{appearance_checked_frames}",
                              f"appearance_detected_defects_{appearance_defects}"])
                for photo_report in photo_reports:
                    lines.append(f"{photo_report['kind']}_photo_frame_{photo_report['frame_number']}_seconds_{photo_report['seconds']:.3f}_result_{photo_report['result']}")
                    if '__range_' in photo_report.get('inspection_task',''):
                        lines.append(f"{photo_report['inspection_task']}_frames_{photo_report['start_frame']}~{photo_report['end_frame']}_result_{photo_report['result']}")
                import json
                lines.extend(f"{format_video_time(second)}_{name}_details_" + json.dumps(detail, ensure_ascii=False)
                             for (second, name), detail in sorted(diagnostics.items()))
            timing = performance.report()
            timing['scatter_plot_recording'] = confidence_recorder is not None
            timing['appearance_backend'] = dict(
                name=getattr(detector,'backend','unknown') if detector.model is not None else 'disabled',
                model_path=str(getattr(detector,'model_path','')),
                source_checkpoint=str(getattr(detector,'weights_path','')))
            mark_model = getattr(special,"mark_model",None)
            if hasattr(mark_model,"parallel_batches"):
                timing["mark_execution"] = dict(max_workers=mark_model.workers,worker_opencv_threads=mark_model.spread,
                    effective_pool_workers=mark_model.pool_workers,
                    variant_cache_limit_mb=mark_model.cache_limit//(1024*1024),
                    parallel_batches=mark_model.parallel_batches,serial_batches=mark_model.serial_batches,
                    search="exact FAIL scan; representative/base-first PASS; full fallback; cached pose variants")
            timing['resource_limits'] = resource_gate.report()
            if settings.get('auto_runtime'):
                timing['automatic_runtime'] = dict(settings.get('_automatic_runtime',{}),
                    save_queue_peak=saver.peak if saver else 0,
                    appearance_prefetch=appearance_stream is not None,
                    appearance_prefetch_capacity=getattr(appearance_stream,'capacity',0),
                    appearance_prefetch_peak=getattr(appearance_stream,'peak',0),
                    appearance_tail_lookahead_enabled=lookahead_enabled,
                    appearance_tail_lookahead_frames=lookahead_frames,
                    appearance_tail_lookahead_depth=1 if lookahead_frames else 0,
                    appearance_parallel=inference_pool is not None)
                timing['automatic_runtime']['inline_file_photo_inspection'] = inline_file_photos
                timing['automatic_runtime']['special_parallel_frames'] = special_pool.parallel_frames if special_pool else 0
                timing['ocr_job_execution'] = {kind:dict(workers=getattr(model,'job_workers',1),
                    inference_threads=getattr(model,'inference_threads',4)) for kind,model in getattr(special,'trained_text_ocr',{}).items()}
            registration = getattr(special,'shape_registration',None)
            if registration is not None and isinstance(getattr(registration,'stats',None),dict):
                timing['reference_tracking'] = dict(registration.stats)
            timing['ocr_backends'] = {kind:dict(getattr(model,'backend_counts',{}))
                                     for kind,model in getattr(special,'trained_text_ocr',{}).items()}
            timing['ocr_backend_errors'] = {kind:list(getattr(model,'backend_errors',[]))
                                           for kind,model in getattr(special,'trained_text_ocr',{}).items()}
            timing['ocr_execution_seconds'] = {kind:dict(getattr(model,'runtime_seconds',{}))
                                              for kind,model in getattr(special,'trained_text_ocr',{}).items()}
            timing['ocr_compile_cache'] = {}
            timing['ocr_compile_cache_totals'] = cache_report(ocr_cache_start)
            if cache_preparation is not None:
                timing['ocr_cache_preparation'] = cache_preparation
            if cache_profile is not None:
                timing['ocr_cache_profile_for_next_run'] = cache_profile
            timing['ocr_runtime_diagnostics'] = inference_telemetry.report()
            if settings.get('openvino_comparison_profile'):
                timing['openvino_comparison'] = dict(profile=settings['openvino_comparison_profile'],
                    repetition=settings.get('comparison_repetition'),repetitions=settings.get('comparison_repetitions'),
                    cache_policy=timing['ocr_compile_cache_totals']['memory_policy'].get('policy_name'),
                    preparation_boundary='profile warmup completes before each profile block; excluded from cycle CT')
            for kind,model in getattr(special,'trained_text_ocr',{}).items():
                counters = getattr(model,'runtime_seconds',{})
                row = {name:int(counters.get('openvino_'+name,0)) for name in
                       ('cache_hits','cache_misses','compiles','recompiles','disk_cache_hits')}
                calls = row['cache_hits']+row['cache_misses']
                row['hit_rate'] = row['cache_hits']/calls if calls else None
                row['fresh_compiles'] = row['compiles']-row['disk_cache_hits']
                timing['ocr_compile_cache'][kind] = row
            if hasattr(capture,"performance"):
                timing["capture"] = capture.performance.report()
            if getattr(capture,'switchbot',None) is not None:
                timing['switchbot'] = capture.switchbot.report()
            import json
            lines.append("performance_" + json.dumps(timing,ensure_ascii=False))
            lines.append(f"performance_total_seconds_{timing['total_wall_seconds']:.6f}")
            lines.append(f"performance_slowest_stage_{timing['slowest_stage']}")
            for name,row in sorted(timing['stages'].items(),key=lambda item:-item[1]['total_seconds']):
                lines.append(f"performance_stage_{name}_count_{row['count']}_total_sec_{row['total_seconds']:.6f}_mean_ms_{row['mean_seconds']*1000:.3f}_max_ms_{row['max_seconds']*1000:.3f}")
            if settings.get('_file_started_at') is not None:
                settings['_live_started_at'] = settings['_file_started_at']
            if settings.get('live_clock_from_capture'):
                if getattr(capture,'started_at',None) is None:
                    raise RuntimeError('Live capture never started; no completed inspection log can be published.')
                settings['_live_started_at'] = capture.started_at
                from datetime import timedelta
                settings['pipeline_start_date'] = (datetime.now()-timedelta(seconds=monotonic()-capture.started_at)).strftime('%Y-%m-%d_%H:%M:%S')
            if settings.get('_live_started_at') is not None:
                from inspection.live_completion import publish_txt
                completed_date = datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
                lines.extend([f"pipeline start date : {settings.get('pipeline_start_date','')}",
                              f"pipeline date : {completed_date}",f"serial_number: {settings.get('_serial_number','no_reading')}"])
                performance.add_since('verdict_txt_prepare',txt_prepare_started)
                settings['_live_result_ready_seconds'] = performance.call('verdict_txt_publish',publish_txt,log_path,lines,
                    settings['_live_started_at'],settings.get('live_inspection_count',1))
                performance.mark('verdict_txt_published')
                settings['_live_result_log'] = str(log_path)
                settings['_pipeline_date'] = completed_date
                if settings.get('cycle_complete_callback'):
                    settings['cycle_complete_callback'](settings['_live_result_ready_seconds'],completed_date)
            else:
                performance.add_since('verdict_txt_prepare',txt_prepare_started)
                performance.call('verdict_txt_publish',log_path.write_text,"\n".join(lines) + "\n",encoding="utf-8")
                performance.mark('verdict_txt_published')
            if file_test_log and settings.get("root_dir"):
                from inspection.pipeline_audit import audit_pipeline
                report = audit_pipeline(settings)
                report.update(input=source, output=str(output_path), processed_frames=frame_count,
                              total_frames=total_frames, status=stop_reason,
                              processing_seconds=round(monotonic() - started_at, 3),
                              ng_frames=ng_frame_count, appearance_checked_frames=appearance_checked_frames,
                              appearance_detected_defects=appearance_defects)
                timing.update(performance.report())
                timing['verdict_timeline'] = performance.verdict_timeline()
                report["performance"] = timing
                if settings.get('speed_benchmark'):
                    report['speed_benchmark'] = settings['speed_benchmark']
                if essential is not None:
                    report['essential_shapes'] = essential.report()
                if settings.get('_live_result_ready_seconds') is not None:
                    report['pipeline_time_seconds'] = settings['_live_result_ready_seconds']
                    report['pipeline_time_boundary'] = ('Actual camera capture start' if settings.get('live_clock_from_capture') else 'Start click')+' to final verdict TXT flushed; media saving may continue'
                    report['inspection_count'] = settings.get('live_inspection_count',1)
                    report['media_save_status'] = 'background'
                    report['pipeline_start_date'] = settings.get('pipeline_start_date')
                    report['pipeline_date'] = settings.get('_pipeline_date')
                    report['serial_number'] = settings.get('_serial_number','no_reading')
                if settings.get("record_condition"):
                    report["record_condition"] = settings["record_condition"]
                report["conn_duplicate_detections_removed"] = getattr(special, "conn_duplicates_removed", 0)
                report["model_load_seconds"] = round(model_load_seconds,3)
                report["photo_selection_seconds"] = round(photo_selection_seconds,3)
                if photo_reports:
                    report["photo_inspections"] = photo_reports
                    report['photo_kind_results'] = range_statuses({p.get('inspection_task',p['kind']):p['result'] for p in photo_reports})
                    report["output_video"] = str(output_path) if video_output else None
                log_path.with_suffix(".pipeline.json").write_text(json.dumps(report, ensure_ascii=False, indent=2),
                                                               encoding="utf-8")

        print(f"output: {output_path}")
    if output_path is not None and serial_finalize is not None:
        if saver is not None:
            saver.submit('serial_result_finalize',serial_finalize)
        else:
            serial_finalize()
    if confidence_recorder is not None and confidence_run is not None and not settings.get('_serial_stage'):
        confidence_recorder.finish(stop_reason)
    if 'test_outcome' in settings:
        settings['test_outcome']['status'] = stop_reason
    return output_path


def inspect_camera(settings: dict[str, Any]) -> None:
    import cv2

    from camera.camera_grab import USBCamera
    from database.save_result import ResultSaver
    from inspection.judge_logic import InspectionJudge
    from model.yolo_inference import YoloInference

    detector = YoloInference(settings)
    judge = InspectionJudge(settings)
    saver = ResultSaver(settings)
    camera = USBCamera(settings)

    try:
        camera.open()
        print("Press SPACE to inspect current frame, ESC to quit.")
        while True:
            frame = camera.read()
            preview = frame.copy()
            cv2.putText(
                preview,
                "SPACE: inspect | ESC: quit",
                (20, 35),
                cv2.FONT_HERSHEY_SIMPLEX,
                0.8,
                (0, 255, 255),
                2,
                cv2.LINE_AA,
            )
            cv2.imshow("USB Camera", preview)

            key = cv2.waitKey(1) & 0xFF
            if key == 27:
                break
            if key == 32:
                detections, annotated = detector.predict(frame)
                result = judge.judge(detections)
                saved = saver.save(
                    image=annotated,
                    source=f"usb_camera_{settings['camera']['index']}",
                    result=result,
                    detections=detections,
                )
                print(f"{result['judge']} - {result['reason']} - {saved['image_path']}")
                cv2.imshow("Inspection Result", annotated)
    finally:
        camera.release()
        cv2.destroyAllWindows()


def main() -> None:
    parser = argparse.ArgumentParser(description="Product vision inspection starter app")
    parser.add_argument("--image", type=Path, help="Inspect one saved image file")
    parser.add_argument("--images", type=Path, help="Inspect every image in a folder")
    parser.add_argument("--video", type=Path, help="Inspect a video and display NG labels only")
    parser.add_argument("--stream", help="Inspect an IP camera or phone stream URL")
    parser.add_argument(
        "--output-video",
        type=Path,
        help=f"Save annotated video output. Default: {DEFAULT_TEST_VIDEO_DIR}\\<video>_test.mp4",
    )
    parser.add_argument("--camera", action="store_true", help="Inspect frames from USB camera")
    parser.add_argument("--gui", action="store_true", help="Start Tkinter GUI")
    parser.add_argument("--video-gui", action="store_true", help="Start video inspection parameter GUI")
    parser.add_argument("--video-dataset", action="store_true", help="Build a YOLO dataset from a video")
    parser.add_argument("--phone-dataset", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()

    settings = load_settings()

    if args.gui:
        from gui.main_gui import run_gui

        run_gui(settings)
        return

    if args.video_gui:
        from video_inspection_gui import main as video_gui_main

        video_gui_main()
        return

    if args.video_dataset or args.phone_dataset:
        from video_dataset_builder import main as phone_dataset_main

        phone_dataset_main()
        return

    if args.image:
        inspect_image(args.image, settings)
        return

    if args.images:
        inspect_images(args.images, settings)
        return

    if args.video:
        inspect_video(args.video, settings, args.output_video)
        return

    if args.stream:
        inspect_video(args.stream, settings, args.output_video)
        return

    if args.camera:
        inspect_camera(settings)
        return

    from video_inspection_gui import main as video_gui_main

    video_gui_main()


if __name__ == "__main__":
    main()
