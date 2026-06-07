from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
from time import monotonic
from typing import Any

import numpy as np
import yaml


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


def draw_ng_detections(frame, detections: list[dict[str, Any]], settings: dict[str, Any]):
    import cv2

    annotated = frame.copy()
    judge_settings = settings.get("judge", {})
    ng_classes = {class_name.lower() for class_name in judge_settings.get("ng_classes", [])}
    min_confidence = float(judge_settings.get("min_confidence", 0.35))

    ng_detections = []
    for detection in detections:
        class_name = str(detection.get("class_name", ""))
        confidence = float(detection.get("confidence", 0.0))
        if class_name.lower() not in ng_classes or confidence < min_confidence:
            continue

        x1, y1, x2, y2 = [int(value) for value in detection["xyxy"]]
        cv2.rectangle(annotated, (x1, y1), (x2, y2), (0, 0, 255), 3)
        label = f"{class_name} {confidence:.2f}"
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
        ng_detections.append(detection)

    status = "NG" if ng_detections else "OK"
    color = (0, 0, 255) if ng_detections else (0, 180, 0)
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
        candidate = path.with_name(f"{path.stem}_{index}{path.suffix}")
        if not candidate.exists():
            return candidate
        index += 1


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


def inspect_video(
    video_path: Path | str,
    settings: dict[str, Any],
    output_path: Path | None = None,
    display_width: int = 1600,
    display_height: int = 900,
    show_window: bool = True,
    max_seconds: float | None = None,
    status_suffix: bool = False,
    log_dir: Path | None = None,
) -> Path:
    import cv2

    from model.yolo_inference import YoloInference

    source = str(video_path)
    if not is_stream_source(source):
        path = Path(video_path)
        if not path.exists():
            raise FileNotFoundError(f"Video file not found: {path}")
        source = str(path)

    if output_path is None:
        output_path = DEFAULT_TEST_VIDEO_DIR / f"{source_stem(source)}_test.mp4"

    detector = YoloInference(settings)
    capture = open_video_capture(source)
    if not capture.isOpened():
        raise RuntimeError(
            f"Video open failed: {source}\n\n"
            "Check that the phone camera app is running, the PC and phone are on the same network, "
            "and the URL looks like http://192.168.x.x:8080/video."
        )

    fps = capture.get(cv2.CAP_PROP_FPS) or 30
    source_width = int(capture.get(cv2.CAP_PROP_FRAME_WIDTH) or 0)
    source_height = int(capture.get(cv2.CAP_PROP_FRAME_HEIGHT) or 0)
    writer = None
    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        fourcc = cv2.VideoWriter_fourcc(*"mp4v")
        if source_width <= 0 or source_height <= 0:
            ok, probe_frame = capture.read()
            if not ok:
                raise RuntimeError(f"Cannot read first frame from: {source}")
            source_height, source_width = probe_frame.shape[:2]
            capture.set(cv2.CAP_PROP_POS_FRAMES, 0)

        writer = cv2.VideoWriter(str(output_path), fourcc, fps, (source_width, source_height))
        if not writer.isOpened():
            raise RuntimeError(f"Output video writer open failed: {output_path}")

    frame_count = 0
    ng_frame_count = 0
    ng_events: dict[tuple[int, str], float] = {}
    started_at = monotonic()
    try:
        if show_window:
            cv2.namedWindow("Video Inspection - NG labels only", cv2.WINDOW_NORMAL)
            cv2.resizeWindow("Video Inspection - NG labels only", display_width, display_height)

        while True:
            if max_seconds is not None and monotonic() - started_at >= max_seconds:
                break

            ok, frame = capture.read()
            if not ok:
                break

            detections, _ = detector.predict(frame)
            annotated, ng_detections = draw_ng_detections(frame, detections, settings)
            display_frame = letterbox_frame(annotated, display_width, display_height)
            if ng_detections:
                ng_frame_count += 1
                elapsed_seconds = monotonic() - started_at if is_stream_source(source) else frame_count / fps
                event_second = int(elapsed_seconds)
                for detection in ng_detections:
                    class_name = str(detection.get("class_name", ""))
                    confidence = float(detection.get("confidence", 0.0))
                    key = (event_second, class_name)
                    ng_events[key] = max(confidence, ng_events.get(key, 0.0))

            if writer is not None:
                writer.write(annotated)

            if show_window:
                cv2.imshow("Video Inspection - NG labels only", display_frame)
                key = cv2.waitKey(1) & 0xFF
                if key in (27, ord("q"), ord("Q")):
                    break

            frame_count += 1
    finally:
        capture.release()
        if writer is not None:
            writer.release()
        cv2.destroyAllWindows()

    print(f"video: {source}")
    print(f"frames: {frame_count}")
    print(f"NG frames: {ng_frame_count}")
    if output_path is not None:
        if status_suffix:
            suffix = "_fail" if ng_events else "_pass"
            final_path = unique_path(output_path.with_name(f"{output_path.stem}{suffix}{output_path.suffix}"))
            output_path.replace(final_path)
            output_path = final_path

        if log_dir is not None and ng_events:
            log_dir.mkdir(parents=True, exist_ok=True)
            log_path = log_dir / f"{output_path.stem}.txt"
            lines = [
                f"{format_video_time(second)}_{class_name}_{confidence:.2f}"
                for (second, class_name), confidence in sorted(ng_events.items())
            ]
            log_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

        print(f"output: {output_path}")
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
