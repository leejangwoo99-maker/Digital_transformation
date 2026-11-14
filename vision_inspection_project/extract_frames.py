from __future__ import annotations

import argparse
import math
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import yaml


ROOT_DIR = Path(__file__).resolve().parent
CONFIG_PATH = ROOT_DIR / "config" / "settings.yaml"


def load_settings(path: Path = CONFIG_PATH) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as file:
        settings = yaml.safe_load(file) or {}

    settings["root_dir"] = str(ROOT_DIR)
    return settings


def parse_resize(value: str | None) -> tuple[int, int] | None:
    if not value:
        return None

    width_text, height_text = value.lower().split("x", maxsplit=1)
    return int(width_text), int(height_text)


def default_output_dir(split: str, settings: dict[str, Any]) -> Path:
    extract_settings = settings.get("extract", {})
    if split in {"train", "val", "test"}:
        return ROOT_DIR / "dataset" / "images" / split

    return ROOT_DIR / extract_settings.get("frame_dir", "captures/frames")


def extract_frames(args: argparse.Namespace, log: Callable[[str], None] | None = None) -> int:
    import cv2

    def report(message: str) -> None:
        if log is None:
            print(message)
        else:
            log(message + "\n")

    settings = load_settings()
    video_path = args.video if args.video.is_absolute() else ROOT_DIR / args.video
    if not video_path.exists():
        raise FileNotFoundError(f"Video file not found: {video_path}")

    output_dir = args.output_dir or default_output_dir(args.split, settings)
    if not output_dir.is_absolute():
        output_dir = ROOT_DIR / output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    resize_to = parse_resize(args.resize)
    requested_fps = getattr(args, "target_fps", None)
    if requested_fps is not None:
        requested_fps = float(requested_fps)
        if not math.isfinite(requested_fps) or requested_fps <= 0:
            raise ValueError("Extraction FPS must be a finite number greater than zero.")
    capture = cv2.VideoCapture(str(video_path))
    if not capture.isOpened():
        raise RuntimeError(f"Video open failed: {video_path}")

    source_fps = capture.get(cv2.CAP_PROP_FPS) or 0
    total_frames = int(capture.get(cv2.CAP_PROP_FRAME_COUNT))
    every_n_frames = args.every_n_frames
    target_fps = requested_fps
    if args.every_seconds:
        if source_fps <= 0:
            raise RuntimeError("Cannot use --every-seconds because video FPS is unknown.")
        every_n_frames = max(1, round(source_fps * args.every_seconds))
    if target_fps:
        if source_fps <= 0:
            raise RuntimeError("Cannot use target FPS because video FPS is unknown.")
        target_fps = min(float(target_fps), float(source_fps))

    prefix = args.prefix or video_path.stem
    frame_index = 0
    saved_count = 0
    next_save_time = args.start_frame / source_fps if target_fps else 0.0
    last_report_time = time.monotonic()
    report(f"Video: {video_path}")
    report(f"Output: {output_dir}")
    report(f"Source FPS: {source_fps:g}; extraction FPS: {target_fps or 'interval mode'}")
    if requested_fps is not None:
        report(f"Requested FPS: {requested_fps:g}; effective FPS: {target_fps:g}")
        if requested_fps > source_fps:
            report("Requested FPS exceeds source FPS; saving available source frames without duplicates.")
        if total_frames > 0:
            estimated = math.ceil(max(0, total_frames - args.start_frame) * target_fps / source_fps - 1e-9)
            if args.max_frames:
                estimated = min(estimated, args.max_frames)
            report(f"Expected saved frames: approximately {estimated}")

    try:
        while True:
            ok, frame = capture.read()
            if not ok:
                break

            if target_fps:
                current_time = frame_index / source_fps
                should_save = frame_index >= args.start_frame and current_time + 1e-9 >= next_save_time
            else:
                should_save = frame_index >= args.start_frame and frame_index % every_n_frames == 0
            if should_save:
                if resize_to is not None:
                    frame = cv2.resize(frame, resize_to, interpolation=cv2.INTER_AREA)

                image_name = f"{prefix}_{frame_index:06d}.jpg"
                image_path = output_dir / image_name
                if not cv2.imwrite(str(image_path), frame, [cv2.IMWRITE_JPEG_QUALITY, args.quality]):
                    raise RuntimeError(f"Frame save failed: {image_path}")
                saved_count += 1
                if target_fps:
                    next_save_time += 1.0 / target_fps

                if args.max_frames and saved_count >= args.max_frames:
                    break

            frame_index += 1
            now = time.monotonic()
            if now - last_report_time >= 0.5:
                progress = (
                    f"{min(100.0, frame_index / total_frames * 100):.1f}% ({frame_index}/{total_frames})"
                    if total_frames > 0 else f"{frame_index} frames processed"
                )
                report(f"Extracting: {progress}; saved: {saved_count}")
                last_report_time = now
    finally:
        capture.release()

    report(f"Extraction finished. Saved frames: {saved_count}")
    return saved_count


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract YOLO training images from a recorded video")
    parser.add_argument("video", type=Path, help="Input video path")
    parser.add_argument("--split", choices=["train", "val", "test", "raw"], default="train")
    parser.add_argument("--output-dir", type=Path, help="Override output image directory")
    parser.add_argument("--prefix", help="Output image file prefix. Default: video file name")
    parser.add_argument("--every-n-frames", type=int, default=10, help="Save one image every N frames")
    parser.add_argument("--every-seconds", type=float, help="Save one image every N seconds")
    parser.add_argument("--target-fps", type=float, help="Save approximately this many frames per second")
    parser.add_argument("--start-frame", type=int, default=0, help="Skip frames before this frame index")
    parser.add_argument("--max-frames", type=int, help="Stop after saving this many images")
    parser.add_argument("--resize", help="Resize saved images, for example 1280x720")
    parser.add_argument("--quality", type=int, default=95, help="JPEG quality, 1 to 100")
    args = parser.parse_args()
    extract_frames(args)


if __name__ == "__main__":
    main()
