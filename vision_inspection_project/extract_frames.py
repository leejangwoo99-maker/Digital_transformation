from __future__ import annotations

import argparse
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


def extract_frames(args: argparse.Namespace) -> int:
    import cv2

    settings = load_settings()
    video_path = args.video if args.video.is_absolute() else ROOT_DIR / args.video
    if not video_path.exists():
        raise FileNotFoundError(f"Video file not found: {video_path}")

    output_dir = args.output_dir or default_output_dir(args.split, settings)
    if not output_dir.is_absolute():
        output_dir = ROOT_DIR / output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    resize_to = parse_resize(args.resize)
    capture = cv2.VideoCapture(str(video_path))
    if not capture.isOpened():
        raise RuntimeError(f"Video open failed: {video_path}")

    source_fps = capture.get(cv2.CAP_PROP_FPS) or 0
    every_n_frames = args.every_n_frames
    if args.every_seconds:
        if source_fps <= 0:
            raise RuntimeError("Cannot use --every-seconds because video FPS is unknown.")
        every_n_frames = max(1, round(source_fps * args.every_seconds))

    prefix = args.prefix or video_path.stem
    frame_index = 0
    saved_count = 0

    try:
        while True:
            ok, frame = capture.read()
            if not ok:
                break

            should_save = frame_index >= args.start_frame and frame_index % every_n_frames == 0
            if should_save:
                if resize_to is not None:
                    frame = cv2.resize(frame, resize_to, interpolation=cv2.INTER_AREA)

                image_name = f"{prefix}_{frame_index:06d}.jpg"
                image_path = output_dir / image_name
                cv2.imwrite(str(image_path), frame, [cv2.IMWRITE_JPEG_QUALITY, args.quality])
                saved_count += 1

                if args.max_frames and saved_count >= args.max_frames:
                    break

            frame_index += 1
    finally:
        capture.release()

    print(f"Video: {video_path}")
    print(f"Output: {output_dir}")
    print(f"Saved frames: {saved_count}")
    return saved_count


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract YOLO training images from a recorded video")
    parser.add_argument("video", type=Path, help="Input video path")
    parser.add_argument("--split", choices=["train", "val", "test", "raw"], default="train")
    parser.add_argument("--output-dir", type=Path, help="Override output image directory")
    parser.add_argument("--prefix", help="Output image file prefix. Default: video file name")
    parser.add_argument("--every-n-frames", type=int, default=10, help="Save one image every N frames")
    parser.add_argument("--every-seconds", type=float, help="Save one image every N seconds")
    parser.add_argument("--start-frame", type=int, default=0, help="Skip frames before this frame index")
    parser.add_argument("--max-frames", type=int, help="Stop after saving this many images")
    parser.add_argument("--resize", help="Resize saved images, for example 1280x720")
    parser.add_argument("--quality", type=int, default=95, help="JPEG quality, 1 to 100")
    args = parser.parse_args()
    extract_frames(args)


if __name__ == "__main__":
    main()
