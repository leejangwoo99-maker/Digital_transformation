from __future__ import annotations

import argparse
import random
import re
import shutil
import sys
import threading
import tkinter as tk
from datetime import datetime
from pathlib import Path
from tkinter import filedialog, messagebox
from types import SimpleNamespace
from typing import Any

import yaml

from extract_frames import extract_frames
from label_images import DATASET_YAML_PATH, YoloBoxLabeler, load_dataset_classes
from main import ORIGINAL_VIDEO_DIR, ensure_vision_dirs
from train import train


ROOT_DIR = Path(__file__).resolve().parent
CONFIG_PATH = ROOT_DIR / "config" / "settings.yaml"
FRAME_NAME_PATTERN = re.compile(r"^(?P<prefix>.+)_(?P<frame>\d{6})$")
DEFAULT_ORIGINAL_VIDEO_DIR = ORIGINAL_VIDEO_DIR


def load_settings(path: Path = CONFIG_PATH) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as file:
        settings = yaml.safe_load(file) or {}

    settings["root_dir"] = str(ROOT_DIR)
    return settings


class VideoDatasetBuilderApp:
    def __init__(self, root: tk.Tk) -> None:
        ensure_vision_dirs()
        self.root = root
        self.settings = load_settings()
        extract_settings = self.settings.get("extract", {})

        self.video_path: Path | None = None
        self.original_video_dir = DEFAULT_ORIGINAL_VIDEO_DIR
        self.split_var = tk.StringVar(value=extract_settings.get("auto_split", "train"))
        self.every_n_frames_var = tk.IntVar(value=int(extract_settings.get("every_n_frames", 10)))
        self.quality_var = tk.IntVar(value=int(extract_settings.get("jpeg_quality", 95)))
        self.status_var = tk.StringVar(value="Select a video.")
        self.video_var = tk.StringVar(value="No video selected")
        self.stream_url_var = tk.StringVar(value="http://192.168.190.191:8080/video")
        self.output_var = tk.StringVar(value=str(ROOT_DIR / "dataset" / "images" / self.split_var.get()))

        self.root.title("Video Dataset Builder")
        self.root.geometry("900x410")
        self.root.resizable(False, False)
        self._build()

    def _build(self) -> None:
        frame = tk.Frame(self.root, padx=18, pady=18)
        frame.pack(fill="both", expand=True)

        tk.Label(frame, text="Video Dataset Builder", font=("Segoe UI", 16, "bold")).pack(anchor="w")
        tk.Label(frame, textvariable=self.video_var, font=("Segoe UI", 10), fg="#555", wraplength=660).pack(
            anchor="w",
            pady=(8, 8),
        )
        stream_frame = tk.Frame(frame)
        stream_frame.pack(anchor="w", fill="x", pady=(0, 8))
        tk.Label(stream_frame, text="phone stream URL", width=18, anchor="w").pack(side="left")
        tk.Entry(stream_frame, textvariable=self.stream_url_var, width=76).pack(side="left", fill="x", expand=True)

        tk.Label(frame, textvariable=self.output_var, font=("Segoe UI", 10), fg="#555", wraplength=660).pack(
            anchor="w",
            pady=(0, 16),
        )

        button_frame = tk.Frame(frame)
        button_frame.pack(anchor="w")
        tk.Button(button_frame, text="select video", width=16, height=2, command=self.select_video).pack(side="left")
        tk.Button(button_frame, text="phone live", width=16, height=2, command=self.record_phone_live).pack(
            side="left",
            padx=(8, 0),
        )
        tk.Button(button_frame, text="extract frames", width=16, height=2, command=self.extract_selected_video).pack(
            side="left",
            padx=8,
        )
        tk.Button(button_frame, text="open labeler", width=16, height=2, command=self.open_labeler).pack(side="left")
        tk.Button(button_frame, text="revision", width=16, height=2, command=self.open_revision_picker).pack(
            side="left",
            padx=(8, 0),
        )

        workflow_frame = tk.Frame(frame)
        workflow_frame.pack(anchor="w", pady=(12, 0))
        tk.Button(workflow_frame, text="val 20% split", width=16, height=2, command=self.split_validation_data).pack(
            side="left",
        )
        tk.Button(workflow_frame, text="train", width=16, height=2, command=self.train_model).pack(side="left", padx=8)

        option_frame = tk.Frame(frame)
        option_frame.pack(anchor="w", pady=(18, 0))

        tk.Label(option_frame, text="split").grid(row=0, column=0, sticky="w")
        tk.OptionMenu(option_frame, self.split_var, "train", "val", "test", command=lambda value: self._update_output()).grid(
            row=0,
            column=1,
            sticky="w",
            padx=(8, 22),
        )

        tk.Label(option_frame, text="every n frames").grid(row=0, column=2, sticky="w")
        tk.Spinbox(option_frame, from_=1, to=300, width=8, textvariable=self.every_n_frames_var).grid(
            row=0,
            column=3,
            sticky="w",
            padx=(8, 22),
        )

        tk.Label(option_frame, text="jpeg quality").grid(row=0, column=4, sticky="w")
        tk.Spinbox(option_frame, from_=1, to=100, width=8, textvariable=self.quality_var).grid(
            row=0,
            column=5,
            sticky="w",
            padx=(8, 0),
        )

        tk.Label(frame, textvariable=self.status_var, font=("Segoe UI", 10), fg="#555").pack(anchor="w", pady=(22, 0))

    def _update_output(self) -> None:
        self.output_var.set(str(ROOT_DIR / "dataset" / "images" / self.split_var.get()))

    def select_video(self) -> None:
        file_path = filedialog.askopenfilename(
            title="Select video",
            filetypes=[
                ("Video files", "*.mp4 *.mov *.m4v *.avi *.mkv"),
                ("All files", "*.*"),
            ],
        )
        if not file_path:
            return

        self.video_path = Path(file_path)
        self.video_var.set(f"Video: {self.video_path}")
        self.status_var.set("Ready to extract frames.")

    def record_phone_live(self) -> None:
        stream_url = self.stream_url_var.get().strip()
        if not stream_url:
            messagebox.showwarning("No stream URL", "Please enter the phone stream URL first.")
            return

        output_dir = self.original_video_dir
        self.status_var.set("Phone live recording started. Press q or ESC in the video window to stop.")
        threading.Thread(target=self._record_phone_live_worker, args=(stream_url, output_dir), daemon=True).start()

    def _record_phone_live_worker(self, stream_url: str, output_dir: Path) -> None:
        try:
            saved_path = record_stream_video(stream_url, output_dir)
            self.video_path = saved_path
            self.root.after(0, lambda: self.video_var.set(f"Video: {saved_path}"))
            self.root.after(0, lambda: self.status_var.set(f"Saved original video: {saved_path}"))
        except Exception as exc:
            error = str(exc)
            self.root.after(0, lambda: messagebox.showerror("Phone live failed", error))
            self.root.after(0, lambda: self.status_var.set("Phone live recording failed."))

    def extract_selected_video(self) -> None:
        if self.video_path is None:
            messagebox.showwarning("No video", "Please select a video first.")
            return

        self.status_var.set("Extracting frames...")
        threading.Thread(target=self._extract_worker, daemon=True).start()

    def _extract_worker(self) -> None:
        try:
            args = SimpleNamespace(
                video=self.video_path,
                split=self.split_var.get(),
                output_dir=None,
                prefix=None,
                every_n_frames=int(self.every_n_frames_var.get()),
                every_seconds=None,
                start_frame=0,
                max_frames=None,
                resize=None,
                quality=int(self.quality_var.get()),
            )
            count = extract_frames(args)
            self.root.after(0, lambda: self.status_var.set(f"Extracted {count} images. Open labeler next."))
        except Exception as exc:
            self.root.after(0, lambda: messagebox.showerror("Frame extraction failed", str(exc)))
            self.root.after(0, lambda: self.status_var.set("Frame extraction failed."))

    def open_labeler(self) -> None:
        split = self.split_var.get()
        image_dir = ROOT_DIR / "dataset" / "images" / split
        label_dir = ROOT_DIR / "dataset" / "labels" / split

        try:
            classes = load_dataset_classes(DATASET_YAML_PATH)
            self.status_var.set(f"Labeling: {image_dir}")
            YoloBoxLabeler(image_dir, label_dir, classes).run()
            self.status_var.set(f"Labels saved to: {label_dir}")
        except Exception as exc:
            messagebox.showerror("Labeler failed", str(exc))
            self.status_var.set("Labeler failed.")

    def open_revision_picker(self) -> None:
        split = self.split_var.get()
        image_dir = ROOT_DIR / "dataset" / "images" / split
        groups = find_frame_groups(image_dir)
        if not groups:
            messagebox.showwarning("No frame groups", f"No frame groups found in:\n{image_dir}")
            return

        picker = tk.Toplevel(self.root)
        picker.title("Select revision group")
        picker.geometry("620x420")
        picker.resizable(False, False)
        picker.transient(self.root)
        picker.grab_set()

        tk.Label(
            picker,
            text=f"Revision groups in {split}",
            font=("Segoe UI", 13, "bold"),
        ).pack(anchor="w", padx=14, pady=(14, 6))

        listbox = tk.Listbox(picker, font=("Consolas", 10), height=14, width=86)
        listbox.pack(fill="both", expand=True, padx=14, pady=(0, 10))

        prefixes = sorted(groups)
        for prefix in prefixes:
            frames = groups[prefix]
            first_frame = frame_number(frames[0])
            last_frame = frame_number(frames[-1])
            listbox.insert(
                tk.END,
                f"{prefix}    count={len(frames):04d}    {first_frame:06d}~{last_frame:06d}",
            )

        listbox.selection_set(0)
        listbox.activate(0)

        button_row = tk.Frame(picker)
        button_row.pack(anchor="e", padx=14, pady=(0, 14))

        def open_selected() -> None:
            selection = listbox.curselection()
            if not selection:
                return
            prefix = prefixes[selection[0]]
            picker.destroy()
            self.open_revision_group(prefix, groups[prefix])

        def delete_selected() -> None:
            selection = listbox.curselection()
            if not selection:
                return

            index = selection[0]
            prefix = prefixes[index]
            image_paths = groups[prefix]
            proceed = messagebox.askyesno(
                "Delete revision group?",
                f"Delete this group from {split}?\n\n"
                f"{prefix}\n"
                f"Images: {len(image_paths)}\n\n"
                "Matching label txt files will also be deleted.",
                parent=picker,
            )
            if not proceed:
                return

            deleted_count = delete_frame_group(image_paths, ROOT_DIR / "dataset" / "labels" / split)
            del groups[prefix]
            del prefixes[index]
            listbox.delete(index)
            if prefixes:
                next_index = min(index, len(prefixes) - 1)
                listbox.selection_set(next_index)
                listbox.activate(next_index)
            self.status_var.set(f"Deleted {deleted_count} files for group: {prefix}")

        tk.Button(button_row, text="open selected", width=16, command=open_selected).pack(side="left", padx=(0, 8))
        tk.Button(button_row, text="delete", width=12, command=delete_selected).pack(side="left", padx=(0, 8))
        tk.Button(button_row, text="cancel", width=12, command=picker.destroy).pack(side="left")
        listbox.bind("<Double-Button-1>", lambda event: open_selected())

    def open_revision_group(self, prefix: str, image_paths: list[Path]) -> None:
        split = self.split_var.get()
        image_dir = ROOT_DIR / "dataset" / "images" / split
        label_dir = ROOT_DIR / "dataset" / "labels" / split

        try:
            classes = load_dataset_classes(DATASET_YAML_PATH)
            self.status_var.set(f"Revision: {prefix} ({len(image_paths)} frames)")
            YoloBoxLabeler(image_dir, label_dir, classes, image_paths=image_paths).run()
            self.status_var.set(f"Revision saved: {prefix}. Train again to update weights/best.pt.")
        except Exception as exc:
            messagebox.showerror("Revision failed", str(exc))
            self.status_var.set("Revision failed.")

    def split_validation_data(self) -> None:
        try:
            moved_count = split_train_to_val(0.2)
            self.status_var.set(f"Moved {moved_count} image/label pairs from train to val.")
        except Exception as exc:
            messagebox.showerror("Validation split failed", str(exc))
            self.status_var.set("Validation split failed.")

    def train_model(self) -> None:
        self.status_var.set("Training started. This can take a while...")
        threading.Thread(target=self._train_worker, daemon=True).start()

    def _train_worker(self) -> None:
        try:
            args = SimpleNamespace(
                data=None,
                model=None,
                epochs=None,
                imgsz=None,
                batch=None,
                device=None,
                project=None,
                name=None,
            )
            train(args)
            self.root.after(0, lambda: self.status_var.set("Training finished. Model saved to weights/best.pt"))
        except Exception as exc:
            error = str(exc)
            if "ultralytics" in error:
                error = (
                    "ultralytics is not installed in this Python environment.\n\n"
                    "Run this command in the project terminal:\n"
                    f"{sys.executable} -m pip install -r requirements.txt"
                )
            self.root.after(0, lambda: messagebox.showerror("Training failed", error))
            self.root.after(0, lambda: self.status_var.set("Training failed."))


def split_train_to_val(val_ratio: float = 0.2, seed: int = 42) -> int:
    train_image_dir = ROOT_DIR / "dataset" / "images" / "train"
    train_label_dir = ROOT_DIR / "dataset" / "labels" / "train"
    val_image_dir = ROOT_DIR / "dataset" / "images" / "val"
    val_label_dir = ROOT_DIR / "dataset" / "labels" / "val"
    val_image_dir.mkdir(parents=True, exist_ok=True)
    val_label_dir.mkdir(parents=True, exist_ok=True)

    image_paths = sorted(
        path
        for extension in ("*.jpg", "*.jpeg", "*.png", "*.bmp")
        for path in train_image_dir.glob(extension)
    )
    if not image_paths:
        raise FileNotFoundError(f"No training images found: {train_image_dir}")

    move_count = max(1, round(len(image_paths) * val_ratio))
    random.Random(seed).shuffle(image_paths)
    selected_images = image_paths[:move_count]

    moved_count = 0
    for image_path in selected_images:
        label_path = train_label_dir / f"{image_path.stem}.txt"
        target_image_path = val_image_dir / image_path.name
        target_label_path = val_label_dir / label_path.name

        shutil.move(str(image_path), str(target_image_path))
        if label_path.exists():
            shutil.move(str(label_path), str(target_label_path))
        moved_count += 1

    return moved_count


def frame_prefix(path: Path) -> str:
    match = FRAME_NAME_PATTERN.match(path.stem)
    return match.group("prefix") if match else path.stem


def frame_number(path: Path) -> int:
    match = FRAME_NAME_PATTERN.match(path.stem)
    return int(match.group("frame")) if match else 0


def find_frame_groups(image_dir: Path) -> dict[str, list[Path]]:
    groups: dict[str, list[Path]] = {}
    for extension in ("*.jpg", "*.jpeg", "*.png", "*.bmp"):
        for image_path in image_dir.glob(extension):
            prefix = frame_prefix(image_path)
            groups.setdefault(prefix, []).append(image_path)

    for prefix, image_paths in groups.items():
        groups[prefix] = sorted(image_paths, key=lambda path: (frame_number(path), path.name))
    return groups


def delete_frame_group(image_paths: list[Path], label_dir: Path) -> int:
    deleted_count = 0
    for image_path in image_paths:
        label_path = label_dir / f"{image_path.stem}.txt"
        if image_path.exists():
            image_path.unlink()
            deleted_count += 1
        if label_path.exists():
            label_path.unlink()
            deleted_count += 1
    return deleted_count


def record_stream_video(stream_url: str, output_dir: Path) -> Path:
    import cv2

    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"phone_live_{datetime.now().strftime('%Y%m%d_%H%M%S')}.mp4"

    capture = cv2.VideoCapture(stream_url)
    if not capture.isOpened():
        raise RuntimeError(f"Phone stream open failed: {stream_url}")

    fps = capture.get(cv2.CAP_PROP_FPS) or 30
    width = int(capture.get(cv2.CAP_PROP_FRAME_WIDTH) or 0)
    height = int(capture.get(cv2.CAP_PROP_FRAME_HEIGHT) or 0)
    writer = None

    try:
        cv2.namedWindow("Phone Live Recording", cv2.WINDOW_NORMAL)
        while True:
            ok, frame = capture.read()
            if not ok:
                break

            if writer is None:
                height, width = frame.shape[:2]
                fourcc = cv2.VideoWriter_fourcc(*"mp4v")
                writer = cv2.VideoWriter(str(output_path), fourcc, fps, (width, height))
                if not writer.isOpened():
                    raise RuntimeError(f"Output video writer open failed: {output_path}")

            writer.write(frame)
            cv2.imshow("Phone Live Recording", frame)
            key = cv2.waitKey(1) & 0xFF
            if key in (27, ord("q"), ord("Q")):
                break
    finally:
        capture.release()
        if writer is not None:
            writer.release()
        cv2.destroyAllWindows()

    return output_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Build YOLO dataset from a video")
    parser.add_argument("--gui", action="store_true", help="Start the video dataset builder GUI")
    parser.parse_args()

    root = tk.Tk()
    VideoDatasetBuilderApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
