from __future__ import annotations

import argparse
import copy
import re
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
from main import DEFAULT_TEST_VIDEO_DIR, OPERATION_DIR, ROOT_DIR, TEST_LOG_DIR, ensure_vision_dirs, inspect_video, source_stem
from train import train
from video_dataset_builder import delete_frame_group, find_frame_groups, frame_number, split_train_to_val


CONFIG_PATH = ROOT_DIR / "config" / "settings.yaml"
LIVE_SECONDS_FILE = "live_seconds.txt"
SAVE_VIDEO_FOLDER_FILE = "save_video_folder.txt"
LIVE_TEST_LOG_SAVE_FILE = "live_test_log_save.txt"
PHONE_STREAM_URL_FILE = "phone_stream_url.txt"
DEFAULT_LIVE_SECONDS = 10
DEFAULT_PHONE_STREAM_URL = "http://192.168.190.191:8080/video"


def load_settings(path: Path = CONFIG_PATH) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as file:
        settings = yaml.safe_load(file) or {}

    settings["root_dir"] = str(ROOT_DIR)
    return settings


def format_seconds(seconds: float) -> str:
    return str(int(seconds)) if seconds.is_integer() else f"{seconds:.1f}"


def info_dir(output_dir: Path) -> Path:
    return output_dir / "info"


def encode_text(value: str) -> str:
    return value.replace(" ", "_").replace(":", "_")


def decode_url(value: str) -> str:
    for scheme in ("http", "https", "rtsp", "rtmp"):
        prefix = f"{scheme}_//"
        if value.startswith(prefix):
            restored = f"{scheme}://{value[len(prefix):]}"
            return re.sub(r"_(\d+)(/|$)", r":\1\2", restored, count=1)
    return value


def extract_setting_value(text: str, label: str) -> str | None:
    normalized = text.strip()
    legacy_marker = f"{label} :"
    if legacy_marker in normalized:
        return normalized.split(legacy_marker, maxsplit=1)[1].strip().strip("[]")

    marker = f"{encode_text(label)}_"
    if normalized.startswith(marker):
        return normalized[len(marker) :].strip()
    return None


def write_info_setting(file_name: str, label: str, value: str) -> None:
    text = f"{encode_text(label)}_{encode_text(str(value))}\n"
    target_dir = info_dir(DEFAULT_TEST_VIDEO_DIR)
    target_dir.mkdir(parents=True, exist_ok=True)
    (target_dir / file_name).write_text(text, encoding="utf-8")


def write_default_info_files() -> None:
    write_info_setting(SAVE_VIDEO_FOLDER_FILE, "save video folder path", str(DEFAULT_TEST_VIDEO_DIR))
    write_info_setting(LIVE_TEST_LOG_SAVE_FILE, "live test log save path", str(TEST_LOG_DIR))


def load_saved_url(folder: Path) -> str:
    file_path = folder / PHONE_STREAM_URL_FILE
    if not file_path.exists():
        return DEFAULT_PHONE_STREAM_URL

    value = extract_setting_value(file_path.read_text(encoding="utf-8", errors="ignore"), "phone stream URL")
    return decode_url(value) if value else DEFAULT_PHONE_STREAM_URL


def write_url_setting(folder: Path, url: str) -> None:
    folder.mkdir(parents=True, exist_ok=True)
    text = f"{encode_text('phone stream URL')}_{encode_text(url)}\n"
    (folder / PHONE_STREAM_URL_FILE).write_text(text, encoding="utf-8")


class VideoInspectionApp:
    def __init__(self, root: tk.Tk) -> None:
        ensure_vision_dirs()
        write_default_info_files()
        self.root = root
        self.settings = load_settings()
        model_settings = self.settings.get("model", {})
        judge_settings = self.settings.get("judge", {})

        self.video_path: Path | None = None
        self.output_dir = DEFAULT_TEST_VIDEO_DIR
        self.log_dir = TEST_LOG_DIR
        self.operation_log_dir = OPERATION_DIR
        self.live_test_running = False
        extract_settings = self.settings.get("extract", {})

        self.video_var = tk.StringVar(value="No video selected")
        self.stream_url_var = tk.StringVar(value=load_saved_url(self.operation_log_dir))
        self.live_seconds_var = tk.DoubleVar(value=self.load_live_seconds(self.output_dir))
        self.status_var = tk.StringVar(value="Select a video and tune parameters.")
        self.split_var = tk.StringVar(value=extract_settings.get("auto_split", "train"))
        self.every_n_frames_var = tk.IntVar(value=int(extract_settings.get("every_n_frames", 10)))
        self.quality_var = tk.IntVar(value=int(extract_settings.get("jpeg_quality", 95)))
        self.confidence_var = tk.DoubleVar(value=float(model_settings.get("confidence", 0.35)))
        self.iou_var = tk.DoubleVar(value=float(model_settings.get("iou", 0.45)))
        self.image_size_var = tk.IntVar(value=int(model_settings.get("image_size", 640)))
        self.min_confidence_var = tk.DoubleVar(value=float(judge_settings.get("min_confidence", 0.35)))
        self.display_width_var = tk.IntVar(value=1600)
        self.display_height_var = tk.IntVar(value=900)

        self.root.title("Video Inspection")
        self.root.geometry("700x590")
        self.root.resizable(False, False)
        self._build()

    def _build(self) -> None:
        frame = tk.Frame(self.root, padx=18, pady=18)
        frame.pack(fill="both", expand=True)

        tk.Label(frame, text="Video Inspection", font=("Segoe UI", 16, "bold")).pack(anchor="w")
        tk.Label(frame, textvariable=self.video_var, font=("Segoe UI", 10), fg="#555", wraplength=700).pack(
            anchor="w",
            pady=(8, 4),
        )
        dataset_button_frame = tk.Frame(frame)
        dataset_button_frame.pack(anchor="w", pady=(4, 0))
        tk.Button(dataset_button_frame, text="select video", width=15, height=2, command=self.select_video).pack(
            side="left",
        )
        tk.Button(
            dataset_button_frame,
            text="extract frames",
            width=15,
            height=2,
            command=self.extract_selected_video,
        ).pack(side="left", padx=(6, 0))
        tk.Button(dataset_button_frame, text="open labeler", width=15, height=2, command=self.open_labeler).pack(
            side="left",
            padx=(6, 0),
        )
        tk.Button(
            dataset_button_frame,
            text="label revision",
            width=15,
            height=2,
            command=self.open_label_revision_picker,
        ).pack(side="left", padx=(6, 0))

        workflow_frame = tk.Frame(frame)
        workflow_frame.pack(anchor="w", pady=(10, 0))
        tk.Button(workflow_frame, text="val 20% split", width=15, height=2, command=self.split_validation_data).pack(
            side="left",
        )
        tk.Button(workflow_frame, text="train", width=15, height=2, command=self.train_model).pack(
            side="left",
            padx=(6, 16),
        )
        tk.Label(workflow_frame, text="split").pack(side="left")
        tk.OptionMenu(workflow_frame, self.split_var, "train", "val", "test").pack(side="left", padx=(4, 14))
        tk.Label(workflow_frame, text="every frames").pack(side="left")
        tk.Spinbox(workflow_frame, from_=1, to=300, width=7, textvariable=self.every_n_frames_var).pack(
            side="left",
            padx=(4, 14),
        )
        tk.Label(workflow_frame, text="jpeg quality").pack(side="left")
        tk.Spinbox(workflow_frame, from_=1, to=100, width=7, textvariable=self.quality_var).pack(
            side="left",
            padx=(4, 0),
        )

        test_button_frame = tk.Frame(frame)
        test_button_frame.pack(anchor="w", pady=(10, 0))
        tk.Button(test_button_frame, text="test and save", width=15, height=2, command=self.test_and_save).pack(
            side="left",
        )
        self.live_test_button = tk.Button(
            test_button_frame,
            text="live test",
            width=15,
            height=2,
            command=self.test_phone_stream,
        )
        self.live_test_button.pack(
            side="left",
            padx=(6, 0),
        )
        tk.Button(
            test_button_frame,
            text="video revision",
            width=15,
            height=2,
            command=self.revision_from_test_video,
        ).pack(side="left", padx=(6, 0))

        stream_frame = tk.Frame(frame)
        stream_frame.pack(anchor="w", fill="x", pady=(14, 4))
        tk.Label(stream_frame, text="phone stream URL", width=16, anchor="w").pack(side="left")
        tk.Entry(stream_frame, textvariable=self.stream_url_var, width=39).pack(side="left", fill="x", expand=True)
        tk.Button(stream_frame, text="save", width=10, command=self.save_phone_stream_url).pack(
            side="left",
            padx=(8, 0),
        )

        live_frame = tk.Frame(frame)
        live_frame.pack(anchor="w", fill="x", pady=(0, 4))
        tk.Label(live_frame, text="live second", width=16, anchor="w").pack(side="left")
        tk.Spinbox(live_frame, from_=0.5, to=3600, increment=0.5, width=10, textvariable=self.live_seconds_var).pack(
            side="left",
        )
        tk.Button(live_frame, text="save", width=10, command=self.save_live_seconds).pack(side="left", padx=(8, 0))

        option_frame = tk.Frame(frame)
        option_frame.pack(anchor="w", pady=(10, 0))

        self._add_spin(option_frame, "confidence", self.confidence_var, 0.01, 0.99, 0, 0, increment=0.01)
        self._add_spin(option_frame, "image size", self.image_size_var, 320, 1920, 0, 2, increment=32)
        self._add_spin(option_frame, "iou", self.iou_var, 0.01, 0.99, 1, 0, increment=0.01)
        self._add_spin(option_frame, "display width", self.display_width_var, 640, 3840, 1, 2, increment=32)
        self._add_spin(option_frame, "min NG conf", self.min_confidence_var, 0.01, 0.99, 2, 0, increment=0.01)
        self._add_spin(option_frame, "display height", self.display_height_var, 480, 2160, 2, 2, increment=32)

        tk.Label(
            frame,
            text="Result preview keeps original aspect ratio with black padding. Press q or ESC in the video window to stop.",
            font=("Segoe UI", 10),
            fg="#555",
            wraplength=700,
        ).pack(anchor="w", pady=(18, 0))
        tk.Label(frame, textvariable=self.status_var, font=("Segoe UI", 10), fg="#555").pack(anchor="w", pady=(12, 0))

    def _add_spin(
        self,
        parent: tk.Frame,
        label: str,
        variable: tk.Variable,
        from_value: float | int,
        to_value: float | int,
        row: int,
        column: int,
        increment: float | int,
    ) -> None:
        tk.Label(parent, text=label, width=13, anchor="w").grid(row=row, column=column, sticky="w", pady=4)
        tk.Spinbox(parent, from_=from_value, to=to_value, increment=increment, width=10, textvariable=variable).grid(
            row=row,
            column=column + 1,
            sticky="w",
            padx=(4, 22 if column == 0 else 0),
            pady=4,
        )

    def select_video(self) -> None:
        file_path = filedialog.askopenfilename(
            title="Select inspection video",
            filetypes=[
                ("Video files", "*.mp4 *.mov *.m4v *.avi *.mkv"),
                ("All files", "*.*"),
            ],
        )
        if not file_path:
            return

        self.video_path = Path(file_path)
        self.video_var.set(f"Video: {self.video_path}")
        self.status_var.set("Ready to test.")

    def save_phone_stream_url(self) -> None:
        write_url_setting(self.operation_log_dir, self.stream_url_var.get().strip())
        self.status_var.set("Saved phone stream URL.")

    def live_seconds_file(self, output_dir: Path | None = None) -> Path:
        return info_dir(output_dir or self.output_dir) / LIVE_SECONDS_FILE

    def load_live_seconds(self, output_dir: Path) -> float:
        file_path = info_dir(output_dir) / LIVE_SECONDS_FILE
        legacy_path = output_dir / LIVE_SECONDS_FILE
        if not file_path.exists() and legacy_path.exists():
            file_path = legacy_path
        if not file_path.exists():
            return DEFAULT_LIVE_SECONDS

        text = file_path.read_text(encoding="utf-8", errors="ignore")
        value = extract_setting_value(text, "live second")
        if not value:
            return DEFAULT_LIVE_SECONDS

        try:
            return max(0.5, float(value))
        except (IndexError, ValueError):
            return DEFAULT_LIVE_SECONDS

    def save_live_seconds(self) -> None:
        output_dir = self.output_dir
        info_dir(output_dir).mkdir(parents=True, exist_ok=True)
        seconds = max(0.5, float(self.live_seconds_var.get()))
        self.live_seconds_file(output_dir).write_text(
            f"{encode_text('live second')}_{format_seconds(seconds)}\n",
            encoding="utf-8",
        )
        self.status_var.set(f"Saved live second: {format_seconds(seconds)}")

    def test_and_save(self) -> None:
        if self.video_path is None:
            messagebox.showwarning("No video", "Please select a video first.")
            return

        self.status_var.set("Inspection started. Result video is being saved...")
        threading.Thread(target=self._inspect_worker, args=(self.video_path,), daemon=True).start()

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
                every_n_frames=max(1, int(self.every_n_frames_var.get())),
                every_seconds=None,
                start_frame=0,
                max_frames=None,
                resize=None,
                quality=max(1, min(100, int(self.quality_var.get()))),
            )
            count = extract_frames(args)
            split = self.split_var.get()
            self.root.after(
                0,
                lambda: self.status_var.set(f"Extracted {count} images to dataset/images/{split}."),
            )
        except Exception as exc:
            error = str(exc)
            self.root.after(0, lambda: messagebox.showerror("Frame extraction failed", error))
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

    def open_label_revision_picker(self) -> None:
        split = self.split_var.get()
        image_dir = ROOT_DIR / "dataset" / "images" / split
        groups = find_frame_groups(image_dir)
        if not groups:
            messagebox.showwarning("No frame groups", f"No frame groups found in:\n{image_dir}")
            return

        picker = tk.Toplevel(self.root)
        picker.title("Select label revision group")
        picker.geometry("620x420")
        picker.resizable(False, False)
        picker.transient(self.root)
        picker.grab_set()

        tk.Label(
            picker,
            text=f"Label revision groups in {split}",
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
            self.open_label_revision_group(prefix, groups[prefix])

        def delete_selected() -> None:
            selection = listbox.curselection()
            if not selection:
                return

            index = selection[0]
            prefix = prefixes[index]
            image_paths = groups[prefix]
            proceed = messagebox.askyesno(
                "Delete label group?",
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

    def open_label_revision_group(self, prefix: str, image_paths: list[Path]) -> None:
        split = self.split_var.get()
        image_dir = ROOT_DIR / "dataset" / "images" / split
        label_dir = ROOT_DIR / "dataset" / "labels" / split

        try:
            classes = load_dataset_classes(DATASET_YAML_PATH)
            self.status_var.set(f"Label revision: {prefix} ({len(image_paths)} frames)")
            YoloBoxLabeler(image_dir, label_dir, classes, image_paths=image_paths).run()
            self.status_var.set(f"Label revision saved: {prefix}. Train again to update weights/best.pt.")
        except Exception as exc:
            messagebox.showerror("Label revision failed", str(exc))
            self.status_var.set("Label revision failed.")

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

    def test_phone_stream(self) -> None:
        if self.live_test_running:
            self.status_var.set("Live test is already running.")
            return

        stream_url = self.stream_url_var.get().strip()
        if not stream_url:
            messagebox.showwarning("No stream URL", "Please enter the phone stream URL first.")
            return

        write_url_setting(self.operation_log_dir, stream_url)
        live_seconds = max(0.5, float(self.live_seconds_var.get()))
        self.live_test_running = True
        self.live_test_button.config(state="disabled")
        self.status_var.set(f"Connecting to phone stream for {format_seconds(live_seconds)} seconds...")
        threading.Thread(target=self._inspect_worker, args=(stream_url, live_seconds), daemon=True).start()

    def revision_from_test_video(self) -> None:
        initial_dir = self.output_dir
        file_path = filedialog.askopenfilename(
            title="Select test video for revision",
            initialdir=str(initial_dir if initial_dir.exists() else DEFAULT_TEST_VIDEO_DIR),
            filetypes=[
                ("Video files", "*.mp4 *.mov *.m4v *.avi *.mkv"),
                ("All files", "*.*"),
            ],
        )
        if not file_path:
            return

        video_path = Path(file_path)
        if "_test" in video_path.stem:
            proceed = messagebox.askyesno(
                "Annotated video selected",
                "This looks like a saved test result video. It may contain red boxes/text.\n\n"
                "For best training quality, use the original unannotated video when possible.\n\n"
                "Continue anyway?",
            )
            if not proceed:
                return

        self.status_var.set("Extracting revision frames...")
        threading.Thread(target=self._revision_worker, args=(video_path,), daemon=True).start()

    def _settings_from_ui(self) -> dict[str, Any]:
        settings = copy.deepcopy(self.settings)
        settings.setdefault("model", {})["confidence"] = float(self.confidence_var.get())
        settings.setdefault("model", {})["iou"] = float(self.iou_var.get())
        settings.setdefault("model", {})["image_size"] = int(self.image_size_var.get())
        settings.setdefault("judge", {})["min_confidence"] = float(self.min_confidence_var.get())
        return settings

    def _inspect_worker(self, source: Path | str, max_seconds: float | None = None) -> None:
        is_live_test = max_seconds is not None
        if is_live_test:
            self.write_operation_log("live_test_start")

        try:
            output_path = self.output_dir / f"{source_stem(str(source))}_test.mp4"
            saved_path = inspect_video(
                video_path=source,
                settings=self._settings_from_ui(),
                output_path=output_path,
                display_width=int(self.display_width_var.get()),
                display_height=int(self.display_height_var.get()),
                show_window=True,
                max_seconds=max_seconds,
                status_suffix=max_seconds is not None,
                log_dir=self.log_dir if max_seconds is not None else None,
            )
            self.root.after(0, lambda: self.status_var.set(f"Saved: {saved_path}"))
        except Exception as exc:
            error = str(exc)
            self.root.after(0, lambda: messagebox.showerror("Inspection failed", error))
            self.root.after(0, lambda: self.status_var.set("Inspection failed."))
        finally:
            if is_live_test:
                self.write_operation_log("live_test_finish")
                self.root.after(0, self._finish_live_test)

    def _finish_live_test(self) -> None:
        self.live_test_running = False
        self.live_test_button.config(state="normal")

    def write_operation_log(self, event_name: str) -> None:
        now = datetime.now()
        self.operation_log_dir.mkdir(parents=True, exist_ok=True)
        log_path = self.operation_log_dir / f"{now:%Y-%m-%d}_live_test_operation_log.txt"
        with log_path.open("a", encoding="utf-8") as file:
            file.write(f"{now:%H:%M:%S}_{event_name}\n")

    def _revision_worker(self, video_path: Path) -> None:
        try:
            extract_settings = self.settings.get("extract", {})
            prefix = f"revision_{video_path.stem}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            image_dir = ROOT_DIR / "dataset" / "images" / "train"
            label_dir = ROOT_DIR / "dataset" / "labels" / "train"
            args = SimpleNamespace(
                video=video_path,
                split="train",
                output_dir=image_dir,
                prefix=prefix,
                every_n_frames=int(extract_settings.get("every_n_frames", 10)),
                every_seconds=None,
                start_frame=0,
                max_frames=None,
                resize=None,
                quality=int(extract_settings.get("jpeg_quality", 95)),
            )
            count = extract_frames(args)
            image_paths = sorted(image_dir.glob(f"{prefix}_*.jpg"))
            if not image_paths:
                raise RuntimeError(f"No revision frames were extracted from: {video_path}")

            self.root.after(0, lambda: self.status_var.set(f"Label revision frames: {count} images"))
            classes = load_dataset_classes(DATASET_YAML_PATH)
            YoloBoxLabeler(image_dir, label_dir, classes, image_paths=image_paths).run()

            self.root.after(0, lambda: self.status_var.set("Revision labels saved."))
            self.root.after(0, self._ask_train_after_revision)
        except Exception as exc:
            error = str(exc)
            self.root.after(0, lambda: messagebox.showerror("Revision failed", error))
            self.root.after(0, lambda: self.status_var.set("Revision failed."))

    def _ask_train_after_revision(self) -> None:
        should_train = messagebox.askyesno(
            "Train updated model?",
            "Revision labels were saved.\n\nTrain again now to update weights/best.pt?",
        )
        if should_train:
            self.status_var.set("Training started. This can take a while...")
            threading.Thread(target=self._train_worker, daemon=True).start()
        else:
            self.status_var.set("Revision saved. Train later to update weights/best.pt.")

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
            self.root.after(0, lambda: self.status_var.set("Training finished. weights/best.pt updated."))
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


def main() -> None:
    parser = argparse.ArgumentParser(description="Video inspection GUI")
    parser.parse_args()

    root = tk.Tk()
    VideoInspectionApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
