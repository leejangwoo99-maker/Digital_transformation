from __future__ import annotations

import argparse
import contextlib
import copy
import math
import random
import re
import shutil
import sys
import threading
import tkinter as tk
from datetime import datetime
from pathlib import Path
from tkinter import filedialog, messagebox, simpledialog
from types import SimpleNamespace
from typing import Any

import yaml

from extract_frames import extract_frames
from label_images import DATASET_YAML_PATH, YoloBoxLabeler, load_dataset_classes
from main import DEFAULT_TEST_VIDEO_DIR, OPERATION_DIR, ROOT_DIR, TEST_LOG_DIR, ensure_vision_dirs, inspect_video, source_stem, centered_preview_geometry, unique_path
from train import train
from video_dataset_builder import delete_frame_group, find_frame_groups, frame_number
from inspection.special_checks import annotation_dir, group_key, labeled_records, load_reference, load_time, save_time, load_manual_exceptions, conn_crop_bounds, pixel_box, write_json
from special_labeler import SpecialLabeler
from inspection.region_masks import mask_regions, filter_training_scratches


CONFIG_PATH = ROOT_DIR / "config" / "settings.yaml"
LIVE_SECONDS_FILE = "live_seconds.txt"
SAVE_VIDEO_FOLDER_FILE = "save_video_folder.txt"
LIVE_TEST_LOG_SAVE_FILE = "live_test_log_save.txt"
PHONE_STREAM_URL_FILE = "phone_stream_url.txt"
TEST_CONDITION_FILE = "test_condition.txt"
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


def test_condition_path(output_dir, group):
    return info_dir(output_dir) / f"{group_key(group)}_test_condition.txt"


def load_test_conditions(output_dir: Path, group=None) -> dict[str, float | str]:
    if not group:
        return {}
    file_path = test_condition_path(output_dir,group)
    if not file_path.exists():
        return {}

    conditions: dict[str, float | str] = {}
    for line in file_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", maxsplit=1)
        key = key.strip()
        value = value.strip()
        try:
            conditions[key] = float(value)
        except ValueError:
            conditions[key] = value
    return conditions


def saved_condition_number(
    conditions: dict[str, float | str],
    key: str,
    default: float | int,
) -> float:
    value = conditions.get(key)
    if value is None:
        return float(default)
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def saved_condition_text(conditions: dict[str, float | str], key: str, default: str) -> str:
    value = conditions.get(key)
    return str(value) if value not in (None, "") else default


class TkTrainLogWriter:
    def __init__(self, app: "VideoInspectionApp") -> None:
        self.app = app

    def write(self, text: str) -> int:
        if text:
            self.app.append_train_log(text)
        return len(text)

    def flush(self) -> None:
        pass


class VideoInspectionApp:
    def __init__(self, root: tk.Tk) -> None:
        ensure_vision_dirs()
        write_default_info_files()
        self.root = root
        self.settings = load_settings()
        model_settings = self.settings.get("model", {})
        judge_settings = self.settings.get("judge", {})

        self.video_path: Path | None = None
        self.active_image_dir: Path | None = None
        self.active_label_dir: Path | None = None
        self.active_image_paths: list[Path] | None = None
        self.active_group_name: str | None = None
        self.active_algorithm_name: str | None = None
        self.active_algorithm_weights: Path | None = None
        self.active_split = "train"
        self.output_dir = DEFAULT_TEST_VIDEO_DIR
        self.log_dir = TEST_LOG_DIR
        self.operation_log_dir = OPERATION_DIR
        self.live_test_running = False
        self.extraction_running = False
        extract_settings = self.settings.get("extract", {})
        saved_conditions = load_test_conditions(self.output_dir)

        self.video_var = tk.StringVar(value="No video selected")
        self.status_var = tk.StringVar(value="Select a video and tune parameters.")
        self.target_fps_var = tk.DoubleVar(value=23.0)
        self.quality_var = tk.IntVar(value=100)
        self.iou_var = tk.DoubleVar(value=45)
        self.min_confidence_var = tk.DoubleVar(
            value=saved_condition_number(
                saved_conditions,
                "threshold_percent",
                50,
            )
        )
        self.test_fps_var = tk.StringVar(value='')

        self.root.title("Video Inspection")
        self.root.geometry("1100x560")
        self.root.minsize(1100, 560)
        self.root.resizable(True, True)
        self._build()

    def _build(self) -> None:
        container = tk.Frame(self.root)
        container.pack(fill="both", expand=True)

        frame = tk.Frame(container, padx=18, pady=18, width=680)
        frame.pack(side="left", fill="y")
        frame.pack_propagate(False)

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
        self.extract_button = tk.Button(
            dataset_button_frame,
            text="extract frames",
            width=15,
            height=2,
            command=self.extract_selected_video,
        )
        self.extract_button.pack(side="left", padx=(6, 0))
        tk.Label(dataset_button_frame, text="fps").pack(side="left", padx=(10, 2))
        tk.Entry(dataset_button_frame, textvariable=self.target_fps_var, width=7).pack(side="left")
        tk.Button(dataset_button_frame, text="select frames", width=15, height=2, command=self.select_frames).pack(
            side="left",
            padx=(6, 0),
        )
        tk.Button(dataset_button_frame, text="open labeler", width=15, height=2, command=self.open_labeler).pack(
            side="left",
            padx=(6, 0),
        )

        workflow_frame = tk.Frame(frame)
        workflow_frame.pack(anchor="w", pady=(10, 0))
        tk.Button(workflow_frame, text="OCR", width=15, height=2, command=lambda: self.open_special_labeler("OCR")).pack(
            side="left",
        )
        tk.Button(workflow_frame, text="Conn", width=15, height=2, command=lambda: self.open_special_labeler("conn")).pack(
            side="left",
            padx=(6, 0),
        )
        tk.Button(workflow_frame, text="label", width=15, height=2, command=lambda: self.open_special_labeler("label")).pack(
            side="left",
            padx=(6, 0),
        )
        tk.Button(
            workflow_frame,
            text="label revision",
            width=15,
            height=2,
            command=self.open_label_revision_picker,
        ).pack(side="left", padx=(6, 0))
        tk.Button(workflow_frame, text="train", width=15, height=2, command=self.train_model).pack(
            side="left",
            padx=(6, 0),
        )

        test_button_frame = tk.Frame(frame)
        test_button_frame.pack(anchor="w", pady=(10, 0))
        self.live_test_button = tk.Button(
            test_button_frame,
            text="test",
            width=15,
            height=2,
            command=self.test_selected_file,
        )
        self.live_test_button.pack(
            side="left",
            padx=(6, 0),
        )
        tk.Button(test_button_frame,text="review threshold",width=15,height=2,
                  command=self.review_threshold).pack(side="left",padx=(6,0))
        self.phone_live_test_button = tk.Button(test_button_frame, text="live test", width=15, height=2,
                  command=lambda: self.open_phone_camera(True))
        self.phone_live_test_button.pack(side="left", padx=(6, 0))

        option_frame = tk.Frame(frame)
        option_frame.pack(anchor="w", pady=(10, 0))

        self._add_spin(option_frame, "threshold %", self.min_confidence_var, 1, 99, 0, 0, increment=1)
        tk.Label(option_frame,text='Test fps').grid(row=0,column=2,padx=(8,2))
        tk.Spinbox(option_frame,from_=0.1,to=240,increment=1,format='%.1f',width=6,textvariable=self.test_fps_var).grid(row=0,column=3)
        self.compare_openvino_var = tk.BooleanVar(value=False)
        self.test_fps_var.set('')
        tk.Button(option_frame, text="save", width=10, command=self.save_test_conditions).grid(
            row=2,
            column=1,
            sticky="e",
            pady=(8, 0),
        )

        tk.Label(
            frame,
            text="Result preview keeps original aspect ratio with black padding. Press q or ESC in the video window to stop.",
            font=("Segoe UI", 10),
            fg="#555",
            wraplength=700,
        ).pack(anchor="w", pady=(18, 0))
        tk.Label(frame, textvariable=self.status_var, font=("Segoe UI", 10), fg="#555").pack(anchor="w", pady=(12, 0))
        self.pipeline_time_var = tk.StringVar(value='pipeline time(sec) : 0.0000000')
        self.pipeline_date_var = tk.StringVar(value='pipeline date : -')
        self.batch_progress_var = tk.StringVar(value='')
        clock_row = tk.Frame(frame)
        clock_row.pack(fill='x')
        tk.Label(clock_row,textvariable=self.pipeline_time_var,anchor='w').pack(side='left')
        tk.Label(clock_row,textvariable=self.batch_progress_var,anchor='w').pack(side='left',padx=(12,0))
        tk.Label(frame,textvariable=self.pipeline_date_var,anchor='w').pack(fill='x')
        tk.Button(frame, text="recording by phone", width=18, height=2,
                  command=self.open_phone_camera).pack(anchor="w", pady=(8, 0))

        log_frame = tk.Frame(container, pady=18)
        log_frame.pack(side="left", fill="both", expand=True, padx=(0, 18))
        tk.Label(log_frame, text="Training Process", font=("Segoe UI", 13, "bold")).pack(anchor="w")
        log_box_frame = tk.Frame(log_frame)
        log_box_frame.pack(fill="both", expand=True, pady=(8, 0))
        self.train_log_text = tk.Text(
            log_box_frame,
            width=48,
            height=28,
            wrap="word",
            state="disabled",
            font=("Consolas", 9),
        )
        log_scroll = tk.Scrollbar(log_box_frame, orient="vertical", command=self.train_log_text.yview)
        self.train_log_text.configure(yscrollcommand=log_scroll.set)
        self.train_log_text.pack(side="left", fill="both", expand=True)
        log_scroll.pack(side="right", fill="y")

    def open_phone_camera(self, live=False):
        if live and getattr(self,'_runtime_preparing',False):
            self.status_var.set('Preparing selected frames; testing is available when Ready.')
            return
        if live and not self.check_parallel_resources(force=True):
            return
        from camera.phone_control_gui import PhoneControlWindow
        if live:
            from camera.live_settings import resolve_live_settings
            try:
                resolve_live_settings(self)
            except Exception as exc:
                messagebox.showerror("Live test", str(exc), parent=self.root)
                return
        name = "phone_live_control" if live else "phone_control"
        other = getattr(self, "phone_control" if live else "phone_live_control", None)
        if other is not None and other.window.winfo_exists():
            if other.busy or getattr(other, "connecting", False):
                messagebox.showwarning("USB Camera", "Close the other camera window before continuing.")
                return
            other.close()
        control = getattr(self, name, None)
        if control is None or not control.window.winfo_exists():
            control = PhoneControlWindow(self, live_mode=live)
            setattr(self, name, control)
        control.window.lift()

    def review_threshold(self):
        folder = filedialog.askdirectory(title="Select group threshold result folder",initialdir=str(self.output_dir),parent=self.root)
        if not folder:
            return
        try:
            from inspection.threshold_review import ThresholdReviewWindow
            view = ThresholdReviewWindow(self.root,folder)
            if len(view.cpk_report['runs']) < 2:
                messagebox.showwarning('Cpk','계산 불가: 반복 데이터가 1회뿐입니다.',parent=view.window)
        except (ValueError,OSError,KeyError,TypeError) as exc:
            messagebox.showerror("Review threshold",str(exc),parent=self.root)

    def trained_frame_groups(self):
        groups = find_frame_groups(ROOT_DIR / "dataset" / "images" / "train")
        return {name: paths for name, paths in groups.items()
                if (ROOT_DIR / "weights" / f"{self._algorithm_name_from_group(name)}.pt").exists()
                or any(load_reference(ROOT_DIR, name, kind) for kind in ("OCR", "conn", "label"))}

    def settings_for_frame_group(self, group, paths):
        selected = copy.copy(self)
        selected.active_group_name = group
        selected.active_image_paths = paths
        selected.active_image_dir = ROOT_DIR / "dataset" / "images" / "train"
        selected.active_label_dir = ROOT_DIR / "dataset" / "labels" / "train"
        selected.active_algorithm_name = self._algorithm_name_from_group(group)
        selected.active_algorithm_weights = ROOT_DIR / "weights" / f"{selected.active_algorithm_name}.pt"
        if group != self.active_group_name:
            from types import SimpleNamespace
            from inspection.frame_conditions import group_fps
            conditions = load_test_conditions(self.output_dir,group)
            selected.test_fps_var = SimpleNamespace(get=lambda:saved_condition_number(conditions,'test_fps',group_fps(group)))
        settings = selected._settings_from_ui()
        if group != self.active_group_name:
            conditions = load_test_conditions(self.output_dir,group)
            threshold = saved_condition_number(conditions,"threshold_percent",50)
            settings["model"]["confidence"] = threshold/100
            settings["judge"]["min_confidence"] = threshold/100
            settings["model"]["iou"] = .45
            settings['test_fps'] = saved_condition_number(conditions,'test_fps',settings['group_fps'])
            settings["result_directory"] = str(self.output_dir / f"{group_key(group)}_threshold_{threshold:g}")
        return settings

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

    def clear_train_log(self) -> None:
        self.train_log_text.configure(state="normal")
        self.train_log_text.delete("1.0", tk.END)
        self.train_log_text.configure(state="disabled")

    def append_train_log(self, text: str) -> None:
        self.root.after(0, lambda: self._append_train_log_on_ui(text))

    def _append_train_log_on_ui(self, text: str) -> None:
        self.train_log_text.configure(state="normal")
        self.train_log_text.insert(tk.END, text)
        self.train_log_text.see(tk.END)
        self.train_log_text.configure(state="disabled")

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

    def select_frames(self) -> None:
        image_dir = ROOT_DIR / "dataset" / "images" / "train"
        label_dir = ROOT_DIR / "dataset" / "labels" / "train"
        groups = find_frame_groups(image_dir)
        if not groups:
            messagebox.showwarning("No frame groups", f"No frame groups found in:\n{image_dir}")
            return

        picker = tk.Toplevel(self.root)
        picker.title("Select extracted frame group")
        picker.geometry("620x420")
        picker.resizable(False, False)
        picker.transient(self.root)
        picker.grab_set()

        tk.Label(
            picker,
            text="Extracted frame groups in train",
            font=("Segoe UI", 13, "bold"),
        ).pack(anchor="w", padx=14, pady=(14, 6))

        listbox = tk.Listbox(picker, font=("Consolas", 10), height=14, width=86)
        listbox.config(selectmode=tk.EXTENDED)
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

        def use_selected() -> None:
            selection = listbox.curselection()
            if not selection:
                return
            if len(selection) != 1:
                messagebox.showwarning("Select one group", "Select one FPS frame group for training/testing.", parent=picker)
                return
            from inspection.frame_conditions import group_fps
            try:
                selected_fps = group_fps(prefixes[selection[0]])
            except ValueError as exc:
                messagebox.showwarning("FPS group required", str(exc), parent=picker)
                return

            selected_paths: list[Path] = []
            selected_names: list[str] = []
            for selected_index in selection:
                prefix = prefixes[selected_index]
                selected_names.append(prefix)
                selected_paths.extend(groups[prefix])

            selected_paths = sorted(selected_paths, key=lambda path: (frame_number(path), path.name))
            group_name = ", ".join(selected_names[:3])
            if len(selected_names) > 3:
                group_name += f" +{len(selected_names) - 3}"

            self.active_image_dir = image_dir
            self.active_label_dir = label_dir
            self.active_image_paths = selected_paths
            self.active_group_name = group_name
            self.load_group_test_conditions()
            self.target_fps_var.set(selected_fps)
            self.active_algorithm_name = self._algorithm_name_from_group(group_name)
            self.active_algorithm_weights = ROOT_DIR / "weights" / f"{self.active_algorithm_name}.pt"
            self.active_split = "train"
            self.video_var.set(f"Frame group: {group_name}")
            self.status_var.set(
                f"Selected {len(selected_paths)} frames. Algorithm: {self.active_algorithm_weights.name}"
            )
            picker.destroy()
            self.prepare_selected_runtime()

        tk.Button(button_row, text="select", width=14, command=use_selected).pack(side="left", padx=(0, 8))
        tk.Button(button_row, text="cancel", width=12, command=picker.destroy).pack(side="left")
        listbox.bind("<Double-Button-1>", lambda event: use_selected())

    def prepare_selected_runtime(self):
        if self.live_test_running or not self.active_group_name:
            return
        try:
            settings = self._settings_from_ui()
        except Exception:
            return
        group = self.active_group_name
        generation = object()
        self._runtime_preparation_generation = generation
        self._runtime_preparing = True
        self.live_test_button.config(state='disabled')
        if hasattr(self,'phone_live_test_button'):
            self.phone_live_test_button.config(state='disabled')
        settings['runtime_calibration'] = not settings.get('compare_openvino',False)
        if settings.get('compare_openvino'):
            settings['openvino_comparison_profile'] = 'jobs2_threads2'
        self.status_var.set('Preparing models and automatic CPU settings...')
        def work():
            special = None
            try:
                from inspection.automatic_runtime import prepare
                _,special = prepare(settings)
                message = f'Ready: {group}'
            except Exception as exc:
                message = f'Preparation deferred: {exc}'
            finally:
                if special is not None and getattr(special,'mark_model',None) and not getattr(special,'cached_mark_runtime',False):
                    special.mark_model.close()
            try:
                def finish():
                    if getattr(self,'_runtime_preparation_generation',None) is not generation:
                        return
                    self._runtime_preparing = False
                    if not self.live_test_running:
                        self.live_test_button.config(state='normal')
                        if hasattr(self,'phone_live_test_button'):
                            self.phone_live_test_button.config(state='normal')
                        if self.active_group_name == group:
                            self.status_var.set(message)
                self.root.after(0,finish)
            except (tk.TclError,RuntimeError):
                pass
        threading.Thread(target=work,daemon=True).start()

    def _label_dir_for_image_dir(self, image_dir: Path) -> Path:
        if image_dir.name in {"train", "val", "test"} and image_dir.parent.name == "images":
            return ROOT_DIR / "dataset" / "labels" / image_dir.name
        return image_dir.parent / f"{image_dir.name}_labels"

    def _active_label_paths(self) -> tuple[Path, Path]:
        image_dir = self.active_image_dir or ROOT_DIR / "dataset" / "images" / "train"
        label_dir = self.active_label_dir or self._label_dir_for_image_dir(image_dir)
        return image_dir, label_dir

    def _algorithm_name_from_group(self, group_name: str) -> str:
        slug = re.sub(r"[^0-9A-Za-z가-힣_-]+", "_", group_name).strip("_")
        slug = re.sub(r"_+", "_", slug)
        return f"{slug or 'selected_frames'}_algorith"

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

    def save_test_conditions(self, status_message: str | None = None) -> None:
        if not self.active_group_name:
            messagebox.showwarning("Select frames","Select a frame group first.")
            return
        if not self.check_parallel_resources(force=True):
            return
        target_dir = info_dir(self.output_dir)
        target_dir.mkdir(parents=True, exist_ok=True)
        condition_path = test_condition_path(self.output_dir,self.active_group_name)
        lines = [
            f"confidence_percent: {float(self.min_confidence_var.get()):.0f}",
            "iou_percent: 45",
            f"threshold_percent: {float(self.min_confidence_var.get()):.0f}",
        ]
        if hasattr(self,'test_fps_var'):
            try:
                lines.append(f"test_fps: {self._settings_from_ui()['test_fps']:g}")
            except (ValueError,tk.TclError) as exc:
                messagebox.showerror('Test fps',str(exc),parent=self.root)
                return
        condition_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        self.status_var.set(status_message or f"Saved test conditions: {condition_path}")

    def load_group_test_conditions(self):
        conditions = load_test_conditions(self.output_dir,self.active_group_name)
        self.min_confidence_var.set(saved_condition_number(conditions,"threshold_percent",50))
        self.iou_var.set(45)
        if hasattr(self,'test_fps_var'):
            from inspection.frame_conditions import group_fps
            self.test_fps_var.set(saved_condition_number(conditions,'test_fps',group_fps(self.active_group_name) or 0))

    def check_parallel_resources(self,force=False):
        return True

    def test_and_save(self) -> None:
        if self.video_path is None:
            messagebox.showwarning("No video", "Please select a video first.")
            return

        self.status_var.set("Inspection started. Result video is being saved...")
        threading.Thread(
            target=self._inspect_worker,
            args=(self.video_path, None, self._inspection_window_position()),
            daemon=True,
        ).start()

    def extract_selected_video(self) -> None:
        if self.extraction_running:
            return
        if self.video_path is None:
            messagebox.showwarning("No video", "Please select a video first.")
            return
        try:
            target_fps = float(self.target_fps_var.get())
            if not math.isfinite(target_fps) or target_fps <= 0:
                raise ValueError("FPS must be a finite number greater than zero.")
        except (ValueError, tk.TclError) as exc:
            messagebox.showerror("Invalid FPS", str(exc))
            return
        self.extraction_running = True
        self.extract_button.config(state="disabled")
        self.status_var.set("Extracting frames...")
        self.append_train_log("\nFrame extraction started...\n")
        threading.Thread(target=self._extract_worker, args=(self.video_path, target_fps), daemon=True).start()

    def _extract_worker(self, video_path: Path, target_fps: float) -> None:
        try:
            from inspection.frame_conditions import frame_group_name
            import cv2
            capture = cv2.VideoCapture(str(video_path))
            source_fps = capture.get(cv2.CAP_PROP_FPS)
            capture.release()
            if source_fps <= 0 or target_fps > source_fps + 1e-6:
                raise ValueError(f"Extraction FPS must not exceed source FPS ({source_fps:g}).")
            image_dir = ROOT_DIR / "dataset" / "images" / "train"
            prefix = frame_group_name(video_path.stem, target_fps)
            existing_groups = find_frame_groups(image_dir)
            if prefix in existing_groups:
                raise ValueError(f"Group already exists: {prefix}. Delete it in label revision or use a different video name/FPS.")
            self.append_train_log(f"Frame group: {prefix}; requested FPS: {target_fps:g}\n")
            args = SimpleNamespace(
                video=video_path,
                split="train",
                output_dir=image_dir,
                prefix=prefix,
                every_n_frames=1,
                every_seconds=None,
                target_fps=target_fps,
                start_frame=0,
                max_frames=None,
                resize=None,
                quality=100,
            )
            count = extract_frames(args, log=self.append_train_log)
            paths = sorted(image_dir.glob(f"{prefix}_*.jpg"))
            if not count or not paths:
                raise RuntimeError("No frames could be extracted from this video.")
            self.root.after(0, lambda: self._use_extracted_frames(prefix, paths, count))
        except Exception as exc:
            error = str(exc)
            self.append_train_log(f"Frame extraction failed: {error}\n")
            self.root.after(0, lambda: messagebox.showerror("Frame extraction failed", error))
            self.root.after(0, lambda: self.status_var.set("Frame extraction failed."))
        finally:
            self.root.after(0, self._finish_extraction)

    def _finish_extraction(self) -> None:
        self.extraction_running = False
        self.extract_button.config(state="normal")

    def _use_extracted_frames(self, prefix: str, paths: list[Path], count: int) -> None:
        self.preferred_training_scope = 'ALL'
        self.active_image_dir = ROOT_DIR / "dataset" / "images" / "train"
        self.active_label_dir = ROOT_DIR / "dataset" / "labels" / "train"
        self.active_image_paths = paths
        self.active_group_name = prefix
        self.load_group_test_conditions()
        self.active_algorithm_name = self._algorithm_name_from_group(prefix)
        self.active_algorithm_weights = ROOT_DIR / "weights" / f"{self.active_algorithm_name}.pt"
        self.active_split = "train"
        self.video_var.set(f"Frame group: {prefix}")
        self.status_var.set(f"Extracted and selected {count} frames: {prefix}")

    def open_labeler(self) -> None:
        image_dir, label_dir = self._active_label_paths()

        try:
            classes = load_dataset_classes(DATASET_YAML_PATH)
            self.status_var.set(f"Labeling: {image_dir}")
            controller = YoloBoxLabeler(image_dir, label_dir, classes, image_paths=self.active_image_paths)
            controller.proto_train = self._train_proto_model
            controller.run(parent=self.root)
            self.status_var.set(f"Labels saved to: {label_dir}")
        except Exception as exc:
            messagebox.showerror("Labeler failed", str(exc))
            self.status_var.set("Labeler failed.")

    def _train_proto_model(self, source_paths):
        if not self.active_group_name or not self.active_image_paths:
            raise ValueError("Select a frame group first.")
        original_paths = self.active_image_paths
        try:
            self.active_image_paths = list(source_paths)
            data_path,_ = self._build_algorithm_dataset()
        finally:
            self.active_image_paths = original_paths
        target = ROOT_DIR / "weights" / f"{self.active_group_name}_proto.pt"
        args = SimpleNamespace(data=data_path,model=None,epochs=None,imgsz=None,batch=None,device=None,
                               project=None,name=f"{self.active_group_name}_proto",target_weights=target)
        train(args)
        from ultralytics import YOLO
        return YOLO(str(target))

    def _saved_labeler_sampling(self, group, kind):
        from inspection.frame_conditions import group_fps
        from inspection.test_sampling import TestSampling

        if kind not in ('OCR', 'label'):
            return None
        fps = group_fps(group)
        if not fps:
            return None
        conditions = load_test_conditions(self.output_dir, group)
        try:
            return TestSampling(fps, saved_condition_number(conditions, 'test_fps', fps))
        except ValueError:
            return None

    def open_special_labeler(self, label_kind: str) -> None:
        if not self.active_group_name or not self.active_image_paths:
            messagebox.showwarning("Select frames", "Select a frame group first.")
            return
        try:
            SpecialLabeler(self.root, ROOT_DIR, self.active_group_name, label_kind, self.active_image_paths,
                           sampling=self._saved_labeler_sampling(self.active_group_name, label_kind)).run()
            self.status_var.set(f"Saved: {self.active_group_name}_{label_kind}")
        except Exception as exc:
            messagebox.showerror(f"{label_kind} labeler failed", str(exc))
            self.status_var.set(f"{label_kind} labeler failed.")

    def open_label_revision_picker(self) -> None:
        split = self.active_split
        image_dir, label_dir = self._active_label_paths()
        groups = find_frame_groups(image_dir)
        from inspection.group_combine import appearance_group_absorbed
        groups = {name:paths for name,paths in groups.items()
                  if not appearance_group_absorbed(info_dir(self.output_dir),name,paths,label_dir)}
        special_groups = {}
        special_root = ROOT_DIR / "dataset" / "special_labels"
        for group_dir in sorted(special_root.iterdir()) if special_root.exists() else []:
            for kind in ("OCR", "conn", "label"):
                records = labeled_records(ROOT_DIR, group_dir.name, kind)
                paths = [Path(data["image"]) for _, data in records if Path(data["image"]).exists()]
                if paths:
                    name = f"{group_dir.name}_{kind}"
                    special_groups[name] = (group_dir.name, kind)
                    groups[name] = paths
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
        listbox.config(selectmode=tk.EXTENDED)
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
            selected_paths: list[Path] = []
            selected_names: list[str] = []
            for selected_index in selection:
                prefix = prefixes[selected_index]
                selected_names.append(prefix)
                selected_paths.extend(groups[prefix])
            picker.destroy()
            ordinary_paths = []
            for name in selected_names:
                if name in special_groups:
                    group, kind = special_groups[name]
                    SpecialLabeler(self.root, ROOT_DIR, group, kind, groups.get(group, groups[name]),
                                   sampling=self._saved_labeler_sampling(group, kind)).run()
                else:
                    ordinary_paths.extend(groups[name])
            if not ordinary_paths:
                self.status_var.set("Special labels saved.")
                return
            revision_name = ", ".join(selected_names[:3])
            if len(selected_names) > 3:
                revision_name += f" +{len(selected_names) - 3}"
            self.open_label_revision_group(revision_name, sorted(ordinary_paths, key=lambda path: path.name), label_dir)

        def delete_selected() -> None:
            selection = listbox.curselection()
            if not selection:
                return

            index = selection[0]
            prefix = prefixes[index]
            selected_indices = sorted(selection, reverse=True)
            selected_prefixes = [prefixes[index] for index in selected_indices]
            image_paths = [path for prefix in selected_prefixes for path in groups[prefix]]
            proceed = messagebox.askyesno(
                "Delete label group?",
                f"Delete this group from {split}?\n\n"
                f"{', '.join(selected_prefixes)}\n"
                f"Images: {len(image_paths)}\n\n"
                "Matching label txt files will also be deleted.",
                parent=picker,
            )
            if not proceed:
                return

            deleted_count = 0
            for name in selected_prefixes:
                if name in special_groups:
                    group, kind = special_groups[name]
                    directory = annotation_dir(ROOT_DIR, group, kind)
                    for path in directory.glob("*.json"):
                        path.unlink()
                        deleted_count += 1
                else:
                    deleted_count += delete_frame_group(groups[name], label_dir)
            for selected_index in selected_indices:
                prefix = prefixes[selected_index]
                del groups[prefix]
                del prefixes[selected_index]
                listbox.delete(selected_index)
            if prefixes:
                next_index = min(selected_indices[-1], len(prefixes) - 1)
                listbox.selection_set(next_index)
                listbox.activate(next_index)
            self.status_var.set(f"Deleted {deleted_count} files for selected groups.")

        tk.Button(button_row, text="open selected", width=16, command=open_selected).pack(side="left", padx=(0, 8))
        def combine_selected():
            if self.live_test_running or getattr(self,'_runtime_preparing',False):
                messagebox.showwarning('Combine','Wait for inspection or preparation to finish.',parent=picker)
                return
            source_name = self.choose_absorption_group(picker,prefixes,'1. Select group to absorb')
            if source_name is None:
                return
            source,kind = special_groups.get(source_name,(source_name,'open_labeler'))
            candidates = [name for name in prefixes if name != source_name
                          and special_groups.get(name,(name,'open_labeler'))[1] == kind]
            if not candidates:
                messagebox.showwarning('Combine',f'No other {kind} group is available.',parent=picker)
                return
            target_name = self.choose_absorption_group(picker,candidates,'2. Select destination group')
            if target_name is None:
                return
            target = special_groups.get(target_name,(target_name,'open_labeler'))[0]
            if not messagebox.askyesno('Absorb selected group?',
                f'{source_name} -> {target_name}\n\nOnly {kind} labels will move.\n'
                'The source selection will be removed after verification.\nDestination conditions and other label types are kept.',
                parent=picker):
                return
            picker.destroy()
            self.status_var.set(f'Absorbing {source_name} into {target_name}...')
            self.live_test_running = True
            threading.Thread(target=self._absorb_worker,args=(source,target,kind,image_dir,label_dir,split),daemon=True).start()
        tk.Button(button_row,text='combine',width=12,command=combine_selected).pack(side='left',padx=(0,8))
        tk.Button(button_row, text="delete", width=12, command=delete_selected).pack(side="left", padx=(0, 8))
        tk.Button(button_row, text="cancel", width=12, command=picker.destroy).pack(side="left")
        listbox.bind("<Double-Button-1>", lambda event: open_selected())

    def choose_absorption_group(self,parent,names,title):
        dialog = tk.Toplevel(parent)
        dialog.title(title)
        dialog.transient(parent)
        dialog.geometry('620x350')
        choices = tk.Listbox(dialog,exportselection=False)
        choices.pack(fill='both',expand=True,padx=12,pady=12)
        for name in names:
            choices.insert(tk.END,name)
        choices.selection_set(0)
        result = []
        def accept(event=None):
            selected = choices.curselection()
            if selected:
                result.append(names[selected[0]])
                dialog.destroy()
        buttons = tk.Frame(dialog)
        buttons.pack(pady=(0,12))
        tk.Button(buttons,text='Select',command=accept).pack(side='left',padx=8)
        tk.Button(buttons,text='Cancel',command=dialog.destroy).pack(side='left',padx=8)
        choices.bind('<Double-Button-1>',accept)
        choices.bind('<Return>',accept)
        dialog.grab_set()
        self.root.wait_window(dialog)
        if parent.winfo_exists():
            parent.grab_set()
        return result[0] if result else None

    def _absorb_worker(self,source,target,kind,image_dir,label_dir,split):
        from inspection.group_combine import absorb_group
        try:
            paths = absorb_group(ROOT_DIR,image_dir,label_dir,info_dir(self.output_dir),source,target,kind,
                                 progress=lambda text:self.append_train_log(text+'\n'))
            def finished():
                self._use_extracted_frames(target,paths,len(paths))
                self.active_image_dir,self.active_label_dir,self.active_split = image_dir,label_dir,split
                self.preferred_training_scope = kind
                self.status_var.set(f'Absorbed {source} -> {target} ({kind}); {len(paths)} frames. Train {kind} to update.')
                self.live_test_running = False
            self.root.after(0,finished)
        except Exception as exc:
            error = str(exc)
            def failed():
                self.live_test_running = False
                messagebox.showerror('Absorption failed',error,parent=self.root)
            self.root.after(0,failed)

    def choose_combine_options(self,sources):
        from tkinter import ttk
        dialog = tk.Toplevel(self.root)
        dialog.title('Combine frame groups')
        dialog.transient(self.root)
        name = tk.StringVar(value='combined')
        tk.Label(dialog,text='New group name').grid(row=0,column=0,padx=12,pady=8,sticky='w')
        tk.Entry(dialog,textvariable=name,width=36).grid(row=0,column=1,padx=12,pady=8)
        tk.Label(dialog,text=' → '.join(sources)).grid(row=1,column=0,columnspan=2,padx=12,pady=8)
        selections = {}
        for row,(key,title) in enumerate((('inspection','inspection ranges'),('record','record_condition'),('test','test_condition')),2):
            selections[key] = tk.StringVar(value=sources[0])
            tk.Label(dialog,text=title).grid(row=row,column=0,padx=12,pady=8,sticky='w')
            ttk.Combobox(dialog,textvariable=selections[key],values=sources,state='readonly',width=36).grid(row=row,column=1,padx=12,pady=8)
        result = []
        def accept():
            from inspection.group_combine import combined_name
            try:
                target = combined_name(name.get(),sources)
            except ValueError as exc:
                messagebox.showerror('Combine',str(exc),parent=dialog)
                return
            result.append((target,{key:variable.get() for key,variable in selections.items()}))
            dialog.destroy()
        tk.Button(dialog,text='Combine',command=accept).grid(row=5,column=0,pady=12)
        tk.Button(dialog,text='Cancel',command=dialog.destroy).grid(row=5,column=1,pady=12)
        dialog.grab_set()
        self.root.wait_window(dialog)
        return result[0] if result else None

    def _combine_worker(self,sources,groups,target,choices,image_dir,label_dir,split,overwrite):
        from inspection.group_combine import combine_groups
        try:
            target,paths,missing = combine_groups(ROOT_DIR,image_dir,label_dir,info_dir(self.output_dir),sources,groups,
                target,choices,overwrite=overwrite,progress=lambda text:self.append_train_log(text+'\n'))
            def finished():
                self._use_extracted_frames(target,paths,len(paths))
                self.active_image_dir,self.active_label_dir,self.active_split = image_dir,label_dir,split
                self.status_var.set(f'Combined: {target}; train the new group before testing.')
                if missing:
                    messagebox.showwarning('Combine','Selected source has missing conditions: '+', '.join(missing)+'. Configure them before testing.',parent=self.root)
            self.root.after(0,finished)
        except Exception as exc:
            error = str(exc)
            self.root.after(0,lambda:messagebox.showerror('Combine failed',error,parent=self.root))

    def open_label_revision_group(self, prefix: str, image_paths: list[Path], label_dir: Path | None = None) -> None:
        image_dir, default_label_dir = self._active_label_paths()
        label_dir = label_dir or default_label_dir

        try:
            classes = load_dataset_classes(DATASET_YAML_PATH)
            self.status_var.set(f"Label revision: {prefix} ({len(image_paths)} frames)")
            YoloBoxLabeler(image_dir, label_dir, classes, image_paths=image_paths).run()
            self.status_var.set(f"Label revision saved: {prefix}. Train again to update weights/best.pt.")
        except Exception as exc:
            messagebox.showerror("Label revision failed", str(exc))
            self.status_var.set("Label revision failed.")

    def edit_special_time(self, kind: str) -> None:
        from inspection.frame_conditions import load_frame_range, save_frame_range
        if not self.active_group_name:
            messagebox.showwarning("Select frames", "Select a frame group first.")
            return
        group = self.active_group_name
        info = self.output_dir / "info"
        try:
            start, end = load_frame_range(info, group, kind)
        except (ValueError, OSError) as exc:
            messagebox.showerror("Invalid time file", str(exc))
            return
        dialog = tk.Toplevel(self.root)
        dialog.title(f"{group} - {kind} time")
        dialog.transient(self.root)
        dialog.grab_set()
        start_var = tk.StringVar(value=f"{start:g}")
        end_var = tk.StringVar(value="" if end is None else f"{end:g}")
        for row, (label, variable) in enumerate((("start frame (1-based)", start_var), ("end frame (blank = last)", end_var))):
            tk.Label(dialog, text=label).grid(row=row, column=0, padx=12, pady=8, sticky="w")
            tk.Spinbox(dialog, from_=1, to=99999999, increment=1, textvariable=variable, width=12).grid(row=row, column=1, padx=12)
        start_var.set(f"{start:g}")
        end_var.set("" if end is None else f"{end:g}")
        def save():
            try:
                save_frame_range(info, group, kind, int(start_var.get()), int(end_var.get()) if end_var.get().strip() else None)
                self.status_var.set(f"Saved {group}_{kind}_test_condition.txt")
                dialog.destroy()
            except (ValueError, OSError) as exc:
                messagebox.showerror("Invalid time", str(exc), parent=dialog)
        tk.Button(dialog, text="save", command=save).grid(row=2, column=1, pady=12)

    def train_model(self) -> None:
        if self.live_test_running:
            messagebox.showwarning('Train','Wait for the current inspection to finish.',parent=self.root)
            return
        scope = self.choose_training_scope()
        if scope is None:
            return
        if self.active_image_paths and self.active_algorithm_name:
            self.status_var.set(f"Training selected algorithm: {self.active_algorithm_name}.pt")
        else:
            self.status_var.set("Preparing val 20% / test 30%, then training...")
        selected = copy.copy(self)
        selected.active_image_paths = list(self.active_image_paths or [])
        threading.Thread(target=selected._train_worker,args=(scope,), daemon=True).start()

    def choose_training_scope(self):
        dialog = tk.Toplevel(self.root)
        dialog.title('Train algorithm')
        dialog.transient(self.root)
        choice = tk.StringVar(value=getattr(self,'preferred_training_scope','ALL'))
        result = []
        for scope in ('ALL','open_labeler','conn','label','OCR'):
            tk.Radiobutton(dialog,text=scope,variable=choice,value=scope).pack(anchor='w',padx=20,pady=4)
        def accept():
            result.append(choice.get())
            dialog.destroy()
        tk.Button(dialog,text='Train',command=accept).pack(side='left',padx=15,pady=12)
        tk.Button(dialog,text='Cancel',command=dialog.destroy).pack(side='left',padx=15,pady=12)
        dialog.grab_set()
        self.root.wait_window(dialog)
        return result[0] if result else None

    def choose_file_test_options(self, comparing=False):
        class TestOptionsDialog(simpledialog.Dialog):
            def body(self, master):
                label = 'Tests per profile per video (2 profiles)' if comparing else 'Tests per video'
                tk.Label(master, text=label).grid(sticky='w')
                self.count = tk.Entry(master)
                self.count.grid(sticky='ew')
                self.count.insert(0, '5' if comparing else '1')
                self.count.select_range(0, tk.END)
                self.mode = tk.StringVar(master=master,value='normal')
                for text,value in (('Normal','normal'),('Test speed condition','speed'),('Save scatter plot data','scatter')):
                    tk.Radiobutton(master,text=text,variable=self.mode,value=value).grid(sticky='w')
                return self.count

            def validate(self):
                if self.mode.get() not in ('scatter','speed','normal'):
                    messagebox.showwarning('Test options','Select a test option.',parent=self)
                    return False
                try:
                    repetitions = int(self.count.get())
                    if repetitions >= 1:
                        self.options = repetitions,self.mode.get()
                        return True
                except ValueError:
                    pass
                messagebox.showwarning('Test options', 'Enter a whole number of tests (at least 1).',
                                       parent=self)
                return False

            def apply(self):
                self.result = self.options

        return TestOptionsDialog(self.root, title='Test options').result

    def choose_scatter_thresholds(self, default):
        count = simpledialog.askinteger('Scatter thresholds','Number of threshold groups:',
                                       initialvalue=1,minvalue=1,maxvalue=100,parent=self.root)
        if count is None:
            return None
        thresholds = []
        for number in range(1,count+1):
            while True:
                value = simpledialog.askinteger('Scatter threshold',f'Threshold {number}/{count} (%):',
                    initialvalue=default if number == 1 else None,minvalue=1,maxvalue=100,parent=self.root)
                if value is None:
                    return None
                if value not in thresholds:
                    thresholds.append(value)
                    break
                messagebox.showwarning('Scatter threshold','Enter a different threshold for each group.',parent=self.root)
        return thresholds

    def test_selected_file(self) -> None:
        if getattr(self,'_runtime_preparing',False):
            self.status_var.set('Preparing selected frames; testing is available when Ready.')
            return
        if self.live_test_running:
            self.status_var.set("Test is already running.")
            return
        if not self.active_image_paths:
            messagebox.showwarning("Select frames", "Select the trained frame group first.")
            return
        has_special = self.active_group_name and any(
            load_reference(ROOT_DIR, self.active_group_name, kind) for kind in ("OCR", "conn", "label")
        )
        file_paths = filedialog.askopenfilenames(
            title="Select videos to test",
            initialdir=str(DEFAULT_TEST_VIDEO_DIR),
            filetypes=[("Video files", "*.mp4 *.mov *.m4v *.avi *.mkv"), ("All files", "*.*")],
        )
        if not file_paths:
            return
        comparing = hasattr(self,'compare_openvino_var') and self.compare_openvino_var.get()
        options = self.choose_file_test_options(comparing)
        if options is None:
            return
        repetitions,mode = options
        try:
            if not self.check_parallel_resources(force=True):
                return
            settings = self._settings_from_ui()
            settings['save_scatter_plot'] = mode == 'scatter'
            if mode == 'scatter':
                thresholds = self.choose_scatter_thresholds(round(settings['judge']['min_confidence']*100))
                if thresholds is None:
                    return
                settings['scatter_thresholds'] = thresholds
            if mode == 'speed':
                from inspection.automatic_runtime import allocation
                from inspection.speed_benchmark import conditions
                weights = Path(settings['root_dir'])/settings['model'].get('weights','weights/best.pt')
                if not weights.is_file() or weights.suffix.lower() != '.pt':
                    raise ValueError('Speed comparison requires an appearance .pt model.')
                settings['speed_sweep'] = conditions(allocation())
                if not settings['speed_sweep']:
                    raise ValueError('Current CPU or memory budget does not allow a 2 x 2 speed comparison.')
            if (self.active_algorithm_weights and not self.active_algorithm_weights.exists()
                    and (settings['require_regular_model'] or not (has_special or settings['essential_shapes']))):
                messagebox.showwarning("Algorithm model not found", "Train this selected frame group first.")
                return
        except Exception as exc:
            messagebox.showerror('Inspection settings',str(exc),parent=self.root)
            return
        sources = list(dict.fromkeys(Path(path) for path in file_paths))
        self.video_path = sources[0]
        self.live_test_running = True
        self.live_test_button.config(state="disabled")
        speed_count = len(settings.get('speed_sweep',[]))
        conditions_count = speed_count or (2 if comparing and mode != 'scatter' else 1)
        measured = repetitions*len(sources)*conditions_count
        self._start_batch_progress(measured+speed_count*len(sources))
        self.status_var.set(f"Testing {len(sources)} videos | {measured} measured tests")
        threading.Thread(
            target=self._batch_inspect_worker,
            args=(sources,repetitions,settings,self._inspection_window_position()),
            daemon=True,
        ).start()

    def _batch_inspect_worker(self,sources,repetitions,settings,position):
        completed = 0
        try:
            if settings.get('speed_sweep'):
                self._run_speed_sweep(sources,repetitions,settings,position)
                return
            for source in sources:
                profiles = ('jobs2_threads2','jobs1_threads4') if settings.get('compare_openvino') and not settings.get('scatter_thresholds') else (None,)
                for profile in profiles:
                    current = copy.deepcopy(settings)
                    if profile:
                        current['openvino_comparison_profile'] = profile
                        from inspection.automatic_runtime import prepare
                        self.root.after(0,lambda profile=profile:self.status_var.set(f'Preparing comparison: {profile}'))
                        prepare(dict(current,runtime_calibration=False))
                    for repeat in range(1,repetitions+1):
                        current.update(comparison_repetition=repeat,comparison_repetitions=repetitions)
                        condition = 'Scatter' if settings.get('scatter_thresholds') else profile or 'standard'
                        message = f'{source.name} | {condition} | {repeat}/{repetitions}'
                        self.root.after(0,lambda message=message:self.status_var.set(message))
                        self.append_train_log(f'\nBatch test: {message}\n')
                        if not self._inspect_worker(source,None,position,file_test=True,batch_settings=current):
                            return
                        completed += 1
                        self.root.after(0,self._complete_batch_run)
            self.root.after(0,lambda:self.status_var.set(f'Tests completed: {completed}'))
        except Exception as exc:
            message = str(exc)
            self.root.after(0,lambda:messagebox.showerror('Test preparation',message,parent=self.root))
        finally:
            self.root.after(0,self._finish_live_test)

    def _run_speed_sweep(self,sources,repetitions,settings,position):
        from uuid import uuid4
        from inspection.speed_benchmark import apply_condition,rank_conditions
        benchmark_id = uuid4().hex
        paths = []
        for condition in settings['speed_sweep']:
            current = copy.deepcopy(settings)
            current.pop('openvino_comparison_profile',None)
            current.update(compare_openvino=False,runtime_calibration=False,save_scatter_plot=False)
            if current.get('result_directory'):
                current['result_directory'] = str(Path(current['result_directory']).parent
                                                  /'speed_benchmark'/benchmark_id/'result')
            apply_condition(current,condition)
            for source in sources:
                for repeat in range(repetitions+1):
                    current['speed_benchmark'] = dict(id=benchmark_id,condition=condition,
                                                     warmup=repeat==0,repetition=repeat,source=str(source))
                    label = (f"{source.name} | {condition['backend']} "
                             f"{condition['workers']} x {condition['threads']} | "
                             + ('Warmup' if repeat == 0 else f'{repeat}/{repetitions}'))
                    self.root.after(0,lambda label=label:self.status_var.set(label))
                    audit = self._inspect_worker(source,None,position,file_test=True,batch_settings=current)
                    if not audit:
                        return
                    paths.append(audit)
                    self.root.after(0,self._complete_batch_run)
        rows = rank_conditions(paths,benchmark_id,repetitions*len(sources))
        if len(rows) != len(settings['speed_sweep']):
            raise ValueError('Speed comparison did not produce a complete set of verified measurements.')
        self.root.after(0,lambda:self._confirm_speed_condition(settings,rows))

    def _confirm_speed_condition(self,settings,rows):
        from inspection.speed_benchmark import save_condition
        best = rows[0]
        backend = '.pt' if best['backend'] == 'pytorch' else 'OpenVINO'
        title = f"{backend} | {best['workers']} workers x {best['threads']} threads"
        message = (f"{title}\n\nAverage CT: {best['mean']:.3f} sec\n"
                   f"Minimum CT: {best['minimum']:.3f} sec\nMaximum CT: {best['maximum']:.3f} sec\n"
                   f"Measured tests: {best['count']}\n\n"
                   f"Use this condition for file and Live tests on this PC for {settings['special_group']}?")
        if messagebox.askokcancel('Recommended speed condition',message,parent=self.root):
            try:
                save_condition(settings,best)
            except (OSError,ValueError) as exc:
                messagebox.showerror('Save speed condition',str(exc),parent=self.root)
                return
            self.status_var.set(f'Speed condition saved: {title}')

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
        settings.setdefault("model", {})["confidence"] = float(self.min_confidence_var.get()) / 100
        settings.setdefault("model", {})["iou"] = .45
        settings.setdefault("model", {})["original_resolution"] = False
        settings["model"]["image_size"] = 640
        settings["model"]["render_predictions"] = False
        settings["cpu_threads"] = 4
        from inspection.automatic_runtime import allocation
        automatic = allocation()
        settings.update(auto_runtime=True,mark_workers=automatic['mark_workers'],mark_spread=automatic['mark_spread'])
        settings['compare_openvino'] = bool(hasattr(self,'compare_openvino_var') and self.compare_openvino_var.get())
        settings['resource_limits'] = True
        if self.active_algorithm_weights is not None:
            settings.setdefault("model", {})["weights"] = str(self.active_algorithm_weights)
        settings["special_group"] = self.active_group_name
        from inspection.frame_conditions import group_fps
        settings['group_fps'] = group_fps(self.active_group_name)
        settings["test_fps"] = (float(self.test_fps_var.get()) if hasattr(self,'test_fps_var')
                                and str(self.test_fps_var.get()).strip() and self.active_group_name else settings['group_fps'])
        if settings['group_fps'] is not None:
            from inspection.test_sampling import TestSampling
            TestSampling(settings['group_fps'],settings['test_fps'])
        settings["special_frame_ranges"] = False
        settings["special_labeled_frames"] = True
        settings["conn_inspection_frame"] = True
        settings["special_frame_paths"] = [str(path) for path in self.active_image_paths or []]
        settings["save_test_details"] = True
        settings["record_confidence_points"] = bool(self.active_group_name)
        settings["special_info_dir"] = str(self.output_dir / "info")
        if self.active_group_name:
            settings["result_directory"] = str(self.output_dir / f"{group_key(self.active_group_name)}_threshold_{float(self.min_confidence_var.get()):g}")
        settings["require_regular_model"] = self._has_regular_labels()
        settings["alignment_enabled"] = False
        settings["alignment_reference"] = None
        settings["match_original_format"] = False
        _, label_dir = self._active_label_paths()
        classes = load_dataset_classes(DATASET_YAML_PATH)
        settings["except_regions"] = load_manual_exceptions(self.active_image_paths or [], label_dir,classes)
        from inspection.essential_shapes import load_essential_shapes
        settings["essential_shapes"] = load_essential_shapes(self.active_image_paths or [],label_dir,classes)
        settings.setdefault("judge", {})["min_confidence"] = float(self.min_confidence_var.get()) / 100
        if self.active_group_name:
            from inspection.speed_benchmark import apply_condition,load_condition
            condition = load_condition(settings)
            if condition is not None:
                apply_condition(settings,condition)
        return settings

    def _inspection_window_position(self) -> tuple[int, int]:
        self.root.update_idletasks()
        width, height = 1600, 900
        if self.video_path:
            import cv2
            capture = cv2.VideoCapture(str(self.video_path))
            if capture.isOpened():
                width = int(capture.get(cv2.CAP_PROP_FRAME_WIDTH)) or width
                height = int(capture.get(cv2.CAP_PROP_FRAME_HEIGHT)) or height
            capture.release()
        width, height, x, y = centered_preview_geometry(width, height, self.root.winfo_screenwidth(), self.root.winfo_screenheight())
        self.inspection_display_size = (width, height)
        return x, y

    def _inspect_worker(
        self,
        source: Path | str,
        max_seconds: float | None = None,
        window_position: tuple[int, int] | None = None,
        file_test: bool = False,
        batch_settings=None,
    ) -> None:
        is_live_test = max_seconds is not None
        if is_live_test:
            self.write_operation_log("live_test_start")

        try:
            self.root.after(0, lambda: self.status_var.set("Loading models and OCR engines..."))
            settings = copy.deepcopy(batch_settings) if batch_settings is not None else self._settings_from_ui()
            speed_benchmark = settings.get('speed_benchmark')
            if file_test:
                import time
                started = time.monotonic()
                stamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
                settings.update(serial_naming=True,live_async_media=True,auto_runtime=True,
                    _file_started_at=started,inspection_timestamp=stamp,
                    pipeline_start_date=datetime.now().strftime('%Y-%m-%d_%H:%M:%S'))
                self.root.after(0,lambda:self._begin_pipeline_clock(started,settings['pipeline_start_date']))
                settings['cycle_complete_callback'] = lambda elapsed,date:self.root.after(0,lambda:self._end_pipeline_clock(started,elapsed,date))
                media_finished = threading.Event() if speed_benchmark else None
                def media_done(error):
                    try:
                        from inspection.serial_results import finish_media_audit
                        finish_media_audit(settings,error)
                    except Exception as exc:
                        error = error or str(exc)
                    if error:
                        message = f'Media save failed ({stamp}): {error}'
                        try:
                            self.root.after(0,lambda:messagebox.showerror('Media save',message))
                        except (tk.TclError,RuntimeError):
                            pass
                    if media_finished is not None:
                        media_finished.set()
                settings['media_done_callback'] = media_done
            settings['confidence_history'] = file_test
            settings['test_outcome'] = {}
            settings['resource_status_callback'] = lambda text:self.root.after(0,lambda:self.status_var.set(text))
            settings["special_photo_inspection"] = file_test
            settings["photo_frame_selection"] = file_test
            from inspection.pipeline_audit import audit_pipeline
            report = audit_pipeline(settings)
            self.append_train_log("\nTest pipeline:\n" + "\n".join(report["messages"]) + "\n")
            if report["errors"]:
                raise RuntimeError("\n".join(report["errors"]))
            output_path = (unique_path(DEFAULT_TEST_VIDEO_DIR / f"{Path(source).stem}_result.mp4")
                           if file_test else self.output_dir / f"{source_stem(str(source))}_test.mp4")
            preview_width, preview_height = getattr(self, "inspection_display_size",
                                                  (1600, 900))
            saved_path = inspect_video(
                video_path=source,
                settings=settings,
                output_path=output_path,
                display_width=preview_width,
                display_height=preview_height,
                show_window=True,
                window_position=window_position,
                max_seconds=max_seconds,
                status_suffix=max_seconds is not None,
                log_dir=TEST_LOG_DIR if file_test else (self.log_dir if max_seconds is not None else None),
                file_test_log=file_test,
                progress_callback=self._inspection_progress if file_test else None,
            )
            self.root.after(0, lambda: self.status_var.set(f"TXT saved; media saving in background: {saved_path}" if file_test else f"Saved: {saved_path}"))
            completed = settings['test_outcome'].get('status','completed') == 'completed'
            if speed_benchmark and file_test:
                media_finished.wait()
                log = settings.get('_live_result_log')
                return Path(log).with_suffix('.pipeline.json') if completed and log else False
            return completed
        except Exception as exc:
            error = str(exc)
            self.root.after(0, lambda: messagebox.showerror("Inspection failed", error))
            self.root.after(0, lambda: self.status_var.set("Inspection failed."))
            return False
        finally:
            if file_test and 'started' in locals() and not settings.get('_live_result_ready_seconds'):
                elapsed = time.monotonic()-started
                self.root.after(0,lambda:self._end_pipeline_clock(started,elapsed,datetime.now().strftime('%Y-%m-%d_%H:%M:%S')))
            if is_live_test:
                self.write_operation_log("live_test_finish")
            if (is_live_test or file_test) and batch_settings is None:
                self.root.after(0, self._finish_live_test)

    def _begin_pipeline_clock(self,started,date):
        self._pipeline_clock = started
        self.pipeline_date_var.set(f'pipeline start date : {date}')
        self._refresh_pipeline_clock(started)

    def _refresh_pipeline_clock(self,started):
        if getattr(self,'_pipeline_clock',None) != started:
            return
        import time
        self.pipeline_time_var.set(f'pipeline time(sec) : {time.monotonic()-started:.7f}')
        self.root.after(50,lambda:self._refresh_pipeline_clock(started))

    def _end_pipeline_clock(self,started,elapsed,date):
        if getattr(self,'_pipeline_clock',None) != started:
            return
        self._pipeline_clock = None
        self.pipeline_time_var.set(f'pipeline time(sec) : {elapsed:.7f}')
        self.pipeline_date_var.set(f'pipeline date : {date}')

    def _finish_live_test(self) -> None:
        progress = getattr(self,'_batch_progress',None)
        if progress is not None:
            if progress.completed == progress.total:
                self._draw_batch_progress(progress)
            else:
                self.batch_progress_var.set(f'Progress: {progress.completed}/{progress.total} | Stopped')
            self._batch_progress = None
        self.live_test_running = False
        self.live_test_button.config(state="normal")

    def _start_batch_progress(self,total):
        from time import monotonic
        from inspection.batch_progress import BatchProgress
        progress = self._batch_progress = BatchProgress(total,monotonic())
        def tick():
            if getattr(self,'_batch_progress',None) is progress:
                self._draw_batch_progress(progress)
                if progress.completed < progress.total:
                    self.root.after(1000,tick)
        tick()

    def _draw_batch_progress(self,progress):
        from time import monotonic
        percent,remaining = progress.estimate(monotonic())
        if remaining is None:
            eta = '-'
        else:
            hours,remainder = divmod(math.ceil(remaining),3600)
            minutes,seconds = divmod(remainder,60)
            eta = f'{hours:02d}h {minutes:02d}m {seconds:02d}s'
        self.batch_progress_var.set(f'Progress: {percent:.1f}% ({progress.completed}/{progress.total}) | ETA: {eta}')

    def _complete_batch_run(self):
        progress = getattr(self,'_batch_progress',None)
        if progress is not None:
            progress.finish_run()
            self._draw_batch_progress(progress)

    def _inspection_progress(self, count, total, video_seconds, wall_seconds):
        message = (f"Testing: {count}/{total or '?'} frames | video {video_seconds:.2f}s | "
                   f"processing {wall_seconds:.1f}s")
        def update():
            self.status_var.set(message)
            progress = getattr(self,'_batch_progress',None)
            if progress is not None:
                progress.update_frames(count,total)
                self._draw_batch_progress(progress)
        self.root.after(0,update)

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

    def _train_worker(self,scope='ALL') -> None:
        self.root.after(0, self.clear_train_log)
        self.append_train_log("Training started...\n")
        try:
            if scope not in ('ALL','open_labeler','conn','label','OCR'):
                raise ValueError('Invalid training target')
            if scope != 'ALL' and not self.active_group_name:
                raise ValueError('Select a frame group first.')
            regular_training = scope in ('ALL','open_labeler') and (not self.active_image_paths or self._has_regular_labels())
            if scope == 'open_labeler' and not regular_training:
                raise ValueError('No appearance NG labels in this group.')
            if not regular_training:
                data_path = None
                split_message = "Special inspection settings saved; no ordinary defect labels to train."
                target_weights = None
                train_name = None
            elif self.active_image_paths and self.active_algorithm_name and self.active_algorithm_weights:
                data_path, split_message = self._build_algorithm_dataset()
                target_weights = self.active_algorithm_weights
                train_name = self.active_algorithm_name
            else:
                data_path = None
                split_message = self._auto_split_train_val_test()
                target_weights = None
                train_name = None
            args = SimpleNamespace(
                data=data_path,
                model=None,
                epochs=None,
                imgsz=None,
                batch=None,
                device=None,
                project=None,
                name=train_name,
                target_weights=target_weights,
            )
            writer = TkTrainLogWriter(self)
            with contextlib.redirect_stdout(writer), contextlib.redirect_stderr(writer):
                if regular_training:
                    train(args)
                if self.active_group_name:
                    if scope in ('ALL','conn'):
                        self._train_conn(args)
                    from inspection.text_image_model import train_text_images
                    for kind in ("OCR", "label"):
                        if scope in ('ALL',kind):
                            train_text_images(ROOT_DIR, self.active_group_name, kind)
                    from inspection.mark_model import train_marks
                    if scope in ('ALL','OCR'):
                        train_marks(ROOT_DIR,self.active_group_name)
                if not regular_training and not self.active_group_name:
                    raise RuntimeError("Select a frame group first.")
            self.append_train_log(f"\n{split_message}\nTraining finished.\n")
            self.root.after(
                0,
                lambda: self.status_var.set(f"Training finished. {split_message}"),
            )
        except Exception as exc:
            error = str(exc)
            self.append_train_log(f"\nTraining failed:\n{error}\n")
            if "ultralytics" in error:
                error = (
                    "ultralytics is not installed in this Python environment.\n\n"
                    "Run this command in the project terminal:\n"
                    f"{sys.executable} -m pip install -r requirements.txt"
                )
            self.root.after(0, lambda: messagebox.showerror("Training failed", error))
            self.root.after(0, lambda: self.status_var.set("Training failed."))

    def _has_regular_labels(self) -> bool:
        if not self.active_image_paths:
            return False
        _, directory = self._active_label_paths()
        classes = load_dataset_classes(DATASET_YAML_PATH)
        return any((path := directory / f"{image.stem}.txt").exists()
                   and self._has_ng_objects(path.read_text(encoding="utf-8"),classes)
                   for image in self.active_image_paths)

    def _has_ng_objects(self,text,classes):
        defaults = ("defect","ng","scratch","dent","breakage","variation","foreign_object")
        names = {str(name).lower() for name in getattr(self,"settings",{}).get("judge",{}).get("ng_classes",defaults)}
        return any(parts and str(classes.get(int(float(parts[0])),"")).lower() in names
                   for line in text.splitlines() if (parts := line.split()))

    def _train_conn(self, base_args) -> None:
        records = labeled_records(ROOT_DIR, self.active_group_name, "conn")
        if not records:
            return
        if len(records) < 3:
            raise RuntimeError("Conn needs at least 3 labeled frames for train / val / test. Unlabeled frames are excluded.")
        if not any(region["role"] == "pin_point" for _, data in records for region in data["regions"]):
            raise RuntimeError("Conn: label actual pin images with pin point before training. Pin area is only an allowed position.")
        records = [item for item in records if any(region["role"] == "pin_point" for region in item[1]["regions"])]
        if len(records) < 3:
            raise RuntimeError("Conn needs pin point image labels on at least 3 frames for train / val / test.")
        name = f"{group_key(self.active_group_name)}_conn_algorith"
        directory = ROOT_DIR / "dataset" / "algorithm_datasets" / f"{name}_{datetime.now():%Y%m%d_%H%M%S_%f}"
        random.Random(42).shuffle(records)
        total = len(records)
        val_count = max(1, round(total * 0.2))
        test_count = min(max(1, round(total * 0.3)), total - val_count - 1)
        splits = {"val": records[:val_count], "test": records[val_count:val_count + test_count],
                  "train": records[val_count + test_count:]}
        def has_pin(item):
            return any(region["role"] == "pin_point" for region in item[1]["regions"])
        if sum(has_pin(item) for item in records) < 2:
            raise RuntimeError("Add actual pin labels on at least 2 frames so train and val both contain pin examples.")
        for split in ("train", "val"):
            if any(has_pin(item) for item in splits[split]):
                continue
            for donor in ("test", "val", "train"):
                positives = [index for index, item in enumerate(splits[donor]) if has_pin(item)]
                if not positives or (donor in ("train", "val") and len(positives) < 2):
                    continue
                index = positives[0]
                splits[split][0], splits[donor][index] = splits[donor][index], splits[split][0]
                break
        for split, items in splits.items():
            images = directory / "images" / split
            labels = directory / "labels" / split
            images.mkdir(parents=True, exist_ok=True)
            labels.mkdir(parents=True, exist_ok=True)
            for _, data in items:
                source = Path(data["image"])
                import cv2
                image = cv2.imread(str(source))
                if image is None:
                    raise RuntimeError(f"Conn training image could not be read: {source}")
                x0, y0, x3, y3 = conn_crop_bounds(data["regions"], image.shape)
                cropped = mask_regions(image, data["regions"])[y0:y3, x0:x3]
                if not cv2.imwrite(str(images / source.name), cropped,
                                                    [cv2.IMWRITE_JPEG_QUALITY, 100]):
                    raise RuntimeError(f"Conn training image could not be saved: {source}")
                lines = []
                for region in data["regions"]:
                    if region["role"] != "pin_point":
                        continue
                    from inspection.rotated_regions import region_polygon
                    polygon = region_polygon(region, image.shape)
                    polygon -= [x0, y0]
                    polygon /= [cropped.shape[1], cropped.shape[0]]
                    lines.append("0 " + " ".join(f"{value:.8f}" for value in polygon.ravel()))
                (labels / f"{source.stem}.txt").write_text("\n".join(lines), encoding="utf-8")
                if split == "train":
                    from inspection.conn_robustness import reflection_samples
                    for suffix, sample in reflection_samples(cropped):
                        sample_name = f"{source.stem}_{suffix}"
                        if not cv2.imwrite(str(images / f"{sample_name}.jpg"), sample,
                                           [cv2.IMWRITE_JPEG_QUALITY, 100]):
                            raise RuntimeError(f"Could not save reflection sample: {sample_name}")
                        (labels / f"{sample_name}.txt").write_text("\n".join(lines), encoding="utf-8")
        if not any(path.read_text(encoding="utf-8").strip() for path in (directory / "labels" / "train").glob("*.txt")):
            raise RuntimeError("Conn training split has no pins. Label more pin examples before training.")
        data_path = directory / "data.yaml"
        data_path.write_text(yaml.safe_dump({"path": directory.as_posix(), "train": "images/train",
                                            "val": "images/val", "test": "images/test", "names": {0: "metal_pin"}},
                                           sort_keys=False), encoding="utf-8")
        args = copy.copy(base_args)
        args.data = data_path
        args.name = name
        args.target_weights = ROOT_DIR / "weights" / f"{name}.pt"
        args.conn_crop = True
        args.conn_pin_point = True
        args.conn_reflection_augmentation = True
        args.task = "obb"
        print(f"Conn training: {total} labeled frames only; model: {args.target_weights}")
        print("Conn lighting augmentation: 3 reflection/contrast samples per training image; validation/test unchanged.")
        train(args)

    def _build_algorithm_dataset(self) -> tuple[Path, str]:
        if not self.active_image_paths or not self.active_algorithm_name:
            raise RuntimeError("No selected frame group for algorithm training.")

        _, source_label_dir = self._active_label_paths()
        classes = load_dataset_classes(DATASET_YAML_PATH)
        oriented = any((source_label_dir / f"{path.stem}.rotation.json").exists() for path in self.active_image_paths)
        prepared = {}
        excluded_scratches = 0
        for image_path in self.active_image_paths:
            label_path = source_label_dir/f"{image_path.stem}.txt"
            if not label_path.exists():
                continue
            text = label_path.read_text(encoding="utf-8")
            if not self._has_ng_objects(text,classes):
                continue
            if oriented:
                import json
                import cv2
                from inspection.rotated_regions import appearance_obb_labels
                rotation_path = label_path.with_suffix(".rotation.json")
                metadata = json.loads(rotation_path.read_text(encoding="utf-8")) if rotation_path.exists() else {}
                angles = metadata.get("angles",[0]*len(text.splitlines()))
                image = cv2.imread(str(image_path))
                if image is None:
                    raise RuntimeError(f"Cannot read training image: {image_path}")
                geometry = metadata.get("geometry") if metadata.get("label_text") == text else None
                text = appearance_obb_labels(text,angles,image.shape,geometry)
            filtered,removed = filter_training_scratches(text,classes)
            filtered = ''.join(line+'\n' for line in filtered.splitlines()
                               if classes.get(int(line.split()[0]),'').lower() != 'essential')
            excluded_scratches += removed
            if self._has_ng_objects(filtered,classes):
                prepared[image_path] = filtered
        if len(prepared) < 3:
            raise RuntimeError("Open labeler needs NG labels on at least 3 frames for train/val/test. "
                               "Unlabeled, empty, OK-only and ROI/exclude-only frames are not used.")
        algorithm_dir = ROOT_DIR / "dataset" / "algorithm_datasets" / self.active_algorithm_name
        if algorithm_dir.exists():
            shutil.rmtree(algorithm_dir)

        for split in ("train", "val", "test"):
            (algorithm_dir / "images" / split).mkdir(parents=True, exist_ok=True)
            (algorithm_dir / "labels" / split).mkdir(parents=True, exist_ok=True)

        image_paths = list(prepared)
        random.Random(42).shuffle(image_paths)
        total = len(image_paths)
        if total < 3:
            raise RuntimeError("Need at least 3 selected frames to create train/val/test algorithm dataset.")

        val_count = max(1, round(total * 0.2))
        test_count = max(1, round(total * 0.3))
        if val_count + test_count >= total:
            test_count = max(1, total - val_count - 1)

        split_map = {
            "val": image_paths[:val_count],
            "test": image_paths[val_count : val_count + test_count],
            "train": image_paths[val_count + test_count :],
        }

        copied_objects = 0
        for split, paths in split_map.items():
            for image_path in paths:
                shutil.copy2(image_path, algorithm_dir / "images" / split / image_path.name)
                label_path = source_label_dir / f"{image_path.stem}.txt"
                filtered = prepared[image_path]
                (algorithm_dir / "labels" / split / label_path.name).write_text(filtered, encoding="utf-8")
                copied_objects += sum(1 for line in filtered.splitlines() if line.strip())

        if copied_objects == 0:
            raise RuntimeError("Selected frame group has no YOLO labels. Add labels before training.")

        data = {
            "path": str(algorithm_dir).replace("\\", "/"),
            "train": "images/train",
            "val": "images/val",
            "test": "images/test",
            "names": {key:name for key,name in classes.items() if name.lower() != 'essential'},
            "task": "obb" if oriented else "detect",
        }
        data_path = algorithm_dir / "data.yaml"
        data_path.write_text(yaml.safe_dump(data, allow_unicode=True, sort_keys=False), encoding="utf-8")

        return (
            data_path,
            f"Algorithm model saved to weights/{self.active_algorithm_name}.pt "
            f"(train {len(split_map['train'])}, val {len(split_map['val'])}, test {len(split_map['test'])}). "
            f"NG-labeled frames only: {len(prepared)}; skipped {len(self.active_image_paths)-len(prepared)} frames. "
            f"Excluded {excluded_scratches} scratches fully inside except; partial overlaps retained.",
        )

    def _auto_split_train_val_test(self) -> str:
        train_image_dir = ROOT_DIR / "dataset" / "images" / "train"
        train_label_dir = ROOT_DIR / "dataset" / "labels" / "train"
        val_image_dir = ROOT_DIR / "dataset" / "images" / "val"
        val_label_dir = ROOT_DIR / "dataset" / "labels" / "val"
        test_image_dir = ROOT_DIR / "dataset" / "images" / "test"
        test_label_dir = ROOT_DIR / "dataset" / "labels" / "test"
        for directory in (val_image_dir, val_label_dir, test_image_dir, test_label_dir):
            directory.mkdir(parents=True, exist_ok=True)

        existing_val = any(val_image_dir.glob("*.*"))
        existing_test = any(test_image_dir.glob("*.*"))
        if existing_val or existing_test:
            return "Existing val/test split kept."

        image_paths = sorted(
            path
            for extension in ("*.jpg", "*.jpeg", "*.png", "*.bmp")
            for path in train_image_dir.glob(extension)
        )
        if len(image_paths) < 3:
            return "Not enough train images for val/test split."

        random.Random(42).shuffle(image_paths)
        val_count = max(1, round(len(image_paths) * 0.2))
        test_count = max(1, round(len(image_paths) * 0.3))
        val_images = image_paths[:val_count]
        test_images = image_paths[val_count : val_count + test_count]

        def move_pair(image_path: Path, target_image_dir: Path, target_label_dir: Path) -> None:
            label_path = train_label_dir / f"{image_path.stem}.txt"
            shutil.move(str(image_path), str(target_image_dir / image_path.name))
            if label_path.exists():
                shutil.move(str(label_path), str(target_label_dir / label_path.name))

        for image_path in val_images:
            move_pair(image_path, val_image_dir, val_label_dir)
        for image_path in test_images:
            move_pair(image_path, test_image_dir, test_label_dir)

        return f"Auto split: val {len(val_images)}, test {len(test_images)}."


def main() -> None:
    parser = argparse.ArgumentParser(description="Video inspection GUI")
    parser.parse_args()

    root = tk.Tk()
    VideoInspectionApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
