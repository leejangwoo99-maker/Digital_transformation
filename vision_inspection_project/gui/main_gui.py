from __future__ import annotations

import threading
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox
from typing import Any

import cv2

from database.save_result import ResultSaver
from inspection.judge_logic import InspectionJudge
from model.yolo_inference import YoloInference


class InspectionApp:
    def __init__(self, root: tk.Tk, settings: dict[str, Any]) -> None:
        self.root = root
        self.settings = settings
        self.detector = YoloInference(settings)
        self.judge = InspectionJudge(settings)
        self.saver = ResultSaver(settings)

        self.root.title("Vision Inspection")
        self.root.geometry("520x260")
        self.root.resizable(False, False)

        self.status_var = tk.StringVar(value="Ready")
        self.result_var = tk.StringVar(value="-")
        self.file_var = tk.StringVar(value="No image selected")

        self._build()

    def _build(self) -> None:
        frame = tk.Frame(self.root, padx=18, pady=18)
        frame.pack(fill="both", expand=True)

        tk.Label(frame, text="Product Vision Inspection", font=("Segoe UI", 16, "bold")).pack(anchor="w")
        tk.Label(frame, textvariable=self.file_var, font=("Segoe UI", 10), fg="#555").pack(anchor="w", pady=(8, 16))

        button_frame = tk.Frame(frame)
        button_frame.pack(anchor="w")

        tk.Button(button_frame, text="Select Image", width=16, command=self.select_image).pack(side="left")
        tk.Button(button_frame, text="Inspect", width=16, command=self.inspect_selected).pack(side="left", padx=8)

        tk.Label(frame, text="Judge", font=("Segoe UI", 11, "bold")).pack(anchor="w", pady=(22, 4))
        tk.Label(frame, textvariable=self.result_var, font=("Segoe UI", 30, "bold")).pack(anchor="w")
        tk.Label(frame, textvariable=self.status_var, font=("Segoe UI", 10), fg="#555").pack(anchor="w", pady=(12, 0))

        self.selected_image: Path | None = None

    def select_image(self) -> None:
        file_path = filedialog.askopenfilename(
            title="Select inspection image",
            filetypes=[
                ("Image files", "*.jpg *.jpeg *.png *.bmp"),
                ("All files", "*.*"),
            ],
        )
        if not file_path:
            return

        self.selected_image = Path(file_path)
        self.file_var.set(str(self.selected_image))
        self.status_var.set("Image selected")

    def inspect_selected(self) -> None:
        if self.selected_image is None:
            messagebox.showwarning("No image", "Please select an image first.")
            return

        thread = threading.Thread(target=self._inspect_worker, daemon=True)
        thread.start()

    def _inspect_worker(self) -> None:
        try:
            self.status_var.set("Inspecting...")
            image = cv2.imread(str(self.selected_image))
            if image is None:
                raise FileNotFoundError(str(self.selected_image))

            detections, annotated = self.detector.predict(image)
            result = self.judge.judge(detections)
            saved = self.saver.save(
                image=annotated,
                source=str(self.selected_image),
                result=result,
                detections=detections,
            )
            self.result_var.set(result["judge"])
            self.status_var.set(f"{result['reason']} | Saved: {saved['image_path']}")
        except Exception as exc:
            self.status_var.set("Inspection failed")
            messagebox.showerror("Inspection failed", str(exc))


def run_gui(settings: dict[str, Any]) -> None:
    root = tk.Tk()
    app = InspectionApp(root, settings)
    root.mainloop()
