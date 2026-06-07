from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import numpy as np
import yaml


ROOT_DIR = Path(__file__).resolve().parent
DATASET_YAML_PATH = ROOT_DIR / "dataset" / "data.yaml"


class YoloBoxLabeler:
    def __init__(
        self,
        image_dir: Path,
        label_dir: Path,
        classes: dict[int, str],
        max_display_width: int = 1600,
        max_display_height: int = 900,
        image_paths: list[Path] | None = None,
    ) -> None:
        self.image_dir = image_dir
        self.label_dir = label_dir
        self.classes = classes
        self.max_display_width = max_display_width
        self.max_display_height = max_display_height
        self.image_paths = sorted(image_paths) if image_paths is not None else self._find_images()
        self.index = 0
        self.current_class_id = min(classes.keys()) if classes else 0
        self.defect_class_id = self._find_defect_class_id()
        self.anchor_class_ids = self._find_anchor_class_ids()
        self.roi_class_id = self._find_roi_class_id()
        self.exclude_class_id = self._find_exclude_class_id()
        self.current_class_id = self.defect_class_id
        self.boxes: list[tuple[int, int, int, int, int]] = []
        self.drag_start: tuple[int, int] | None = None
        self.drag_end: tuple[int, int] | None = None
        self.drag_class_id: int | None = None
        self.delete_drag = False
        self.copy_drag = False
        self.anchor_drag = False
        self.roi_drag = False
        self.roi_mode = False
        self.exclude_drag = False
        self.exclude_mode = False
        self.pan_drag = False
        self.pan_start: tuple[int, int] | None = None
        self.pan_offset_start: tuple[int, int] | None = None
        self.clipboard_boxes: list[tuple[int, int, int, int, int]] = []
        self.image = None
        self.display = None
        self.base_scale = 1.0
        self.zoom = 1.0
        self.scale = 1.0
        self.offset_x = 0
        self.offset_y = 0
        self.header_height = 130
        self.playing = False

    def _find_images(self) -> list[Path]:
        extensions = ("*.jpg", "*.jpeg", "*.png", "*.bmp")
        images: list[Path] = []
        for extension in extensions:
            images.extend(self.image_dir.glob(extension))
        return sorted(images)

    def _find_defect_class_id(self) -> int:
        preferred = {"defect", "ng", "scratch", "dent", "bad"}
        for class_id, class_name in self.classes.items():
            if class_name.lower() in preferred:
                return class_id
        return self.current_class_id

    def _find_anchor_class_ids(self) -> tuple[int | None, int | None]:
        anchor_1_names = {"anchor_1", "anchor1", "ref_1", "ref1", "reference_1", "align_1"}
        anchor_2_names = {"anchor_2", "anchor2", "ref_2", "ref2", "reference_2", "align_2"}
        anchor_1_id = None
        anchor_2_id = None

        for class_id, class_name in self.classes.items():
            normalized = class_name.lower()
            if normalized in anchor_1_names:
                anchor_1_id = class_id
            elif normalized in anchor_2_names:
                anchor_2_id = class_id

        return anchor_1_id, anchor_2_id

    def _find_roi_class_id(self) -> int | None:
        preferred = {"roi", "inspection_roi", "inspect_roi", "area", "region"}
        for class_id, class_name in self.classes.items():
            if class_name.lower() in preferred:
                return class_id
        return None

    def _find_exclude_class_id(self) -> int | None:
        preferred = {"exclude", "ignore", "mask", "ignore_area", "excluded_area"}
        for class_id, class_name in self.classes.items():
            if class_name.lower() in preferred:
                return class_id
        return None

    def run(self) -> None:
        if not self.image_paths:
            raise FileNotFoundError(f"No images found: {self.image_dir}")

        import cv2

        self.label_dir.mkdir(parents=True, exist_ok=True)
        cv2.namedWindow("YOLO Labeler", cv2.WINDOW_NORMAL)
        cv2.resizeWindow("YOLO Labeler", self.max_display_width, self.max_display_height + self.header_height)
        cv2.setMouseCallback("YOLO Labeler", self._on_mouse)
        self._load_current_image()

        while True:
            self._draw()
            key_code = cv2.waitKeyEx(20)
            key = key_code & 0xFF
            lower_key = key
            if ord("A") <= lower_key <= ord("Z"):
                lower_key += ord("a") - ord("A")

            if lower_key == ord("q"):
                self._save_label()
                break
            if key == 27:
                self._cancel_current_action()
                continue
            if key_code == 2424832:
                self._pan_by(80, 0)
            elif key_code == 2555904:
                self._pan_by(-80, 0)
            elif key_code == 2490368:
                self._pan_by(0, 80)
            elif key_code == 2621440:
                self._pan_by(0, -80)
            if key == ord(" "):
                self.playing = not self.playing
            if lower_key == ord("s"):
                self._save_label()
            elif lower_key == ord("n"):
                self._save_label()
                self._move(1)
            elif lower_key == ord("p"):
                self._save_label()
                self._move(-1)
            elif lower_key == ord("u") and self.boxes:
                self.boxes.pop()
            elif key == 22:
                self._paste_clipboard_boxes()
            elif lower_key == ord("r"):
                if self.roi_class_id is not None:
                    self.roi_mode = True
                    self.exclude_mode = False
                    self.current_class_id = self.roi_class_id
            elif lower_key == ord("d"):
                if self.exclude_class_id is not None:
                    self.exclude_mode = True
                    self.roi_mode = False
                    self.current_class_id = self.exclude_class_id
            elif ord("0") <= key <= ord("9"):
                class_id = key - ord("0")
                if class_id in self.classes:
                    self.current_class_id = class_id
                    self.roi_mode = class_id == self.roi_class_id
                    self.exclude_mode = class_id == self.exclude_class_id
            elif self.playing:
                self._move(1, save=False)

        cv2.destroyAllWindows()

    def _load_current_image(self) -> None:
        import cv2

        path = self.image_paths[self.index]
        self.image = cv2.imread(str(path))
        if self.image is None:
            raise RuntimeError(f"Cannot read image: {path}")
        height, width = self.image.shape[:2]
        self.base_scale = min(self.max_display_width / width, self.max_display_height / height)
        self.zoom = 1.0
        self._recenter_image()
        self.boxes = self._load_existing_label(path)

    def _load_existing_label(self, image_path: Path) -> list[tuple[int, int, int, int, int]]:
        label_path = self.label_dir / f"{image_path.stem}.txt"
        if not label_path.exists() or self.image is None:
            return []

        height, width = self.image.shape[:2]
        boxes = []
        for line in label_path.read_text(encoding="utf-8").splitlines():
            parts = line.split()
            if len(parts) != 5:
                continue
            class_id = int(float(parts[0]))
            x_center, y_center, box_width, box_height = [float(value) for value in parts[1:]]
            x1 = int((x_center - box_width / 2) * width)
            y1 = int((y_center - box_height / 2) * height)
            x2 = int((x_center + box_width / 2) * width)
            y2 = int((y_center + box_height / 2) * height)
            boxes.append((class_id, x1, y1, x2, y2))
        return boxes

    def _save_label(self) -> None:
        if self.image is None:
            return

        image_path = self.image_paths[self.index]
        label_path = self.label_dir / f"{image_path.stem}.txt"
        height, width = self.image.shape[:2]
        lines = []

        for class_id, x1, y1, x2, y2 in self.boxes:
            left, right = sorted((max(0, x1), min(width - 1, x2)))
            top, bottom = sorted((max(0, y1), min(height - 1, y2)))
            box_width = (right - left) / width
            box_height = (bottom - top) / height
            x_center = ((left + right) / 2) / width
            y_center = ((top + bottom) / 2) / height
            if box_width <= 0 or box_height <= 0:
                continue
            lines.append(f"{class_id} {x_center:.6f} {y_center:.6f} {box_width:.6f} {box_height:.6f}")

        label_path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")

    def _move(self, offset: int, save: bool = True) -> None:
        if save:
            self._save_label()
        self.index = max(0, min(len(self.image_paths) - 1, self.index + offset))
        if self.index == len(self.image_paths) - 1:
            self.playing = False
        self._load_current_image()

    def _on_mouse(self, event: int, x: int, y: int, flags: int, param: Any) -> None:
        import cv2

        if event == cv2.EVENT_MOUSEWHEEL:
            if flags & cv2.EVENT_FLAG_CTRLKEY:
                self._zoom_at(x, y, 1.15 if flags > 0 else 1 / 1.15)
            return

        if event == cv2.EVENT_MBUTTONDOWN:
            self.playing = False
            self.pan_drag = True
            self.pan_start = (x, y)
            self.pan_offset_start = (self.offset_x, self.offset_y)
            return

        if event == cv2.EVENT_MOUSEMOVE and self.pan_drag:
            if self.pan_start is not None and self.pan_offset_start is not None:
                dx = x - self.pan_start[0]
                dy = y - self.pan_start[1]
                self.offset_x = self.pan_offset_start[0] + dx
                self.offset_y = self.pan_offset_start[1] + dy
                self._clamp_offsets()
            return

        if event == cv2.EVENT_MBUTTONUP and self.pan_drag:
            self.pan_drag = False
            self.pan_start = None
            self.pan_offset_start = None
            return

        image_x, image_y = self._display_to_image_point(x, y)
        if image_x is None or image_y is None:
            return

        if event == cv2.EVENT_LBUTTONDOWN:
            self.playing = False
            self.delete_drag = bool(flags & cv2.EVENT_FLAG_CTRLKEY)
            self.copy_drag = bool(flags & cv2.EVENT_FLAG_SHIFTKEY)
            self.anchor_drag = bool(flags & cv2.EVENT_FLAG_ALTKEY)
            self.roi_drag = self.roi_mode and not (self.delete_drag or self.copy_drag or self.anchor_drag)
            self.exclude_drag = self.exclude_mode and not (
                self.delete_drag or self.copy_drag or self.anchor_drag or self.roi_drag
            )
            if self.anchor_drag:
                self.drag_class_id = self._next_anchor_class_id()
            elif self.roi_drag:
                self.drag_class_id = self.roi_class_id
            elif self.exclude_drag:
                self.drag_class_id = self.exclude_class_id
            else:
                self.drag_class_id = None if (self.delete_drag or self.copy_drag) else self.current_class_id
            self.drag_start = (image_x, image_y)
            self.drag_end = (image_x, image_y)
        elif event == cv2.EVENT_MOUSEMOVE and self.drag_start is not None:
            self.drag_end = (image_x, image_y)
        elif event == cv2.EVENT_LBUTTONUP and self.drag_start is not None:
            x1, y1 = self.drag_start
            x2, y2 = image_x, image_y
            if abs(x2 - x1) > 3 and abs(y2 - y1) > 3:
                if self.delete_drag:
                    self._delete_overlapping_boxes(x1, y1, x2, y2)
                    self._save_label()
                elif self.copy_drag:
                    self._copy_overlapping_boxes(x1, y1, x2, y2)
                elif self.anchor_drag and self.drag_class_id is not None:
                    self._replace_single_class_box(self.drag_class_id, x1, y1, x2, y2)
                    self._save_label()
                elif self.roi_drag and self.drag_class_id is not None:
                    self._replace_single_class_box(self.drag_class_id, x1, y1, x2, y2)
                    self.roi_mode = False
                    self.current_class_id = self.defect_class_id
                    self._save_label()
                elif self.exclude_drag and self.drag_class_id is not None:
                    self.boxes.append((self.drag_class_id, x1, y1, x2, y2))
                    self.exclude_mode = False
                    self.current_class_id = self.defect_class_id
                    self._save_label()
                else:
                    self.boxes.append((self.drag_class_id or self.current_class_id, x1, y1, x2, y2))
                    self._save_label()
            self.drag_start = None
            self.drag_end = None
            self.drag_class_id = None
            self.delete_drag = False
            self.copy_drag = False
            self.anchor_drag = False
            self.roi_drag = False
            self.exclude_drag = False

    def _pan_by(self, dx: int, dy: int) -> None:
        self.offset_x += dx
        self.offset_y += dy
        self._clamp_offsets()

    def _cancel_current_action(self) -> None:
        self.drag_start = None
        self.drag_end = None
        self.drag_class_id = None
        self.delete_drag = False
        self.copy_drag = False
        self.anchor_drag = False
        self.roi_drag = False
        self.roi_mode = False
        self.exclude_drag = False
        self.exclude_mode = False
        self.pan_drag = False
        self.pan_start = None
        self.pan_offset_start = None

    def _next_anchor_class_id(self) -> int:
        anchor_1_id, anchor_2_id = self.anchor_class_ids
        anchor_ids = [class_id for class_id in (anchor_1_id, anchor_2_id) if class_id is not None]
        if not anchor_ids:
            return self.current_class_id

        existing_anchor_count = sum(1 for box in self.boxes if box[0] in anchor_ids)
        return anchor_ids[existing_anchor_count % len(anchor_ids)]

    def _replace_single_class_box(self, class_id: int, x1: int, y1: int, x2: int, y2: int) -> None:
        self.boxes = [box for box in self.boxes if box[0] != class_id]
        self.boxes.append((class_id, x1, y1, x2, y2))

    def _delete_overlapping_boxes(self, x1: int, y1: int, x2: int, y2: int) -> None:
        delete_box = self._normalized_box(x1, y1, x2, y2)
        self.boxes = [
            box
            for box in self.boxes
            if not self._boxes_overlap(delete_box, self._normalized_box(box[1], box[2], box[3], box[4]))
        ]

    def _copy_overlapping_boxes(self, x1: int, y1: int, x2: int, y2: int) -> None:
        copy_box = self._normalized_box(x1, y1, x2, y2)
        self.clipboard_boxes = [
            box
            for box in self.boxes
            if self._boxes_overlap(copy_box, self._normalized_box(box[1], box[2], box[3], box[4]))
        ]

    def _paste_clipboard_boxes(self) -> None:
        if not self.clipboard_boxes:
            return

        existing = set(self.boxes)
        for box in self.clipboard_boxes:
            if box not in existing:
                self.boxes.append(box)
                existing.add(box)

    def _normalized_box(self, x1: int, y1: int, x2: int, y2: int) -> tuple[int, int, int, int]:
        left, right = sorted((x1, x2))
        top, bottom = sorted((y1, y2))
        return left, top, right, bottom

    def _boxes_overlap(self, a: tuple[int, int, int, int], b: tuple[int, int, int, int]) -> bool:
        left = max(a[0], b[0])
        top = max(a[1], b[1])
        right = min(a[2], b[2])
        bottom = min(a[3], b[3])
        return right > left and bottom > top

    def _recenter_image(self) -> None:
        if self.image is None:
            return

        height, width = self.image.shape[:2]
        self.scale = self.base_scale * self.zoom
        display_width = int(width * self.scale)
        display_height = int(height * self.scale)
        self.offset_x = (self.max_display_width - display_width) // 2
        self.offset_y = self.header_height + (self.max_display_height - display_height) // 2

    def _zoom_at(self, display_x: int, display_y: int, zoom_factor: float) -> None:
        if self.image is None:
            return

        old_scale = self.scale
        old_zoom = self.zoom
        self.zoom = max(0.25, min(8.0, self.zoom * zoom_factor))
        self.scale = self.base_scale * self.zoom
        if self.zoom == old_zoom or old_scale == 0:
            return

        image_x = (display_x - self.offset_x) / old_scale
        image_y = (display_y - self.offset_y) / old_scale
        self.offset_x = int(display_x - image_x * self.scale)
        self.offset_y = int(display_y - image_y * self.scale)
        self._clamp_offsets()

    def _clamp_offsets(self) -> None:
        if self.image is None:
            return

        height, width = self.image.shape[:2]
        display_width = int(width * self.scale)
        display_height = int(height * self.scale)

        if display_width <= self.max_display_width:
            self.offset_x = (self.max_display_width - display_width) // 2
        else:
            self.offset_x = min(0, max(self.max_display_width - display_width, self.offset_x))

        min_y = self.header_height + self.max_display_height - display_height
        max_y = self.header_height
        if display_height <= self.max_display_height:
            self.offset_y = self.header_height + (self.max_display_height - display_height) // 2
        else:
            self.offset_y = min(max_y, max(min_y, self.offset_y))

    def _display_to_image_point(self, x: int, y: int) -> tuple[int | None, int | None]:
        if self.image is None:
            return None, None

        height, width = self.image.shape[:2]
        image_x = int((x - self.offset_x) / self.scale)
        image_y = int((y - self.offset_y) / self.scale)
        if image_x < 0 or image_y < 0 or image_x >= width or image_y >= height:
            return None, None
        return image_x, image_y

    def _draw(self) -> None:
        import cv2

        if self.image is None:
            return

        image_layer = self.image.copy()
        for class_id, x1, y1, x2, y2 in self.boxes:
            self._draw_box(image_layer, class_id, x1, y1, x2, y2, self._box_color(class_id))

        if self.drag_start is not None and self.drag_end is not None:
            x1, y1 = self.drag_start
            x2, y2 = self.drag_end
            class_id = self.drag_class_id or self.current_class_id
            if self.delete_drag:
                self._draw_box(image_layer, class_id, x1, y1, x2, y2, (0, 255, 255), "delete")
            elif self.copy_drag:
                self._draw_box(image_layer, class_id, x1, y1, x2, y2, (255, 0, 255), "copy")
            elif self.anchor_drag:
                self._draw_box(image_layer, class_id, x1, y1, x2, y2, self._box_color(class_id), "anchor")
            elif self.roi_drag:
                self._draw_box(image_layer, class_id, x1, y1, x2, y2, self._box_color(class_id), "roi")
            elif self.exclude_drag:
                self._draw_box(image_layer, class_id, x1, y1, x2, y2, self._box_color(class_id), "exclude")
            else:
                self._draw_box(image_layer, class_id, x1, y1, x2, y2, self._box_color(class_id))

        image_path = self.image_paths[self.index]
        canvas = np.zeros((self.max_display_height + self.header_height, self.max_display_width, 3), dtype=np.uint8)
        resized = cv2.resize(image_layer, None, fx=self.scale, fy=self.scale, interpolation=cv2.INTER_AREA)
        display_height, display_width = resized.shape[:2]
        self._paste_visible_image(canvas, resized, display_width, display_height)

        line1 = (
            f"{self.index + 1}/{len(self.image_paths)}  {image_path.name}  "
            f"class {self.current_class_id}:{self.classes.get(self.current_class_id, '')}  zoom {self.zoom:.2f}x"
        )
        if self.roi_mode:
            line1 += "  ROI MODE: drag inspection area"
        if self.exclude_mode:
            line1 += "  EXCLUDE MODE: drag excluded area"
        line2 = (
            "drag=defect, Ctrl+drag=delete, Shift+drag=copy, Ctrl+V=paste, no box=OK  "
            "Alt+drag=anchor_1/anchor_2  Ctrl+wheel=zoom  arrows/middle-drag=pan"
        )
        line3 = (
            "r then drag=ROI  d then drag=exclude  space=play/pause  s=save  n=next  p=prev"
        )
        line4 = (
            "u=undo  0-9=class  ESC=cancel  q=quit"
        )
        cv2.putText(canvas, line1, (16, 25), cv2.FONT_HERSHEY_SIMPLEX, 0.65, (235, 235, 235), 1, cv2.LINE_AA)
        cv2.putText(canvas, line2, (16, 55), cv2.FONT_HERSHEY_SIMPLEX, 0.58, (235, 235, 235), 1, cv2.LINE_AA)
        cv2.putText(canvas, line3, (16, 85), cv2.FONT_HERSHEY_SIMPLEX, 0.58, (235, 235, 235), 1, cv2.LINE_AA)
        cv2.putText(canvas, line4, (16, 115), cv2.FONT_HERSHEY_SIMPLEX, 0.58, (235, 235, 235), 1, cv2.LINE_AA)
        cv2.imshow("YOLO Labeler", canvas)

    def _paste_visible_image(self, canvas, resized, display_width: int, display_height: int) -> None:
        canvas_x1 = max(0, self.offset_x)
        canvas_y1 = max(self.header_height, self.offset_y)
        canvas_x2 = min(self.max_display_width, self.offset_x + display_width)
        canvas_y2 = min(self.header_height + self.max_display_height, self.offset_y + display_height)

        if canvas_x1 >= canvas_x2 or canvas_y1 >= canvas_y2:
            return

        image_x1 = canvas_x1 - self.offset_x
        image_y1 = canvas_y1 - self.offset_y
        image_x2 = image_x1 + (canvas_x2 - canvas_x1)
        image_y2 = image_y1 + (canvas_y2 - canvas_y1)
        canvas[canvas_y1:canvas_y2, canvas_x1:canvas_x2] = resized[image_y1:image_y2, image_x1:image_x2]

    def _box_color(self, class_id: int) -> tuple[int, int, int]:
        if class_id == self.defect_class_id:
            return (0, 0, 255)
        if class_id in self.anchor_class_ids:
            return (255, 180, 0)
        if class_id == self.roi_class_id:
            return (0, 180, 255)
        if class_id == self.exclude_class_id:
            return (180, 180, 180)
        return (0, 255, 0)

    def _draw_box(
        self,
        canvas,
        class_id: int,
        x1: int,
        y1: int,
        x2: int,
        y2: int,
        color: tuple[int, int, int],
        label_override: str | None = None,
    ) -> None:
        import cv2

        left, right = sorted((x1, x2))
        top, bottom = sorted((y1, y2))
        cv2.rectangle(canvas, (left, top), (right, bottom), color, 2)
        label = label_override or f"{class_id}:{self.classes.get(class_id, '')}"
        cv2.putText(
            canvas,
            label,
            (left, max(20, top - 6)),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.6,
            color,
            2,
            cv2.LINE_AA,
        )


def load_dataset_classes(data_yaml: Path) -> dict[int, str]:
    with data_yaml.open("r", encoding="utf-8") as file:
        data = yaml.safe_load(file) or {}

    names = data.get("names", {})
    if isinstance(names, list):
        return {index: name for index, name in enumerate(names)}
    return {int(index): str(name) for index, name in names.items()}


def main() -> None:
    parser = argparse.ArgumentParser(description="Simple YOLO bounding box labeler")
    parser.add_argument("--split", choices=["train", "val", "test"], default="train")
    parser.add_argument("--image-dir", type=Path, help="Override image directory")
    parser.add_argument("--label-dir", type=Path, help="Override label directory")
    parser.add_argument("--data", type=Path, default=DATASET_YAML_PATH, help="YOLO data.yaml path")
    args = parser.parse_args()

    image_dir = args.image_dir or ROOT_DIR / "dataset" / "images" / args.split
    label_dir = args.label_dir or ROOT_DIR / "dataset" / "labels" / args.split
    if not image_dir.is_absolute():
        image_dir = ROOT_DIR / image_dir
    if not label_dir.is_absolute():
        label_dir = ROOT_DIR / label_dir

    classes = load_dataset_classes(args.data)
    YoloBoxLabeler(image_dir, label_dir, classes).run()


if __name__ == "__main__":
    main()
