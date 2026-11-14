from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import numpy as np
import yaml
from rotation_interaction import RotationInteraction
from translation_interaction import TranslationInteraction
from edit_interaction import EditInteraction


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
        self.auto_exclude_class_id = self._find_auto_exclude_class_id()
        self.except_class_id = next((class_id for class_id, name in classes.items() if name.lower() == "except"), None)
        self.essential_class_id = next((class_id for class_id, name in classes.items() if name.lower() == "essential"), None)
        self.essential_group = 1
        self.essential_groups = {}
        self.clipboard_essential_groups = {}
        self.current_class_id = 0 if 0 in classes else self.defect_class_id
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
        self.delete_mode = False
        self.copy_mode = False
        self.anchor_mode = False
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
        self.header_height = 160
        self.playing = False
        self.frame_input = None
        self.rotations = {}
        self.clipboard_rotations = {}
        self.mouse_image_point = None
        self.rotation_selection = RotationInteraction()
        self.rotation_mode = False
        self.edit_mode = True
        self.editor = EditInteraction()
        self.edit_undo = []
        self.rotation_undo = []

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

    def _find_auto_exclude_class_id(self) -> int | None:
        preferred = {"auto_exclude", "auto-exclude", "autoexclude", "background_exclude"}
        for class_id, class_name in self.classes.items():
            if class_name.lower() in preferred:
                return class_id
        return None

    def run(self,parent=None) -> None:
        if not self.image_paths:
            raise FileNotFoundError(f"No images found: {self.image_dir}")
        from appearance_labeler_gui import AppearanceLabelerWindow
        AppearanceLabelerWindow(self,parent).run()

    def _run_opencv(self) -> None:
        if not self.image_paths:
            raise FileNotFoundError(f"No images found: {self.image_dir}")

        import cv2

        self.label_dir.mkdir(parents=True, exist_ok=True)
        cv2.namedWindow("YOLO Labeler", cv2.WINDOW_NORMAL)
        cv2.resizeWindow("YOLO Labeler", self.max_display_width, self.max_display_height + self.header_height)
        cv2.setMouseCallback("YOLO Labeler", self._on_mouse)
        self._load_current_image()

        while True:
            if cv2.getWindowProperty("YOLO Labeler", cv2.WND_PROP_VISIBLE) < 1:
                self._save_label()
                break
            self._draw()

            key_code = cv2.waitKeyEx(20)
            key = key_code & 0xFF
            lower_key = key
            if ord("A") <= lower_key <= ord("Z"):
                lower_key += ord("a") - ord("A")

            if key == 27:
                self._cancel_current_action()
                continue
            if self.frame_input is not None:
                if key in (10, 13):
                    if self.jump_to_frame(self.frame_input):
                        self.frame_input = None
                elif key in (8, 127):
                    self.frame_input = self.frame_input[:-1]
                elif ord("0") <= key <= ord("9"):
                    self.frame_input = (self.frame_input + chr(key))[:9]
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
            if lower_key == ord("n"):
                self._save_label()
                self._move(1)
            elif lower_key == ord("p"):
                self._save_label()
                self._move(-1)
            elif lower_key == ord("u") and self.edit_undo and self.edit_undo[-1][0] == self.boxes:
                _,self.boxes,self.rotations = self.edit_undo.pop()
                self.editor = EditInteraction()
                self._save_label()
            elif lower_key == ord("u") and getattr(self,"translation_undo",[]) and self.translation_undo[-1][0] == self.boxes:
                _,self.boxes,self.rotations = self.translation_undo.pop()
                self.translation = TranslationInteraction()
                self.rotation_selection.clear()
                self._save_label()
            elif lower_key == ord("u") and self.rotation_undo and self.rotation_undo[-1][0] == self.boxes:
                boxes, angles = self.rotation_undo.pop()
                if boxes == self.boxes:
                    self.rotations = angles
                    self._save_label()
            elif lower_key == ord("u") and self.boxes:
                self.boxes.pop()
                self.rotation_selection.clear()
            elif key == 22:
                self._paste_clipboard_boxes()
            elif lower_key == ord("a"):
                self._set_mode("anchor")
            elif lower_key == ord("r"):
                if self.roi_class_id is not None:
                    self._set_mode("roi")
                    self.current_class_id = self.roi_class_id
            elif lower_key == ord("e"):
                if self.exclude_class_id is not None:
                    self._set_mode("exclude")
                    self.current_class_id = self.exclude_class_id
            elif lower_key == ord("d"):
                self._set_mode("delete")
            elif lower_key == ord("c"):
                self._set_mode("copy")
            elif key in (ord("["), ord("]"), ord("{"), ord("}")):
                self._rotate_hovered((-1 if key in (ord("["), ord("{")) else 1) * (1 if key in (ord("{"), ord("}")) else 5))
            elif lower_key == ord("t") and self.except_class_id is not None:
                self._set_mode("label")
                self.current_class_id = self.except_class_id
            elif lower_key == ord("f") and self.essential_class_id is not None:
                self._set_mode("label")
                self.current_class_id = self.essential_class_id
            elif ord("0") <= key <= ord("9"):
                class_id = key - ord("0")
                if class_id in self.classes:
                    self._set_mode("label")
                    self.current_class_id = class_id
            elif self.playing:
                self._move(1, save=False)

        cv2.destroyAllWindows()

    def jump_to_frame(self, value) -> bool:
        try:
            number = int(value)
        except (ValueError, TypeError):
            return False
        if not 1 <= number <= len(self.image_paths):
            return False
        self.playing = False
        self._save_label()
        self.index = number - 1
        self._load_current_image()
        return True

    def _load_current_image(self) -> None:
        self.editor = EditInteraction()
        self.edit_undo = []
        self.translation = TranslationInteraction()
        self.translation_undo = []
        import cv2

        self.rotation_selection.clear()
        self.rotation_undo.clear()
        self.drag_start = self.drag_end = None

        path = self.image_paths[self.index]
        self.image = cv2.imread(str(path))
        if self.image is None:
            raise RuntimeError(f"Cannot read image: {path}")
        height, width = self.image.shape[:2]
        self.base_scale = min(self.max_display_width / width, self.max_display_height / height)
        self.zoom = 1.0
        self._recenter_image()
        self.boxes = self._load_existing_label(path)
        import json
        rotations_path = self.label_dir / f"{path.stem}.rotation.json"
        metadata = json.loads(rotations_path.read_text(encoding="utf-8")) if rotations_path.exists() else {}
        angles = metadata.get("angles", [])
        self.rotations = {box: float(angle) for box, angle in zip(self.boxes, angles)}
        label_path = self.label_dir / f"{path.stem}.txt"
        groups = (metadata.get("essential_groups",[]) if label_path.exists() and
                  metadata.get("label_text") == label_path.read_text(encoding="utf-8") else [])
        self.essential_groups = {box:group for box,group in zip(self.boxes,groups)
                                 if box[0] == self.essential_class_id and type(group) is int and group > 0}
        self.boxes = [box for box in self.boxes if box[0] != self.auto_exclude_class_id]

    def _load_existing_label(self, image_path: Path) -> list[tuple[int, int, int, int, int]]:
        label_path = self.label_dir / f"{image_path.stem}.txt"
        if not label_path.exists() or self.image is None:
            return []

        height, width = self.image.shape[:2]
        import json
        text = label_path.read_text(encoding="utf-8")
        sidecar = label_path.with_suffix(".rotation.json")
        metadata = json.loads(sidecar.read_text(encoding="utf-8")) if sidecar.exists() else {}
        geometry = metadata.get("geometry",[]) if metadata.get("label_text") == text else []
        rows = [line.split() for line in text.splitlines() if line.strip()]
        if geometry and len(geometry) == len(rows):
            return [(int(float(parts[0])),*(round(float(v*s),8) for v,s in zip(region["box"],(width,height,width,height))))
                    for parts,region in zip(rows,geometry)]
        boxes = []
        for line in label_path.read_text(encoding="utf-8").splitlines():
            parts = line.split()
            if len(parts) != 5:
                continue
            class_id = int(float(parts[0]))
            x_center, y_center, box_width, box_height = [float(value) for value in parts[1:]]
            x1 = round((x_center - box_width / 2) * width)
            y1 = round((y_center - box_height / 2) * height)
            x2 = round((x_center + box_width / 2) * width)
            y2 = round((y_center + box_height / 2) * height)
            boxes.append((class_id, x1, y1, x2, y2))
        return boxes

    def _save_label(self) -> None:
        if self.image is None:
            return

        image_path = self.image_paths[self.index]
        label_path = self.label_dir / f"{image_path.stem}.txt"
        height, width = self.image.shape[:2]
        lines = []
        angles = []
        geometry = []
        groups = []

        for class_id, x1, y1, x2, y2 in self.boxes:
            from inspection.rotated_regions import region_polygon,raw_region_polygon
            angle = self.rotations.get((class_id,x1,y1,x2,y2),0)
            region = {"box":[x1/width,y1/height,x2/width,y2/height],"angle":angle,"clip_to_frame":True}
            polygon = region_polygon(region,self.image.shape)
            if len(polygon) < 3:
                continue
            raw = raw_region_polygon(region,self.image.shape)
            if (raw < 0).any() or (raw[:,0] >= width).any() or (raw[:,1] >= height).any():
                left,top = polygon.min(axis=0)
                right,bottom = polygon.max(axis=0)
            else:
                left,right = sorted((max(0,x1),min(width-1,x2)))
                top,bottom = sorted((max(0,y1),min(height-1,y2)))
            box_width = (right - left) / width
            box_height = (bottom - top) / height
            x_center = ((left + right) / 2) / width
            y_center = ((top + bottom) / 2) / height
            if box_width <= 0 or box_height <= 0:
                continue
            lines.append(f"{class_id} {x_center:.6f} {y_center:.6f} {box_width:.6f} {box_height:.6f}")
            angles.append(self.rotations.get((class_id, x1, y1, x2, y2), 0))
            geometry.append(region)
            groups.append(self.essential_groups.get((class_id,x1,y1,x2,y2)))

        label_path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
        import json
        label_path.with_suffix(".rotation.json").write_text(json.dumps({"angles": angles,"geometry":geometry,
            "essential_groups":groups,
            "label_text":label_path.read_text(encoding="utf-8")}), encoding="utf-8")

    def _add_auto_exclude_boxes(self) -> None:
        return

    def _move(self, offset: int, save: bool = True) -> None:
        if save:
            self._save_label()
        self.index = max(0, min(len(self.image_paths) - 1, self.index + offset))
        if self.playing and self.index == len(self.image_paths) - 1:
            self.playing = False
            self.index = 0
        self._load_current_image()

    def _on_mouse(self, event: int, x: int, y: int, flags: int, param: Any) -> None:
        import cv2
        if self.header_height and event == cv2.EVENT_LBUTTONDOWN and 10 <= x <= 140 and 2 <= y <= 32:
            self.playing = False
            self.frame_input = ""
            return

        if event == cv2.EVENT_MOUSEWHEEL:
            self._zoom_at(x, y, 1.15 if flags > 0 else 1 / 1.15)
            return

        if getattr(self,"edit_mode",False) and self.image is not None:
            point = ((x-self.offset_x)/self.scale,(y-self.offset_y)/self.scale)
            regions = self._rotation_regions()
            if event == cv2.EVENT_LBUTTONDOWN:
                self.playing = False
                self._edit_boxes,self._edit_angles = list(self.boxes),dict(self.rotations)
                self.editor.begin(point,regions,self.image.shape,self.scale,bool(flags & cv2.EVENT_FLAG_SHIFTKEY))
            elif event in (cv2.EVENT_MOUSEMOVE,cv2.EVENT_LBUTTONUP) and self.editor.action is not None:
                if event == cv2.EVENT_MOUSEMOVE:
                    self.editor.move(point,regions,self.image.shape)
                    before = None
                else:
                    before = self.editor.finish(point,regions,self.image.shape)
                self._apply_edit_regions(regions)
                if event == cv2.EVENT_LBUTTONUP and before is not None:
                    self.edit_undo.append((list(self.boxes),self._edit_boxes,self._edit_angles))
                    self._save_label()
            return

        if not hasattr(self,"translation"):
            self.translation = TranslationInteraction()
            self.translation_undo = []
        if self.image is not None:
            point = ((x-self.offset_x)/self.scale,(y-self.offset_y)/self.scale)
            enabled = self.rotation_mode or not any((self.delete_mode,self.copy_mode,self.anchor_mode,self.roi_mode,self.exclude_mode))
            if self.rotation_mode:
                self.translation.selected = list(self.rotation_selection.selected)
            if event == cv2.EVENT_LBUTTONDOWN and enabled and self.translation.begin(
                    point,self._rotation_regions(),self.image.shape,self.scale):
                self.playing = False
                self._translation_boxes = list(self.boxes)
                self._translation_angles = dict(self.rotations)
                return
            if self.translation.baseline is not None:
                if event in (cv2.EVENT_MOUSEMOVE,cv2.EVENT_LBUTTONUP):
                    regions = self._rotation_regions()
                    self.translation.move(point,regions,self.image.shape)
                    height,width = self.image.shape[:2]
                    self.boxes = [(old[0],*(int(round(v*s)) for v,s in zip(region["box"],(width,height,width,height))))
                                  for old,region in zip(self._translation_boxes,regions)]
                    self.rotations = {new:self._translation_angles.get(old,0)
                                      for old,new in zip(self._translation_boxes,self.boxes)}
                    self._carry_essential_groups(self._translation_boxes)
                    if event == cv2.EVENT_LBUTTONUP:
                        self.translation.finish()
                        if self.boxes != self._translation_boxes:
                            self.translation_undo.append((list(self.boxes),self._translation_boxes,self._translation_angles))
                            self._save_label()
                return

        if self.rotation_mode and self.image is not None:
            point = ((x - self.offset_x) / self.scale, (y - self.offset_y) / self.scale)
            regions = self._rotation_regions()
            shape = self.image.shape
            if event == cv2.EVENT_LBUTTONDOWN:
                self.playing = False
                self.rotation_selection.begin(point, regions, shape, self.scale)
                self._rotation_before = dict(self.rotations)
            elif event == cv2.EVENT_MOUSEMOVE:
                if self.rotation_selection.move(point, regions, shape):
                    self.rotations.update({box: region.get("angle", 0) for box, region in zip(self.boxes, regions)})
            elif event == cv2.EVENT_LBUTTONUP:
                if self.rotation_selection.baseline is not None:
                    self.rotation_selection.move(point, regions, shape)
                    self.rotations.update({box: region.get("angle", 0) for box, region in zip(self.boxes, regions)})
                baseline = self.rotation_selection.finish(point, regions, shape)
                if baseline is not None and self.rotations != self._rotation_before:
                    self.rotation_undo.append((list(self.boxes), self._rotation_before))
                    self._save_label()
            self.mouse_image_point = point
            return

        image_x, image_y = self._display_to_image_point(x, y)
        self.mouse_image_point = (image_x, image_y) if image_x is not None else None
        if image_x is None or image_y is None:
            return

        if event == cv2.EVENT_LBUTTONDOWN:
            self.playing = False
            self.delete_drag = self.delete_mode
            self.copy_drag = self.copy_mode
            self.anchor_drag = self.anchor_mode
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
            if max(abs(x2-x1),abs(y2-y1))*self.scale < 3 and not any((self.delete_drag,self.copy_drag,self.anchor_drag,self.roi_drag,self.exclude_drag)):
                self.translation.select((image_x,image_y),self._rotation_regions(),self.image.shape,
                                        bool(flags & cv2.EVENT_FLAG_SHIFTKEY))
            if abs(x2 - x1) > 3 and abs(y2 - y1) > 3:
                if self.delete_drag:
                    self._delete_overlapping_boxes(x1, y1, x2, y2)
                    self._save_label()
                    self._set_mode("label")
                elif self.copy_drag:
                    self._copy_overlapping_boxes(x1, y1, x2, y2)
                    self._set_mode("label")
                elif self.anchor_drag and self.drag_class_id is not None:
                    self._replace_single_class_box(self.drag_class_id, x1, y1, x2, y2)
                    self._save_label()
                    self._set_mode("label")
                elif self.roi_drag and self.drag_class_id is not None:
                    self._replace_single_class_box(self.drag_class_id, x1, y1, x2, y2)
                    self._save_label()
                    self._set_mode("label")
                elif self.exclude_drag and self.drag_class_id is not None:
                    self.boxes.append((self.drag_class_id, x1, y1, x2, y2))
                    self._save_label()
                    self._set_mode("label")
                else:
                    box = (self.drag_class_id or self.current_class_id, x1, y1, x2, y2)
                    if box[0] == self.essential_class_id:
                        box = (box[0],*self._normalized_box(x1,y1,x2,y2))
                    self.boxes.append(box)
                    if box[0] == self.essential_class_id:
                        self.essential_groups[box] = self.essential_group
                    self._save_label()
            self.drag_start = None
            self.drag_end = None
            self.drag_class_id = None
            self.delete_drag = False
            self.copy_drag = False
            self.anchor_drag = False
            self.roi_drag = False
            self.exclude_drag = False

    def _set_mode(self, mode: str) -> None:
        self.edit_mode = mode == "edit"
        self.editor = EditInteraction()
        self.translation = TranslationInteraction()
        self.rotation_mode = mode == "rotation"
        self.rotation_selection.clear()
        self.drag_start = self.drag_end = None
        self.delete_mode = mode == "delete"
        self.copy_mode = mode == "copy"
        self.anchor_mode = mode == "anchor"
        self.roi_mode = mode == "roi"
        self.exclude_mode = mode == "exclude"

    def _apply_edit_regions(self,regions):
        height,width = self.image.shape[:2]
        old = list(self.boxes)
        self.boxes = [(box[0],*(round(float(v*s),8) for v,s in zip(region["box"],(width,height,width,height))))
                      for box,region in zip(old,regions)]
        self.rotations = {box:region.get("angle",0) for box,region in zip(self.boxes,regions)}
        self._carry_essential_groups(old)

    def _carry_essential_groups(self, previous):
        self.essential_groups.update({new:self.essential_groups[old] for old,new in zip(previous,self.boxes)
                                      if old in self.essential_groups})

    def select_essential_group(self, group):
        from inspection.essential_shapes import load_essential_shapes

        self.essential_group = group
        if any(self.essential_groups.get(box) == group for box in self.boxes):
            return 0
        schedule = load_essential_shapes(self.image_paths,self.label_dir,self.classes)
        sources = [(number,[r for r in record['regions'] if r.get('group') == group])
                   for number,record in schedule.items() if number != self.index+1]
        sources = [(number,regions) for number,regions in sources if regions]
        if not sources:
            return 0
        _,regions = min(sources,key=lambda row:abs(row[0]-self.index-1))
        height,width = self.image.shape[:2]
        additions = [(self.essential_class_id,*(round(v*s,8) for v,s in
                       zip(region['box'],(width,height,width,height)))) for region in regions]
        if any(box in self.boxes for box in additions):
            raise ValueError('Essential group overlaps an identical existing box; no boxes copied.')
        for box,region in zip(additions,regions):
            self.boxes.append(box)
            self.rotations[box] = region.get('angle',0)
            self.essential_groups[box] = group
        self._save_label()
        return len(additions)

    def _rotation_regions(self):
        height, width = self.image.shape[:2]
        return [{"box": [min(x1, x2)/width, min(y1, y2)/height,
                         max(x1, x2)/width, max(y1, y2)/height],
                 "angle": self.rotations.get(box, 0),
                 "clip_to_frame": True,
                 "role": "auto_exclude" if box[0] == self.auto_exclude_class_id else "label"}
                for box in self.boxes for _, x1, y1, x2, y2 in [box]]

    def _pan_by(self, dx: int, dy: int) -> None:
        self.offset_x += dx
        self.offset_y += dy
        self._clamp_offsets()

    def _cancel_current_action(self) -> None:
        if getattr(self,"edit_mode",False) and self.editor.baseline is not None:
            self.boxes,self.rotations = self._edit_boxes,self._edit_angles
        self.edit_mode = True
        self.editor = EditInteraction()
        if getattr(self,"translation",None) is not None and self.translation.baseline is not None:
            self.boxes = self._translation_boxes
            self.rotations = self._translation_angles
        elif self.rotation_selection.baseline is not None:
            self.rotations = self._rotation_before
        self.rotation_mode = False
        self.rotation_selection.clear()
        self.translation = TranslationInteraction()
        self.playing = False
        self.frame_input = None
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
        self.delete_mode = False
        self.copy_mode = False
        self.anchor_mode = False
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
        from inspection.rotated_regions import region_polygon,polygon_border_overlaps_rectangle
        self.boxes = [
            box
            for box,region in zip(self.boxes,self._rotation_regions())
            if not polygon_border_overlaps_rectangle(region_polygon(region,self.image.shape),delete_box)
        ]

    def _copy_overlapping_boxes(self, x1: int, y1: int, x2: int, y2: int) -> None:
        copy_box = self._normalized_box(x1, y1, x2, y2)
        from inspection.rotated_regions import region_polygon,polygon_border_overlaps_rectangle
        self.clipboard_boxes = [
            box
            for box,region in zip(self.boxes,self._rotation_regions())
            if polygon_border_overlaps_rectangle(region_polygon(region,self.image.shape),copy_box)
        ]
        self.clipboard_rotations = {box: self.rotations.get(box, 0) for box in self.clipboard_boxes}
        self.clipboard_essential_groups = {box:self.essential_groups[box] for box in self.clipboard_boxes
                                           if box in self.essential_groups}

    def _paste_clipboard_boxes(self) -> None:
        if not self.clipboard_boxes:
            return

        existing = set(self.boxes)
        for box in self.clipboard_boxes:
            if box not in existing:
                self.boxes.append(box)
                self.rotations[box] = self.clipboard_rotations.get(box, 0)
                if box in self.clipboard_essential_groups:
                    self.essential_groups[box] = self.clipboard_essential_groups[box]
                existing.add(box)

    def _rotate_hovered(self, delta):
        from inspection.rotated_regions import region_polygon, point_in_region
        if self.mouse_image_point is None or self.image is None:
            return
        height, width = self.image.shape[:2]
        for box in reversed(self.boxes):
            _, x1, y1, x2, y2 = box
            region = {"box": [x1/width, y1/height, x2/width, y2/height], "angle": self.rotations.get(box, 0)}
            if not point_in_region(self.mouse_image_point, region_polygon(region, self.image.shape)):
                continue
            region["angle"] = (region["angle"] + delta) % 360
            region["clip_to_frame"] = True
            polygon = region_polygon(region, self.image.shape)
            if len(polygon) >= 3 and np.isfinite(polygon).all():
                self.rotations[box] = region["angle"]
                self._save_label()
            break

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
            if class_id == self.auto_exclude_class_id:
                continue
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
        if getattr(self,"edit_mode",False):
            polygons,handles = self.editor.geometry(self._rotation_regions(),self.image.shape,self.scale)
            offset = np.array([self.offset_x,self.offset_y])
            for polygon in polygons:
                cv2.polylines(canvas,[np.round(polygon*self.scale+offset).astype(np.int32)],True,(0,255,255),2)
            if handles:
                a,b = [tuple(np.round(handles[key]*self.scale+offset).astype(int)) for key in ("move","rotate")]
                cv2.line(canvas,a,b,(0,255,255),1)
            for name,point in handles.items():
                x,y = np.round(point*self.scale+offset).astype(int)
                if name in ("move","rotate"):
                    cv2.circle(canvas,(x,y),6,(255,255,0) if name == "move" else (0,255,255),-1)
                else:
                    cv2.rectangle(canvas,(x-4,y-4),(x+4,y+4),(255,255,255),-1)
            if self.editor.action == "select":
                a,b = [tuple(np.round(point*self.scale+offset).astype(int)) for point in (self.editor.start,self.editor.end)]
                cv2.rectangle(canvas,a,b,(0,255,255),2)
        if hasattr(self,"translation"):
            if self.rotation_mode:
                self.translation.selected = list(self.rotation_selection.selected)
            polygons,center = self.translation.geometry(self._rotation_regions(),self.image.shape)
            offset = np.array([self.offset_x,self.offset_y])
            for polygon in polygons:
                cv2.polylines(canvas,[np.round(polygon*self.scale+offset).astype(np.int32)],True,(0,255,255),2)
            if center is not None:
                cv2.circle(canvas,tuple(np.round(center*self.scale+offset).astype(int)),6,(255,255,0),-1)
        if self.rotation_mode:
            selection = self.rotation_selection
            polygons, center, handle = selection.geometry(self._rotation_regions(), self.image.shape, self.scale)
            offset = np.array([self.offset_x, self.offset_y])
            for polygon in polygons:
                cv2.polylines(canvas, [np.round(polygon * self.scale + offset).astype(np.int32)], True, (0, 255, 255), 3)
            if handle is not None:
                a, b = [tuple(np.round(p * self.scale + offset).astype(int)) for p in (center, handle)]
                cv2.line(canvas, a, b, (0, 255, 255), 2)
                cv2.circle(canvas, b, 9, (0, 255, 255), -1)
            if selection.start is not None:
                a, b = [tuple(np.round(np.asarray(p) * self.scale + offset).astype(int)) for p in (selection.start, selection.end)]
                cv2.rectangle(canvas, a, b, (0, 255, 255), 2)

        line1 = (
            f"{image_path.name}  "
            f"class {self.current_class_id}:{self.classes.get(self.current_class_id, '')}  zoom {self.zoom:.2f}x"
        )
        if self.roi_mode:
            line1 += "  ROI MODE: drag inspection area"
        if self.exclude_mode:
            line1 += "  EXCLUDE MODE: drag excluded area"
        if self.anchor_mode:
            line1 += "  ANCHOR MODE: drag anchor_1/anchor_2"
        if self.delete_mode:
            line1 += "  DELETE MODE: drag boxes to delete"
        if self.copy_mode:
            line1 += "  COPY MODE: drag boxes to copy"
        line2 = (
            "0=ok  1=defect  2=scratch  3=dent  4=breakage  5=variation  6=foreign_object"
        )
        if self.essential_class_id is not None:
            line2 += "  F=13:Essential"
        line3 = (
            "T=12:except  Ctrl+V=paste  no box=OK  a then drag=anchor_1/anchor_2  R=10:ROI  E=11:exclude"
        )
        line4 = (
            "d=delete  c=copy  wheel=zoom  arrows=pan  space=play/pause  n=next  p=prev  u=undo  ESC=cancel; Alt+F4/X=save close"
        )
        line5 = "ESC=edit; center=move; edges=resize; upper handle=rotate; U=undo | hover [ / ]=5deg (Shift=1deg)"
        if self.rotation_mode:
            line1 += f"  ROTATION SELECT: {len(self.rotation_selection.selected)}"
        value = self.frame_input if self.frame_input is not None else str(self.index + 1)
        cv2.rectangle(canvas, (10, 2), (140, 32), (160, 160, 160), 1)
        cv2.putText(canvas, value, (16, 25), cv2.FONT_HERSHEY_SIMPLEX, 0.65, (235, 235, 235), 1, cv2.LINE_AA)
        cv2.putText(canvas, f"/{len(self.image_paths)}", (145, 25), cv2.FONT_HERSHEY_SIMPLEX, 0.65, (235, 235, 235), 1, cv2.LINE_AA)
        cv2.putText(canvas, line1, (240, 25), cv2.FONT_HERSHEY_SIMPLEX, 0.65, (235, 235, 235), 1, cv2.LINE_AA)
        cv2.putText(canvas, line2, (16, 55), cv2.FONT_HERSHEY_SIMPLEX, 0.58, (235, 235, 235), 1, cv2.LINE_AA)
        cv2.putText(canvas, line3, (16, 85), cv2.FONT_HERSHEY_SIMPLEX, 0.58, (235, 235, 235), 1, cv2.LINE_AA)
        cv2.putText(canvas, line4, (16, 115), cv2.FONT_HERSHEY_SIMPLEX, 0.58, (235, 235, 235), 1, cv2.LINE_AA)
        cv2.putText(canvas, line5, (16, 145), cv2.FONT_HERSHEY_SIMPLEX, 0.58, (235, 235, 235), 1, cv2.LINE_AA)
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
        class_name = self.classes.get(class_id, "").lower().replace(" ", "_")
        class_colors = {
            "ok": (80, 220, 80),
            "defect": (0, 0, 255),
            "ng": (0, 0, 255),
            "scratch": (255, 0, 0),
            "dent": (0, 165, 255),
            "breakage": (0, 255, 255),
            "variation": (255, 0, 255),
            "foreign_object": (128, 0, 255),
            "foreignobject": (128, 0, 255),
            "auto_exclude": (160, 160, 160),
            "auto-exclude": (160, 160, 160),
            "autoexclude": (160, 160, 160),
            "anchor_1": (255, 180, 0),
            "anchor1": (255, 180, 0),
            "anchor_2": (255, 120, 0),
            "essential": (255, 210, 0),
            "anchor2": (255, 120, 0),
            "roi": (0, 180, 255),
            "exclude": (180, 180, 180),
            "except": (210, 100, 80),
        }
        if class_name in class_colors:
            return class_colors[class_name]
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
        angle = self.rotations.get((class_id, x1, y1, x2, y2), 0)
        left,right,top,bottom = map(lambda value:int(round(value)),(left,right,top,bottom))
        if angle:
            from inspection.rotated_regions import region_polygon
            height, width = canvas.shape[:2]
            polygon = region_polygon({"box": [left/width, top/height, right/width, bottom/height], "angle": angle}, canvas.shape)
            cv2.polylines(canvas, [np.round(polygon).astype(np.int32)], True, color, 2)
        else:
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
