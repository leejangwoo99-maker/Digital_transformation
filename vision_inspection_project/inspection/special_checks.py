from __future__ import annotations

import json
import math
import re
from threading import RLock
from pathlib import Path

import cv2
import numpy as np

from inspection.region_masks import mask_regions
from inspection.flexible_fields import accepts as accepts_text,paired_matches,variable as variable_text


KINDS = ("OCR", "conn", "label")


def group_key(name: str) -> str:
    return re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", name).strip(" .") or "frames"


def annotation_dir(root: Path, group: str, kind: str) -> Path:
    return root / "dataset" / "special_labels" / group_key(group) / kind


def read_json(path: Path, default=None):
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else default


def write_json(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(path)


def labeled_records(root: Path, group: str, kind: str) -> list[tuple[Path, dict]]:
    directory = annotation_dir(root, group, kind)
    records = []
    for path in sorted(directory.glob("*.json")):
        if path.name == "reference.json":
            continue
        data = read_json(path)
        if any(region["role"] in ("text", "mark", "qr", "slot", "pin", "pin_point") for region in data.get("regions", [])):
            records.append((path, data))
    return records


def load_reference(root: Path, group: str, kind: str):
    directory = annotation_dir(root, group, kind)
    records = labeled_records(root, group, kind)
    return max(records, key=lambda item: item[1].get("saved_at_ns", item[0].stat().st_mtime_ns))[1] if records else None


def labeled_frame_schedule(root, group, kind, image_paths):
    numbers = {Path(path).name: index + 1 for index, path in enumerate(image_paths)}
    schedule = {}
    for path, data in labeled_records(root, group, kind):
        name = Path(data["image"]).name
        if name not in numbers:
            raise ValueError(f"Labeled frame is not in the selected group: {path}")
        schedule[numbers[name]] = data
    return schedule


def time_path(info: Path, group: str, kind: str) -> Path:
    return info / f"{group_key(group)}_{kind}_test_condition.txt"


def load_time(info: Path, group: str, kind: str) -> tuple[float, float | None]:
    path = time_path(info, group, kind)
    if not path.exists():
        return 0.0, None
    values = dict(line.split("=", 1) for line in path.read_text(encoding="utf-8").splitlines() if "=" in line)
    start = float(values.get("start_seconds", "0"))
    end = float(values["end_seconds"]) if values.get("end_seconds", "").strip() else None
    validate_time(start, end)
    return start, end


def validate_time(start: float, end: float | None) -> None:
    if not math.isfinite(start) or start < 0 or (end is not None and (not math.isfinite(end) or end <= start)):
        raise ValueError("Start must be >= 0 and end must be greater than start. Blank end = entire remaining test.")


def save_time(info: Path, group: str, kind: str, start: float, end: float | None) -> None:
    validate_time(start, end)
    info.mkdir(parents=True, exist_ok=True)
    time_path(info, group, kind).write_text(
        f"start_seconds={start:g}\nend_seconds={'' if end is None else format(end, 'g')}\n", encoding="utf-8")


def pixel_box(region: dict, shape) -> tuple[int, int, int, int]:
    height, width = shape[:2]
    x1, y1, x2, y2 = region["box"]
    return max(0, round(x1 * width)), max(0, round(y1 * height)), min(width, round(x2 * width)), min(height, round(y2 * height))


def load_manual_exceptions(image_paths: list[Path], label_dir: Path, classes: dict) -> list[dict]:
    except_ids = {str(class_id) for class_id, name in classes.items() if str(name).lower() == "except"}
    files = [label_dir / f"{image.stem}.txt" for image in image_paths]
    files = [path for path in files if path.exists()]
    if not files:
        return []
    # Match the current inspection layout to the most recently edited frame.
    latest = max(files, key=lambda path: path.stat().st_mtime_ns)
    regions = []
    rotation_path = latest.with_suffix(".rotation.json")
    metadata = read_json(rotation_path,{})
    angles = metadata.get("angles", [])
    text = latest.read_text(encoding="utf-8")
    geometry = metadata.get("geometry",[]) if metadata.get("label_text") == text else []
    for index, line in enumerate(text.splitlines()):
        parts = line.split()
        if len(parts) != 5 or parts[0] not in except_ids:
            continue
        x, y, width, height = map(float, parts[1:])
        regions.append({"role": "except", "box": [x-width/2, y-height/2, x+width/2, y+height/2],
                        "angle": angles[index] if index < len(angles) else 0})
        if index < len(geometry):
            regions[-1].update(geometry[index])
    return regions


def matches_format(text: str, pattern: list[str]) -> bool:
    if not pattern or len(text) != len(pattern):
        return False
    return all((character in "0123456789") if kind == "num" else
               (kind == "text" and character not in "0123456789") for character, kind in zip(text, pattern))


def conn_failures(slots: list[tuple], pins: list[tuple]) -> list[str]:
    counts = [0] * len(slots)
    failures = []
    for pin in pins:
        matches = [index for index, slot in enumerate(slots)
                   if slot[0] < pin[0] and slot[1] < pin[1] and pin[2] < slot[2] and pin[3] < slot[3]]
        if len(matches) != 1:
            failures.append("conn_outside_or_boundary")
        else:
            counts[matches[0]] += 1
    failures.extend(f"conn_slot_{index + 1}_count_{count}" for index, count in enumerate(counts) if count != 1)
    return failures


def conn_position_failures(slots, positions, pins):
    from inspection.rotated_regions import point_in_region
    from inspection.conn_geometry import center_inside
    if not positions:
        return ["conn_missing_pin_regions"]
    counts = [0] * len(positions)
    failures = []
    for x1, y1, x2, y2 in pins:
        cx, cy = (x1 + x2) / 2, (y1 + y2) / 2
        matches = [i for i, area in enumerate(positions) if center_inside((cx, cy), area)]
        if len(matches) == 1:
            counts[matches[0]] += 1
        else:
            failures.append("conn_outside_or_boundary")
    failures.extend(f"conn_pin_{i + 1}_count_{count}" for i, count in enumerate(counts) if count != 1)
    if slots and any(not any(point_in_region(((pin[0]+pin[2])/2, (pin[1]+pin[3])/2), slot)
                   for slot in slots) for pin in pins):
        failures.append("conn_outside_connector_or_boundary")
    return failures


def compose_ocr(results: list) -> str:
    # Rebuild observed spaces and line breaks from OCR geometry, never from the expected answer.
    rows = []
    for polygon, text, confidence in sorted(results, key=lambda item: min(point[1] for point in item[0])):
        if confidence <= 0 or not text:
            continue
        left = min(point[0] for point in polygon)
        right = max(point[0] for point in polygon)
        top = min(point[1] for point in polygon)
        bottom = max(point[1] for point in polygon)
        center = (top + bottom) / 2
        row = next((row for row in rows if abs(row["center"] - center) < max(1, bottom - top) * 0.5), None)
        if row is None:
            row = {"center": center, "height": max(1, bottom - top), "items": []}
            rows.append(row)
        row["items"].append((left, right, text))
    output = []
    previous_row = None
    for row in sorted(rows, key=lambda row: row["center"]):
        if previous_row is not None:
            gap = row["center"] - previous_row["center"]
            output.append("\n" * max(1, round(gap / (previous_row["height"] * 1.5))))
        previous = None
        for left, right, text in sorted(row["items"]):
            if previous is not None:
                char_width = max(1, (previous[1] - previous[0]) / max(1, len(previous[2])))
                output.append(" " * max(0, round((left - previous[1]) / char_width)))
            output.append(text)
            previous = (left, right, text)
        previous_row = row
    return "".join(output)


class MultilingualOCR:
    reader_lock = RLock()
    def __init__(self):
        import easyocr
        # Chinese readers cannot be combined with French in one EasyOCR reader.
        self.readers = [easyocr.Reader(languages, gpu=False, verbose=False) for languages in
                        (["en", "fr"], ["ch_sim", "en"], ["ch_tra", "en"])]
        self.cache = {}

    def candidates(self, crop) -> list[str]:
        return self.candidates_for(crop)

    def candidates_for(self, crop, expected=None) -> list[str]:
        if not crop.size:
            return []
        cache = getattr(self, "cache", None)
        key = (crop.shape, crop.tobytes(), expected)
        if cache is not None and key in cache:
            return list(cache[key])
        images = [crop]
        if crop.shape[0] > crop.shape[1] * 1.5:
            images = [cv2.rotate(crop, cv2.ROTATE_90_CLOCKWISE),
                      cv2.rotate(crop, cv2.ROTATE_90_COUNTERCLOCKWISE)]
        output = []
        readers = self.readers
        if expected is not None:
            has_chinese = any("\u3400" <= character <= "\u9fff" for character in expected)
            readers = self.readers[1:] if has_chinese else self.readers[:1]
        for reader in readers:
            for image in images:
                with self.reader_lock:
                    value = compose_ocr(reader.readtext(image, detail=1, paragraph=False))
                if value not in output:
                    output.append(value)
                if expected is not None and value == expected:
                    break
            if expected is not None and expected in output:
                break
        if cache is not None:
            if len(cache) >= 128:
                cache.pop(next(iter(cache)))
            cache[key] = output
        return output


class CodeReader:
    def __init__(self):
        self.qr = cv2.QRCodeDetector()

    def detectAndDecode(self, image):
        if not image.size:
            return "", None, None
        text, points, straight = self.qr.detectAndDecode(image)
        if text:
            return text, points, straight
        import zxingcpp
        codes = zxingcpp.read_barcodes(image, formats=[zxingcpp.BarcodeFormat.QRCode, zxingcpp.BarcodeFormat.DataMatrix])
        if not codes:
            # Tight annotation crops can omit the quiet zone needed by the decoder.
            margin = max(4,round(min(image.shape[:2])*.05))
            value = 255 if image.ndim == 2 else (255,255,255)
            padded = cv2.copyMakeBorder(image,margin,margin,margin,margin,cv2.BORDER_CONSTANT,value=value)
            codes = zxingcpp.read_barcodes(padded, formats=[zxingcpp.BarcodeFormat.QRCode, zxingcpp.BarcodeFormat.DataMatrix])
        return (codes[0].text, None, None) if len(codes) == 1 else ("", None, None)


def conn_crop_bounds(regions, shape):
    from inspection.rotated_regions import region_polygon
    # A slot limits judging; resizing it must not change the model's training scale.
    if any(region["role"] in ("pin", "pin_point") for region in regions):
        regions = [region for region in regions if region["role"] != "slot"]
    boxes = []
    for region in regions:
        if region["role"] in ("slot", "pin", "pin_point"):
            polygon = region_polygon(region, shape)
            boxes.append((*polygon.min(axis=0), *polygon.max(axis=0)))
    if not boxes:
        return (0, 0, shape[1], shape[0])
    x1, y1 = min(box[0] for box in boxes), min(box[1] for box in boxes)
    x2, y2 = max(box[2] for box in boxes), max(box[3] for box in boxes)
    margin = max(8, round(max(x2 - x1, y2 - y1) * 0.1))
    return (max(0, math.floor(x1 - margin)), max(0, math.floor(y1 - margin)),
            min(shape[1], math.ceil(x2 + margin)), min(shape[0], math.ceil(y2 + margin)))


class SpecialInspector:
    def fresh_run(self, settings):
        """Share prepared models, but never share a previous product's verdict state."""
        import copy
        from inspection.shape_registration import ShapeRegistration
        from inspection.polygon_roi import PolygonAligner
        result = type(self).__new__(type(self))
        fixed = ('root','group','references','times','text_image_models','trained_text_ocr',
                 'mark_model','pin_model','pin_crop_model','reference_transforms','frame_ranges',
                 'labeled_frames_only','annotation_frames','conn_inspection_range','confidence',
                 'iou','image_size','original_resolution')
        for name in fixed:
            value = getattr(self,name)
            if name in ('references','times','annotation_frames','reference_transforms'):
                value = copy.deepcopy(value)
            setattr(result,name,value)
        result.trained_text_ocr = {}
        for kind,model in self.trained_text_ocr.items():
            if hasattr(model,'readers') and model.readers is None:
                model.readers = {}
            reader = copy.copy(model)
            reader.backend_counts,reader.runtime_seconds,reader.backend_errors = {},{},[]
            result.trained_text_ocr[kind] = reader
        result.ocr = copy.copy(self.ocr) if self.ocr is not None else None
        if result.ocr is not None:
            result.ocr.cache = {}
        result.shape_registration = ShapeRegistration(tracking=settings.get('reference_tracking',True))
        result.polygon_aligner = PolygonAligner()
        result.polygon_aligner.cache = self.polygon_aligner.cache
        result.qr = CodeReader()
        result.status_lines = []
        return result

    def __init__(self, settings: dict):
        import cv2
        self.root = Path(settings["root_dir"])
        self.group = settings.get("special_group")
        self.references = {}
        self.times = {}
        self.ocr = None
        self.text_image_models = {}
        self.trained_text_ocr = {}
        from inspection.shape_registration import ShapeRegistration
        self.shape_registration = ShapeRegistration(tracking=settings.get('reference_tracking',True))
        self.mark_model = None
        from inspection.polygon_roi import PolygonAligner
        self.polygon_aligner = PolygonAligner()
        self.pin_model = None
        self.pin_crop_model = False
        self.reference_transforms = {}
        self.frame_ranges = bool(settings.get("special_frame_ranges"))
        self.labeled_frames_only = bool(settings.get("special_labeled_frames"))
        self.annotation_frames = {}
        self.conn_inspection_range = None
        self.status_lines = []
        self.confidence = float(settings.get("model", {}).get("confidence", 0.1))
        self.iou = float(settings.get("model", {}).get("iou", 0.45))
        self.image_size = int(settings.get("model", {}).get("image_size", 640))
        self.original_resolution = bool(settings.get("model", {}).get("original_resolution", False))
        self.qr = CodeReader()
        if not self.group:
            return
        for kind in KINDS:
            reference = load_reference(self.root, self.group, kind)
            if reference:
                self.references[kind] = reference
                if self.labeled_frames_only:
                    self.annotation_frames[kind] = labeled_frame_schedule(
                        self.root, self.group, kind, settings.get("special_frame_paths", []))
                    self.times[kind] = (0, None)
                elif self.frame_ranges:
                    from inspection.frame_conditions import load_frame_range
                    self.times[kind] = load_frame_range(Path(settings["special_info_dir"]), self.group, kind)
                else:
                    self.times[kind] = load_time(Path(settings["special_info_dir"]), self.group, kind)
        if settings.get("conn_inspection_frame") and "conn" in self.references:
            from inspection.photo_inspection import load_photo_frame_range
            self.conn_inspection_range = load_photo_frame_range(Path(settings["special_info_dir"]),self.group,"conn")
            if not self.annotation_frames.get("conn"):
                raise ValueError("Conn requires labeled training frames for nearest-frame position conditions.")
        base_path = settings.get("alignment_reference")
        if settings.get("alignment_enabled") and base_path:
            from inspection.frame_alignment import FrameAligner
            base_image = cv2.imread(str(base_path))
            if base_image is None:
                raise RuntimeError(f"Alignment reference cannot be read: {base_path}")
            for kind, reference in self.references.items():
                path = Path(reference["image"])
                if path.resolve() == Path(base_path).resolve():
                    continue
                result = FrameAligner(path).align(base_image)
                if not result.ok:
                    raise RuntimeError(f"Cannot align {kind} label frame to training frame: {result.message}")
                self.reference_transforms[kind] = (result.matrix, result.image.shape[:2])
        from inspection.text_image_model import model_path, TextImageModel, annotation_signature
        for kind in ("OCR", "label"):
            if kind not in self.references:
                continue
            records = (self.annotation_frames[kind].values() if self.labeled_frames_only else [self.references[kind]])
            required = [region for data in records for region in data["regions"]
                        if region["role"] == "text" and region.get("verdict") in ("PASS", "FAIL")]
            if required:
                path = model_path(self.root, self.group, kind)
                if not path.exists():
                    raise RuntimeError(f"Train the selected group Text PASS/FAIL labels first: {path}")
                model = TextImageModel(path)
                from inspection.supervised_text_ocr import TrainedTextOCR
                if not path.with_suffix(".ocr.json").exists():
                    raise RuntimeError(f"{kind}: character recognizer missing. Train the selected group again.")
                self.trained_text_ocr[kind] = TrainedTextOCR(path.with_suffix(".ocr.json"))
                self.trained_text_ocr[kind].force_torch = settings.get('ocr_backend') == 'pytorch'
                signatures = {item["signature"] for item in model.metadata}
                if any(annotation_signature(region) not in signatures for region in required):
                    raise RuntimeError(f"{kind} Text labels changed. Train the selected group again.")
                self.text_image_models[kind] = model
        if "OCR" in self.references:
            from inspection.mark_model import model_path as mark_model_path,MarkModel,signature as mark_signature
            records = self.annotation_frames["OCR"].values() if self.labeled_frames_only else [self.references["OCR"]]
            marks = [region for data in records for region in data["regions"] if region["role"] == "mark"]
            if marks:
                path = mark_model_path(self.root,self.group)
                if not path.exists():
                    raise RuntimeError(f"Train the selected group's Mark images first: {path}")
                self.mark_model = MarkModel(path,workers=settings.get("mark_workers",2),
                    spread=settings.get('mark_spread',1),resource_gate=settings.get('_resource_gate'))
                signatures = {item["signature"] for item in self.mark_model.metadata}
                if any(mark_signature(region) not in signatures for region in marks):
                    raise RuntimeError("Mark boxes changed. Train the selected group again.")
        has_text = any(region["role"] == "text" for kind,data in self.references.items()
                       if kind in ("OCR","label") for region in data["regions"])
        if has_text and not self.text_image_models:
            try:
                self.ocr = MultilingualOCR()
            except ImportError as exc:
                raise RuntimeError("OCR requires easyocr. Install requirements.txt in this Python environment.") from exc
        if "conn" in self.references:
            from ultralytics import YOLO
            weights = self.root / "weights" / f"{group_key(self.group)}_conn_algorith.pt"
            if not weights.exists():
                raise RuntimeError(f"Train the Conn pin labels first: {weights}")
            manifest = read_json(weights.with_suffix(".json"), {})
            conn_records = (self.annotation_frames.get("conn", {}).values() if self.labeled_frames_only else [self.references["conn"]])
            if any(region.get("angle", 0) and region["role"] == "pin_point"
                   for data in conn_records for region in data["regions"]) and manifest.get("task") != "obb":
                raise RuntimeError("Rotated Conn pin point labels require OBB training. Train the selected group again.")
            if any(region["role"] == "pin_point" for region in self.references["conn"]["regions"]) and not manifest.get("conn_pin_point"):
                raise RuntimeError("Conn pin point labels changed the training method. Train this frame group again before testing.")
            self.pin_model = YOLO(str(weights))
            self.pin_crop_model = bool(read_json(weights.with_suffix(".json"), {}).get("conn_crop"))

    def conn_reference(self, number):
        schedule = self.annotation_frames.get("conn", {})
        interval = getattr(self,"conn_inspection_range",None)
        if interval is None:
            return schedule.get(number)
        if not interval[0] <= number <= interval[1] or not schedule:
            return None
        nearest = min(schedule,key=lambda value:(abs(value-number),value))
        return schedule[nearest]

    def draw_conn_guides(self, frame, number):
        reference = (self.conn_reference(number)
                     if getattr(self, "labeled_frames_only", False) else self.references.get("conn"))
        if not reference:
            return frame
        from inspection.conn_geometry import scoped_conn_regions
        regions = [region for region in reference["regions"] if region["role"] in ("conn_boundary", "pin", "pin_point", "slot")]
        if not regions:
            return frame
        from inspection.rotated_regions import region_polygon
        annotated = frame.copy()
        transform = getattr(self, "active_registration", {}).get("conn") or getattr(self, "reference_transforms", {}).get("conn")
        shape = transform[1] if transform else frame.shape
        regions = scoped_conn_regions(regions, shape)
        def project(polygon):
            if transform:
                polygon = cv2.perspectiveTransform(polygon.reshape(1, -1, 2), np.linalg.inv(transform[0]))[0]
            return np.round(polygon).astype(np.int32)
        def label(text, polygon, color):
            x, y = polygon.min(axis=0)
            cv2.putText(annotated, text, (max(0, int(x)), max(16, int(y)-5)), cv2.FONT_HERSHEY_SIMPLEX, .45, color, 1, cv2.LINE_AA)
        counts = {}
        visual = getattr(self, "conn_visuals", None)
        for region in regions:
            role = region["role"]
            counts[role] = counts.get(role, 0)+1
            if role == "pin_point" and visual is not None:
                continue
            color = {"conn_boundary": (0, 0, 255), "pin": (210, 70, 180), "pin_point": (255, 220, 0), "slot": (55, 157, 244)}[role]
            if region.get("shape") == "circle":
                from inspection.conn_geometry import pin_center
                cx, cy = pin_center(region, shape)
                radius = (region["box"][2]-region["box"][0])*shape[1]/2
                angles = np.linspace(0, 2*np.pi, 65)
                polygon = np.column_stack((cx+radius*np.cos(angles), cy+radius*np.sin(angles))).astype(np.float32)
            else:
                polygon = region_polygon(region, shape)
            polygon = project(polygon)
            cv2.polylines(annotated, [polygon], True, color, 2)
            label(("maximum" if role == "conn_boundary" else "pin area" if role == "pin" else "slot" if role == "slot" else "pin point (reference)") + f" {counts[role]}", polygon, color)
        if visual is not None:
            for index, box in enumerate(visual["pins"]):
                x1, y1, x2, y2 = box
                polygon = project(np.array([[x1,y1],[x2,y1],[x2,y2],[x1,y2]], np.float32))
                color = (255, 220, 0)
                cv2.polylines(annotated, [polygon], True, color, 2)
                center = project(np.array([[(x1+x2)/2, (y1+y2)/2]], np.float32))[0]
                cv2.drawMarker(annotated, tuple(center), color, cv2.MARKER_CROSS, 9, 1)
                number = visual.get("numbers", [None]*len(visual["pins"]))[index]
                label(f"pin point {number}" if number is not None else f"pin point extra {index+1}", polygon, color)
        return annotated

    def _confirm_conn(self, failures, number):
        from inspection.conn_robustness import ConnConfirmation
        if not hasattr(self, "conn_confirmation"):
            self.conn_confirmation = ConnConfirmation()
        return self.conn_confirmation.update(failures, number)

    def draw_mark_guides(self,frame):
        annotated = frame.copy()
        for mark in getattr(self,"mark_visuals",[]):
            if mark["verdict"] != "OK":
                continue
            x1,y1,x2,y2 = map(int,mark["xyxy"])
            color = (0,165,230) if mark["verdict"] == "OK" else (0,0,255)
            cv2.rectangle(annotated,(x1,y1),(x2,y2),color,2)
            cv2.putText(annotated,f"mark {mark['number']} {mark['verdict']}",(x1,max(18,y1-5)),
                        cv2.FONT_HERSHEY_SIMPLEX,.55,color,1,cv2.LINE_AA)
        return annotated

    @staticmethod
    def region_item_key(kind, region, index):
        if kind == 'OCR':
            return f"{region['role']}_{index}"
        number = region.get('number')
        if number in (1,2,3,4,5,6,7,8):
            return f'pair_{number if number % 2 else number-1}'
        return f'text_{number}'

    @staticmethod
    def item_keys(kind, reference):
        regions = reference['regions']
        if kind == 'OCR':
            return {f"{region['role']}_{index}" for index,region in enumerate(regions)
                    if region['role'] in ('text','mark')}
        return {f'pair_{odd}' for odd in (1,3,5,7) if any(
            r['role'] == 'qr' and r.get('number') == odd or
            r['role'] == 'text' and r.get('number') == odd+1 for r in regions)} | {
            f'text_{n}' for n in (None,9,0,*range(10,21)) if any(
                r['role'] == 'text' and r.get('number') == n for r in regions)}

    def begin_item_range(self, start, end):
        from inspection.frame_conditions import WindowResults
        self.window_results = WindowResults()
        self.item_required = {(kind,key) for kind,schedule in self.annotation_frames.items()
                              for number,reference in schedule.items() if start <= number <= end
                              for key in self.item_keys(kind,reference)}
        if not self.item_required:
            raise ValueError('No inspectable Text/Mark/QR items in the selected frame range.')
        self.item_pass_records = {}
        self.component_reads = {}

    def result_photo_boxes(self, kind, number, shape):
        if not hasattr(self, 'item_required'):
            return None
        reference = self.annotation_frames.get(kind, {}).get(number)
        if reference is None:
            return None
        from inspection.rotated_regions import region_polygon
        regions = reference['regions']
        transform = getattr(self, 'active_registration', {}).get(kind)
        if transform is None and any(r['role'] == 'registration' for r in regions):
            return None
        inverse = np.linalg.inv(transform[0]) if transform else None
        boxes = []
        mark_number = 0
        for index, region in enumerate(regions):
            role = region['role']
            if role == 'mark':
                mark_number += 1
            if role not in (('text', 'mark') if kind == 'OCR' else ('text', 'qr')):
                continue
            key = (kind, self.region_item_key(kind, region, index))
            if key not in self.item_required:
                continue
            records = self.item_pass_records.get(key, [])
            ok = key in self.window_results.successes
            passed_at = records[0]['source_frame_number'] if records else number
            title = 'QR' if role == 'qr' else role.title()
            box_number = (mark_number if role == 'mark' else index+1 if kind == 'OCR'
                          else region.get('number'))
            label = f"{title} {box_number if box_number is not None else index+1} {'OK' if ok else 'NG'}"
            if ok and passed_at != number:
                label += f' F{passed_at}'
            polygon = region_polygon(region, transform[1] if transform else shape)
            if not polygon.size:
                continue
            if inverse is not None:
                polygon = cv2.perspectiveTransform(polygon.reshape(1,-1,2),inverse)[0]
            boxes.append(dict(class_name=label, inspection_pass=ok, polygon=polygon.tolist(),
                              xyxy=[*polygon.min(axis=0), *polygon.max(axis=0)]))
        return boxes

    def _outstanding_items(self, frame):
        remaining = self.item_required-self.window_results.successes
        outstanding = []
        for kind,key in sorted(remaining):
            outstanding.extend(self.window_results.last_failures.get((kind,key),
                self.window_results.last_failures.get((kind,None),[{
                'class_name':f'{kind}_item_pending','inspection_kind':kind,'item_key':key,
                'special_failure':True,'confidence':1.,'xyxy':[0,0,frame.shape[1],frame.shape[0]],
                'expected_text':key,'recognized_text':'not yet passed'}])))
        self.status_lines = [f"Items: {'PENDING' if remaining else 'OK'} | {len(self.item_required)-len(remaining)}/{len(self.item_required)} OK"]
        return outstanding

    def check_frame(self, frame, number, total, valid_mask=None):
        if getattr(self, "labeled_frames_only", False):
            references = self.references
            active = {kind: schedule[number] for kind, schedule in self.annotation_frames.items() if number in schedule}
            if getattr(self,"conn_inspection_range",None) is not None:
                active.pop("conn",None)
                reference = self.conn_reference(number)
                if reference is not None:
                    active["conn"] = reference
            self.references = active
            try:
                failures = self.check(frame, number, valid_mask)
            finally:
                self.references = references
            failures = self._confirm_conn(failures, number)
            if hasattr(self,'item_required'):
                for detail in getattr(self,'pass_details',[]):
                    key = (detail.get('inspection_kind'),detail.get('item_key'))
                    if key in self.item_required and not any(
                            f.get('item_key') in (None,key[1]) and f.get('inspection_kind') == key[0] for f in failures):
                        self.window_results.successes.add(key)
                        self.item_pass_records.setdefault(key,[]).append(dict(detail,source_frame_number=number))
                for failure in failures:
                    key = (failure.get('inspection_kind'),failure.get('item_key'))
                    self.window_results.last_failures[key] = [failure]
                outstanding = self._outstanding_items(frame)
                current = getattr(self,'pass_details',[])
                current_keys = {(d.get('inspection_kind'),d.get('item_key')) for d in current}
                history = [dict(d,historical=True) for key,records in self.item_pass_records.items()
                           if key not in current_keys for d in records]
                self.detail_detections = failures + current + history
                return outstanding
            self.status_lines = [f"{kind}: " + (
                "NG" if any(failure.get("inspection_kind") == kind for failure in failures) else
                "PENDING (1/2 consecutive frames)" if kind == "conn" and self.conn_confirmation.pending else "OK")
                + f" (labeled frame {number})" if kind in active else f"{kind}: SKIPPED (no label on frame {number})"
                for kind in self.annotation_frames]
            self.detail_detections = failures + self.conn_confirmation.pending + getattr(self,"pass_details",[])
            if getattr(self,"conn_inspection_range",None) is not None:
                self.status_lines = [line for line in self.status_lines if not line.startswith("conn:")]
                start,end = self.conn_inspection_range
                state = ("NG" if any(f.get("inspection_kind") == "conn" for f in failures) else
                         "PENDING (1/2 consecutive frames)" if self.conn_confirmation.pending else "OK")
                if "conn" not in active:
                    state = "OUTSIDE RANGE"
                self.status_lines.append(f"conn: {state} | frames {start}~{end}; nearest labeled conditions")
            return failures
        from inspection.frame_conditions import WindowResults
        if not hasattr(self, "window_results"):
            self.window_results = WindowResults()
        raw = self._confirm_conn(self.check(frame, number, valid_mask), number)
        self.detail_detections = raw
        output = [failure for failure in raw if failure.get("inspection_kind") == "conn"]
        self.window_results.status_lines = []
        for kind, reference in self.references.items():
            start, end = self.times[kind]
            if kind == "conn":
                active = number >= start and (end is None or number <= end)
                state = "NG" if output else "PENDING (1/2 consecutive frames)" if self.conn_confirmation.pending else "OK"
                self.window_results.status_lines.append(f"Conn: {state if active else 'OUTSIDE RANGE'}")
                continue
            items = ([f"{region['role']}_{i}" for i, region in enumerate(reference["regions"]) if region["role"] in ("text","mark")]
                     if kind == "OCR" else [f"pair_{odd}" for odd in (1, 3, 5, 7)
                     if any(region.get("number") in (odd, odd + 1) and region["role"] in ("qr", "text")
                            for region in reference["regions"])] +
                     [f"text_{number}" for number in (None, 9, 0, *range(10, 21)) if any(
                         region["role"] == "text" and region.get("number") == number for region in reference["regions"])])
            failures = [failure for failure in raw if failure.get("inspection_kind") == kind]
            output.extend(self.window_results.update(kind, items, failures, number, start, end, total))
        self.status_lines = self.window_results.status_lines
        self.detail_detections = output + getattr(self,"pass_details",[]) + [failure for failure in raw if failure not in output
            and (failure.get("inspection_kind"), failure.get("item_key")) not in self.window_results.successes]
        return output

    def check(self, frame, seconds: float, valid_mask=None) -> list[dict]:
        failures = []
        self.pass_details = []
        self.conn_visuals = None
        self.mark_visuals = []
        self.active_registration = {}
        from inspection.performance import Performance
        performance = getattr(self,"performance",None) or Performance()
        for kind, reference in performance.items(self.references.items()):
            start, end = self.times[kind]
            if seconds < start or (end is not None and seconds > end):
                continue
            regions = reference["regions"]
            def eligible(key):
                return not hasattr(self,'window_results') or (kind,key) not in self.window_results.successes
            if hasattr(self,'item_required') and not any(eligible(key) for key in self.item_keys(kind,reference)):
                continue
            text_model = getattr(self, "text_image_models", {}).get(kind)
            if text_model is not None:
                regions = [dict(region, text=text_model.expected_for(region) or region.get("text", ""))
                           if region["role"] == "text" and region.get("verdict") == "FAIL" else region
                           for region in regions]
            transform = getattr(self, "reference_transforms", {}).get(kind)
            registration = next((region for region in regions if region["role"] == "registration"), None)
            if registration:
                from inspection.shape_registration import ShapeRegistration
                if not hasattr(self,"shape_registration"):
                    self.shape_registration = ShapeRegistration()
                result = performance.call("reference_registration",self.shape_registration.fit,frame,reference["image"],registration)
                if result is None:
                    failures.append({"class_name":f"{kind}_registration_not_found","confidence":1.,
                        "xyxy":[0,0,frame.shape[1],frame.shape[0]],"special_failure":True,
                        "inspection_kind":kind,"display_label":"Reference area tracking failed",
                        "expected_text":"matching reference shape","recognized_text":"not found"})
                    continue
                transform = (np.linalg.inv(result[0]),result[1][:2])
                self.active_registration[kind] = transform
            matrix = transform[0] if transform else None
            kind_frame = frame
            kind_valid = valid_mask
            if transform:
                height, width = transform[1]
                kind_frame = cv2.warpPerspective(frame, matrix, (width, height))
                input_valid = valid_mask if valid_mask is not None else np.full(frame.shape[:2], 255, dtype=np.uint8)
                kind_valid = cv2.warpPerspective(input_valid, matrix, (width, height), flags=cv2.INTER_NEAREST)
            masks = [region for region in regions if region["role"] == "exclude"]
            working_frame = mask_regions(kind_frame, masks)
            item_key = None
            def fail(reason, region=None):
                box = pixel_box(region, kind_frame.shape) if region else (0, 0, frame.shape[1], frame.shape[0])
                if matrix is not None and region is not None:
                    from inspection.frame_alignment import project_box
                    box = project_box(box, np.linalg.inv(matrix), frame.shape)
                failures.append({"class_name": reason, "confidence": 1.0, "xyxy": box, "special_failure": True,
                                 "inspection_kind": kind, "item_key": item_key})
                if region and region.get("angle", 0) and matrix is None:
                    from inspection.rotated_regions import region_polygon
                    failures[-1]["polygon"] = region_polygon(region, frame.shape).tolist()
            def passed(label, region, expected, recognized):
                from inspection.rotated_regions import region_polygon
                polygon = region_polygon(region,kind_frame.shape)
                if matrix is not None:
                    polygon = cv2.perspectiveTransform(polygon.reshape(1,-1,2),np.linalg.inv(matrix))[0]
                left,top = polygon.min(axis=0)
                right,bottom = polygon.max(axis=0)
                self.pass_details.append({"class_name":label,"display_label":label,"inspection_kind":kind,
                    "item_key":item_key,
                    "inspection_pass":True,"confidence":1.,"xyxy":[float(left),float(top),float(right),float(bottom)],
                    "polygon":polygon.tolist(),"expected_text":expected,"recognized_text":recognized})
            def crop(region):
                rois = [] if registration else reference.get("rois", [])
                if rois:
                    from inspection.polygon_roi import containing_roi
                    roi = containing_roi(region, rois, kind_frame.shape)
                    if roi is not None:
                        key = id(roi)
                        if key not in aligned_rois:
                            aligned_rois[key] = self.polygon_aligner.align(kind_frame, reference["image"], roi)
                        aligned = aligned_rois[key]
                        if aligned is None:
                            if key not in failed_rois:
                                fail(f"{kind}_roi_not_found", region)
                                failures[-1]["display_label"] = "ROI tracking failed"
                                failed_rois.add(key)
                            return np.empty((0,0,3), np.uint8)
                        from inspection.text_image_model import text_crop
                        return text_crop(mask_regions(aligned,masks), region)
                    if len(rois) >= 3:
                        key = "corner_anchors"
                        if key not in aligned_rois:
                            aligned_rois[key] = self.polygon_aligner.align_anchors(kind_frame,reference["image"],rois)
                        aligned = aligned_rois[key]
                        if aligned is None:
                            if key not in failed_rois:
                                fail(f"{kind}_roi_not_found",region)
                                failures[-1]["display_label"] = "Corner ROI tracking failed"
                                failed_rois.add(key)
                            return np.empty((0,0,3),np.uint8)
                        from inspection.text_image_model import text_crop
                        return text_crop(mask_regions(aligned,masks),region)
                if region.get("angle", 0):
                    from inspection.rotated_regions import crop_rotated_region
                    return crop_rotated_region(working_frame, region)
                x1, y1, x2, y2 = pixel_box(region, kind_frame.shape)
                return working_frame[y1:y2, x1:x2]
            def recognize_text(region, expected=None):
                image = crop(region)
                model = getattr(self, "text_image_models", {}).get(kind)
                if model is not None and region.get("verdict") in ("PASS", "FAIL") and not variable_text(region):
                    prediction = performance.call("text_image_comparison."+kind,model.predict,image, region.get("number")) if image.size else {"verdict":"UNKNOWN", "text":None}
                    # UNKNOWN defers to exact OCR/QR checks below; a confident FAIL is a veto.
                    if prediction["verdict"] == "FAIL":
                        fail(f"{kind}_text_image_{prediction['verdict'].lower()}", region)
                        failures[-1].update(expected_text="PASS image", recognized_text=prediction,
                                            display_label=f"Text image {prediction['verdict']}")
                        if id(region) in recognition_cache:
                            failures[-1]["recognized_text"] = dict(prediction,ocr=recognition_cache[id(region)])
                    recognizer = getattr(self, "trained_text_ocr", {}).get(kind)
                    if id(region) in recognition_cache:
                        return recognition_cache[id(region)]
                    if recognizer is not None:
                        return performance.call("text_recognition."+kind,recognizer.candidates,image, len(region.get("text", "").split("\n")))
                if id(region) in recognition_cache:
                    return recognition_cache[id(region)]
                recognizer = getattr(self,'trained_text_ocr',{}).get(kind)
                if recognizer is not None:
                    return performance.call('text_recognition.'+kind,recognizer.candidates,image,len(region.get('text','').split('\n')))
                if self.ocr is None:
                    self.ocr = MultilingualOCR()
                if hasattr(self.ocr, "candidates_for"):
                    return performance.call("fallback_ocr."+kind,self.ocr.candidates_for,image, expected)
                return performance.call("fallback_ocr."+kind,self.ocr.candidates,image)
            def read_text(region, expected=None):
                index = next(i for i,r in enumerate(regions) if r is region)
                key = (kind,index)
                cache = getattr(self,'component_reads',{})
                if key in cache:
                    return cache[key]
                before = len(failures)
                values = recognize_text(region,expected)
                if not hasattr(self,'component_reads'):
                    return values
                valid = any(accepts_text(region,value) for value in values)
                if hasattr(self,'component_reads') and valid and len(failures) == before and not any(
                        f.get('inspection_kind') == kind and f.get('item_key') is None for f in failures):
                    cache[key] = values
                return values

            aligned_rois = {}
            failed_rois = set()
            recognition_cache = {}
            missing = False
            if kind_valid is not None:
                for index,region in enumerate(regions):
                    if region["role"] not in ("qr", "text", "mark", "slot"):
                        continue
                    if hasattr(self,'item_required') and not eligible(self.region_item_key(kind,region,index)):
                        continue
                    x1, y1, x2, y2 = pixel_box(region, kind_frame.shape)
                    visible = kind_valid[y1:y2, x1:x2]
                    if not visible.size or np.count_nonzero(visible) / visible.size < 0.98:
                        fail(f"{kind}_region_not_visible", region)
                        missing = True
            if missing:
                continue
            recognizer = getattr(self, "trained_text_ocr", {}).get(kind)
            if recognizer is not None and hasattr(recognizer,"candidates_batch"):
                text_regions = [region for index,region in enumerate(regions) if region["role"] == "text"
                                and eligible(self.region_item_key(kind,region,index))
                                and (kind,index) not in getattr(self,'component_reads',{})]
                requests = [dict(region,check=variable_text(region),image=crop(region),line_count=len(region.get("text", "").split("\n")))
                            for region in text_regions]
                recognition_cache = dict(zip(map(id,text_regions),performance.call("text_recognition_batch."+kind,recognizer.candidates_batch,requests)))
            if kind == "OCR":
                mark_predictions = {}
                model = getattr(self,"mark_model",None)
                mark_regions = [region for index,region in enumerate(regions) if region["role"] == "mark"
                    and eligible(f"mark_{index}")]
                if model is not None and hasattr(model,"predict_many") and mark_regions:
                    predictions = performance.call("mark_comparison",model.predict_many,[(crop(region),region) for region in mark_regions])
                    mark_predictions = dict(zip(map(id,mark_regions),predictions))
                mark_number = 0
                for index, region in enumerate(regions):
                    if region["role"] == "mark":
                        mark_number += 1
                        item_key = f"mark_{index}"
                        if not eligible(item_key):
                            continue
                        model = getattr(self,"mark_model",None)
                        if model is None:
                            raise RuntimeError("Mark model is missing. Train the selected group first.")
                        prediction = mark_predictions.get(id(region))
                        if prediction is None:
                            prediction = performance.call("mark_comparison",model.predict,crop(region),region)
                        box = pixel_box(region,kind_frame.shape)
                        if matrix is not None:
                            from inspection.frame_alignment import project_box
                            box = project_box(box,np.linalg.inv(matrix),frame.shape)
                        self.mark_visuals.append({"xyxy":box,"number":mark_number,"verdict":prediction["verdict"]})
                        if prediction["verdict"] != "OK":
                            fail(f"OCR_mark_{mark_number}_mismatch",region)
                            failures[-1].update(display_label=f"mark {mark_number} NG",mark_number=mark_number,
                                expected_text="trained normal mark",recognized_text=prediction)
                        else:
                            passed(f"Mark {mark_number} OK",region,"trained normal mark",prediction)
                    if region["role"] == "text":
                        item_key = f"text_{index}"
                        if not eligible(item_key):
                            continue
                        before = len(failures)
                        candidates = read_text(region, None if variable_text(region) else region["text"])
                        expected = {'pattern':region.get('pattern',[])} if variable_text(region) else region['text']
                        if not any(accepts_text(region,value) for value in candidates):
                            fail('OCR_flexible_condition_missing' if region.get('flexible') and not region.get('pattern') else "OCR_mismatch", region)
                            failures[-1].update(expected_text=expected, recognized_text=candidates)
                        elif len(failures) == before:
                            passed(f"Text {index+1} OK",region,expected,candidates)
            elif kind == "conn":
                from inspection.rotated_regions import region_polygon
                from inspection.conn_geometry import inside_slots, scoped_conn_regions
                training_crop = conn_crop_bounds(regions, working_frame.shape) if self.pin_crop_model else None
                regions = scoped_conn_regions(regions, kind_frame.shape)
                has_slots = any(region["role"] == "slot" for region in regions)
                slots = []
                crop_bounds = None
                full_frame = working_frame
                if self.pin_crop_model:
                    crop_bounds = training_crop
                    x1, y1, x2, y2 = crop_bounds
                    working_frame = working_frame[y1:y2, x1:x2]
                inference_frame = working_frame
                if self.original_resolution:
                    height, width = working_frame.shape[:2]
                    inference_frame = cv2.copyMakeBorder(working_frame, 0, (-height) % 32, 0, (-width) % 32,
                                                         cv2.BORDER_CONSTANT, value=(114, 114, 114))
                image_size = list(inference_frame.shape[:2]) if self.original_resolution else self.image_size
                result = self.pin_model.predict(inference_frame, conf=self.confidence, iou=self.iou,
                                                imgsz=image_size, rect=True, verbose=False)[0]
                boxes = result.obb if getattr(result, "obb", None) is not None else result.boxes
                pins = [tuple(box.xyxy[0].tolist()) for box in boxes]
                scores = [float(value) for value in boxes.conf] if getattr(boxes, "conf", None) is not None else [1.0]*len(pins)
                if crop_bounds is not None:
                    ox, oy = crop_bounds[:2]
                    pins = [(x1 + ox, y1 + oy, x2 + ox, y2 + oy) for x1, y1, x2, y2 in pins]
                    # Whole-frame detections are candidates, not confirmed connector pins.
                    if not has_slots:
                        outside = self.pin_model.predict(full_frame, conf=self.confidence, iou=self.iou,
                                                         imgsz=self.image_size, verbose=False)[0]
                        from inspection.conn_detection import result_boxes, verify_external_pins
                        pins.extend(verify_external_pins(self.pin_model, full_frame, crop_bounds, result_boxes(outside),
                                                        self.confidence, self.iou, self.image_size))
                scores.extend([0.0]*(len(pins)-len(scores)))
                scoped = [(pin, score) for pin, score in zip(pins, scores)
                          if inside_slots(((pin[0]+pin[2])/2, (pin[1]+pin[3])/2), regions, kind_frame.shape)]
                pins = [pin for pin, _ in scoped]
                scores = [score for _, score in scoped]
                from inspection.conn_detection import deduplicate_pins
                raw_count = len(pins)
                pins = deduplicate_pins(pins, scores)
                removed = raw_count-len(pins)
                self.conn_duplicates_removed = getattr(self, "conn_duplicates_removed", 0) + removed
                self.conn_visuals = {"pins": pins, "duplicates_removed": removed}
                pin_regions = [region for region in regions if region["role"] == "pin"]
                from inspection.conn_geometry import area_geometry
                positions = [area_geometry(region, kind_frame.shape) for region in pin_regions]
                from inspection.conn_geometry import center_inside
                if crop_bounds is not None and positions:
                    from inspection.conn_detection import verify_external_pins
                    extras = [pin for pin in pins if not any(
                        center_inside(((pin[0]+pin[2])/2, (pin[1]+pin[3])/2), area) for area in positions)]
                    if extras:
                        verified = verify_external_pins(self.pin_model, full_frame, crop_bounds, extras,
                            self.confidence, self.iou, self.image_size, include_inside=True)
                        pins = [pin for pin in pins if pin not in extras] + verified
                        self.conn_visuals["pins"] = pins
                self.conn_visuals["numbers"] = []
                for x1, y1, x2, y2 in pins:
                    matches = [i+1 for i, area in enumerate(positions) if center_inside(((x1+x2)/2, (y1+y2)/2), area)]
                    self.conn_visuals["numbers"].append(matches[0] if len(matches) == 1 else None)
                unmatched = iter([((p[0]+p[2])/2, (p[1]+p[3])/2) for p in pins if not any(
                    center_inside(((p[0]+p[2])/2, (p[1]+p[3])/2), area) for area in positions)])
                for reason in conn_position_failures(slots, positions, pins):
                    match = re.fullmatch(r"conn_pin_(\d+)_count_(\d+)", reason)
                    target = pin_regions[int(match[1]) - 1] if match else next(
                        (region for region in regions if region["role"] == "slot"), None)
                    fail(reason, target)
                    if match:
                        failures[-1].update(display_label=f"pin {match[1]} NG",
                                            pin_number=int(match[1]),
                                            expected_text="1 pin center inside this area",
                                            recognized_text=f"{match[2]} pins")
                    else:
                        if reason in ("conn_outside_or_boundary", "conn_outside_connector_or_boundary"):
                            center = next(unmatched, None)
                            failures[-1]["evidence_centers"] = [center] if center is not None else []
                        failures[-1]["display_label"] = "Conn NG: extra/outside pin" if reason in (
                            "conn_outside_or_boundary", "conn_outside_connector_or_boundary") else "Conn NG: missing regions"
                centers = [((x1+x2)/2, (y1+y2)/2) for x1, y1, x2, y2 in pins]
                from inspection.conn_geometry import boundary_outside_indices
                outside_indices = boundary_outside_indices(centers, regions, kind_frame.shape)
                if len(outside_indices) >= 2:
                    boundary = next(region for region in regions if region["role"] == "conn_boundary")
                    fail("conn_boundary_outside_pins", boundary)
                    failures[-1].update(display_label=f"Conn NG: {len(outside_indices)} pins outside boundary",
                                        expected_text="Fewer than 2 pin centers outside boundary",
                                        recognized_text=f"{len(outside_indices)} outside", outside_count=len(outside_indices))
            else:
                for odd, text_number in ((1, 2), (3, 4), (5, 6), (7, 8),
                                         *((None, number) for number in (None, 9, 0, *range(10, 21)))):
                    item_key = f"pair_{odd}" if odd is not None else f"text_{text_number}"
                    if not eligible(item_key):
                        continue
                    qr_regions = [region for region in regions if odd is not None and region["role"] == "qr" and region.get("number") == odd]
                    text_regions = [region for region in regions if region["role"] == "text" and region.get("number") == text_number]
                    if not qr_regions and not text_regions:
                        continue
                    before = len(failures)
                    qr_values = []
                    for region in qr_regions:
                        index = next(i for i,r in enumerate(regions) if r is region)
                        key = (kind,index)
                        cache = getattr(self,'component_reads',{})
                        if key in cache:
                            value = cache[key]
                        else:
                            image = crop(region)
                            value = self.qr.detectAndDecode(image)[0] if image.size else ''
                            if value and len(qr_regions) == 1 and hasattr(self,'component_reads') and not any(r.get('flexible') for r in text_regions):
                                offset = 0
                                valid_code = bool(text_regions)
                                for text_region in text_regions:
                                    pattern = text_region.get('pattern',[])
                                    length = len(pattern) if text_region.get('check') else len(text_region['text'])
                                    segment = value[offset:offset+length]
                                    valid_code = valid_code and (matches_format(segment,pattern)
                                        if text_region.get('check') else segment == text_region['text'])
                                    offset += length
                                if valid_code and offset == len(value):
                                    cache[key] = value
                        qr_values.append(value)
                        if region.get('serial_order') and value:
                            if not hasattr(self,'serial_reads'):
                                self.serial_reads = {}
                            self.serial_reads.setdefault(int(region['serial_order']),value)
                    qr_text = "".join(qr_values)
                    candidates = [read_text(region, None if variable_text(region) else region["text"]) for region in text_regions]
                    check_mode = any(variable_text(region) for region in text_regions)
                    if not qr_regions:
                        read_ok = bool(text_regions) and all(
                            any(accepts_text(region,value) for value in values)
                            for region, values in zip(text_regions, candidates))
                    elif check_mode:
                        read_ok = paired_matches(qr_text,text_regions,candidates)
                    else:
                        expected = "".join(region["text"] for region in text_regions)
                        read_ok = qr_text == expected and all(region["text"] in values for region, values in zip(text_regions, candidates))
                    if not text_regions or (qr_regions and not all(qr_values)) or not read_ok:
                        unread = bool(qr_regions) and not all(qr_values)
                        reason = (f"label_pair_{odd}_{text_number}_noreading" if unread else
                                  f"label_pair_{odd}_{text_number}_mismatch" if qr_regions else f"label_text_{text_number}_mismatch")
                        if not qr_regions and any(r.get('flexible') and not r.get('pattern') for r in text_regions):
                            reason = f'label_text_{text_number}_flexible_condition_missing'
                        fail(reason, (qr_regions or text_regions)[0])
                        if unread:
                            failures[-1]['display_label'] = f"QR {odd} noreading"
                        failures[-1].update(expected_text="".join(region.get("text", "") for region in text_regions),
                                            recognized_text={"code": qr_values, "text": candidates})
                        if check_mode:
                            failures[-1]["expected_format"] = [
                                {"position": index + 1, "pattern": region.get("pattern",[]),"flexible":bool(region.get('flexible'))}
                                if variable_text(region) else {"position": index + 1, "text": region["text"]}
                                for index, region in enumerate(text_regions)]
                    elif len(failures) == before:
                        for region,value in zip(qr_regions,qr_values):
                            passed(f"QR {region.get('number','')} OK",region,"matching paired text",{"code":[value],"text":candidates})
                        for region,values in zip(text_regions,candidates):
                            expected = {"pattern":region.get("pattern",[])} if variable_text(region) else region["text"]
                            passed(f"Text {region.get('number','')} OK",region,expected,{"code":qr_values,"text":values})
        return failures
