import json
from functools import lru_cache

import numpy as np
from PIL import Image, ImageDraw, ImageFont


def failure_detail_lines(detections):
    lines = []
    for detection in detections:
        if "recognized_text" not in detection:
            continue
        lines.append(str(detection["class_name"]))
        if detection.get('source_frame_number') is not None:
            lines.append(f"Passed at test frame: {detection['source_frame_number']}")
        expected = detection.get("expected_format") or detection.get("expected_text", "")
        lines.append("Expected: " + json.dumps(expected, ensure_ascii=False))
        observed = detection["recognized_text"]
        if isinstance(observed,dict) and "verdict" in observed:
            lines.append("Image: " + json.dumps({key:value for key,value in observed.items() if key not in ("text","ocr")},ensure_ascii=False))
            if "ocr" in observed:
                lines.append("OCR: " + json.dumps(observed["ocr"],ensure_ascii=False))
        elif isinstance(observed, dict):
            lines.append("Code: " + json.dumps(observed.get("code", []), ensure_ascii=False))
            lines.append("OCR: " + json.dumps(observed.get("text", []), ensure_ascii=False))
        else:
            lines.append("Read: " + json.dumps(observed, ensure_ascii=False))
    return lines


def draw_pass_labels(frame, details):
    return draw_inspection_labels(frame,[item for item in details if item.get("inspection_pass")])


def draw_inspection_labels(frame, details, *, copy_frame=True):
    import cv2
    output = frame.copy() if copy_frame else frame
    for item in details:
        if item.get('historical'):
            continue
        color = (0,190,0) if item.get("inspection_pass") else (0,190,255) if item.get("confirmation_state") == "pending" else (0,0,255)
        polygon = np.round(item.get("polygon",[])).astype(np.int32)
        if polygon.size:
            cv2.polylines(output,[polygon],True,color,2)
        else:
            x1,y1,x2,y2 = map(round,item["xyxy"])
            cv2.rectangle(output,(x1,y1),(x2,y2),color,2)
        x,y = item["xyxy"][:2]
        label = item.get("display_label",item["class_name"])
        if not item.get("inspection_pass") and not any(value in label.upper() for value in ("NG","FAIL","PENDING")):
            label += " NG"
        cv2.putText(output,label,(max(0,round(x)),max(16,round(y)-4)),
                    cv2.FONT_HERSHEY_SIMPLEX,.45,color,1,cv2.LINE_AA)
    return output


@lru_cache(maxsize=16)
def detail_font(size):
    for name in ("C:/Windows/Fonts/malgun.ttf", "C:/Windows/Fonts/arial.ttf", "DejaVuSans.ttf"):
        try:
            return ImageFont.truetype(name, size)
        except OSError:
            continue
    return ImageFont.load_default()


def wrap_lines(lines, font, width):
    output = []
    for line in lines:
        part = ""
        for character in line:
            if part and font.getlength(part + character) > width:
                output.append(part)
                part = ""
            part += character
        output.append(part)
    return output


def render_failure_details(detections, width=640, status_lines=None):
    pins = [str(item["pin_number"]) for item in detections if "pin_number" in item]
    lines = list(status_lines or []) + (["Conn NG pins: " + ", ".join(pins)] if pins else [])
    lines.extend(failure_detail_lines([item for item in detections if "pin_number" not in item]))
    font = detail_font(16)
    wrapped = wrap_lines(lines or ["No inspection failures"], font, width - 24)
    panel = Image.new("RGB", (width, max(80, 24 + len(wrapped) * 24)), (20, 20, 20))
    draw = ImageDraw.Draw(panel)
    for index, line in enumerate(wrapped):
        draw.text((12, 12 + index * 24), line, font=font, fill=(255, 235, 235))
    return np.asarray(panel)[:, :, ::-1].copy()


def preview_with_details(video, detections, panel_width=480, status_lines=None, detail_height=None):
    import cv2
    height, width = video.shape[:2]
    target_height = detail_height or height
    details = render_failure_details(detections, panel_width, status_lines)
    if details.shape[0] > target_height:
        scale = target_height / details.shape[0]
        details = cv2.resize(details, (max(1, int(panel_width * scale)), target_height),
                             interpolation=cv2.INTER_AREA)
    if detail_height:
        panel = np.full((detail_height, panel_width, 3), 20, dtype=np.uint8)
        panel[:details.shape[0], :details.shape[1]] = details
        panel_width = max(2, round(panel_width * height / detail_height / 2) * 2)
        details = cv2.resize(panel, (panel_width, height), interpolation=cv2.INTER_LINEAR)
    preview = np.full((height, width + panel_width, 3), 20, dtype=np.uint8)
    preview[:, :width] = video
    preview[:details.shape[0], width:width + details.shape[1]] = details
    return preview


def draw_failure_details(frame, detections):
    lines = failure_detail_lines(detections)
    if not lines:
        return frame
    height, width = frame.shape[:2]
    padding = max(3, min(16, width // 40))
    size = max(10, min(64, height // 55))
    max_height = max(1, height // 2)
    while True:
        font = detail_font(size)
        wrapped = wrap_lines(lines, font, max(1, width - padding * 2))
        spacing = max(size + 4, font.getbbox("Ag")[3] + 4)
        if len(wrapped) * spacing + padding * 2 <= max_height or size <= 10:
            break
        size -= 2
    capacity = max(1, (max_height - padding * 2) // spacing)
    if len(wrapped) > capacity:
        wrapped = wrapped[:capacity]
        wrapped[-1] = "More details in result log"
    panel_height = min(height, len(wrapped) * spacing + padding * 2)
    panel = Image.new("RGB", (width, panel_height), (20, 20, 20))
    draw = ImageDraw.Draw(panel)
    for index, line in enumerate(wrapped):
        draw.text((padding, padding + index * spacing), line, font=font, fill=(255, 235, 235))
    frame[height - panel_height:] = np.asarray(panel)[:, :, ::-1]
    return frame
