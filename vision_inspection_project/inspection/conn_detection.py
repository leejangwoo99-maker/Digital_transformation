import cv2
import numpy as np


def result_boxes(result):
    boxes = result.obb if getattr(result, "obb", None) is not None else result.boxes
    return [tuple(box.xyxy[0].tolist()) for box in boxes] if boxes is not None else []


def deduplicate_pins(pins, scores=None):
    """Suppress strongly overlapping, nearby hypotheses, not pins sharing an area."""
    order = sorted(range(len(pins)), key=lambda i: scores[i], reverse=True) if scores is not None else range(len(pins))
    kept = []
    for i in order:
        box = pins[i]
        width, height = box[2]-box[0], box[3]-box[1]
        duplicate = False
        for old in kept:
            old_width, old_height = old[2]-old[0], old[3]-old[1]
            overlap = max(0, min(box[2],old[2])-max(box[0],old[0])) * max(0,min(box[3],old[3])-max(box[1],old[1]))
            union = width*height + old_width*old_height - overlap
            near = (abs((box[0]+box[2]-old[0]-old[2])/2) <= .6*min(width,old_width)
                    and abs((box[1]+box[3]-old[1]-old[3])/2) <= .6*min(height,old_height))
            if union > 0 and overlap/union >= .25 and near:
                duplicate = True
                break
        if not duplicate:
            kept.append(box)
    return kept


def verify_external_pins(model, frame, crop_bounds, candidates, confidence, iou, image_size, include_inside=False):
    """Recheck coarse whole-frame candidates at the connector training scale."""
    x0, y0, x3, y3 = crop_bounds
    width, height = x3-x0, y3-y0
    pending = [box for box in candidates if include_inside or not (x0 <= (box[0]+box[2])/2 <= x3
                                                and y0 <= (box[1]+box[3])/2 <= y3)]
    verified = []
    while pending:
        box = pending[0]
        cx, cy = (box[0]+box[2])/2, (box[1]+box[3])/2
        left, top = round(cx-width/2), round(cy-height/2)
        right, bottom = left+width, top+height
        group = [item for item in pending if left <= (item[0]+item[2])/2 <= right
                 and top <= (item[1]+item[3])/2 <= bottom]
        pending = [item for item in pending if item not in group]
        tile = np.full((height, width, 3), 114, np.uint8)
        sx1, sy1 = max(0, left), max(0, top)
        sx2, sy2 = min(frame.shape[1], right), min(frame.shape[0], bottom)
        if sx2 <= sx1 or sy2 <= sy1:
            continue
        tile[sy1-top:sy2-top, sx1-left:sx2-left] = frame[sy1:sy2, sx1:sx2]
        result = model.predict(tile, conf=confidence, iou=iou, imgsz=image_size, rect=True, verbose=False)[0]
        for px1, py1, px2, py2 in result_boxes(result):
            candidate = (px1+left, py1+top, px2+left, py2+top)
            px, py = (candidate[0]+candidate[2])/2, (candidate[1]+candidate[3])/2
            if (not include_inside and x0 <= px <= x3 and y0 <= py <= y3) or not (0 <= px < frame.shape[1] and 0 <= py < frame.shape[0]):
                continue
            if not any(b[0] <= px <= b[2] and b[1] <= py <= b[3] for b in group):
                continue
            if not any(abs((old[0]+old[2])/2-px) < 2 and abs((old[1]+old[3])/2-py) < 2 for old in verified):
                verified.append(candidate)
    return verified
