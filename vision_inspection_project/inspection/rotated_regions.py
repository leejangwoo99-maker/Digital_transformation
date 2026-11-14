import cv2
import numpy as np


def raw_region_polygon(region, shape):
    height, width = shape[:2]
    if "points" in region:
        return np.asarray(region["points"],np.float32)*[width,height]
    x1, y1, x2, y2 = region["box"]
    cx, cy = (x1 + x2) * width / 2, (y1 + y2) * height / 2
    half_width, half_height = (x2 - x1) * width / 2, (y2 - y1) * height / 2
    corners = np.array([[-half_width, -half_height], [half_width, -half_height],
                        [half_width, half_height], [-half_width, half_height]], np.float32)
    angle = np.deg2rad(0 if region.get("shape") == "circle" else float(region.get("angle", 0)))
    rotation = np.array([[np.cos(angle), -np.sin(angle)], [np.sin(angle), np.cos(angle)]])
    return (corners @ rotation.T + [cx, cy]).astype(np.float32)


def clip_polygon(polygon,shape):
    points = np.asarray(polygon,dtype=np.float64)
    for axis,bound,minimum in ((0,0,True),(0,shape[1]-1,False),(1,0,True),(1,shape[0]-1,False)):
        output = []
        if not len(points):
            break
        for a,b in zip(points,np.roll(points,-1,axis=0)):
            inside_a = a[axis] >= bound if minimum else a[axis] <= bound
            inside_b = b[axis] >= bound if minimum else b[axis] <= bound
            if inside_a:
                output.append(a)
            if inside_a != inside_b:
                output.append(a+(b-a)*((bound-a[axis])/(b[axis]-a[axis])))
        points = np.asarray(output,dtype=np.float64).reshape(-1,2)
    compact = []
    for point in points:
        if not compact or np.linalg.norm(point-compact[-1]) > 1e-6:
            compact.append(point)
    if len(compact) > 1 and np.linalg.norm(compact[0]-compact[-1]) <= 1e-6:
        compact.pop()
    return np.asarray(compact,dtype=np.float32).reshape(-1,2)


def region_polygon(region,shape):
    polygon = raw_region_polygon(region,shape)
    return clip_polygon(polygon,shape) if region.get("clip_to_frame") else polygon


def point_in_region(point, region):
    polygon = np.asarray(region, np.float32)
    if len(polygon) < 3:
        return False
    if polygon.shape == (4,):
        x1, y1, x2, y2 = polygon
        polygon = np.array([[x1, y1], [x2, y1], [x2, y2], [x1, y2]], np.float32)
    return cv2.pointPolygonTest(polygon, tuple(map(float, point)), False) >= 0


def polygon_inside(polygon, area):
    return all(point_in_region(point, area) for point in polygon)


def polygon_overlaps_rectangle(points, box):
    polygon = np.asarray(points,dtype=np.float64)
    x1,y1,x2,y2 = box
    corners = np.array([[x1,y1],[x2,y1],[x2,y2],[x1,y2]])
    if any(x1 <= x <= x2 and y1 <= y <= y2 for x,y in polygon):
        return True
    if any(point_in_region(point,polygon) for point in corners):
        return True
    def cross(a,b,c):
        u,v = b-a,c-a
        return u[0]*v[1]-u[1]*v[0]
    for a,b in zip(polygon,np.roll(polygon,-1,axis=0)):
        for c,d in zip(corners,np.roll(corners,-1,axis=0)):
            if (np.maximum(np.minimum(a,b),np.minimum(c,d)) <=
                    np.minimum(np.maximum(a,b),np.maximum(c,d))+1e-10).all():
                if cross(a,b,c)*cross(a,b,d) <= 1e-12 and cross(c,d,a)*cross(c,d,b) <= 1e-12:
                    return True
    return False


def polygon_border_overlaps_rectangle(points, box):
    """An interior-only selection does not touch the surrounding label."""
    polygon = np.asarray(points,dtype=np.float64)
    x1,y1,x2,y2 = box
    for a,b in zip(polygon,np.roll(polygon,-1,axis=0)):
        low,high = 0.,1.
        for origin,delta,minimum,maximum in zip(a,b-a,(x1,y1),(x2,y2)):
            if abs(delta) < 1e-12:
                if not minimum <= origin <= maximum:
                    break
            else:
                enter,leave = sorted(((minimum-origin)/delta,(maximum-origin)/delta))
                low,high = max(low,enter),min(high,leave)
                if low > high:
                    break
        else:
            return True
    return False


def crop_rotated_region(frame, region):
    polygon = raw_region_polygon(region, frame.shape)
    width = max(1, round(np.linalg.norm(polygon[1] - polygon[0])))
    height = max(1, round(np.linalg.norm(polygon[3] - polygon[0])))
    target = np.array([[0, 0], [width - 1, 0], [width - 1, height - 1], [0, height - 1]], np.float32)
    return cv2.warpPerspective(frame, cv2.getPerspectiveTransform(polygon, target), (width, height))


def row_polygon(parts):
    if len(parts) >= 7 and len(parts) % 2 == 1:
        return np.array(list(map(float, parts[1:])), np.float32).reshape(-1, 2)
    if len(parts) != 5:
        raise ValueError("YOLO row must have 5 (rectangle) or 9 (OBB) values")
    x, y, width, height = map(float, parts[1:])
    return np.array([[x-width/2, y-height/2], [x+width/2, y-height/2],
                     [x+width/2, y+height/2], [x-width/2, y+height/2]], np.float32)


def obb_row(class_id, region, shape):
    polygon = region_polygon(region, shape)
    height, width = shape[:2]
    polygon /= np.array([width, height])
    if np.any(polygon < -1e-6) or np.any(polygon > 1 + 1e-6):
        raise ValueError("Rotated box must be fully inside the image")
    return str(class_id) + " " + " ".join(f"{value:.8f}" for value in polygon.ravel())


def appearance_obb_labels(text, angles, shape, geometry=None):
    lines = [line for line in text.splitlines() if line.strip()]
    if len(angles) != len(lines):
        raise ValueError("Rotation annotations do not match YOLO labels; reopen and save labels")
    rows = []
    for index,(line, angle) in enumerate(zip(lines, angles)):
        parts = line.split()
        x, y, width, height = map(float, parts[1:])
        region = geometry[index] if geometry is not None else {"box": [x-width/2, y-height/2, x+width/2, y+height/2], "angle": angle}
        rows.append(obb_row(parts[0], region, shape))
    return "\n".join(rows) + ("\n" if rows else "")
