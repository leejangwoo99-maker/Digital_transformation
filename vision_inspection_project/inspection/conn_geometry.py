import math
import json

from inspection.rotated_regions import region_polygon, point_in_region


def validate_gap_limits(limits):
    result = {}
    for key in ("max_horizontal_px", "max_vertical_px"):
        value = limits.get(key)
        value = None if value is None or str(value).strip() == "" else float(value)
        if value is not None and (not math.isfinite(value) or value < 0):
            raise ValueError("Maximum pin gaps must be nonnegative pixels; blank means no limit.")
        result[key] = value
    return result


def load_gap_limits(directory):
    path = directory / "conditions.json"
    return validate_gap_limits(json.loads(path.read_text(encoding="utf-8")) if path.exists() else {})


def save_gap_limits(directory, limits):
    limits = validate_gap_limits(limits)
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "conditions.json"
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(limits, indent=2), encoding="utf-8")
    temporary.replace(path)


def gap_failures(centers, limits):
    limits = validate_gap_limits(limits)
    if len(centers) < 2:
        return []
    failures = []
    for axis, key, index in (("horizontal", "max_horizontal_px", 0), ("vertical", "max_vertical_px", 1)):
        values = [point[index] for point in centers]
        span = max(values) - min(values)
        maximum = limits[key]
        if maximum is not None and span > maximum + 1e-6:
            failures.append({"axis": axis, "actual": span, "maximum": maximum})
    return failures


def boundary_outside_indices(centers, regions, shape):
    boundaries = [region for region in regions if region["role"] == "conn_boundary"]
    if not boundaries:
        return []
    polygons = [region_polygon(region, shape) for region in boundaries]
    return [index for index, center in enumerate(centers)
            if not any(point_in_region(center, polygon) for polygon in polygons)]


def inside_slots(center, regions, shape):
    slots = [region for region in regions if region["role"] == "slot"]
    return not slots or any(point_in_region(center, region_polygon(region, shape)) for region in slots)


def scoped_conn_regions(regions, shape):
    return [region for region in regions if region["role"] not in ("pin", "pin_point")
            or inside_slots(pin_center(region, shape), regions, shape)]


def shift_region_group(regions, dx, dy, axis, shape):
    import copy
    height, width = shape[:2]
    polygons = [region_polygon(region, shape) for region in regions]
    if not polygons:
        return []
    if axis == "x":
        dx = min(width - max(polygon[:, 0].max() for polygon in polygons),
                 max(-min(polygon[:, 0].min() for polygon in polygons), dx))
        dy = 0
    else:
        dy = min(height - max(polygon[:, 1].max() for polygon in polygons),
                 max(-min(polygon[:, 1].min() for polygon in polygons), dy))
        dx = 0
    result = copy.deepcopy(regions)
    for region in result:
        x1, y1, x2, y2 = region["box"]
        region["box"] = [x1+dx/width, y1+dy/height, x2+dx/width, y2+dy/height]
    return result


def pin_center(region, shape):
    height, width = shape[:2]
    x1, y1, x2, y2 = region["box"]
    return ((x1 + x2) * width / 2, (y1 + y2) * height / 2)


def circle_area(center, edge, shape):
    height, width = shape[:2]
    cx, cy = center[0] * width, center[1] * height
    radius = math.hypot((edge[0] - center[0]) * width, (edge[1] - center[1]) * height)
    radius = min(radius, cx, cy, width - cx, height - cy)
    if radius < 2:
        return None
    return {"role": "pin", "shape": "circle",
            "box": [(cx-radius)/width, (cy-radius)/height, (cx+radius)/width, (cy+radius)/height]}


def area_geometry(region, shape):
    if region.get("shape") == "circle":
        center = pin_center(region, shape)
        radius = (region["box"][2] - region["box"][0]) * shape[1] / 2
        return {"center": center, "radius": radius}
    return region_polygon(region, shape)


def center_inside(center, geometry):
    if isinstance(geometry, dict):
        cx, cy = geometry["center"]
        return math.hypot(center[0] - cx, center[1] - cy) <= geometry["radius"] + 1e-6
    return point_in_region(center, geometry)


def annotation_status(regions, shape):
    points = [region for region in regions if region["role"] == "pin_point"]
    areas = [area_geometry(region, shape) for region in regions if region["role"] == "pin"]
    counts = [0] * len(areas)
    matches = []
    for point in points:
        found = [i for i, area in enumerate(areas) if center_inside(pin_center(point, shape), area)]
        matches.append(found)
        if len(found) == 1:
            counts[found[0]] += 1
    point_pass = [len(found) == 1 and counts[found[0]] == 1 for found in matches]
    return point_pass, [count == 1 for count in counts]
