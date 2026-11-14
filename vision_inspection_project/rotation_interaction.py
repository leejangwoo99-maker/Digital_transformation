"""Shared image-pixel interaction for independently centered label rotation."""
import math
import copy

import numpy as np

from inspection.rotated_regions import region_polygon, raw_region_polygon, clip_polygon, polygon_overlaps_rectangle


class RotationInteraction:
    def __init__(self):
        self.clear()

    def clear(self):
        self.selected = []
        self.start = None
        self.end = None
        self.baseline = None

    def geometry(self, regions, shape, scale):
        polygons = [region_polygon(regions[i], shape) for i in self.selected]
        if not polygons:
            return [], None, None
        points = np.concatenate(polygons)
        center = (points.min(axis=0) + points.max(axis=0)) / 2
        handle = np.array([center[0], points[:, 1].min() - 26 / scale])
        # Keep the shared handle reachable for labels near an image edge.
        handle = np.clip(handle, [0, 0], [shape[1] - 1, shape[0] - 1])
        if np.linalg.norm(handle - center) < 10 / scale:
            handle = np.array([min(shape[1] - 1, center[0] + 30 / scale), center[1]])
        return polygons, center, handle

    def begin(self, point, regions, shape, scale):
        _, center, handle = self.geometry(regions, shape, scale)
        if handle is not None and np.linalg.norm(np.asarray(point) - handle) * scale <= 11:
            self.baseline = {i: regions[i].get("angle", 0) for i in self.selected}
            self.point_baseline = {i:copy.deepcopy(regions[i]["points"]) for i in self.selected if "points" in regions[i]}
            self.pivot = center
            self.last_angle = math.atan2(point[1] - center[1], point[0] - center[0])
            self.delta = 0.0
        else:
            self.selected = []
            self.start = self.end = point

    def move(self, point, regions, shape):
        if self.baseline is None:
            if self.start is not None:
                self.end = point
            return False
        angle = math.atan2(point[1] - self.pivot[1], point[0] - self.pivot[0])
        self.delta += math.degrees((angle - self.last_angle + math.pi) % (2 * math.pi) - math.pi)
        self.last_angle = angle
        candidates = {i: dict(regions[i], angle=old if regions[i].get("shape") == "circle" else (old + self.delta) % 360)
                      for i, old in self.baseline.items()}
        radians = math.radians(self.delta)
        rotation = np.array([[math.cos(radians),-math.sin(radians)],[math.sin(radians),math.cos(radians)]])
        for i,points in self.point_baseline.items():
            pixels = np.asarray(points)*[shape[1],shape[0]]
            center = (pixels.min(axis=0)+pixels.max(axis=0))/2
            candidates[i] = dict(regions[i],points=(((pixels-center)@rotation.T+center)/[shape[1],shape[0]]).tolist())
        for candidate in candidates.values():
            polygon = raw_region_polygon(candidate, shape)
            if not np.isfinite(polygon).all():
                return False
            clipped = clip_polygon(polygon,shape)
            if len(clipped) < 3 or abs(np.sum(clipped[:,0]*np.roll(clipped[:,1],-1)-clipped[:,1]*np.roll(clipped[:,0],-1))) < 1e-6:
                return False
            if "points" in candidate:
                candidate["points"] = (clipped/[shape[1],shape[0]]).tolist()
            elif (polygon < 0).any() or (polygon[:,0] >= shape[1]).any() or (polygon[:,1] >= shape[0]).any():
                candidate["clip_to_frame"] = True
        for i, candidate in candidates.items():
            if "points" in candidate:
                regions[i]["points"] = candidate["points"]
            else:
                regions[i]["angle"] = candidate["angle"]
                if candidate.get("clip_to_frame"):
                    regions[i]["clip_to_frame"] = True
        return True

    def finish(self, point, regions, shape):
        baseline = self.baseline
        if baseline is None and self.start is not None:
            low = np.minimum(self.start, point)
            high = np.maximum(self.start, point)
            self.selected = [i for i, region in enumerate(regions)
                             if region.get("role") != "auto_exclude"
                             and polygon_overlaps_rectangle(region_polygon(region,shape),
                                                            [low[0],low[1],high[0],high[1]])]
        self.start = self.end = self.baseline = None
        return baseline
