"""Fit a bounded similarity transform using features inside a user reference area."""
from pathlib import Path
from functools import lru_cache

import cv2
import numpy as np


@lru_cache(maxsize=32)
def reference_features(path, modified_ns, box, angle):
    reference = cv2.imread(path)
    if reference is None:
        return None
    scale = min(1.,1400/max(reference.shape[:2]))
    gray = cv2.resize(cv2.cvtColor(reference,cv2.COLOR_BGR2GRAY),None,fx=scale,fy=scale)
    mask = None
    if box:
        from inspection.rotated_regions import region_polygon
        mask = np.zeros(gray.shape,np.uint8)
        area = dict(box=box,angle=angle)
        cv2.fillPoly(mask,[np.round(region_polygon(area,reference.shape)*scale).astype(np.int32)],255)
    features = cv2.SIFT_create(nfeatures=3500,contrastThreshold=.02)
    points,descriptors = features.detectAndCompute(gray,mask)
    return reference.shape,scale,points,descriptors


class ShapeRegistration:
    def __init__(self, min_inlier_ratio=.6, tracking=True):
        self.min_inlier_ratio = min_inlier_ratio
        self.features = cv2.SIFT_create(nfeatures=3500, contrastThreshold=.02)
        self.cache = {}
        self.current_frame = None
        self.track = None
        self.tracking = tracking
        self.stats = dict(full_extractions=0,flow_attempts=0,flow_accepted=0,flow_fallbacks=0)

    def _flow(self, gray):
        if self.track is None or self.track[0].shape != gray.shape:
            return None
        previous,points,values = self.track
        self.stats['flow_attempts'] += 1
        target,status,error = cv2.calcOpticalFlowPyrLK(previous,gray,points,None,
            winSize=(31,31),maxLevel=4,criteria=(cv2.TERM_CRITERIA_EPS|cv2.TERM_CRITERIA_COUNT,30,.01))
        if target is None:
            return None
        back,reverse,_ = cv2.calcOpticalFlowPyrLK(gray,previous,target,None,
            winSize=(31,31),maxLevel=4)
        if back is None:
            return None
        keep = (status.ravel() > 0) & (reverse.ravel() > 0) & (error.ravel() < 25)
        keep &= np.linalg.norm(back-points,axis=2).ravel() < 1.5
        if keep.sum() < 30 or keep.mean() < .5:
            return None
        positions = target[keep].reshape(-1,2)
        if cv2.contourArea(cv2.convexHull(positions)) < 256:
            return None
        return positions,values[keep]

    def _full(self, gray, scale):
        current,values = self.features.detectAndCompute(gray,None)
        self.stats['full_extractions'] += 1
        positions = np.float32([point.pt for point in current]).reshape(-1,2)
        self.current_features = scale,positions,values
        self.flow_used = False

    def _remember(self, gray, positions, values):
        indices = np.arange(len(positions))
        self.track = gray,positions[indices].reshape(-1,1,2).copy(),values[indices].copy()

    def prepare_frame(self, frame):
        if self.current_frame is frame:
            return
        scale = min(1.,1400/max(frame.shape[:2]))
        gray = cv2.resize(cv2.cvtColor(frame,cv2.COLOR_BGR2GRAY),None,fx=scale,fy=scale)
        tracked = self._flow(gray) if self.tracking else None
        self.current_frame,self.current_gray = frame,gray
        self.flow_used = tracked is not None
        if tracked is not None:
            self.current_features = scale,*tracked
        else:
            self._full(gray,scale)

    def fork(self, frame):
        import copy
        self.prepare_frame(frame)
        child = copy.copy(self)
        child.cache = dict(self.cache)
        child.features = cv2.SIFT_create(nfeatures=3500,contrastThreshold=.02)
        child.stats = {key:0 for key in self.stats}
        child.track = None
        return child

    def merge_forks(self, children):
        selected = None
        for child in children:
            self.cache.update(child.cache)
            for key,value in child.stats.items():
                self.stats[key] += value
            if selected is None and child.track is not None:
                selected = child
        while len(self.cache) > 32:
            self.cache.pop(next(iter(self.cache)))
        if selected is not None:
            self.track = selected.track
            self.current_features = selected.current_features
            self.flow_used = selected.flow_used
        elif children:
            self.track = None

    def fit(self, frame, reference_path, area=None):
        path = Path(reference_path)
        key = (str(path), path.stat().st_mtime_ns, tuple(area.get("box", ())) if area else (),
               float(area.get("angle", 0)) if area else 0)
        if key not in self.cache:
            value = reference_features(*key)
            if value is None:
                return None
            if len(self.cache) >= 32:
                self.cache.pop(next(iter(self.cache)))
            self.cache[key] = value
        shape, ref_scale, points, descriptors = self.cache[key]
        if descriptors is None or len(points) < 8:
            return None
        self.prepare_frame(frame)
        result = self._match(descriptors,points,ref_scale)
        if result is None and self.flow_used:
            self.stats['flow_fallbacks'] += 1
            self._full(self.current_gray,self.current_features[0])
            result = self._match(descriptors,points,ref_scale)
        if result is None:
            self.track = None
            return None
        if self.flow_used:
            self.stats['flow_accepted'] += 1
        _,positions,values = self.current_features
        self._remember(self.current_gray,positions,values)
        return result,shape

    def _match(self, descriptors, points, ref_scale):
        scale,current,values = self.current_features
        if values is None or len(current) < 8:
            return None
        pairs = cv2.BFMatcher(cv2.NORM_L2).knnMatch(descriptors, values, k=2)
        matches = [pair[0] for pair in pairs if len(pair) == 2 and pair[0].distance < .7*pair[1].distance]
        if len(matches) < 8:
            return None
        source = np.float32([points[m.queryIdx].pt for m in matches])/ref_scale
        target = np.float32([current[m.trainIdx] for m in matches])/scale
        matrix, inliers = cv2.estimateAffinePartial2D(source, target, method=cv2.RANSAC,
                                                     ransacReprojThreshold=3/scale)
        if matrix is None or inliers is None or inliers.sum() < 8 or inliers.mean() < self.min_inlier_ratio:
            return None
        support = source[inliers.ravel().astype(bool)]
        if cv2.contourArea(cv2.convexHull(support)) < 64:
            return None
        gain = np.linalg.norm(matrix[:2, 0])
        if not np.isfinite(matrix).all() or not .25 < gain < 4:
            return None
        # Reference-to-current; all labels share this one transform.
        return np.vstack((matrix, [0, 0, 1]))


def transform_region(region, matrix, source_shape, target_shape):
    import copy
    result = copy.deepcopy(region)
    source_size = np.array([source_shape[1], source_shape[0]])
    target_size = np.array([target_shape[1], target_shape[0]])
    def project(points):
        return cv2.perspectiveTransform(np.float32(points).reshape(1, -1, 2), matrix)[0]
    if "points" in result:
        from inspection.rotated_regions import clip_polygon
        points = clip_polygon(project(np.asarray(result["points"])*source_size), target_shape)
        if len(points) < 3:
            raise ValueError("Transformed ROI is outside the target frame")
        result["points"] = (points/target_size).tolist()
    else:
        x1, y1, x2, y2 = np.asarray(result["box"])*np.tile(source_size, 2)
        center = project([[(x1+x2)/2, (y1+y2)/2]])[0]
        gain = np.linalg.norm(matrix[:2, 0])
        half = np.array([x2-x1, y2-y1])*gain/2
        result["box"] = (np.concatenate((center-half, center+half))/np.tile(target_size, 2)).tolist()
        result["angle"] = float(result.get("angle", 0)+np.degrees(np.arctan2(matrix[1, 0], matrix[0, 0])))
        result["clip_to_frame"] = True
        from inspection.rotated_regions import region_polygon
        if len(region_polygon(result, target_shape)) < 3:
            raise ValueError("Transformed box is outside the target frame")
    return result
