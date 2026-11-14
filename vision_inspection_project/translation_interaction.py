"""Shared selection and center-handle translation in image pixels."""
import copy
import numpy as np
from inspection.rotated_regions import region_polygon,raw_region_polygon,clip_polygon,point_in_region


class TranslationInteraction:
    def __init__(self):
        self.selected = []
        self.baseline = None

    def geometry(self, regions, shape):
        self.selected = [i for i in self.selected if i < len(regions)]
        polygons = [region_polygon(regions[i],shape) for i in self.selected]
        points = np.concatenate(polygons) if polygons else None
        return polygons, (points.min(axis=0)+points.max(axis=0))/2 if points is not None else None

    def select(self, point, regions, shape, additive=False):
        hit = next((i for i in reversed(range(len(regions)))
                    if regions[i].get("role") != "auto_exclude"
                    and point_in_region(point,region_polygon(regions[i],shape))),None)
        if not additive:
            self.selected = [] if hit is None else [hit]
        elif hit is not None:
            if hit in self.selected:
                self.selected.remove(hit)
            else:
                self.selected.append(hit)

    def begin(self, point, regions, shape, scale):
        _,center = self.geometry(regions,shape)
        if center is None or np.linalg.norm(np.asarray(point)-center)*scale > 9:
            return False
        self.start = np.asarray(point,dtype=np.float64)
        self.baseline = copy.deepcopy(regions)
        return True

    def move(self, point, regions, shape):
        if self.baseline is None:
            return
        dx,dy = np.asarray(point,dtype=np.float64)-self.start
        candidates = {}
        for i in self.selected:
            candidate = copy.deepcopy(self.baseline[i])
            if "points" in candidate:
                candidate["points"] = (np.asarray(candidate["points"])+[dx/shape[1],dy/shape[0]]).tolist()
            else:
                x1,y1,x2,y2 = candidate["box"]
                candidate["box"] = [x1+dx/shape[1],y1+dy/shape[0],x2+dx/shape[1],y2+dy/shape[0]]
            polygon = raw_region_polygon(candidate,shape)
            if not np.isfinite(polygon).all():
                return
            clipped = clip_polygon(polygon,shape)
            if len(clipped) < 3 or abs(np.sum(clipped[:,0]*np.roll(clipped[:,1],-1)-clipped[:,1]*np.roll(clipped[:,0],-1))) < 1e-6:
                return
            if "points" in candidate:
                candidate["points"] = (clipped/[shape[1],shape[0]]).tolist()
            elif (polygon < 0).any() or (polygon[:,0] >= shape[1]).any() or (polygon[:,1] >= shape[0]).any():
                candidate["clip_to_frame"] = True
            candidates[i] = candidate
        for i,candidate in candidates.items():
            regions[i].clear()
            regions[i].update(candidate)

    def finish(self):
        baseline,self.baseline = self.baseline,None
        return baseline
