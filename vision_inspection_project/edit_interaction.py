"""Selection, movement, opposite-handle resizing and own-center rotation."""
import copy
import numpy as np
from rotation_interaction import RotationInteraction
from translation_interaction import TranslationInteraction
from inspection.rotated_regions import region_polygon,raw_region_polygon,point_in_region,polygon_overlaps_rectangle


class EditInteraction:
    def __init__(self):
        self.selected = []
        self.action = None
        self.baseline = None
        self.rotation = RotationInteraction()
        self.translation = TranslationInteraction()

    def geometry(self,regions,shape,scale):
        self.selected = [i for i in self.selected if i < len(regions)]
        self.rotation.selected = list(self.selected)
        polygons,center,rotate = self.rotation.geometry(regions,shape,scale)
        if center is None:
            return [],{}
        points = np.concatenate(polygons)
        left,top = points.min(axis=0)
        right,bottom = points.max(axis=0)
        handles = {"move":center,"rotate":rotate}
        for name,point in (("nw",(left,top)),("n",(center[0],top)),("ne",(right,top)),
                           ("e",(right,center[1])),("se",(right,bottom)),("s",(center[0],bottom)),
                           ("sw",(left,bottom)),("w",(left,center[1]))):
            handles[name] = np.asarray(point)
        if len(self.selected) == 1 and "points" not in regions[self.selected[0]]:
            corners = raw_region_polygon(regions[self.selected[0]],shape)
            for name,point in zip(("nw","ne","se","sw"),corners):
                handles[name] = point
            for name,a,b in (("n",0,1),("e",1,2),("s",2,3),("w",3,0)):
                handles[name] = (corners[a]+corners[b])/2
            if regions[self.selected[0]].get("clip_to_frame"):
                handles = {name:np.clip(point,[0,0],[shape[1]-1,shape[0]-1]) for name,point in handles.items()}
        return polygons,handles

    def begin(self,point,regions,shape,scale,additive=False):
        point = np.asarray(point,dtype=float)
        self.baseline = copy.deepcopy(regions)
        self.start = self.end = point
        _,handles = self.geometry(regions,shape,scale)
        near = sorted(handles,key=lambda key:np.linalg.norm(point-handles[key]))
        handle = near[0] if near and np.linalg.norm(point-handles[near[0]])*scale <= 9 else None
        hit = next((i for i in reversed(range(len(regions))) if regions[i].get("role") != "auto_exclude"
                    and point_in_region(point,region_polygon(regions[i],shape))),None)
        if handle and not additive:
            self.action = handle
        elif hit is not None:
            if additive:
                if hit in self.selected:
                    self.selected.remove(hit)
                else:
                    self.selected.append(hit)
                self.action = "click"
            else:
                if hit not in self.selected:
                    self.selected = [hit]
                self.action = "move"
        else:
            self.action = "select"
            self.additive = additive
            self.previous = list(self.selected)
            if not additive:
                self.selected = []
        if self.action == "rotate":
            self.rotation.begin(handles["rotate"],regions,shape,scale)
        elif self.action == "move":
            self.translation.selected = list(self.selected)
            self.translation.baseline = copy.deepcopy(regions)
            self.translation.start = point
        elif self.action in ("nw","n","ne","e","se","s","sw","w"):
            opposite = {"nw":"se","n":"s","ne":"sw","e":"w","se":"nw","s":"n","sw":"ne","w":"e"}
            self.resize_anchor = handles[opposite[self.action]].astype(float)
            angle = (float(regions[self.selected[0]].get("angle",0))
                     if len(self.selected) == 1 and "points" not in regions[self.selected[0]]
                     and regions[self.selected[0]].get("shape") != "circle" else 0.)
            radians = np.deg2rad(angle)
            self.resize_basis = np.array([[np.cos(radians),-np.sin(radians)],[np.sin(radians),np.cos(radians)]])
            self.resize_start = (handles[self.action]-self.resize_anchor)@self.resize_basis

    def move(self,point,regions,shape):
        if self.action is None:
            return
        self.end = np.asarray(point,dtype=float)
        if self.action == "rotate":
            self.rotation.move(point,regions,shape)
            return
        if self.action == "move":
            self.translation.move(point,regions,shape)
            return
        if self.action in ("select","click"):
            return
        delta = (self.end-self.start)@self.resize_basis
        sx = max(.05,1+delta[0]/self.resize_start[0]) if self.action not in ("n","s") and abs(self.resize_start[0]) > 1e-6 else 1.
        sy = max(.05,1+delta[1]/self.resize_start[1]) if self.action not in ("e","w") and abs(self.resize_start[1]) > 1e-6 else 1.
        candidates = {}
        for i in self.selected:
            region = copy.deepcopy(self.baseline[i])
            ax,ay = sx,sy
            if region.get("shape") == "circle":
                ax = ay = sy if self.action in ("n","s") else sx if self.action in ("e","w") else max(sx,sy)
            if "points" in region:
                points = np.asarray(region["points"])*[shape[1],shape[0]]
                region["points"] = ((((points-self.resize_anchor)@self.resize_basis*[ax,ay])@self.resize_basis.T+self.resize_anchor)/[shape[1],shape[0]]).tolist()
            else:
                x1,y1,x2,y2 = region["box"]
                center = np.array([(x1+x2)*shape[1]/2,(y1+y2)*shape[0]/2])
                center_scale = (ax,ay) if len(self.selected) == 1 else (sx,sy)
                center = ((center-self.resize_anchor)@self.resize_basis*center_scale)@self.resize_basis.T+self.resize_anchor
                cx,cy = center/[shape[1],shape[0]]
                w,h = (x2-x1)*ax/2,(y2-y1)*ay/2
                if min(2*w*shape[1],2*h*shape[0]) < 2:
                    return
                region["box"] = [cx-w,cy-h,cx+w,cy+h]
            polygon = region_polygon(region,shape)
            if len(polygon) < 3 or not np.isfinite(polygon).all() or (polygon < 0).any() or (polygon[:,0] >= shape[1]).any() or (polygon[:,1] >= shape[0]).any():
                return
            candidates[i] = region
        for i,region in candidates.items():
            regions[i].clear()
            regions[i].update(region)

    def finish(self,point,regions,shape):
        self.move(point,regions,shape)
        if self.action == "select":
            low,high = np.minimum(self.start,point),np.maximum(self.start,point)
            found = [i for i,region in enumerate(regions) if region.get("role") != "auto_exclude"
                     and polygon_overlaps_rectangle(region_polygon(region,shape),[*low,*high])]
            self.selected = sorted(set(self.previous+found)) if self.additive else found
        baseline = self.baseline
        self.action = self.baseline = None
        return baseline if baseline != regions else None

    def cancel(self,regions):
        if self.baseline is not None:
            for region,old in zip(regions,self.baseline):
                region.clear()
                region.update(copy.deepcopy(old))
        self.__init__()
