"""Per-label polygon feature alignment; no transform is guessed on failure."""
import cv2
import numpy as np


def polygon_pixels(region, shape):
    return np.asarray(region["points"], np.float32) * [shape[1], shape[0]]


def containing_roi(region, regions, shape):
    x1, y1, x2, y2 = region["box"]
    center = ((x1+x2)*shape[1]/2, (y1+y2)*shape[0]/2)
    return next((roi for roi in regions if roi["role"] == "label_roi" and
                 cv2.pointPolygonTest(polygon_pixels(roi, shape).astype(np.float32), center, False) >= 0), None)


class PolygonAligner:
    def __init__(self):
        self.cache = {}
        self.orb = cv2.ORB_create(nfeatures=1600)

    def align_anchors(self, frame, reference_path, rois):
        """Use several small corner ROIs jointly, rather than requiring Text inside each."""
        if len(rois) < 3:
            return None
        key = ("anchors",reference_path,tuple(tuple(map(tuple,roi["points"])) for roi in rois))
        if key not in self.cache:
            reference = cv2.imread(reference_path,cv2.IMREAD_GRAYSCALE)
            if reference is None:
                return None
            self.cache[key] = reference
        reference = self.cache[key]
        if frame.shape[:2] != reference.shape:
            return None
        current = cv2.cvtColor(frame,cv2.COLOR_BGR2GRAY)
        height,width = current.shape
        source,target = [],[]
        for roi in rois:
            polygon = polygon_pixels(roi,reference.shape)
            x,y,w,h = cv2.boundingRect(polygon.astype(np.float32))
            x,y = max(0,x),max(0,y)
            w,h = min(w,width-x),min(h,height-y)
            if min(w,h) < 5 or w*h > width*height*.05:
                continue
            template = reference[y:y+h,x:x+w]
            if template.std() < 4:
                continue
            margin = max(12,round(max(width,height)*.025))
            left,top = max(0,x-margin),max(0,y-margin)
            right,bottom = min(width,x+w+margin),min(height,y+h+margin)
            scores = cv2.matchTemplate(current[top:bottom,left:right],template,cv2.TM_CCOEFF_NORMED)
            _,score,_,location = cv2.minMaxLoc(scores)
            if score < .8:
                continue
            source.append((left+location[0]+w/2,top+location[1]+h/2))
            target.append((x+w/2,y+h/2))
        if len(source) < 3:
            return None
        source,target = np.float32(source),np.float32(target)
        if abs(cv2.contourArea(cv2.convexHull(target))) < width*height*.005:
            return None
        if len(source) >= 4:
            matrix,inliers = cv2.findHomography(source,target,cv2.RANSAC,2)
        else:
            affine,inliers = cv2.estimateAffinePartial2D(source,target,method=cv2.RANSAC,ransacReprojThreshold=2)
            matrix = np.vstack((affine,[0,0,1])) if affine is not None else None
        if matrix is None or inliers is None or inliers.sum() < 3 or inliers.mean() < .8:
            return None
        bounds = np.float32([[0,0],[width,0],[width,height],[0,height]])
        projected = cv2.perspectiveTransform(bounds.reshape(1,-1,2),matrix)[0]
        if not np.isfinite(projected).all() or not cv2.isContourConvex(projected):
            return None
        area = abs(cv2.contourArea(projected))/(width*height)
        if not .85 < area < 1.15 or np.max(np.linalg.norm(projected-bounds,axis=1)) > max(width,height)*.08:
            return None
        return cv2.warpPerspective(frame,matrix,(width,height))

    def align(self, frame, reference_path, roi):
        key = (reference_path, tuple(map(tuple, roi["points"])))
        if key not in self.cache:
            reference = cv2.imread(reference_path)
            if reference is None:
                return None
            gray = cv2.cvtColor(reference, cv2.COLOR_BGR2GRAY)
            mask = np.zeros(gray.shape, np.uint8)
            polygon = polygon_pixels(roi, gray.shape).astype(np.int32)
            cv2.fillPoly(mask, [polygon], 255)
            points, features = self.orb.detectAndCompute(gray, mask)
            self.cache[key] = (gray.shape, points, features, polygon)
        shape, points, features, polygon = self.cache[key]
        if frame.shape[:2] != shape or features is None:
            return None
        mask = np.zeros(shape, np.uint8)
        cv2.fillPoly(mask, [polygon], 255)
        mask = cv2.dilate(mask, np.ones((61, 61), np.uint8))
        current_points, current_features = self.orb.detectAndCompute(cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY), mask)
        if current_features is None or len(current_features) < 2:
            return None
        matches = cv2.BFMatcher(cv2.NORM_HAMMING).knnMatch(features, current_features, k=2)
        good = [pair[0] for pair in matches if len(pair) == 2 and pair[0].distance < .7*pair[1].distance]
        if len(good) < 8:
            return None
        source = np.float32([current_points[m.trainIdx].pt for m in good])
        target = np.float32([points[m.queryIdx].pt for m in good])
        matrix, inliers = cv2.findHomography(source, target, cv2.RANSAC, 3)
        if matrix is None or inliers is None or inliers.sum() < 8 or inliers.mean() < .6:
            return None
        projected = cv2.perspectiveTransform(polygon.astype(np.float32).reshape(1,-1,2), np.linalg.inv(matrix))[0]
        original_area = abs(cv2.contourArea(polygon))
        ratio = abs(cv2.contourArea(projected)) / max(original_area, 1)
        if not .6 < ratio < 1.6 or not cv2.isContourConvex(projected.astype(np.float32)):
            return None
        return cv2.warpPerspective(frame, matrix, (shape[1], shape[0]))
