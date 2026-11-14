"""White-label illumination normalization and bounded glyph-shape comparison."""
import cv2
import numpy as np

SHAPE = (64,256)


def ink_map(image):
    if image is None or not image.size:
        raise ValueError("Empty text image")
    gray = cv2.cvtColor(image,cv2.COLOR_BGR2GRAY) if image.ndim == 3 else image.copy()
    if gray.shape[0] > gray.shape[1]*1.5:
        gray = cv2.rotate(gray,cv2.ROTATE_90_COUNTERCLOCKWISE)
    gray = cv2.resize(gray,(SHAPE[1],SHAPE[0]),interpolation=cv2.INTER_CUBIC)
    background = cv2.morphologyEx(gray,cv2.MORPH_CLOSE,cv2.getStructuringElement(cv2.MORPH_ELLIPSE,(25,25)))
    dark = np.maximum(background.astype(np.float32)-gray.astype(np.float32),0)
    amplitude = max(float(np.percentile(dark,99)),24.)
    ink = np.clip(dark/amplitude,0,1)
    ink[ink < .06] = 0
    return ink.astype(np.float32)


def align_ink(reference, query):
    warp = np.eye(2,3,dtype=np.float32)
    if np.count_nonzero(reference > .25) < 4 or np.count_nonzero(query > .25) < 4:
        return query,False
    try:
        _,warp = cv2.findTransformECC(reference,query,warp,cv2.MOTION_EUCLIDEAN,
            (cv2.TERM_CRITERIA_COUNT|cv2.TERM_CRITERIA_EPS,35,1e-4),None,3)
        angle = abs(np.degrees(np.arctan2(warp[1,0],warp[0,0])))
        if angle > 2 or abs(warp[0,2]) > 4 or abs(warp[1,2]) > 4:
            return query,False
        aligned = cv2.warpAffine(query,warp,(SHAPE[1],SHAPE[0]),
            flags=cv2.INTER_LINEAR|cv2.WARP_INVERSE_MAP,borderMode=cv2.BORDER_CONSTANT,borderValue=0)
        return aligned,True
    except cv2.error:
        return query,False


def compare_ink(reference, query):
    aligned,registered = align_ink(reference,query)
    a,b = (reference > .25).astype(np.uint8),(aligned > .25).astype(np.uint8)
    if a.sum() < 4 or b.sum() < 4:
        return {"distance":1.,"missing_component":1.,"aligned":registered}
    da = cv2.distanceTransform(1-a,cv2.DIST_L2,cv2.DIST_MASK_PRECISE)
    db = cv2.distanceTransform(1-b,cv2.DIST_L2,cv2.DIST_MASK_PRECISE)
    distance = float((np.minimum(db[a>0],4).mean()+np.minimum(da[b>0],4).mean())/8)
    # Keep punctuation-sized components: a missing dot must not be diluted by a long word.
    components,labels,stats,_ = cv2.connectedComponentsWithStats(a,8)
    missing = 0.
    clipped = 0
    for label in range(1,components):
        if stats[label,cv2.CC_STAT_AREA] >= 2:
            x,y,w,h,_ = stats[label]
            if x == 0 or y == 0 or x+w == a.shape[1] or y+h == a.shape[0]:
                # A clipped edge fragment is still counted in distance, but is not a complete glyph.
                clipped += 1
                continue
            missing = max(missing,float(np.mean(db[labels == label] > 1.5)))
    return {"distance":distance,"missing_component":missing,"clipped_components":clipped,"aligned":registered}
