"""Group-scoped editable prelabels; manual rows are never overwritten."""
import json
import time
from functools import lru_cache
from pathlib import Path

import cv2
import numpy as np

from inspection.shape_registration import ShapeRegistration, transform_region
from inspection.essential_shapes import shape_score

DEFECT_CLASSES = {'defect','ng','scratch','dent','breakage','variation','foreign_object'}


def parse_frames(text, count):
    result = set()
    for part in text.replace(" ", ",").split(","):
        if not part:
            continue
        numbers = part.split("-")
        if len(numbers) > 2:
            raise ValueError("Enter frame numbers, for example 1,10,20-25")
        start,end = int(numbers[0]),int(numbers[-1])
        if not 1 <= start <= end <= count:
            raise ValueError(f"Frame numbers must be from 1 to {count}")
        result.update(range(start,end+1))
    return result


def generate(controller, source_numbers=None, progress=None):
    from label_images import YoloBoxLabeler
    paths = list(controller.image_paths)
    label_dir = controller.label_dir
    sources = []
    class_name = lambda region: controller.classes.get(region['class_id'],'').lower()
    loader = YoloBoxLabeler(controller.image_dir,label_dir,controller.classes,image_paths=paths)
    for index,path in enumerate(paths):
        if source_numbers and index+1 not in source_numbers:
            continue
        label_path = label_dir/f"{path.stem}.txt"
        if not label_path.exists() or not label_path.read_text(encoding="utf-8").strip():
            continue
        metadata_path = label_path.with_suffix(".rotation.json")
        if metadata_path.exists() and json.loads(metadata_path.read_text(encoding="utf-8")).get("proto_generated"):
            continue
        loader.index = index
        loader._load_current_image()
        if not loader.boxes:
            continue
        regions = [dict(region,class_id=box[0],essential_group=loader.essential_groups.get(box))
                   for box,region in zip(loader.boxes,loader._rotation_regions())]
        automatic = set(json.loads(metadata_path.read_text(encoding='utf-8')).get('proto_added_indices',[])) if metadata_path.exists() else set()
        automatic_groups = {region['essential_group'] for n,region in enumerate(regions) if n in automatic}
        regions = [region for n,region in enumerate(regions) if n not in automatic
                   and (class_name(region) != 'essential' or
                        (region['essential_group'] is not None and region['essential_group'] not in automatic_groups))]
        if regions:
            sources.append((index,path,loader.image.shape,regions))
    if not sources:
        raise ValueError("No manually labeled source frames selected")
    has_essential = any(region['essential_group'] is not None for _,_,_,regions in sources for region in regions)
    defect_sources = [path for _,path,_,regions in sources
                      if any(class_name(region) in DEFECT_CLASSES for region in regions)]
    model = None
    if defect_sources or not has_essential:
        if not hasattr(controller,"proto_train"):
            raise ValueError("Open this labeler from a selected frame group to train proto-label")
        if progress:
            progress("Training proto model...")
        model = controller.proto_train(defect_sources or [item[1] for item in sources])
    read_reference = lru_cache(maxsize=2)(cv2.imread)
    matcher = ShapeRegistration(min_inlier_ratio=.7)
    generated,skipped,preserved = [],[],[]
    try:
        for index,path in enumerate(paths):
            target = label_dir/f"{path.stem}.txt"
            original = target.read_text(encoding='utf-8') if target.exists() else ''
            loader.index = index
            loader._load_current_image()
            existing = [dict(region,class_id=box[0]) for box,region in zip(loader.boxes,loader._rotation_regions())]
            original_metadata_path = target.with_suffix('.rotation.json')
            original_metadata = json.loads(original_metadata_path.read_text(encoding='utf-8')) if original_metadata_path.exists() else {}
            if progress:
                progress(f"proto-label {index+1}/{len(paths)}")
            image = cv2.imread(str(path))
            if image is None:
                skipped.append({"frame":index+1,"reason":"image cannot be read"})
                continue
            matched = []
            groups = {loader.essential_groups.get(box) for box in loader.boxes}
            essential_additions = []
            for seed,reference,shape,regions in sorted(sources,key=lambda item:abs(item[0]-index)):
                supports = [region for region in regions if controller.classes.get(region["class_id"],"").lower()
                            in ("roi","anchor_1","anchor_2")]
                if not supports:
                    supports = [region for region in regions if controller.classes.get(region["class_id"],"").lower()
                                not in ("exclude","auto_exclude","except")]
                if not supports:
                    continue
                boxes = np.array([region["box"] for region in supports])
                area = {"box":[float(boxes[:,0].min()),float(boxes[:,1].min()),
                               float(boxes[:,2].max()),float(boxes[:,3].max())]}
                fit = matcher.fit(image,reference,area)
                if fit is None:
                    continue
                try:
                    transferred = [transform_region(region,fit[0],shape,image.shape) for region in regions
                                   if class_name(region) != 'essential']
                except ValueError:
                    transferred = []
                matched.append((seed,transferred))
                for group in sorted({r['essential_group'] for r in regions if r['essential_group'] is not None}-groups):
                    members = [r for r in regions if r['essential_group'] == group]
                    normal = read_reference(str(reference))
                    if normal is None or not all(
                            (score := shape_score(normal,image,r,fit[0])) is not None and score >= .7
                            for r in members):
                        continue
                    try:
                        boxes = [transform_region(r,fit[0],shape,image.shape) for r in members]
                    except ValueError:
                        continue
                    if any(same_location(r,other,image.shape) for r in boxes
                           for other in existing+[r for _,r in essential_additions]):
                        continue
                    essential_additions.extend((seed,r) for r in boxes)
                    groups.add(group)
            if not matched:
                skipped.append({"frame":index+1,"reason":"reference shape not found or labels outside frame"})
                continue
            regions = [region for _,region in essential_additions]
            # The trained model refines matching defect locations; utility regions retain the common shape transform.
            result = model.predict(image,conf=.7,iou=.45,imgsz=640,verbose=False)[0] if model is not None else None
            predictions = result.obb if getattr(result,"obb",None) is not None else result.boxes if result is not None else []
            used = set()
            source_frames = [seed+1 for seed,_ in essential_additions]
            for seed,region in ((seed,region) for seed,transferred in matched for region in transferred):
                name = controller.classes.get(region["class_id"],"").lower()
                defect = name in DEFECT_CLASSES
                x1,y1,x2,y2 = np.array(region["box"])*np.tile([image.shape[1],image.shape[0]],2)
                center = ((x1+x2)/2,(y1+y2)/2)
                candidates = []
                for p,box in enumerate(predictions):
                    if p in used or int(box.cls[0]) != region["class_id"] or float(box.conf[0]) < .7:
                        continue
                    left,top,right,bottom = box.xyxy[0].tolist()
                    distance = np.linalg.norm(np.array(center)-[(left+right)/2,(top+bottom)/2])
                    if distance <= max(x2-x1,y2-y1,8):
                        candidates.append((distance,p,box))
                if defect and not candidates:
                    continue
                if defect and candidates:
                    _,p,box = min(candidates,key=lambda value:value[0])
                    used.add(p)
                    if getattr(result,"obb",None) is not None:
                        cx,cy,w,h,angle = box.xywhr[0].tolist()
                        region.update(box=[(cx-w/2)/image.shape[1],(cy-h/2)/image.shape[0],
                                           (cx+w/2)/image.shape[1],(cy+h/2)/image.shape[0]],angle=float(np.degrees(angle)))
                    else:
                        region.update(box=(np.asarray(box.xyxy[0].tolist())/np.tile([image.shape[1],image.shape[0]],2)).tolist(),angle=0.)
                if any(same_location(region,other,image.shape) for other in existing+regions):
                    continue
                regions.append(region)
                source_frames.append(seed+1)
            # Re-check immediately before writing; preserve any existing manual work.
            if not regions:
                preserved.append(index+1)
                continue
            if (target.read_text(encoding='utf-8') if target.exists() else '') != original:
                skipped.append({'frame':index+1,'reason':'labels changed while generating'})
                continue
            loader.index = index
            loader.image = image
            original_boxes = loader.boxes
            loader.boxes = [(region["class_id"],*(np.array(region["box"])*np.tile([image.shape[1],image.shape[0]],2)))
                            for region in existing+regions]
            loader.rotations = {box:region.get("angle",0) for box,region in zip(loader.boxes,existing+regions)}
            loader._carry_essential_groups(original_boxes)
            loader.essential_groups.update({box:region['essential_group']
                                            for box,region in zip(loader.boxes[len(existing):],regions)
                                            if region['essential_group'] is not None})
            loader._save_label()
            # Keep pre-existing YOLO rows byte-for-byte; append new rows only.
            saved = target.read_text(encoding='utf-8').splitlines()
            if original.strip():
                target.write_text(original + ('' if original.endswith('\n') else '\n') + '\n'.join(saved[len(existing):])+'\n',encoding='utf-8')
            metadata_path = target.with_suffix(".rotation.json")
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            metadata = original_metadata | metadata
            metadata.update(proto_generated=not bool(original.strip()) or original_metadata.get('proto_generated',False),
                            source_frame=source_frames[0],label_text=target.read_text(encoding='utf-8'),
                            proto_added_indices=original_metadata.get('proto_added_indices',[])+list(range(len(existing),len(existing)+len(regions))))
            metadata_path.write_text(json.dumps(metadata,indent=2),encoding="utf-8")
            generated.append({"frame":index+1,"source_frame":source_frames[0],"added":len(regions)})
    finally:
        report = {"generated":generated,"skipped":skipped,"preserved":preserved,"confidence_threshold":.7,"created_at_ns":time.time_ns()}
        report_path = label_dir/f"{paths[0].stem.rsplit('_',1)[0]}.proto_report.json"
        report_path.write_text(json.dumps(report,indent=2),encoding="utf-8")
    return report_path,report


def same_location(left, right, shape):
    if left['class_id'] != right['class_id']:
        return False
    from inspection.rotated_regions import region_polygon
    a,b = (cv2.convexHull(region_polygon(region,shape).astype(np.float32)) for region in (left,right))
    areas = [cv2.contourArea(p) for p in (a,b)]
    if min(areas) <= 0:
        return False
    intersection,_ = cv2.intersectConvexConvex(a,b)
    return intersection/(sum(areas)-intersection+1e-9) >= .3 or intersection/min(areas) >= .6
