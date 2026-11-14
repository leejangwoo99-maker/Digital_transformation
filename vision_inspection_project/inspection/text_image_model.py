"""Supervised whole-text image exemplars; unknown images require real OCR."""
import hashlib
import json
from pathlib import Path

import cv2
import numpy as np

from inspection.rotated_regions import crop_rotated_region


def text_crop(image, region):
    if not region.get("angle", 0):
        from inspection.special_checks import pixel_box
        x1,y1,x2,y2 = pixel_box(region, image.shape)
        return image[y1:y2,x1:x2]
    return crop_rotated_region(image, region)


def descriptor(image):
    from inspection.text_ink import ink_map
    return ink_map(image).ravel()


def annotation_signature(region):
    value = {key: region.get(key) for key in ("box", "angle", "text", "number", "verdict", "check", "pattern")}
    if region.get("check") and region.get("refer"):
        value["refer"] = region["refer"]
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def model_path(root, group, kind):
    from inspection.special_checks import group_key
    return Path(root) / "weights" / f"{group_key(group)}_{kind}_text_image.npz"


def train_text_images(root, group, kind):
    from inspection.special_checks import labeled_records
    features, metadata = [], []
    for _, data in labeled_records(root, group, kind):
        image = None
        for region in data["regions"]:
            if region["role"] != "text" or region.get("verdict") not in ("PASS", "FAIL"):
                continue
            if not region.get("text") and not region.get("check"):
                raise ValueError(f"{kind}: actual training text is empty for {data['image']} (number {region.get('number')}). Enter the printed characters, including for check-mode fields.")
            if region.get("check"):
                if not region.get("refer"):
                    raise ValueError(f"{kind}: enter actual printed characters in refer for {data['image']} (number {region.get('number')}). Refer is for TRAIN only.")
            if image is None:
                image = cv2.imread(data["image"])
                if image is None:
                    raise ValueError(f"Cannot read training image: {data['image']}")
                from inspection.region_masks import mask_regions
                image = mask_regions(image,data["regions"])
            features.append(descriptor(text_crop(image, region)))
            metadata.append({"number": region.get("number"), "verdict": region["verdict"],
                             "box": region["box"], "angle": region.get("angle", 0),
                             "text": region.get("text", ""), "signature": annotation_signature(region),
                             "check":bool(region.get("check")),"pattern":region.get("pattern",[]),
                             "refer":region.get("refer", ""),
                             "masks":[r for r in data["regions"] if r["role"] == "exclude"],
                             "image": data["image"]})
    if not features:
        return None
    if not any(item["verdict"] == "PASS" for item in metadata):
        raise ValueError(f"{kind}: add PASS text samples before training.")
    path = model_path(root, group, kind)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp.npz")
    np.savez_compressed(temporary,version=2,features=np.asarray(features), metadata=json.dumps(metadata, ensure_ascii=False))
    from inspection.supervised_text_ocr import train_recognizers
    train_recognizers(metadata, path)
    temporary.replace(path)
    print(f"{kind} text image model: {len(features)} labeled crops; {path}")
    print("Supervised image exemplars + fine-tuned CTC character recognizer; QR comparison retained.")
    if not any(item["verdict"] == "FAIL" for item in metadata):
        print("WARNING: no FAIL examples; unseen images are rejected, but add actual FAIL examples for validation.")
    return path


class TextImageModel:
    def __init__(self, path):
        with np.load(path, allow_pickle=False) as data:
            self.features = data["features"]
            self.metadata = json.loads(str(data["metadata"]))
            version = int(data["version"]) if "version" in data else 1
        if version < 2:
            # Rebuild only image features in memory; never alter saved OCR weights or labels.
            rebuilt = []
            previous_path,previous_image = None,None
            for sample in self.metadata:
                if sample["image"] != previous_path:
                    previous_path = sample["image"]
                    previous_image = cv2.imread(previous_path)
                if previous_image is None:
                    raise ValueError(f"Image model migration requires source image: {previous_path}")
                rebuilt.append(descriptor(text_crop(previous_image,sample)))
            self.features = np.asarray(rebuilt)

    def expected_for(self, region):
        candidates = [item for item in self.metadata if item["verdict"] == "PASS"
                      and item["number"] == region.get("number")]
        if not candidates:
            return None
        center = np.asarray(region["box"]).reshape(2,2).mean(axis=0)
        distances = [np.linalg.norm(np.asarray(item["box"]).reshape(2,2).mean(axis=0)-center) for item in candidates]
        best = int(np.argmin(distances))
        return candidates[best]["text"] if distances[best] < .08 else None

    def predict(self, image, number=None):
        candidates = [i for i, item in enumerate(self.metadata) if item["number"] == number]
        if not candidates:
            return {"verdict": "UNKNOWN", "text": None}
        query = descriptor(image)
        from inspection.text_ink import compare_ink,SHAPE
        ranked = sorted(candidates,key=lambda i:float(np.mean((self.features[i]-query)**2)))
        shortlist = ranked[:6]
        # Include both grades in the geometric check, even when one is uncommon.
        for grade in ("PASS","FAIL"):
            match = next((i for i in ranked if self.metadata[i]["verdict"] == grade),None)
            if match is not None and match not in shortlist:
                shortlist.append(match)
        scores = {i:compare_ink(self.features[i].reshape(SHAPE),query.reshape(SHAPE)) for i in shortlist}
        index = min(scores,key=lambda i:scores[i]["distance"] + .1*max(0,scores[i]["missing_component"]-.3))
        best = scores[index]["distance"]
        item = self.metadata[index]
        conflicting = [score["distance"] for i,score in scores.items()
                       if (self.metadata[i]["verdict"], self.metadata[i]["text"]) != (item["verdict"], item["text"])]
        certain = best <= .06 and scores[index]["missing_component"] < .5 and (not conflicting or min(conflicting)-best > .015)
        return {"verdict": item["verdict"] if certain else "UNKNOWN",
                "text": item["text"] if certain else None,"comparison":"aligned_ink_shape",
                **scores[index]}
