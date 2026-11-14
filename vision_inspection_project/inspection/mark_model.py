"""Group-specific normal mark templates with bounded pose tolerance."""
import hashlib
import json
import os
import multiprocessing
from concurrent.futures import ProcessPoolExecutor,wait,FIRST_COMPLETED
from collections import OrderedDict
from pathlib import Path

import cv2
import numpy as np


def mark_key(region):
    return region.get("mark_id") or hashlib.sha256(json.dumps(region["box"]).encode()).hexdigest()


def signature(region):
    values = {key:region.get(key) for key in ("mark_id","box","angle")}
    if region.get("verdict"):
        values["verdict"] = region["verdict"]
    return hashlib.sha256(json.dumps(values,sort_keys=True).encode()).hexdigest()


def model_path(root,group):
    from inspection.special_checks import group_key
    return Path(root)/"weights"/f"{group_key(group)}_OCR_mark_algorith.npz"


def normalized_mark(image):
    if image is None or not image.size:
        raise ValueError("Empty mark image")
    if image.ndim == 2:
        image = cv2.cvtColor(image,cv2.COLOR_GRAY2BGR)
    height,width = image.shape[:2]
    scale = 80/max(height,width)
    resized = cv2.resize(image,(max(1,round(width*scale)),max(1,round(height*scale))),interpolation=cv2.INTER_CUBIC)
    border = np.concatenate((image[0],image[-1],image[:,0],image[:,-1]))
    background = np.median(border,axis=0).astype(np.uint8)
    canvas = np.empty((96,96,3),np.uint8)
    canvas[:] = background
    y,x = (96-resized.shape[0])//2,(96-resized.shape[1])//2
    canvas[y:y+resized.shape[0],x:x+resized.shape[1]] = resized
    return canvas


def train_marks(root,group):
    from inspection.special_checks import labeled_records
    from inspection.text_image_model import text_crop
    images,metadata = [],[]
    for _,data in labeled_records(root,group,"OCR"):
        marks = [region for region in data["regions"] if region["role"] == "mark"]
        if not marks:
            continue
        image = cv2.imread(data["image"])
        if image is None:
            raise ValueError(f"Cannot read mark training image: {data['image']}")
        from inspection.region_masks import mask_regions
        image = mask_regions(image,data["regions"])
        for region in marks:
            template = normalized_mark(text_crop(image,region))
            if region.get("verdict", "PASS") == "PASS" and cv2.cvtColor(template,cv2.COLOR_BGR2GRAY).std() < 3:
                raise ValueError(f"Blank mark training box: {data['image']}")
            images.append(template)
            metadata.append({"key":mark_key(region),"signature":signature(region),
                             "verdict":region.get("verdict", "PASS")})
    if not images:
        return None
    for key in {item["key"] for item in metadata}:
        if not any(item["key"] == key and item["verdict"] == "PASS" for item in metadata):
            raise ValueError("Each mark needs at least one PASS training image")
    path = model_path(root,group)
    path.parent.mkdir(parents=True,exist_ok=True)
    temporary = path.with_suffix(".tmp.npz")
    np.savez_compressed(temporary,images=np.asarray(images),metadata=json.dumps(metadata))
    temporary.replace(path)
    print(f"Mark training: {len(images)} normal examples; {path}")
    return path


class MarkModel:
    def __init__(self,path,workers=0,spread=1,resource_gate=None):
        self.path = str(Path(path).resolve())
        with np.load(path,allow_pickle=False) as data:
            self.images = data["images"]
            self.metadata = json.loads(str(data["metadata"]))
        from inspection.resource_policy import parallel_settings,ResourceGate
        self.workers,self.spread = parallel_settings(workers,spread) if workers else (0,int(spread))
        self.resource_gate = resource_gate or ResourceGate(enabled=False)
        self.executor = None
        self.pool_workers = 0
        self.parallel_batches = self.serial_batches = 0
        self.variants = OrderedDict()
        self.cache_bytes = 0
        self.cache_limit = 256*1024*1024
        self.groups = {}
        # Remove byte-identical duplicates only; retain every distinct training example.
        seen = set()
        for index,(image,item) in enumerate(zip(self.images,self.metadata)):
            grade = item.get("verdict","PASS")
            identity = (item["key"],grade,hashlib.sha256(image.tobytes()).digest())
            if identity in seen:
                continue
            seen.add(identity)
            self.groups.setdefault((item["key"],grade),[]).append(index)
        for key,indices in self.groups.items():
            features = np.asarray([cv2.resize(self.images[i],(12,12)).reshape(-1) for i in indices],np.float32)
            distances = np.linalg.norm(features-features.mean(axis=0),axis=1)
            self.groups[key] = [indices[i] for i in np.argsort(distances,kind="stable")]

    def close(self):
        if self.executor is not None:
            self.executor.shutdown(wait=True,cancel_futures=True)
            self.executor = None
        self.variants.clear()
        self.cache_bytes = 0

    def predict_many(self, requests):
        if self.workers < 2 or len(requests) < 4 or len(self.metadata) < 64:
            self.serial_batches += 1
            from threadpoolctl import threadpool_limits
            from contextlib import nullcontext
            previous = cv2.getNumThreads()
            global_limits = getattr(self,'allow_global_thread_limits',True)
            try:
                if global_limits:
                    cv2.setNumThreads(self.spread)
                with threadpool_limits(limits=self.spread) if global_limits else nullcontext():
                    self.prepare_variants({mark_key(region) for _,region in requests})
                    results = []
                    for image,region in requests:
                        self.resource_gate.before_submit()
                        results.append(self.predict(image,region))
                    return results
            finally:
                if global_limits:
                    cv2.setNumThreads(previous)
        self.parallel_batches += 1
        if self.executor is None:
            self.pool_workers = min(self.workers,len(requests))
            if self.resource_gate.enabled:
                from inspection.resource_policy import resource_estimate
                limits = resource_estimate(self.workers,self.spread,model_bytes=self.images.nbytes)
                self.pool_workers = min(self.pool_workers,max(1,limits['memory_worker_limit']),
                                        max(1,int(limits['cores']*.8)//self.spread))
            self.executor = ProcessPoolExecutor(max_workers=self.pool_workers,
                mp_context=multiprocessing.get_context("spawn"),initializer=_initialize_worker,initargs=(self.path,self.spread))
        results = [None]*len(requests)
        pending = {}
        index = 0
        while index < len(requests) or pending:
            while index < len(requests) and len(pending) < self.pool_workers:
                self.resource_gate.before_submit()
                image,region = requests[index]
                pending[self.executor.submit(_predict_worker,image,region)] = index
                index += 1
            completed,_ = wait(pending,return_when=FIRST_COMPLETED)
            for future in completed:
                results[pending.pop(future)] = future.result()
        return results

    def prepare_variants(self,keys):
        indices = {index for (key,_),values in self.groups.items() if key in keys for index in values}
        # Only prewarm when all requested poses fit; otherwise retain lazy bounded caching.
        if len(indices)*25*96*96*3 <= self.cache_limit:
            for index in sorted(indices):
                if index not in self.variants:
                    self.resource_gate.before_submit()
                    self.template_variants(index)

    def template_variants(self,index):
        if index not in self.variants:
            template = self.images[index]
            fill = np.median(np.concatenate((template[0],template[-1],template[:,0],template[:,-1])),axis=0)
            values = []
            for angle,scale in [(0,1.)]+[(a,s) for a in (-5,-2.5,0,2.5,5)
                                         for s in (.92,.96,1.,1.04,1.08) if (a,s) != (0,1.)]:
                matrix = cv2.getRotationMatrix2D((47.5,47.5),angle,scale)
                values.append(cv2.warpAffine(template,matrix,(96,96),borderValue=tuple(map(float,fill))))
            size = sum(value.nbytes for value in values)
            while self.variants and self.cache_bytes+size > self.cache_limit:
                _,removed = self.variants.popitem(last=False)
                self.cache_bytes -= sum(value.nbytes for value in removed)
            self.variants[index] = values
            self.cache_bytes += size
        self.variants.move_to_end(index)
        return self.variants[index]

    def predict(self,image,region):
        if image is None or not image.size:
            return {"verdict":"NG","reason":"region not visible","score":0.}
        query = normalized_mark(image)
        if cv2.cvtColor(query,cv2.COLOR_BGR2GRAY).std() < 3:
            return {"verdict":"NG","reason":"mark missing","score":0.}
        border = np.median(np.concatenate((query[0],query[-1],query[:,0],query[:,-1])),axis=0)
        search = cv2.copyMakeBorder(query,4,4,4,4,cv2.BORDER_CONSTANT,value=tuple(map(float,border)))
        best = -1.
        fail_best = -1.
        comparisons = 0
        def score(index,variant):
            nonlocal comparisons
            comparisons += 1
            _,value,_,_ = cv2.minMaxLoc(cv2.matchTemplate(search,self.template_variants(index)[variant],cv2.TM_CCOEFF_NORMED))
            return float(value) if np.isfinite(value) else -1.
        key = mark_key(region)
        # All FAIL candidates are exhausted before any early PASS is permitted.
        for index in self.groups.get((key,"FAIL"),[]):
            for variant in range(25):
                fail_best = max(fail_best,score(index,variant))
        indices = self.groups.get((key,"PASS"),[])
        order = [(i,0) for i in indices]+[(i,v) for i in indices for v in range(1,25)]
        passed = False
        for index,variant in order:
            best = max(best,score(index,variant))
            passed = best >= .92 and (fail_best < .92 or best-fail_best > .015)
            if passed:
                break
        return {"verdict":"OK" if passed else "NG",
                "reason":"matched" if passed else "missing, different or FAIL mark",
                "score":max(0.,best), "fail_score":max(0.,fail_best),"comparisons":comparisons,
                "score_is_exhaustive":not passed or (bool(order) and (index,variant) == order[-1])}


_worker_model = None


def _initialize_worker(path,spread=1):
    global _worker_model
    global _thread_limit
    for name in ('OMP_NUM_THREADS','MKL_NUM_THREADS','OPENBLAS_NUM_THREADS','NUMEXPR_NUM_THREADS'):
        os.environ[name] = str(spread)
    from threadpoolctl import threadpool_limits
    _thread_limit = threadpool_limits(limits=spread)
    cv2.setNumThreads(spread)
    _worker_model = MarkModel(path)


def _predict_worker(image,region):
    _worker_model.prepare_variants({mark_key(region)})
    return _worker_model.predict(image,region)
