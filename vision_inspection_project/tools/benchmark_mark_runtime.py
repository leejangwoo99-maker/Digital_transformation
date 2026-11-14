"""Compare exhaustive, cached serial and capped two-process Mark inspection."""
import json
import sys
from pathlib import Path
from time import perf_counter

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

import cv2
import numpy as np

from inspection.mark_model import MarkModel,model_path,normalized_mark,mark_key
from inspection.special_checks import labeled_records
from inspection.text_image_model import text_crop


def legacy(model,image,region):
    query = normalized_mark(image)
    border = np.median(np.concatenate((query[0],query[-1],query[:,0],query[:,-1])),axis=0)
    search = cv2.copyMakeBorder(query,4,4,4,4,cv2.BORDER_CONSTANT,value=tuple(map(float,border)))
    scores = {'PASS':-1.,'FAIL':-1.}
    for template,item in zip(model.images,model.metadata):
        if item['key'] != mark_key(region):
            continue
        fill = np.median(np.concatenate((template[0],template[-1],template[:,0],template[:,-1])),axis=0)
        for angle in (-5,-2.5,0,2.5,5):
            for scale in (.92,.96,1.,1.04,1.08):
                variant = cv2.warpAffine(template,cv2.getRotationMatrix2D((47.5,47.5),angle,scale),(96,96),borderValue=tuple(map(float,fill)))
                value = cv2.minMaxLoc(cv2.matchTemplate(search,variant,cv2.TM_CCOEFF_NORMED))[1]
                if np.isfinite(value):
                    grade = item.get('verdict','PASS')
                    scores[grade] = max(scores[grade],value)
    return 'OK' if scores['PASS'] >= .92 and (scores['FAIL'] < .92 or scores['PASS']-scores['FAIL'] > .015) else 'NG'


def main():
    group = 'full_1-4_merged_23fps'
    record = next(data for _,data in labeled_records(ROOT,group,'OCR') if any(r['role']=='mark' for r in data['regions']))
    image = cv2.imread(record['image'])
    if image is None:
        raise RuntimeError('Training source unavailable')
    requests = [(text_crop(image,r),r) for r in record['regions'] if r['role']=='mark'][:8]
    report = dict(group=group,requests_per_case=len(requests),opencv_threads=cv2.getNumThreads(),
                  note='Training crops and horizontally flipped crops; this is a Mark microbenchmark, not an end-to-end live guarantee.',cases={})
    for label,case in [('training_crops',requests),('flipped_crops',[(cv2.flip(crop,1),r) for crop,r in requests])]:
        baseline = MarkModel(model_path(ROOT,group))
        start = perf_counter()
        expected = [legacy(baseline,crop,r) for crop,r in case]
        data = dict(exhaustive_seconds=perf_counter()-start,expected=expected)
        baseline.close()
        for workers in (0,2):
            model = MarkModel(model_path(ROOT,group),workers=workers)
            try:
                for run in ('cold','warm'):
                    start = perf_counter()
                    results = model.predict_many(case)
                    actual = [item['verdict'] for item in results]
                    data[f'workers_{workers}_{run}'] = dict(seconds=perf_counter()-start,
                        same_verdicts=actual==expected,comparisons=sum(item.get('comparisons',0) for item in results))
                    if actual != expected:
                        raise AssertionError('Mark verdict changed')
            finally:
                model.close()
        report['cases'][label] = data
        print(label,json.dumps(data),flush=True)
    path = Path(r'C:\vision_inspection\testlog\mark_optimization_benchmark.json')
    path.write_text(json.dumps(report,indent=2),encoding='utf-8')
    print(path,flush=True)


if __name__ == '__main__':
    main()
