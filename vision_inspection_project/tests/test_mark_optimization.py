import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import cv2
import numpy as np

from inspection.mark_model import MarkModel,normalized_mark


def exhaustive(model,image,region):
    query = normalized_mark(image)
    border = np.median(np.concatenate((query[0],query[-1],query[:,0],query[:,-1])),axis=0)
    search = cv2.copyMakeBorder(query,4,4,4,4,cv2.BORDER_CONSTANT,value=tuple(map(float,border)))
    scores = {'PASS':-1.,'FAIL':-1.}
    for template,item in zip(model.images,model.metadata):
        if item['key'] != region['mark_id']:
            continue
        fill = np.median(np.concatenate((template[0],template[-1],template[:,0],template[:,-1])),axis=0)
        for angle in (-5,-2.5,0,2.5,5):
            for scale in (.92,.96,1.,1.04,1.08):
                variant = cv2.warpAffine(template,cv2.getRotationMatrix2D((47.5,47.5),angle,scale),(96,96),borderValue=tuple(map(float,fill)))
                value = cv2.minMaxLoc(cv2.matchTemplate(search,variant,cv2.TM_CCOEFF_NORMED))[1]
                if np.isfinite(value):
                    scores[item['verdict']] = max(scores[item['verdict']],value)
    return 'OK' if scores['PASS'] >= .92 and (scores['FAIL'] < .92 or scores['PASS']-scores['FAIL'] > .015) else 'NG'


class MarkOptimizationTests(unittest.TestCase):
    def make_model(self, root,workers=0):
        images = []
        metadata = []
        for i in range(4):
            image = np.full((96,96,3),230,np.uint8)
            cv2.putText(image,str(i),(15,74),cv2.FONT_HERSHEY_SIMPLEX,2,(10,10,10),4)
            for grade in ('PASS','FAIL'):
                sample = image.copy()
                if grade == 'FAIL':
                    cv2.line(sample,(10,15),(85,85),(10,10,10),5)
                images.extend([normalized_mark(sample)]*9)
                metadata.extend([dict(key=str(i),verdict=grade)]*9)
        path = root/'model.npz'
        np.savez_compressed(path,images=np.asarray(images),metadata=json.dumps(metadata))
        return MarkModel(path,workers),images

    def test_cache_and_verdicts_equal_exhaustive_with_fail_veto(self):
        with tempfile.TemporaryDirectory() as directory:
            model,images = self.make_model(Path(directory))
            for i in range(4):
                region = dict(mark_id=str(i))
                for sample in (images[i*18],images[i*18+9],cv2.flip(images[i*18],1)):
                    self.assertEqual(model.predict(sample,region)['verdict'],exhaustive(model,sample,region))
            with patch('inspection.mark_model.cv2.warpAffine',wraps=cv2.warpAffine) as warp:
                model.predict(images[0],dict(mark_id='0'))
                self.assertEqual(warp.call_count,0)
            model.close()

    def test_two_process_results_equal_serial_and_shutdown(self):
        with tempfile.TemporaryDirectory() as directory:
            serial,images = self.make_model(Path(directory))
            parallel = MarkModel(serial.path,workers=2)
            requests = [(images[i*18],dict(mark_id=str(i))) for i in range(4)]
            try:
                self.assertEqual(parallel.predict_many(requests),serial.predict_many(requests))
                self.assertIsNotNone(parallel.executor)
            finally:
                parallel.close()
                serial.close()
            self.assertIsNone(parallel.executor)

    def test_variant_cache_is_bounded_and_recomputed_without_verdict_change(self):
        with tempfile.TemporaryDirectory() as directory:
            model,images = self.make_model(Path(directory))
            model.cache_limit = 25*96*96*3
            expected = model.predict(images[0],dict(mark_id='0'))['verdict']
            model.predict(images[36],dict(mark_id='2'))
            self.assertLessEqual(model.cache_bytes,model.cache_limit)
            self.assertEqual(model.predict(images[0],dict(mark_id='0'))['verdict'],expected)
            model.close()

    def test_three_requested_workers_obey_memory_admission_and_keep_result_order(self):
        from inspection.resource_policy import ResourceGate
        with tempfile.TemporaryDirectory() as directory:
            serial,images = self.make_model(Path(directory))
            gate = ResourceGate(sampler=lambda:(0.,0.))
            parallel = MarkModel(serial.path,workers=3,spread=2,resource_gate=gate)
            requests = [(images[i*18],dict(mark_id=str(i))) for i in range(4)]
            try:
                with patch('inspection.resource_policy.resource_estimate',return_value=dict(memory_worker_limit=2,cores=12)):
                    result = parallel.predict_many(requests)
                self.assertEqual(result,serial.predict_many(requests))
                self.assertEqual(parallel.workers,3)
                self.assertEqual(parallel.pool_workers,2)
            finally:
                parallel.close()
                serial.close()
