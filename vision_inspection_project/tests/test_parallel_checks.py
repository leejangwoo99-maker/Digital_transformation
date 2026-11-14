import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import cv2
import numpy as np

from inspection.parallel_checks import ParallelChecks
from inspection.performance import Performance
from inspection.shape_registration import ShapeRegistration


class ParallelCheckTests(unittest.TestCase):
    def test_independent_lanes_overlap_and_return_in_input_order(self):
        barrier = threading.Barrier(2)
        def inspector(name):
            def check(frame,number,total):
                barrier.wait(3)
                return [name]
            return SimpleNamespace(references={},check_frame=check)
        runner = ParallelChecks(2,Performance())
        try:
            result = runner.run(np.zeros((30,30,3),np.uint8),1,2,
                                {'OCR':inspector('ocr'),'label':inspector('label')},None)
            self.assertEqual(result,{'OCR':['ocr'],'label':['label']})
            self.assertEqual(runner.parallel_frames,1)
        finally:
            runner.close()

    def test_registration_forks_share_extraction_but_not_match_state(self):
        image = cv2.GaussianBlur(np.random.default_rng(42).integers(0,256,(240,320,3),dtype=np.uint8),(3,3),0)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'reference.png'
            cv2.imwrite(str(path),image)
            area = dict(role='registration',box=[.1,.1,.9,.9])
            parent = ShapeRegistration()
            def inspector():
                result = SimpleNamespace(references={'OCR':dict(regions=[area])},shape_registration=parent)
                result.check_frame = lambda frame,number,total:result.shape_registration.fit(frame,path,area)
                return result
            first,second = inspector(),inspector()
            runner = ParallelChecks(2,Performance())
            try:
                results = runner.run(image,1,2,{'OCR':first,'label':second},parent)
                self.assertTrue(all(value is not None for value in results.values()))
                np.testing.assert_allclose(results['OCR'][0],results['label'][0],atol=1e-4)
                self.assertEqual(parent.stats['full_extractions'],1)
                self.assertIs(first.shape_registration,parent)
                self.assertIs(second.shape_registration,parent)
            finally:
                runner.close()

    def test_unused_registration_is_not_computed_outside_labeled_frames(self):
        parent = ShapeRegistration()
        parent.fork = Mock(side_effect=AssertionError('No active registration'))
        inspector = SimpleNamespace(labeled_frames_only=True,references={'OCR':dict(regions=[{'role':'registration'}])},
                                    annotation_frames={'OCR':{2:dict(regions=[{'role':'registration'}])}},
                                    check_frame=lambda *args:[])
        runner = ParallelChecks(2,Performance())
        try:
            runner.run(np.zeros((20,20,3),np.uint8),1,2,{'OCR':inspector},parent)
            parent.fork.assert_not_called()
        finally:
            runner.close()
