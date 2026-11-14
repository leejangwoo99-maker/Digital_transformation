import unittest
from types import SimpleNamespace
import numpy as np
from inspection.special_checks import SpecialInspector


class SpecialExclusionTests(unittest.TestCase):
    def test_manual_mask_applies_to_text_crop_with_and_without_roi_alignment(self):
        for kind in ("OCR","label"):
            for aligned in (False,True):
                with self.subTest(kind=kind,aligned=aligned):
                    frame = np.full((100,100,3),200,np.uint8)
                    observed = []
                    inspector = SpecialInspector.__new__(SpecialInspector)
                    reference = {"image":"reference.jpg","regions":[
                        {"role":"text","text":"AB","box":[.1,.1,.9,.9]},
                        {"role":"exclude","box":[.4,.4,.6,.6]}]}
                    if aligned:
                        reference["rois"] = [{"role":"label_roi","points":[[0,0],[1,0],[1,1],[0,1]]}]
                        inspector.polygon_aligner = SimpleNamespace(align=lambda *args:frame.copy())
                    inspector.references = {kind:reference}
                    inspector.times = {kind:(0,None)}
                    inspector.ocr = SimpleNamespace(candidates=lambda image:observed.append(image.copy()) or ["AB"])
                    self.assertEqual(inspector.check(frame,0),[])
                    self.assertEqual(observed[0][30:50,30:50].max(),0)
                    self.assertEqual(frame.min(),200)
