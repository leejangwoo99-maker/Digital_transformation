import tempfile
import unittest
from pathlib import Path

import cv2
import numpy as np

from inspection.pipeline_audit import audit_pipeline
from inspection.special_checks import CodeReader, MultilingualOCR, annotation_dir, write_json


class PipelineAuditTest(unittest.TestCase):
    def test_all_four_checks_reported_and_missing_models_are_errors(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for kind, role in (("OCR", "text"), ("conn", "pin"), ("label", "qr")):
                regions = [{"role": role, "box": [0, 0, 1, 1]}]
                if kind == "conn":
                    regions.append({"role": "slot", "box": [0, 0, 1, 1]})
                write_json(annotation_dir(root, "A", kind) / "A_000000.json",
                           {"image": "A.jpg", "regions": regions})
            report = audit_pipeline({"root_dir": str(root), "special_group": "A", "model": {"weights": "weights/A_algorith.pt"}})
            self.assertEqual(len(report["errors"]), 2)
            messages = "\n".join(report["messages"])
            for kind in ("Appearance YOLO", "OCR:", "Conn YOLO", "label:"):
                self.assertIn(kind, messages)

    def test_data_matrix_decoder_keeps_payload_exact(self):
        import zxingcpp
        payload = "AB ',:/_"
        barcode = zxingcpp.create_barcode(payload, zxingcpp.BarcodeFormat.DataMatrix)
        image = np.asarray(barcode.to_image(scale=8))
        text, _, _ = CodeReader().detectAndDecode(image)
        self.assertEqual(text, payload)

    def test_decoder_retries_tight_crop_with_white_quiet_zone(self):
        from unittest.mock import Mock,patch
        from types import SimpleNamespace
        reader = CodeReader()
        reader.qr = Mock()
        reader.qr.detectAndDecode.return_value = ('',None,None)
        image = np.zeros((82,86,3),np.uint8)
        with patch('zxingcpp.read_barcodes',side_effect=[[],[SimpleNamespace(text='WCBS')]]) as decode:
            self.assertEqual(reader.detectAndDecode(image)[0],'WCBS')
        padded = decode.call_args_list[1].args[0]
        self.assertEqual(padded.shape,(90,94,3))
        self.assertTrue(np.all(padded[:4] == 255))

    def test_unread_code_is_not_reported_as_content_mismatch(self):
        from unittest.mock import Mock
        from inspection.special_checks import SpecialInspector
        inspector = SpecialInspector.__new__(SpecialInspector)
        inspector.references = {'label':{'image':'unused','regions':[
            {'role':'qr','number':1,'box':[0,0,.5,.5]},
            {'role':'text','number':2,'box':[.5,.5,1,1],'text':'AB'}]}}
        inspector.times = {'label':(0,None)}
        inspector.qr = Mock()
        inspector.qr.detectAndDecode.return_value = ('',None,None)
        inspector.ocr = Mock()
        inspector.ocr.candidates.return_value = ['AB']
        failures = inspector.check(np.full((80,80,3),255,np.uint8),1)
        self.assertEqual(failures[-1]['class_name'],'label_pair_1_2_noreading')
        inspector.qr.detectAndDecode.return_value = ('WRONG',None,None)
        self.assertEqual(inspector.check(np.full((80,80,3),255,np.uint8),1)[-1]['class_name'],'label_pair_1_2_mismatch')

    def test_ocr_exact_match_short_circuits_readers_and_caches_exact_crop_only(self):
        from unittest.mock import Mock
        engine = MultilingualOCR.__new__(MultilingualOCR)
        engine.cache = {}
        first, second = Mock(), Mock()
        first.readtext.return_value = [([[0, 0], [40, 0], [40, 10], [0, 10]], "ABC.", .9)]
        engine.readers = [first, second]
        crop = np.zeros((20, 80, 3), dtype=np.uint8)
        self.assertEqual(engine.candidates_for(crop, "ABC."), ["ABC."])
        second.readtext.assert_not_called()
        engine.candidates_for(crop.copy(), "ABC.")
        self.assertEqual(first.readtext.call_count, 1)
        crop[0, 0] = 1
        engine.candidates_for(crop, "ABC.")
        self.assertEqual(first.readtext.call_count, 2)


if __name__ == "__main__":
    unittest.main()
