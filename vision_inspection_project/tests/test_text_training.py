import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import cv2
import numpy as np

from inspection.special_checks import SpecialInspector, annotation_dir, write_json
from inspection.text_image_model import TextImageModel, train_text_images, descriptor
from inspection.polygon_roi import PolygonAligner, containing_roi


class TextTrainingTests(unittest.TestCase):
    def image(self, text):
        image = np.full((64,256,3),255,np.uint8)
        cv2.putText(image,text,(5,45),cv2.FONT_HERSHEY_SIMPLEX,1,(0,0,0),2)
        return image

    def test_supervised_samples_keep_actual_fail_transcript_and_reject_unknown(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for index,(text,verdict) in enumerate((("AB","PASS"),("AC","FAIL"))):
                path = root/f"{index}.png"
                cv2.imwrite(str(path),self.image(text))
                write_json(annotation_dir(root,"A","label")/f"{index}.json", {
                    "image":str(path),"regions":[{"role":"text","number":9,"box":[0,0,1,1],
                                                 "text":text,"verdict":verdict}]})
            with patch("inspection.supervised_text_ocr.train_recognizers") as trainer:
                path = train_text_images(root,"A","label")
            self.assertEqual([s["text"] for s in trainer.call_args.args[0]],["AB","AC"])
            model = TextImageModel(path)
            self.assertEqual(model.predict(self.image("AB"),9)["verdict"],"PASS")
            self.assertEqual(model.predict(self.image("AC"),9)["verdict"],"FAIL")
            self.assertEqual(model.predict(self.image("XYZ"),9)["verdict"],"UNKNOWN")
            self.assertEqual(model.expected_for({"number":9,"box":[0,0,1,1]}),"AB")

    def test_empty_descriptor_is_rejected(self):
        with self.assertRaises(ValueError):
            descriptor(np.zeros((0,0,3),np.uint8))

    def test_classifier_fail_overrides_correct_ocr_and_qr(self):
        inspector = SpecialInspector.__new__(SpecialInspector)
        inspector.references = {"label":{"regions":[
            {"role":"text","box":[0,0,1,1],"number":2,"text":"AB","verdict":"PASS"},
            {"role":"qr","box":[0,0,1,1],"number":1}]}}
        inspector.times = {"label":(0,None)}
        inspector.text_image_models = {"label":SimpleNamespace(predict=lambda *a:{"verdict":"FAIL","text":"AB"})}
        inspector.trained_text_ocr = {"label":SimpleNamespace(candidates=lambda *a:["AB"])}
        inspector.qr = SimpleNamespace(detectAndDecode=lambda image:("AB",None,None))
        failures = inspector.check(self.image("AB"),0)
        self.assertEqual([f["class_name"] for f in failures],["label_text_image_fail"])

    def test_classifier_pass_does_not_override_wrong_recognized_characters(self):
        inspector = SpecialInspector.__new__(SpecialInspector)
        inspector.references = {"label":{"regions":[
            {"role":"text","box":[0,0,1,1],"number":9,"text":"AB","verdict":"PASS"}]}}
        inspector.times = {"label":(0,None)}
        inspector.text_image_models = {"label":SimpleNamespace(predict=lambda *a:{"verdict":"PASS","text":"AB"})}
        inspector.trained_text_ocr = {"label":SimpleNamespace(candidates=lambda *a:["AC"])}
        failures = inspector.check(self.image("AB"),0)
        self.assertEqual(failures[0]["class_name"],"label_text_9_mismatch")
        self.assertEqual(failures[0]["recognized_text"]["text"],[["AC"]])

    def test_unknown_image_defers_to_exact_ocr_for_both_inspection_kinds(self):
        for kind in ("OCR", "label"):
            for recognized in ("AB", "AC", "ab", "AB ", "A B", "AB.", ""):
                with self.subTest(kind=kind,recognized=recognized):
                    inspector = SpecialInspector.__new__(SpecialInspector)
                    inspector.references = {kind:{"regions":[
                        {"role":"text","box":[0,0,1,1],"number":9,"text":"AB","verdict":"PASS"}]}}
                    inspector.times = {kind:(0,None)}
                    inspector.text_image_models = {kind:SimpleNamespace(
                        predict=lambda *a:{"verdict":"UNKNOWN","text":None})}
                    inspector.trained_text_ocr = {kind:SimpleNamespace(candidates=lambda *a:[recognized])}
                    failures = inspector.check(self.image("AB"),0)
                    self.assertEqual(bool(failures),recognized != "AB")
                    self.assertFalse(any("image_unknown" in f["class_name"] for f in failures))

    def test_ocr_confident_fail_vetoes_exact_text(self):
        inspector = SpecialInspector.__new__(SpecialInspector)
        inspector.references = {"OCR":{"regions":[
            {"role":"text","box":[0,0,1,1],"text":"AB","verdict":"PASS"}]}}
        inspector.times = {"OCR":(0,None)}
        inspector.text_image_models = {"OCR":SimpleNamespace(
            predict=lambda *a:{"verdict":"FAIL","text":"AB"})}
        inspector.trained_text_ocr = {"OCR":SimpleNamespace(candidates=lambda *a:["AB"])}
        self.assertEqual([f["class_name"] for f in inspector.check(self.image("AB"),0)],
                         ["OCR_text_image_fail"])

    def test_polygon_tracking_and_missing_reference(self):
        image = np.random.default_rng(42).integers(0,256,(300,400,3),np.uint8)
        roi = {"role":"label_roi","points":[[.1,.1],[.8,.1],[.9,.5],[.8,.9],[.1,.9]]}
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/"ref.png"
            cv2.imwrite(str(path),image)
            aligner = PolygonAligner()
            moved = cv2.warpAffine(image,np.float32([[1,0,8],[0,1,5]]),(400,300))
            aligned = aligner.align(moved,str(path),roi)
            self.assertIsNotNone(aligned)
            self.assertLess(np.mean(abs(aligned[60:240,60:300].astype(float)-image[60:240,60:300])),20)
            self.assertIsNone(aligner.align(np.zeros_like(image),str(path),roi))
            other = dict(roi,points=[[.91,.91],[.99,.91],[.99,.99]])
            self.assertIs(containing_roi({"box":[.4,.4,.5,.5]},[other,roi],image.shape),roi)
