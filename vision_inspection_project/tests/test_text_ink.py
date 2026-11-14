import json
import tempfile
import unittest
from pathlib import Path

import cv2
import numpy as np

from inspection.text_ink import ink_map, compare_ink
from inspection.text_image_model import TextImageModel, descriptor
from inspection.polygon_roi import PolygonAligner
from inspection.annotation_details import failure_detail_lines


class TextInkTests(unittest.TestCase):
    def image(self,text):
        image = np.full((64,256,3),240,np.uint8)
        cv2.putText(image,text,(16,45),cv2.FONT_HERSHEY_SIMPLEX,1,(15,15,15),2)
        return image

    def model(self,root,image):
        path = root/"model.npz"
        np.savez_compressed(path,version=2,features=np.asarray([descriptor(image)]),
            metadata=json.dumps([{"number":9,"text":"AB:12.","verdict":"PASS","box":[0,0,1,1]}]))
        return TextImageModel(path)

    def test_brightness_small_movement_and_soft_blur_are_not_defects(self):
        original = self.image("AB:12.")
        moved = cv2.warpAffine(original,np.float32([[1,0,1],[0,1,1]]),(256,64),borderValue=(240,240,240))
        moved = cv2.GaussianBlur(cv2.convertScaleAbs(moved,alpha=.8,beta=12),(3,3),.5)
        with tempfile.TemporaryDirectory() as directory:
            prediction = self.model(Path(directory),original).predict(moved,9)
            self.assertEqual(prediction["verdict"],"PASS",prediction)
            self.assertEqual(prediction["comparison"],"aligned_ink_shape")

    def test_missing_character_punctuation_blank_and_large_shift_are_not_passed(self):
        original = self.image("AB:12.")
        with tempfile.TemporaryDirectory() as directory:
            model = self.model(Path(directory),original)
            for changed in (self.image("AB:1"), self.image("AB:12"),np.full_like(original,240),
                cv2.warpAffine(original,np.float32([[1,0,20],[0,1,0]]),(256,64),borderValue=(240,240,240))):
                self.assertNotEqual(model.predict(changed,9)["verdict"],"PASS")
        score = compare_ink(ink_map(original),ink_map(self.image("AB:12")))
        self.assertGreaterEqual(score["missing_component"],.5)

    def test_old_image_model_rebuild_does_not_modify_saved_weights(self):
        original = self.image("AB:12.")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            image = root/"source.png"
            cv2.imwrite(str(image),original)
            path = root/"old.npz"
            np.savez_compressed(path,features=np.zeros((1,16384)),metadata=json.dumps([
                {"image":str(image),"box":[0,0,1,1],"number":9,"text":"AB:12.","verdict":"PASS"}]))
            before = path.read_bytes()
            self.assertEqual(TextImageModel(path).predict(original,9)["verdict"],"PASS")
            self.assertEqual(path.read_bytes(),before)

    def test_separate_corner_rois_register_the_entire_label(self):
        image = np.random.default_rng(24).integers(10,245,(300,400,3),np.uint8)
        rois = [{"role":"label_roi","points":[[x,y],[x+.06,y],[x+.06,y+.08],[x,y+.08]]}
                for x,y in ((.1,.1),(.8,.1),(.8,.8),(.1,.8),(.4,.1))]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/"reference.png"
            cv2.imwrite(str(path),image)
            moved = cv2.warpAffine(image,np.float32([[1,0,5],[0,1,3]]),(400,300))
            aligner = PolygonAligner()
            aligned = aligner.align_anchors(moved,str(path),rois)
            self.assertIsNotNone(aligned)
            self.assertLess(np.mean(abs(aligned[40:260,40:360].astype(float)-image[40:260,40:360])),2)
            self.assertIsNone(aligner.align_anchors(np.zeros_like(image),str(path),rois))

    def test_image_diagnostics_do_not_display_ocr_null(self):
        lines = failure_detail_lines([{"class_name":"label_text_image_unknown","expected_text":"PASS image",
            "recognized_text":{"verdict":"UNKNOWN","text":None,"distance":.1,"ocr":["AB"]}}])
        self.assertTrue(any(line.startswith("Image:") for line in lines))
        self.assertIn('OCR: ["AB"]',lines)
        self.assertNotIn("OCR: null",lines)
