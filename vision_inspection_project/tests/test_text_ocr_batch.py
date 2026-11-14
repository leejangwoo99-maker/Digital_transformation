import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import numpy as np
import torch

from inspection.supervised_text_ocr import TrainedTextOCR, recognition_tensor, oriented
from inspection.text_image_model import train_text_images
from inspection.special_checks import annotation_dir, write_json


class TextOCRBatchTests(unittest.TestCase):
    def test_different_width_jobs_overlap_and_merge_results_deterministically(self):
        import threading
        model,_ = self.recognizer([dict(languages=['en'],preprocessing_version=2)])
        model.job_workers = 2
        barrier = threading.Barrier(2)
        def job(key,items):
            barrier.wait(3)
            return ([(number,line,str(key[1])) for number,line,_ in items],{'test_job':1.},{'openvino':1},[])
        model._recognize_job = job
        values = model.candidates_batch([dict(image=np.zeros((32,width,3),np.uint8),text='A') for width in (80,180)])
        self.assertNotEqual(values[0],values[1])
        self.assertEqual(model.backend_counts['openvino'],2)
        self.assertEqual(model.runtime_seconds['test_job'],2.)

    def recognizer(self, manifest):
        model = TrainedTextOCR.__new__(TrainedTextOCR)
        model.manifest = manifest
        model.readers = None
        reader = SimpleNamespace(
            recognizer=Mock(side_effect=lambda tensors,text:torch.zeros((len(tensors),8,3))),
            converter=SimpleNamespace(decode_greedy=lambda indices,lengths:["OBSERVED"]*len(lengths)))
        model.reader = Mock(return_value=reader)
        return model,reader

    def test_same_width_fields_batch_and_expected_value_is_never_inserted(self):
        model,reader = self.recognizer([{"languages":["en","fr"],"preprocessing_version":2}])
        image = np.full((32,80,3),255,np.uint8)
        requests = [{"image":image,"text":"EXPECTED"} for _ in range(10)]
        values = model.candidates_batch(requests)
        self.assertEqual(values,[["OBSERVED"]]*10)
        self.assertEqual(reader.recognizer.call_count,2)

    def test_script_routing_skips_chinese_for_numeric_field(self):
        model,reader = self.recognizer([{"languages":["en","fr"]},{"languages":["ch_sim","en"]}])
        image = np.zeros((32,80,3),np.uint8)
        model.candidates_batch([{"image":image,"text":"11"}])
        self.assertEqual([call.args[0] for call in model.reader.call_args_list],[0])

    def test_saved_training_orientation_uses_one_direction(self):
        model,reader = self.recognizer([{"languages":["en"],"preprocessing_version":2,
            "routes":[{"number":2,"box":[.1,.2,.2,.4],"rotation":270}]}])
        image = np.arange(30*10*3,dtype=np.uint8).reshape(30,10,3)
        request = {"image":image,"number":2,"box":[.1,.2,.2,.4],"text":"11"}
        self.assertEqual(model.routes_for(request),[(0,[270])])
        model.candidates_batch([request])
        self.assertEqual(reader.recognizer.call_count,1)

    def test_rotation_and_padding_are_consistent_for_small_digits(self):
        image = np.full((40,15,3),255,np.uint8)
        image[4:36,6:9] = 0
        rotated = oriented(image,90)
        tensor = recognition_tensor(rotated)
        self.assertEqual(tuple(tensor.shape[:3]),(1,1,64))
        self.assertGreaterEqual(tensor.shape[-1],128)

    def test_check_mode_without_fixed_transcript_is_allowed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            import cv2
            source = root/"sample.png"
            cv2.imwrite(str(source),np.full((32,80,3),255,np.uint8))
            write_json(annotation_dir(root,"A","label")/"a.json",{"image":str(source),"regions":[
                {"role":"text","box":[0,0,1,1],"text":"","refer":"11","check":True,"pattern":["num","num"],"verdict":"PASS"}]})
            with patch("inspection.supervised_text_ocr.train_recognizers") as trainer:
                self.assertIsNotNone(train_text_images(root,"A","label"))
            self.assertEqual(trainer.call_args.args[0][0]["text"],"")
            self.assertEqual(trainer.call_args.args[0][0]["refer"],"11")
            self.assertTrue(trainer.call_args.args[0][0]["check"])

    def test_dynamic_numeric_fields_ignore_saved_fixed_orientation_and_transcript(self):
        model,_ = self.recognizer([{"languages":["en"],"routes":[
            {"number":2,"box":[0,0,1,1],"rotation":90}]}])
        request = {"image":np.zeros((40,50,3),np.uint8),"number":2,"box":[0,0,1,1],
                   "check":True,"pattern":["num","num"],"text":""}
        self.assertEqual(model.routes_for(request),[(-1,[0,90,180,270])])

    def test_refer_is_train_transcript_not_a_fixed_test_answer(self):
        from inspection.supervised_text_ocr import training_transcript
        from inspection.text_image_model import annotation_signature
        sample = {"text":"","check":True,"refer":"11","pattern":["num","num"]}
        self.assertEqual(training_transcript(sample),"11")
        self.assertNotEqual(annotation_signature(sample),annotation_signature(dict(sample,refer="12")))
        self.assertEqual(training_transcript({"text":"Electronics","check":False,"refer":"ignored"}),"Electronics")

    def test_dynamic_numeric_routing_uses_learned_font_with_base_fallback(self):
        model,_ = self.recognizer([{"languages":["en"],"routes":[
            {"number":2,"box":[0,0,1,1],"rotation":90,"check":True}]}])
        request = {"image":np.zeros((40,50,3),np.uint8),"number":2,"box":[0,0,1,1],
                   "check":True,"pattern":["num","num"],"refer":"11"}
        self.assertEqual(model.routes_for(request),[(0,[90]),(-1,[90])])
