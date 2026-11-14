import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import cv2
import numpy as np

from inspection.shape_registration import ShapeRegistration


class TrackingTests(unittest.TestCase):
    def test_flow_follows_rotation_and_falls_back_after_occlusion(self):
        rng = np.random.default_rng(16)
        image = cv2.GaussianBlur(rng.integers(0,256,(360,480,3),dtype=np.uint8),(3,3),0)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'reference.png'
            cv2.imwrite(str(path),image)
            matcher = ShapeRegistration()
            area = {'box':[.1,.1,.9,.9]}
            for angle in (0,2,4,6):
                affine = cv2.getRotationMatrix2D((240,180),angle,1)
                current = cv2.warpAffine(image,affine,(480,360))
                result = matcher.fit(current,path,area)
                self.assertIsNotNone(result)
                points = np.float32([[[120,100],[320,100],[240,250]]])
                expected = cv2.transform(points,affine)
                np.testing.assert_allclose(cv2.perspectiveTransform(points,result[0]),expected,atol=1.5)
            self.assertGreater(matcher.stats['flow_accepted'],0)
            self.assertLess(matcher.stats['full_extractions'],4)
            self.assertIsNone(matcher.fit(np.zeros_like(image),path,area))
            self.assertIsNotNone(matcher.fit(image.copy(),path,area))

    def test_bad_tracked_features_force_full_search(self):
        rng = np.random.default_rng(17)
        image = cv2.GaussianBlur(rng.integers(0,256,(300,400,3),dtype=np.uint8),(3,3),0)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'reference.png'
            cv2.imwrite(str(path),image)
            matcher = ShapeRegistration()
            self.assertIsNotNone(matcher.fit(image,path))
            wrong = (np.zeros((50,2),np.float32),np.ones((50,128),np.float32))
            with patch.object(matcher,'_flow',return_value=wrong):
                self.assertIsNotNone(matcher.fit(image.copy(),path))
            self.assertEqual(matcher.stats['flow_fallbacks'],1)


class OpenVINOTests(unittest.TestCase):
    def test_dynamic_fp32_conversion_and_checkpoint_invalidation(self):
        try:
            import openvino
        except ImportError:
            self.skipTest('OpenVINO is optional')
        import torch
        import json
        from easyocr.utils import CTCLabelConverter
        from inspection.supervised_text_ocr import TrainedTextOCR
        from unittest.mock import Mock
        from inspection.openvino_ocr import export_recognizer,infer,artifact
        class TinyCTC(torch.nn.Module):
            def __init__(self):
                super().__init__()
                self.conv = torch.nn.Conv2d(1,4,(64,3),padding=(0,1))
                self.head = torch.nn.Linear(4,5)
            def forward(self,image,text):
                return self.head(self.conv(image).squeeze(2).permute(0,2,1))
        torch.manual_seed(6)
        model = TinyCTC().eval()
        with tempfile.TemporaryDirectory() as directory:
            checkpoint = Path(directory)/'model.pt'
            torch.save(model.state_dict(),checkpoint)
            export_recognizer(SimpleNamespace(recognizer=model,converter=CTCLabelConverter('ABCD')),checkpoint)
            tensor = torch.randn((3,1,64,192))
            with torch.inference_mode():
                expected = model(tensor,None).numpy()
            np.testing.assert_allclose(infer(checkpoint,tensor),expected,atol=2e-3,rtol=2e-3)
            manifest = Path(directory)/'model.ocr.json'
            manifest.write_text(json.dumps([dict(file=checkpoint.name,languages=['en'],preprocessing_version=2)]))
            runtime = TrainedTextOCR(manifest)
            from inspection.inference_telemetry import InferenceTelemetry
            runtime.telemetry = InferenceTelemetry()
            runtime.telemetry_kind = 'label'
            runtime.reader = Mock(side_effect=AssertionError('Must not load duplicate PyTorch weights'))
            values = runtime.candidates_batch([dict(image=np.zeros((32,80,3),np.uint8),text='AB')])
            self.assertEqual(len(values),1)
            runtime.reader.assert_not_called()
            self.assertGreater(runtime.backend_counts['openvino'],0)
            diagnostics = runtime.telemetry.report()
            self.assertGreater(diagnostics['openvino_calls'],0)
            self.assertEqual(diagnostics['native_thread_configurations'][0]['requested_threads'],4)
            self.assertEqual(diagnostics['batches'][0]['backend'],'openvino')
            self.assertIn('request_creation_seconds',diagnostics['batches'][0])
            runtime.reader = Mock(return_value=SimpleNamespace(recognizer=model,converter=CTCLabelConverter('ABCD')))
            with patch('inspection.openvino_ocr.infer',side_effect=RuntimeError('CPU backend unavailable')):
                fallback = runtime.candidates_batch([dict(image=np.zeros((32,80,3),np.uint8),text='AB')])
            self.assertEqual(fallback,values)
            self.assertEqual(runtime.backend_counts['pytorch'],1)
            self.assertEqual(len(runtime.backend_errors),1)
            old = artifact(checkpoint)
            checkpoint.write_bytes(checkpoint.read_bytes()+b'new generation')
            self.assertNotEqual(artifact(checkpoint),old)
            self.assertIsNone(infer(checkpoint,tensor))
