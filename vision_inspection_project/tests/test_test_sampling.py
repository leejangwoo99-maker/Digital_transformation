import unittest
import json
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import numpy as np
import cv2

from inspection.test_sampling import TestSampling
from inspection.special_checks import SpecialInspector


class TestSamplingTests(unittest.TestCase):
    def test_start_groups_follow_sample_mapping_and_inspection_bounds(self):
        sampling = TestSampling(23, 5)
        groups = list(sampling.start_groups((8, 20)))
        self.assertEqual(groups, [range(8, 11), range(11, 16), range(16, 20)])
        for sample, group in enumerate(groups, 3):
            self.assertTrue(all(sampling.first_sample_at_or_after(n) == sample for n in group))
        self.assertEqual(list(TestSampling(23, 23).start_groups((8, 9))),
                         [range(8, 9), range(9, 10)])
        with self.assertRaises(ValueError):
            list(sampling.start_groups((8, 9)))

    def test_ranges_and_original_annotations(self):
        sampling = TestSampling(23,10)
        self.assertEqual(sampling.interval((24,47)),(11,21))
        self.assertEqual(sampling.first_sample_at_or_after(24),11)
        self.assertEqual(TestSampling(23,5).first_sample_at_or_after(7),3)
        original = {n:object() for n in range(1,70)}
        sampled = sampling.schedule(original)
        self.assertIs(sampled[11],original[24])
        self.assertIs(sampled[21],original[47])
        self.assertIs(sampled[12],original[26])
        del original[26]
        self.assertNotIn(12,sampling.schedule(original))
        self.assertEqual(TestSampling(23,23).schedule(original),original)
        with self.assertRaises(ValueError):
            sampling.interval((2,2))
        for invalid in (0,-1,24,float('nan')):
            with self.assertRaises(ValueError):
                TestSampling(23,invalid)

    def inspector(self,kind,regions):
        inspector = SpecialInspector.__new__(SpecialInspector)
        record = {'regions':regions}
        inspector.references = {kind:record}
        inspector.annotation_frames = {kind:{n:record for n in range(1,5)}}
        inspector.times = {kind:(0,None)}
        inspector.labeled_frames_only = True
        return inspector

    def test_ocr_latches_text_and_mark_without_reinspection(self):
        inspector = self.inspector('OCR',[
            {'role':'text','box':[.1,.1,.4,.4],'text':'A'},
            {'role':'mark','box':[.5,.5,.8,.8],'mark_id':'M'}])
        reader = Mock(return_value=['A'])
        mark = Mock(side_effect=[{'verdict':'NG'},{'verdict':'OK'}])
        inspector.ocr = SimpleNamespace(candidates=reader)
        inspector.mark_model = SimpleNamespace(predict=mark)
        frame = np.zeros((80,80,3),np.uint8)
        inspector.begin_item_range(1,2)
        self.assertTrue(inspector.check_frame(frame,1,4))
        original = inspector.annotation_frames['OCR'][1]['regions']
        inspector.annotation_frames['OCR'][2] = {'regions':[dict(original[0],box=[.2,.1,.5,.4]),original[1]]}
        self.assertFalse(inspector.check_frame(frame,2,4))
        boxes = inspector.result_photo_boxes('OCR',2,frame.shape)
        self.assertEqual([b['class_name'] for b in boxes],['Text 1 OK F1','Mark 1 OK'])
        self.assertAlmostEqual(boxes[0]['xyxy'][0],16)
        self.assertFalse(inspector.check_frame(frame,2,4))
        self.assertEqual(reader.call_count,1)
        self.assertEqual(mark.call_count,2)
        self.assertTrue(any(d.get('historical') for d in inspector.detail_detections))
        inspector.begin_item_range(3,4)
        self.assertFalse(inspector.window_results.successes)

    def test_label_reuses_successful_text_while_qr_retries(self):
        inspector = self.inspector('label',[
            {'role':'qr','number':1,'box':[.1,.1,.4,.4]},
            {'role':'text','number':2,'box':[.5,.5,.8,.8],'text':'A'},
            {'role':'text','number':9,'box':[.1,.5,.3,.8],'text':'B'}])
        reader = Mock(side_effect=[['A'],['B']])
        decoder = Mock(side_effect=[('',None,None),('A',None,None)])
        inspector.ocr = SimpleNamespace(candidates=reader)
        inspector.qr = SimpleNamespace(detectAndDecode=decoder)
        inspector.begin_item_range(1,3)
        frame = np.zeros((80,80,3),np.uint8)
        self.assertTrue(inspector.check_frame(frame,1,4))
        boxes = inspector.result_photo_boxes('label',1,frame.shape)
        self.assertEqual([b['class_name'] for b in boxes],['QR 1 NG','Text 2 NG','Text 9 OK'])
        self.assertFalse(inspector.check_frame(frame,2,4))
        boxes = inspector.result_photo_boxes('label',2,frame.shape)
        self.assertEqual([b['class_name'] for b in boxes],['QR 1 OK','Text 2 OK','Text 9 OK F1'])
        self.assertEqual(reader.call_count,2)
        self.assertEqual(decoder.call_count,2)

    def test_file_and_live_use_original_group_labels_at_lower_fps(self):
        from main import inspect_video
        from inspection.photo_inspection import save_photo_frame_range
        for live in (False,True):
            with self.subTest(live=live),tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                source = root/'source.avi'
                writer = cv2.VideoWriter(str(source),cv2.VideoWriter_fourcc(*'MJPG'),23,(80,80))
                for _ in range(69):
                    writer.write(np.zeros((80,80,3),np.uint8))
                writer.release()
                inspector = self.inspector('OCR',[{'role':'text','box':[.1,.1,.4,.4],'text':'A'}])
                record = inspector.references['OCR']
                inspector.annotation_frames = {'OCR':{n:dict(record,original_number=n) for n in range(1,70)}}
                inspector.ocr = SimpleNamespace(candidates=Mock(side_effect=[['bad'],['A']]))
                seen = []
                check = SpecialInspector.check_frame
                def checking(self,frame,number,total,valid_mask=None):
                    if 'OCR' in self.annotation_frames:
                        seen.append((number,self.annotation_frames['OCR'][number]['original_number']))
                    return check(self,frame,number,total,valid_mask)
                inspector.__class__ = type('TrackingInspector',(SpecialInspector,),{'check_frame':checking})
                save_photo_frame_range(root/'info','A_23fps','OCR',24,47)
                (root/'info'/'A_23fps_OCR_item_start.json').write_text('{"text_0": 47}',encoding='utf-8')
                settings = dict(root_dir=str(root),special_info_dir=str(root/'info'),special_group='A_23fps',
                    group_fps=23,test_fps=10,special_photo_inspection=True,photo_frame_selection=True,
                    special_labeled_frames=True,require_regular_model=False)
                capture = None
                if live:
                    settings.update(live_photo_inspection=True,live_expected_frames=30)
                    capture = cv2.VideoCapture(str(source))
                detector = SimpleNamespace(model=None,predict=lambda image:([],image))
                with patch('model.yolo_inference.YoloInference',return_value=detector),patch(
                        'inspection.special_checks.SpecialInspector',return_value=inspector),patch(
                        'inspection.video_export.finalize_result_video'):
                    inspect_video(source,settings,root/'result.mp4',show_window=False,
                                  display_width=80,display_height=80,log_dir=root/'logs',
                                  file_test_log=True,capture_override=capture)
                self.assertEqual(seen,[(11,24),(12,26)])
                report = json.loads(next((root/'logs').glob('*.pipeline.json')).read_text(encoding='utf-8'))
                self.assertEqual(report['photo_inspections'][0]['annotation_frame_number'],26)
                self.assertEqual(report['photo_inspections'][0]['result'],'OK')


if __name__ == '__main__':
    unittest.main()
