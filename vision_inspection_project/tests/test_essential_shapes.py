import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import cv2
import numpy as np

from inspection.essential_shapes import EssentialShapes, load_essential_shapes
from inspection.test_sampling import TestSampling
from label_images import YoloBoxLabeler


class EssentialShapeTests(unittest.TestCase):
    def test_proto_copies_complete_essential_groups_only_and_preserves_manual_groups(self):
        from inspection.proto_labels import generate
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = [root/f'A_{n:06}.png' for n in range(4)]
            normal = np.zeros((120,160,3),np.uint8)
            cv2.rectangle(normal,(28,30),(52,57),(255,255,255),3)
            cv2.circle(normal,(113,45),12,(255,255,255),3)
            missing = normal.copy()
            missing[:,80:] = 0
            for path,image in zip(paths,(normal,normal,missing,normal)):
                cv2.imwrite(str(path),image)
            classes = {0:'ok',13:'Essential'}
            labeler = YoloBoxLabeler(root,root,classes,image_paths=paths)
            labeler._load_current_image()
            labeler.boxes = [(13,16,18,64,78),(13,85,18,139,78)]
            labeler.essential_groups = dict.fromkeys(labeler.boxes,7)
            labeler._save_label()
            labeler.index = 3
            labeler._load_current_image()
            labeler.boxes = [(13,16,18,64,78)]
            labeler.essential_groups = dict.fromkeys(labeler.boxes,7)
            labeler._save_label()
            labeler.proto_train = Mock()
            with patch('inspection.proto_labels.ShapeRegistration') as registration:
                registration.return_value.fit.return_value = (np.eye(3),normal.shape)
                generate(labeler,{1})
                generate(labeler,{1})
            labeler.proto_train.assert_not_called()
            schedule = load_essential_shapes(paths,root,classes)
            self.assertEqual([r['group'] for r in schedule[2]['regions']],[7,7])
            self.assertNotIn(3,schedule)
            self.assertEqual(len(schedule[4]['regions']),1)
            labeler.index = 1
            labeler._load_current_image()
            labeler._delete_overlapping_boxes(13,12,67,81)
            labeler._save_label()
            self.assertEqual(len(load_essential_shapes(paths,root,classes)[2]['regions']),1)

    def test_select_group_copies_all_boxes_to_next_frame_without_overwriting(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = [root/'a.png',root/'b.png']
            for path in paths:
                cv2.imwrite(str(path),np.zeros((100,100,3),np.uint8))
            labeler = YoloBoxLabeler(root,root,{0:'ok',13:'Essential'},image_paths=paths)
            labeler._load_current_image()
            labeler.boxes = [(13,20,20,50,50),(13,60,60,80,80)]
            labeler.essential_groups = dict.fromkeys(labeler.boxes,7)
            labeler._save_label()
            labeler.index = 1
            labeler._load_current_image()
            labeler.boxes = [(0,10,10,15,15)]
            self.assertEqual(labeler.select_essential_group(7),2)
            self.assertEqual(labeler.select_essential_group(7),0)
            self.assertEqual(len(labeler.boxes),3)
            schedule = load_essential_shapes(paths,root,labeler.classes)
            self.assertEqual([r['group'] for r in schedule[2]['regions']],[7,7])

    def test_group_needs_all_boxes_to_pass_on_one_frame(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'reference.png'
            normal = np.zeros((120,160,3),np.uint8)
            cv2.rectangle(normal,(28,30),(52,57),(255,255,255),3)
            cv2.circle(normal,(113,45),12,(255,255,255),3)
            cv2.imwrite(str(path),normal)
            originals = {n:dict(image=str(path),regions=[
                dict(id=f'F{n}_B1',group=7,box=[.10,.15,.40,.65]),
                dict(id=f'F{n}_B2',group=7,box=[.53,.15,.87,.65])]) for n in (1,2,3)}
            inspector = EssentialShapes(originals,TestSampling(5,5))
            inspector.registration.fit = Mock(return_value=(np.eye(2,3,dtype=np.float32),normal.shape))
            left,right = normal.copy(),normal.copy()
            left[:,80:] = 0
            right[:,:80] = 0
            inspector.check(left,1)
            inspector.check(right,2)
            self.assertEqual(inspector.failures(),['Essential_G7_missing'])
            inspector.check(normal,3)
            self.assertEqual(inspector.failures(),[])
            self.assertEqual(inspector.report()['groups'][7],dict(result='OK',test_frame=3))

    def test_each_box_gets_a_log_key_without_entering_an_id(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root/'frame.png'
            cv2.imwrite(str(path),np.zeros((100,100,3),np.uint8))
            labeler = YoloBoxLabeler(root,root,{0:'ok',13:'Essential'},image_paths=[path])
            labeler._load_current_image()
            original = (13,20,20,50,50)
            labeler.boxes = [original]
            labeler._apply_edit_regions([dict(box=[.3,.3,.6,.6],angle=5)])
            labeler._save_label()
            schedule = load_essential_shapes([path],root,labeler.classes)
            self.assertEqual(schedule[1]['regions'][0]['id'],'F1_B1')
            self.assertEqual(schedule[1]['regions'][0]['angle'],5)
            labeler._load_current_image()
            self.assertEqual(labeler.boxes[0][0],13)

    def test_boxes_on_different_frames_are_independent(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'reference.png'
            normal = np.zeros((120,120,3),np.uint8)
            cv2.rectangle(normal,(34,40),(57,68),(255,255,255),3)
            cv2.imwrite(str(path),normal)
            record = lambda key:dict(image=str(path),regions=[dict(id=key,box=[.2,.2,.65,.75])])
            inspector = EssentialShapes({1:record('F1_B1'),3:record('F3_B1')},TestSampling(10,5))
            inspector.registration.fit = Mock(return_value=(np.eye(2,3,dtype=np.float32),normal.shape))
            inspector.check(np.zeros_like(normal),1)
            self.assertIn('Essential_F1_B1_missing',inspector.failures())
            inspector.check(normal,2)
            self.assertEqual(inspector.failures(),['Essential_F1_B1_missing'])
            inspector.check(np.zeros_like(normal),3)
            self.assertEqual(inspector.report()['items']['F3_B1']['test_frame'],2)
            self.assertEqual(inspector.registration.fit.call_count,2)

    def test_unsampled_or_unregistered_shape_is_not_a_pass(self):
        record = dict(image='absent.png',regions=[dict(id=7,box=[.2,.2,.65,.75])])
        inspector = EssentialShapes({2:record},TestSampling(10,5))
        self.assertEqual(inspector.schedule,{})
        self.assertEqual(inspector.failures(),['Essential_7_unavailable'])

    def test_missing_shape_marks_final_verdict_without_a_yolo_model(self):
        import json
        from main import inspect_video
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            reference = root/'reference.png'
            normal = np.zeros((120,120,3),np.uint8)
            cv2.rectangle(normal,(34,40),(57,68),(255,255,255),3)
            cv2.imwrite(str(reference),normal)
            source = root/'sample.avi'
            writer = cv2.VideoWriter(str(source),cv2.VideoWriter_fourcc(*'MJPG'),5,(120,120))
            writer.write(np.zeros_like(normal))
            writer.release()
            settings = dict(root_dir=directory,group_fps=5,test_fps=5,require_regular_model=False,
                            essential_shapes={1:dict(image=str(reference),regions=[
                                dict(id=7,box=[.2,.2,.65,.75])])})
            detector = SimpleNamespace(model=None,predict=lambda frame:([],frame))
            with patch('model.yolo_inference.YoloInference',return_value=detector),patch(
                    'inspection.essential_shapes.ShapeRegistration.fit',
                    return_value=(np.eye(2,3,dtype=np.float32),normal.shape)),patch(
                    'inspection.video_export.finalize_result_video'):
                inspect_video(source,settings,root/'result.mp4',show_window=False,
                              file_test_log=True,log_dir=root/'logs')
            verdict = next((root/'logs').glob('*.txt'))
            self.assertTrue(verdict.stem.endswith('_f'))
            self.assertIn('Essential_7_missing',verdict.read_text(encoding='utf-8'))
            report = json.loads(verdict.with_suffix('.pipeline.json').read_text(encoding='utf-8'))
            self.assertEqual(report['essential_shapes']['items']['7']['result'],'missing')
