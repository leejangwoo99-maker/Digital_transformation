import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch,Mock

import cv2
import numpy as np

from inspection.shape_registration import ShapeRegistration,transform_region
from inspection.conn_outcome import ConnOutcome
from inspection.proto_labels import parse_frames


class ShapeRegistrationTests(unittest.TestCase):
    def test_reference_area_tracks_translation_rotation_and_scale(self):
        rng = np.random.default_rng(2026)
        image = cv2.GaussianBlur(rng.integers(0,256,(400,500,3),dtype=np.uint8),(3,3),0)
        affine = cv2.getRotationMatrix2D((250,200),12,1.1)
        affine[:,2] += [22,-9]
        expected = np.vstack((affine,[0,0,1]))
        current = cv2.warpPerspective(image,expected,(500,400))
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/"reference.png"
            cv2.imwrite(str(path),image)
            matcher = ShapeRegistration()
            result = matcher.fit(current,path,{"box":[.2,.2,.8,.8]})
            self.assertIsNotNone(result)
            points = np.float32([[[150,130],[350,130],[250,280]]])
            np.testing.assert_allclose(cv2.perspectiveTransform(points,result[0]),
                                       cv2.perspectiveTransform(points,expected),atol=1)
            self.assertIsNone(matcher.fit(np.zeros_like(image),path,{"box":[.2,.2,.8,.8]}))

    def test_all_geometry_uses_common_similarity(self):
        matrix = np.array([[0,-2,200],[2,0,0],[0,0,1]],dtype=float)
        source = {"role":"pin","shape":"circle","box":[.2,.2,.4,.4]}
        result = transform_region(source,matrix,(100,100),(200,200))
        np.testing.assert_allclose(result["box"],[.6,.2,.8,.4])
        self.assertEqual(source["box"],[.2,.2,.4,.4])
        polygon = {"role":"label_roi","points":[[.2,.2],[.4,.2],[.4,.4]]}
        result = transform_region(polygon,matrix,(100,100),(200,200))
        self.assertEqual(len(result["points"]),3)

    def test_conn_single_frame_ok_vetoes_earlier_and_later_confirmed_failure(self):
        state = ConnOutcome()
        state.observe(1,[],[],"ok")
        state.observe(2,[],[{}],"pending")
        state.observe(3,[{}],[],"confirmed")
        state.observe(4,[],[],"later ok")
        self.assertEqual(state.result(),("OK",(1,"ok")))
        state.observe(5,[],[{}],"last NG")
        self.assertEqual(state.result(),("OK",(1,"ok")))

    def test_conn_confirmed_failure_is_ng_without_complete_ok_frame(self):
        state = ConnOutcome()
        state.observe(1,[],[{}],"pending")
        state.observe(2,[{}],[],"confirmed")
        self.assertEqual(state.result(),("NG",(2,"confirmed")))
        state.observe(3,[],[],"ok")
        self.assertEqual(state.result(),("OK",(3,"ok")))

    def test_conn_unconfirmed_glitch_does_not_veto_ok(self):
        state = ConnOutcome()
        state.observe(1,[],[{}],"pending")
        state.observe(2,[],[],"ok")
        self.assertEqual(state.result(),("OK",(2,"ok")))

    def test_proto_source_frame_parser(self):
        self.assertEqual(parse_frames("1,3-5",10),{1,3,4,5})
        self.assertEqual(parse_frames("",10),set())
        for text in ("0","11","5-3","one"):
            with self.assertRaises(ValueError):
                parse_frames(text,10)

    def test_proto_preserves_manual_labels_only_writes_group_and_skips_failed_alignment(self):
        from inspection.proto_labels import generate
        from label_images import YoloBoxLabeler
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            labels = root/"labels"
            labels.mkdir()
            paths = []
            for index,value in enumerate((120,130,0)):
                path = root/f"A_5fps_{index:06}.png"
                cv2.imwrite(str(path),np.full((100,100,3),value,np.uint8))
                paths.append(path)
            manual = labels/f"{paths[0].stem}.txt"
            manual.write_text("1 0.5 0.5 0.2 0.2\n8 0.5 0.5 0.8 0.8\n",encoding="utf-8")
            original = manual.read_bytes()
            unrelated = labels/"B_5fps_000001.txt"
            unrelated.write_text("untouched",encoding="utf-8")
            controller = YoloBoxLabeler(root,labels,{1:"scratch",8:"except"},image_paths=paths)
            model = Mock()
            model.predict.return_value = [SimpleNamespace(obb=None,boxes=[])]
            controller.proto_train = Mock(return_value=model)
            def fit(image,*args):
                return (np.eye(3),image.shape) if image.mean() else None
            with patch("inspection.proto_labels.ShapeRegistration") as matcher:
                matcher.return_value.fit.side_effect = fit
                _,report = generate(controller,{1})
            self.assertEqual(manual.read_bytes(),original)
            self.assertEqual(unrelated.read_text(),"untouched")
            self.assertEqual(report["generated"],[{"frame":2,"source_frame":1,"added":1}])
            self.assertEqual(report["skipped"][0]["frame"],3)
            self.assertEqual(len((labels/f"{paths[1].stem}.txt").read_text().splitlines()),1)
            self.assertFalse((labels/f"{paths[2].stem}.txt").exists())
            controller.proto_train.assert_called_once_with([paths[0]])

    def test_proto_supplements_partial_labels_at_seventy_percent_without_duplicates(self):
        from inspection.proto_labels import generate
        from label_images import YoloBoxLabeler
        for confidence in (.69,.7):
            with self.subTest(confidence=confidence), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                labels = root/'labels'
                labels.mkdir()
                paths = [root/f'A_{n:06}.png' for n in range(2)]
                for path in paths:
                    cv2.imwrite(str(path),np.full((100,100,3),100,np.uint8))
                (labels/f'{paths[0].stem}.txt').write_text('1 0.2 0.2 0.1 0.1\n')
                target = labels/f'{paths[1].stem}.txt'
                original = '1 0.700000 0.700000 0.100000 0.100000\n'
                target.write_text(original)
                controller = YoloBoxLabeler(root,labels,{1:'scratch'},image_paths=paths)
                prediction = SimpleNamespace(cls=np.array([1]),conf=np.array([confidence]),xyxy=np.array([[15,15,25,25]]))
                model = Mock()
                model.predict.return_value = [SimpleNamespace(obb=None,boxes=[prediction])]
                controller.proto_train = Mock(return_value=model)
                with patch('inspection.proto_labels.ShapeRegistration') as matcher:
                    matcher.return_value.fit.return_value = (np.eye(3),(100,100,3))
                    generate(controller,{1})
                    content = target.read_text()
                    self.assertTrue(content.startswith(original))
                    self.assertEqual(len(content.splitlines()),2 if confidence >= .7 else 1)
                    generate(controller,{1})
                    self.assertEqual(target.read_text(),content)
                self.assertTrue(all(call.kwargs['conf'] == .7 for call in model.predict.call_args_list))

    def test_delete_selection_must_touch_label_border(self):
        from inspection.rotated_regions import polygon_border_overlaps_rectangle
        outer = [[0,0],[100,0],[100,100],[0,100]]
        self.assertFalse(polygon_border_overlaps_rectangle(outer,[20,20,40,40]))
        self.assertTrue(polygon_border_overlaps_rectangle(outer,[-1,20,1,40]))
        self.assertTrue(polygon_border_overlaps_rectangle(outer,[-1,-1,101,101]))
        self.assertTrue(polygon_border_overlaps_rectangle([[25,25],[35,25],[35,35],[25,35]],[20,20,40,40]))

    def test_registration_failure_does_not_run_ocr_and_reports_separate_reason(self):
        from inspection.special_checks import SpecialInspector
        inspector = SpecialInspector.__new__(SpecialInspector)
        inspector.references = {"OCR":{"image":"unused.png","regions":[
            {"role":"registration","box":[.1,.1,.9,.9]},
            {"role":"text","box":[.2,.2,.4,.4],"text":"AB"}]}}
        inspector.times = {"OCR":(0,None)}
        inspector.ocr = Mock()
        inspector.shape_registration = Mock()
        inspector.shape_registration.fit.return_value = None
        failures = inspector.check(np.zeros((100,100,3),np.uint8),1)
        self.assertEqual(failures[0]["class_name"],"OCR_registration_not_found")
        self.assertEqual(failures[0]["inspection_kind"],"OCR")
        inspector.ocr.candidates.assert_not_called()

    def test_registered_inspection_projects_failure_back_to_live_coordinates(self):
        from inspection.special_checks import SpecialInspector
        inspector = SpecialInspector.__new__(SpecialInspector)
        inspector.references = {"OCR":{"image":"unused.png","regions":[
            {"role":"registration","box":[.1,.1,.9,.9]},
            {"role":"text","box":[.2,.2,.4,.4],"text":"AB"}]}}
        inspector.times = {"OCR":(0,None)}
        inspector.ocr = SimpleNamespace(candidates=lambda crop:[])
        inspector.shape_registration = Mock()
        inspector.shape_registration.fit.return_value = (np.array([[1.,0,20],[0,1,10],[0,0,1]]),(100,100,3))
        failures = inspector.check(np.zeros((100,100,3),np.uint8),1)
        self.assertEqual(failures[0]["class_name"],"OCR_mismatch")
        self.assertEqual(failures[0]["xyxy"],(40,30,60,50))
