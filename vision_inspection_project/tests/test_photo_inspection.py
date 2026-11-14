import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import cv2
import numpy as np

from inspection.photo_inspection import load_time, save_time, select_photos, time_path
from inspection.photo_inspection import load_frame, save_frame, select_numbered_photos
from inspection.photo_inspection import load_photo_frame_range,save_photo_frame_range,select_frame_range_photos


class PhotoInspectionTests(unittest.TestCase):
    def test_range_skips_unlabeled_and_stops_on_whole_frame_pass(self):
        with tempfile.TemporaryDirectory() as directory:
            source = self.video(Path(directory))
            calls = []
            def evaluate(kind, image, number):
                calls.append((kind,number))
                return number == 4
            photos = select_frame_range_photos(source,5,{"label":{2:{},4:{},5:{}}},
                                               {"label":(1,5)},evaluate=evaluate)
            self.assertEqual(calls,[("label",2),("label",4)])
            self.assertEqual(photos["label"]["result"],"OK")
            self.assertEqual(photos["label"]["frame_number"],4)
            self.assertEqual(photos["label"]["decision_frame"],4)
            self.assertEqual(photos["label"]["attempted_frames"],2)

    def test_range_all_candidates_fail_and_empty_schedule_errors(self):
        with tempfile.TemporaryDirectory() as directory:
            source = self.video(Path(directory))
            calls = []
            def evaluate(kind, image, number):
                calls.append(number)
                return False
            photos = select_frame_range_photos(source,5,{"OCR":{1:{},3:{}}},
                                               {"OCR":(1,5)},evaluate=evaluate)
            self.assertEqual(calls,[1,3])
            self.assertEqual(photos["OCR"]["result"],"NG")
            self.assertEqual(photos["OCR"]["decision_frame"],5)
            with self.assertRaisesRegex(ValueError,"no matching training labels"):
                select_frame_range_photos(source,5,{"OCR":{}},{"OCR":(1,5)},evaluate=evaluate)

    def video(self, root):
        path = root/"source.avi"
        writer = cv2.VideoWriter(str(path),cv2.VideoWriter_fourcc(*"MJPG"),10,(64,48))
        for index in range(10):
            value = 220 if index == 4 else index*10
            writer.write(np.full((48,64,3),value,np.uint8))
        writer.release()
        return path

    def test_time_files_are_separate_by_group_and_kind(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            save_time(root,"A_5fps","label",0,.5)
            save_time(root,"A_5fps","OCR",.6,.9)
            self.assertEqual(load_time(root,"A_5fps","label"),(0,.5))
            self.assertEqual(load_time(root,"A_5fps","OCR"),(.6,.9))
            self.assertEqual(time_path(root,"A_5fps","OCR").name,"A_5fps_OCR_inspection_time.txt")
            with self.assertRaises(ValueError):
                load_time(root,"B_5fps","label")
            for start,end in ((-1,1),(2,1),(1,1),(0,float("inf"))):
                with self.assertRaises(ValueError):
                    save_time(root,"A","label",start,end)

    def test_fixed_frames_are_separate_and_exact_at_group_fps(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = self.video(root)
            save_frame(root,"A_5fps","label",3)
            save_frame(root,"A_5fps","OCR",5)
            self.assertEqual(load_frame(root,"A_5fps","label"),3)
            self.assertEqual(load_frame(root,"A_5fps","OCR"),5)
            schedules = {kind:{n:{} for n in range(1,6)} for kind in ("label","OCR")}
            with patch("inspection.photo_inspection.sharpness",side_effect=AssertionError("No sharpness scan")):
                photos = select_numbered_photos(source,5,schedules,{"label":3,"OCR":5})
            self.assertEqual(photos["label"]["source_frame_index"],4)
            self.assertAlmostEqual(float(photos["label"]["image"].mean()),220,delta=2)
            self.assertEqual(photos["OCR"]["source_frame_index"],8)
            with self.assertRaisesRegex(ValueError,"exceeds"):
                select_numbered_photos(source,5,schedules,{"label":6})
            schedules["label"].pop(3)
            with self.assertRaisesRegex(ValueError,"no matching training label"):
                select_numbered_photos(source,5,schedules,{"label":3})
            for value in (0,-1,2.5,float("nan")):
                with self.assertRaises(ValueError):
                    save_frame(root,"A_5fps","label",value)

    def test_sharpest_selection_uses_group_fps_and_exact_training_number(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = self.video(root)
            schedules = {kind:{n:{} for n in range(1,6)} for kind in ("label","OCR")}
            ranges = {"label":(0,.5),"OCR":(.6,.9)}
            with patch("inspection.photo_inspection.sharpness",side_effect=lambda image:float(image.mean())):
                selections = select_photos(source,5,schedules,ranges)
                self.assertEqual(selections["label"]["frame_number"],3)
                self.assertEqual(selections["OCR"]["frame_number"],5)
                self.assertEqual(selections["label"]["seconds"],.4)
                schedules["label"].pop(3)
                fallback = select_photos(source,5,schedules,ranges)
                self.assertNotEqual(fallback["label"]["frame_number"],3)
            with self.assertRaisesRegex(ValueError,"exceed video duration"):
                select_photos(source,5,schedules,{"label":(0,2)})

    def run_inspection(self, root, mixed=False, fixed=False, multi=False, failed_frames=None, live=False):
        from main import inspect_video
        source = self.video(root)
        for kind,start,end in (("label",0,.5),("OCR",.6,.9)):
            save_time(root/"info","A_5fps",kind,start,end)
        if fixed:
            save_frame(root/"info","A_5fps","label",3)
            save_frame(root/"info","A_5fps","OCR",5)
        if multi:
            from inspection.photo_inspection import save_photo_frame_ranges
            for kind in ('label','OCR'):
                save_photo_frame_ranges(root/'info','A_5fps',kind,[(1,2),(4,5)])
        calls = []
        class Inspector:
            def begin_item_range(self,start,end):
                pass
            def __init__(self,settings):
                kinds = ["label","OCR"] + (["conn"] if mixed else [])
                self.references = {kind:{} for kind in kinds}
                self.annotation_frames = {kind:{n:{} for n in range(1,6)} for kind in kinds}
                self.status_lines = []
                self.detail_detections = []
            def check_frame(self,frame,number,total,valid_mask=None):
                active = [kind for kind,schedule in self.annotation_frames.items() if number in schedule]
                calls.append((number,active))
                self.detail_detections = ([{"inspection_kind":"OCR","class_name":"OCR_mismatch","xyxy":[1,1,10,10],
                    "confidence":1,"special_failure":True,"expected_text":"AB","recognized_text":"AC"}]
                    if "OCR" in active and (failed_frames is None or number in failed_frames) else [])
                return self.detail_detections
        settings = {"root_dir":str(root),"special_group":"A_5fps","special_info_dir":str(root/"info"),
                    "special_photo_inspection":True,"special_labeled_frames":True,"test_fps":5,
                    "require_regular_model":False}
        settings["photo_frame_selection"] = fixed
        capture = None
        if live:
            capture = cv2.VideoCapture(str(source))
            settings.update(live_photo_inspection=True,live_expected_frames=5)
        detector = SimpleNamespace(model=None,predict=lambda frame:([],frame))
        with patch("inspection.special_checks.SpecialInspector",Inspector), \
             patch("model.yolo_inference.YoloInference",return_value=detector), \
             patch("inspection.photo_inspection.sharpness",side_effect=lambda frame:float(frame.mean())), \
             patch("inspection.video_export.finalize_result_video"):
            output = inspect_video(source,settings,root/"result.mp4",show_window=False,
                                   display_width=64,display_height=48,log_dir=root/"logs",file_test_log=True,capture_override=capture)
        return output,calls

    def test_multiple_ranges_all_pass_file_and_live(self):
        for live in (False,True):
            with self.subTest(live=live), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                self.run_inspection(root,fixed=True,multi=True,failed_frames={1},live=live)
                report = json.loads((root/'logs/result_p.pipeline.json').read_text())
                photos = report['photo_inspections']
                self.assertEqual(len(photos),4)
                self.assertTrue(all(p['result'] == 'OK' for p in photos))
                self.assertEqual(sorted(p['frame_number'] for p in photos if p['kind'] == 'OCR'),[2,4])
                self.assertEqual(len({p['output'] for p in photos}),4)

    def test_later_range_pass_cannot_override_failed_range_file_and_live(self):
        for live in (False,True):
            with self.subTest(live=live), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                self.run_inspection(root,fixed=True,multi=True,failed_frames={1,2},live=live)
                report = json.loads((root/'logs/result_f.pipeline.json').read_text())
                ocr = [p for p in report['photo_inspections'] if p['kind'] == 'OCR']
                self.assertEqual([p['result'] for p in ocr],['NG','OK'])

    def test_multi_range_persistence_validation_and_aggregate(self):
        from inspection.photo_inspection import save_photo_frame_ranges,load_photo_frame_ranges,range_statuses
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            save_photo_frame_ranges(root,'A','OCR',[(1,10),(30,40)])
            self.assertEqual(load_photo_frame_ranges(root,'A','OCR'),[(1,10),(30,40)])
            for ranges in ([],[(1,3),(3,4)],[(5,6),(1,2)],[(0,1)],[(1,2.5)]):
                with self.assertRaises(ValueError):
                    save_photo_frame_ranges(root,'A','label',ranges)
            with self.assertRaises(ValueError):
                save_photo_frame_ranges(root,'A','conn',[(1,2),(4,5)])
        self.assertEqual(range_statuses({'OCR__range_1':'OK','OCR__range_2':'PENDING'}),{'OCR':'PENDING'})
        self.assertEqual(range_statuses({'OCR__range_1':'NG','OCR__range_2':'OK'}),{'OCR':'NG'})

    def test_legacy_single_frame_becomes_inclusive_one_frame_range(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output,calls = self.run_inspection(root,fixed=True)
            self.assertTrue(output.exists())
            self.assertEqual(calls,[(3,["label"]),(5,["OCR"])])
            report = json.loads((root/"logs/result_f.pipeline.json").read_text())
            self.assertTrue(all(photo["selection"] == "first_complete_pass_in_range" for photo in report["photo_inspections"]))

    def test_frame_ranges_persist_and_choose_sharpest_including_endpoints(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = self.video(root)
            save_frame(root,"A_5fps","label",3)
            self.assertEqual(load_photo_frame_range(root,"A_5fps","label"),(3,3))
            save_photo_frame_range(root,"A_5fps","label",1,4)
            save_photo_frame_range(root,"A_5fps","OCR",4,5)
            self.assertEqual(load_photo_frame_range(root,"A_5fps","label"),(1,4))
            self.assertEqual(load_photo_frame_range(root,"A_5fps","OCR"),(4,5))
            schedules={kind:{n:{} for n in range(1,6)} for kind in ("label","OCR")}
            with patch("inspection.photo_inspection.sharpness",side_effect=lambda image:float(image.mean())):
                photos=select_frame_range_photos(source,5,schedules,{"label":(1,4),"OCR":(4,5)})
                self.assertEqual(photos["label"]["frame_number"],3)
                self.assertEqual(photos["OCR"]["frame_number"],5)
                self.assertEqual(photos["label"]["start_frame"],1)
                self.assertEqual(photos["label"]["end_frame"],4)
                schedules["label"].pop(3)
                fallback = select_frame_range_photos(source,5,schedules,{"label":(1,4)})
                self.assertNotEqual(fallback["label"]["frame_number"],3)
            with self.assertRaises(ValueError):
                select_frame_range_photos(source,5,schedules,{"label":(1,6)})
            for start,end in ((0,5),(4,3),(1.5,4),(1,float("nan"))):
                with self.assertRaises(ValueError):
                    save_photo_frame_range(root,"A_5fps","label",start,end)

    def test_photo_only_saves_two_photos_and_one_unified_log_no_video(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output,calls = self.run_inspection(root)
            self.assertEqual(output.suffix,".png")
            self.assertFalse((root/"result.mp4").exists())
            self.assertEqual(sorted(p.name for p in root.glob("*.png")),["result_OCR_f.png","result_label_p.png"])
            self.assertEqual(calls,[(1,["label"]),(4,["OCR"]),(5,["OCR"])])
            self.assertEqual(len(list((root/"logs").glob("*.txt"))),1)
            log = (root/"logs/result_f.txt").read_text()
            self.assertIn("label_photo_frame_1",log)
            self.assertIn("OCR_photo_frame_5",log)
            report = json.loads((root/"logs/result_f.pipeline.json").read_text())
            self.assertIsNone(report["output_video"])
            self.assertEqual(len(report["photo_inspections"]),2)

    def test_conn_keeps_video_processing_while_text_checks_only_selected_frames(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output,calls = self.run_inspection(root,True)
            self.assertEqual(output.suffix,".mp4")
            self.assertTrue(output.exists())
            self.assertEqual(len([1 for _,a in calls if "conn" in a]),5)
            self.assertEqual([n for n,a in calls if "label" in a],[1])
            self.assertEqual([n for n,a in calls if "OCR" in a],[4,5])
            self.assertTrue((root/"result_conn_p.png").exists())
            report = json.loads((root/"logs/result_f.pipeline.json").read_text())
            conn = next(item for item in report["photo_inspections"] if item["kind"] == "conn")
            self.assertEqual(conn["result"],"OK")
            self.assertEqual(conn["frame_number"],1)
