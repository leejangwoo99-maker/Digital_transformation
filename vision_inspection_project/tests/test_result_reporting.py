import tempfile
import tkinter as tk
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import cv2
import numpy as np

from inspection.special_checks import SpecialInspector
from inspection.photo_inspection import select_photos
from inspection.annotation_details import draw_pass_labels,failure_detail_lines
from main import result_file_path,inspect_video
from video_inspection_gui import VideoInspectionApp,load_test_conditions


class ResultReportingTests(unittest.TestCase):
    def test_group_conditions_defaults_save_and_switch_independently(self):
        root = tk.Tk()
        root.withdraw()
        try:
            app = VideoInspectionApp(root)
            with tempfile.TemporaryDirectory() as directory:
                app.output_dir = Path(directory)
                app.active_group_name = "A_23fps"
                app.load_group_test_conditions()
                self.assertEqual(app.min_confidence_var.get(),50)
                self.assertEqual(app.iou_var.get(),45)
                app.min_confidence_var.set(70)
                app.save_test_conditions()
                saved = load_test_conditions(app.output_dir,"A_23fps")
                self.assertEqual(saved,{"confidence_percent":70.,"iou_percent":45.,"threshold_percent":70.,'test_fps':23.})
                app.active_group_name = "B_23fps"
                app.load_group_test_conditions()
                self.assertEqual(app.min_confidence_var.get(),50)
                settings = app._settings_from_ui()
                self.assertEqual(Path(settings["result_directory"]).name,"B_23fps_threshold_50")
                self.assertEqual(settings["model"]["confidence"],.5)
                app.save_test_conditions()
                app.active_group_name = "A_23fps"
                app.load_group_test_conditions()
                self.assertEqual(app.min_confidence_var.get(),70)
                other = app.settings_for_frame_group("B_23fps",[])
                self.assertEqual(other["model"]["confidence"],.5)
                self.assertEqual(app.min_confidence_var.get(),70)
                self.assertFalse((app.output_dir/"info/test_condition.txt").exists())
        finally:
            root.destroy()

    def test_result_marker_is_last_even_with_collision(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/"result_OCR.png"
            result_file_path(path,True).touch()
            self.assertEqual(result_file_path(path,True).name,"result_OCR_1_p.png")
            self.assertEqual(result_file_path(path,False).name,"result_OCR_f.png")

    def test_video_is_saved_under_group_threshold_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root/"source.avi"
            writer = cv2.VideoWriter(str(source),cv2.VideoWriter_fourcc(*"MJPG"),5,(100,100))
            writer.write(np.zeros((100,100,3),np.uint8))
            writer.release()
            target = root/"test/A_23fps_threshold_50"
            settings = {"result_directory":str(target)}
            detector = SimpleNamespace(model=True,predict=lambda frame:([],frame))
            special = SimpleNamespace(references={},check=lambda *args:[])
            with patch("model.yolo_inference.YoloInference",return_value=detector), \
                 patch("inspection.special_checks.SpecialInspector",return_value=special), \
                 patch("inspection.video_export.finalize_result_video"):
                output = inspect_video(source,settings,root/"result.mp4",show_window=False,status_suffix=True,
                                       file_test_log=True,log_dir=root/"logs")
            self.assertEqual(output.parent,target.parent/'source_group_30fps_threshold_50_[30fps]')
            self.assertEqual(output.name,"result_p_[30fps].mp4")
            self.assertTrue(output.exists())
            log = next((root/"logs").glob("*.txt")).read_text(encoding="utf-8")
            self.assertIn("performance_slowest_stage_",log)
            self.assertIn("performance_stage_appearance_inference_count_1",log)

    def test_label_success_records_both_qr_and_actual_text(self):
        inspector = SpecialInspector.__new__(SpecialInspector)
        inspector.references = {"label":{"regions":[
            {"role":"qr","number":1,"box":[.1,.1,.4,.4]},
            {"role":"text","number":2,"box":[.5,.5,.8,.8],"text":"A"}]}}
        inspector.times = {"label":(0,None)}
        inspector.ocr = SimpleNamespace(candidates=lambda image:["A"])
        inspector.qr = SimpleNamespace(detectAndDecode=lambda image:("A",None,None))
        self.assertEqual(inspector.check(np.zeros((100,100,3),np.uint8),1),[])
        self.assertEqual(len(inspector.pass_details),2)
        self.assertEqual(inspector.pass_details[1]["expected_text"],"A")
        self.assertEqual(inspector.pass_details[1]["recognized_text"],{"code":["A"],"text":["A"]})

    def test_ocr_requires_text_and_mark_to_pass_on_same_frame(self):
        inspector = SpecialInspector.__new__(SpecialInspector)
        record = {"regions":[{"role":"text","box":[.1,.1,.4,.4],"text":"A"},
                              {"role":"mark","box":[.5,.5,.8,.8],"mark_id":"M"}]}
        inspector.references = {"OCR":record}
        inspector.times = {"OCR":(0,None)}
        inspector.annotation_frames = {"OCR":{1:record,2:record,3:record}}
        inspector.labeled_frames_only = True
        inspector.ocr = SimpleNamespace(candidates=lambda image:["A"] if image.mean()<80 or image.mean()>180 else ["X"])
        inspector.mark_model = SimpleNamespace(predict=lambda image,region:{"verdict":"NG" if image.mean()<80 else "OK"})
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory)/"source.avi"
            writer = cv2.VideoWriter(str(source),cv2.VideoWriter_fourcc(*"MJPG"),5,(100,100))
            for value in (60,120,220):
                writer.write(np.full((100,100,3),value,np.uint8))
            writer.release()
            def evaluate(kind,image,number):
                return not inspector.check_frame(image,number,3)
            failed = select_photos(source,5,inspector.annotation_frames,{"OCR":(0,.2)},evaluate=evaluate)
            self.assertEqual(failed["OCR"]["result"],"NG")
            passed = select_photos(source,5,inspector.annotation_frames,{"OCR":(0,.4)},evaluate=evaluate)
            self.assertEqual(passed["OCR"]["result"],"OK")
            self.assertEqual(passed["OCR"]["attempted_frames"],3)
            details = inspector.detail_detections
            self.assertEqual(len(details),2)
            self.assertTrue(all(item["inspection_pass"] for item in details))
            self.assertTrue(any('A' in line for line in failure_detail_lines(details)))
            rendered = draw_pass_labels(np.zeros((100,100,3),np.uint8),details)
            self.assertGreater(np.count_nonzero(rendered[:,:,1]),0)
