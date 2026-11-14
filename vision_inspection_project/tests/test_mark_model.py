import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import cv2
import numpy as np

from inspection.mark_model import train_marks,MarkModel
from inspection.special_checks import annotation_dir,write_json,SpecialInspector,labeled_records


class MarkModelTests(unittest.TestCase):
    def test_live_range_selects_one_photo_without_reading_a_source_file(self):
        from types import SimpleNamespace
        from main import inspect_video
        from inspection.photo_inspection import save_photo_frame_range
        import json
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source,region,_ = self.trained(root)
            second = root/"A_000001.png"
            cv2.imwrite(str(second),self.mark())
            write_json(annotation_dir(root,"A_5fps","OCR")/"A_000001.json",
                       {"image":str(second),"regions":[region]})
            save_photo_frame_range(root/"info","A_5fps","OCR",1,2)
            frames = [cv2.GaussianBlur(self.mark(),(15,15),5),self.mark(),self.mark()]
            class Capture:
                def __init__(self): self.index = 0
                def isOpened(self): return True
                def get(self,prop):
                    return {cv2.CAP_PROP_FPS:5,cv2.CAP_PROP_FRAME_WIDTH:96,cv2.CAP_PROP_FRAME_HEIGHT:96}.get(prop,0)
                def read(self):
                    self.index += 1
                    return (True,frames[self.index-1]) if self.index <= len(frames) else (False,None)
                def release(self): pass
            settings = {"root_dir":str(root),"special_group":"A_5fps","special_labeled_frames":True,
                "special_frame_paths":[str(source),str(second)],"special_info_dir":str(root/"info"),
                "require_regular_model":False,"special_photo_inspection":True,"photo_frame_selection":True,
                "live_photo_inspection":True,"test_fps":5,"live_expected_frames":3}
            detector = SimpleNamespace(model=None,model_error="",predict=lambda frame:([],frame))
            with patch("model.yolo_inference.YoloInference",return_value=detector), \
                    patch("inspection.video_export.finalize_result_video"), \
                    patch("inspection.photo_inspection.select_frame_range_photos",side_effect=AssertionError("No file selection")):
                output = inspect_video("http://127.0.0.1/usb-phone",settings,root/"live_test.mp4",
                    show_window=False,log_dir=root/"logs",file_test_log=True,capture_factory=Capture)
            self.assertTrue(output.exists())
            self.assertEqual(len(list(root.glob("live_test_OCR*.png"))),1)
            report = json.loads((root/"logs/live_test_p.pipeline.json").read_text())
            self.assertIn("OCR_photo_frame_2",(root/"logs/live_test_p.txt").read_text())
            self.assertEqual(report["processed_frames"],3)
            frames[:] = frames[:1]
            with patch("model.yolo_inference.YoloInference",return_value=detector), \
                    self.assertRaisesRegex(RuntimeError,"before all OCR/Label inspection ranges completed"):
                inspect_video("http://127.0.0.1/usb-phone",settings,root/"short_live_test.mp4",
                    show_window=False,log_dir=root/"logs",file_test_log=True,capture_factory=Capture)

    def test_fail_examples_are_learned_and_veto_normal_match(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source,region,_ = self.trained(root)
            region["verdict"] = "PASS"
            write_json(annotation_dir(root,"A_5fps","OCR")/"A_000000.json",
                       {"image":str(source),"regions":[region]})
            failed = root/"A_000001.png"
            cv2.imwrite(str(failed),self.mark())
            write_json(annotation_dir(root,"A_5fps","OCR")/"A_000001.json",
                       {"image":str(failed),"regions":[dict(region,verdict="FAIL")]})
            model = MarkModel(train_marks(root,"A_5fps"))
            self.assertEqual(model.predict(self.mark(),region)["verdict"],"NG")
            self.assertEqual({m["verdict"] for m in model.metadata},{"PASS","FAIL"})

    def mark(self):
        image = np.full((96,96,3),245,np.uint8)
        cv2.circle(image,(48,48),29,(20,20,20),4)
        cv2.line(image,(30,49),(43,62),(20,20,20),5)
        cv2.line(image,(43,62),(65,32),(20,20,20),5)
        return image

    def trained(self,root):
        source = root/"A_000000.png"
        cv2.imwrite(str(source),self.mark())
        region = {"role":"mark","mark_id":"normal-logo","box":[0,0,1,1]}
        write_json(annotation_dir(root,"A_5fps","OCR")/"A_000000.json",{"image":str(source),"regions":[region]})
        path = train_marks(root,"A_5fps")
        return source,region,MarkModel(path)

    def test_normal_and_small_pose_changes_match_but_blank_and_other_images_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            source,region,model = self.trained(Path(directory))
            self.assertEqual(model.predict(self.mark(),region)["verdict"],"OK")
            matrix = cv2.getRotationMatrix2D((47.5,47.5),3,1.04)
            matrix[:,2] += [1,-1]
            moved = cv2.warpAffine(self.mark(),matrix,(96,96),borderValue=(245,245,245))
            result = model.predict(moved,region)
            self.assertEqual(result["verdict"],"OK",result)
            self.assertEqual(model.predict(np.full((96,96,3),245,np.uint8),region)["verdict"],"NG")
            other = np.full((96,96,3),245,np.uint8)
            cv2.putText(other,"X",(20,75),cv2.FONT_HERSHEY_SIMPLEX,2,(20,20,20),4)
            self.assertEqual(model.predict(other,region)["verdict"],"NG")
            self.assertEqual(model.predict(self.mark(),dict(region,mark_id="other"))["verdict"],"NG")

    def test_mark_only_group_loads_without_ocr_engine_and_checks_saved_frame(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source,region,model = self.trained(root)
            settings = {"root_dir":str(root),"special_group":"A_5fps","special_labeled_frames":True,
                        "special_frame_paths":[str(source)]}
            self.assertEqual(len(labeled_records(root,"A_5fps","OCR")),1)
            with patch("inspection.special_checks.MultilingualOCR",side_effect=AssertionError("No text OCR for mark-only group")):
                inspector = SpecialInspector(settings)
            self.assertEqual(inspector.check_frame(self.mark(),1,1),[])
            self.assertEqual(inspector.mark_visuals[0]["verdict"],"OK")
            failures = inspector.check_frame(np.full((96,96,3),245,np.uint8),1,1)
            self.assertEqual(failures[0]["class_name"],"OCR_mark_1_mismatch")
            self.assertEqual(failures[0]["inspection_kind"],"OCR")
            self.assertEqual(failures[0]["mark_number"],1)

    def test_changed_mark_box_requires_retraining(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source,region,_ = self.trained(root)
            region["box"] = [.1,.1,.9,.9]
            write_json(annotation_dir(root,"A_5fps","OCR")/"A_000000.json",{"image":str(source),"regions":[region]})
            with self.assertRaisesRegex(RuntimeError,"Mark boxes changed"):
                SpecialInspector({"root_dir":str(root),"special_group":"A_5fps","special_labeled_frames":True,
                                  "special_frame_paths":[str(source)]})

    def test_mark_only_photo_and_unified_log_without_result_video(self):
        from main import inspect_video
        from inspection.photo_inspection import save_photo_frame_range
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source,region,_ = self.trained(root)
            save_photo_frame_range(root/"info","A_5fps","OCR",1,1)
            settings = {"root_dir":str(root),"special_group":"A_5fps","special_labeled_frames":True,
                "special_frame_paths":[str(source)],"special_info_dir":str(root/"info"),"require_regular_model":False,
                "special_photo_inspection":True,"photo_frame_selection":True,"test_fps":5}
            for label,image in (("normal",self.mark()),("missing",np.full((96,96,3),245,np.uint8))):
                video = root/f"{label}.avi"
                writer = cv2.VideoWriter(str(video),cv2.VideoWriter_fourcc(*"MJPG"),10,(96,96))
                for _ in range(10):
                    writer.write(image)
                writer.release()
                with patch("inspection.special_checks.MultilingualOCR",side_effect=AssertionError("No text engine")):
                    output = inspect_video(video,settings,root/f"{label}_result.mp4",show_window=False,
                        display_width=96,display_height=96,file_test_log=True,log_dir=root/"logs")
                self.assertEqual(output.suffix,".png")
                self.assertTrue(output.exists())
                self.assertFalse((root/f"{label}_result.mp4").exists())
                marker = "p" if label == "normal" else "f"
                text = (root/f"logs/{label}_result_{marker}.txt").read_text()
                self.assertIn("result_PASS" if label == "normal" else "OCR_mark_1_mismatch",text)
