import json
import tempfile
import tkinter as tk
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock,patch

import cv2
import numpy as np

from inspection.threshold_review import ConfidenceRecorder,ThresholdReviewWindow,load_review
from inspection.photo_inspection import save_time
from main import inspect_video
from model.yolo_inference import YoloInference


class ThresholdReviewTests(unittest.TestCase):
    def test_review_thresholds_reuse_the_same_saved_image_and_detections(self):
        from inspection.confidence_runs import reserve_run
        from inspection.threshold_review import changed_box_scores, load_review_sets
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory)/'part_A_23fps_threshold_0.20_[5fps]'
            recorder = ConfidenceRecorder(folder,'A_23fps',20,'part.mp4',5,
                                          run=reserve_run(folder,1),review_thresholds=[10,20,30])
            recorder.add(1,np.zeros((100,100,3),np.uint8),[
                dict(class_name='scratch',confidence=.15,xyxy=[10,10,30,30])])
            recorder.finish('completed')
            self.assertEqual(len(list(folder.rglob('*.png'))),1)
            _,sets = load_review_sets(folder)
            self.assertEqual(list(sets),[10,20,30])
            frames = [data['frames'][0] for data in sets.values()]
            self.assertEqual([item['result'] for item in frames],['NG','OK','OK'])
            self.assertEqual(len({item['image'] for item in frames}),1)
            self.assertIs(frames[0]['detections'],frames[1]['detections'])
            self.assertEqual(changed_box_scores(sets[10],sets[20]),set())

    def test_openvino_appearance_uses_exported_task_and_cpu(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            weights = root/'appearance.pt'
            weights.write_bytes(b'checkpoint')
            artifact = root/'appearance_openvino_model'
            artifact.mkdir()
            (artifact/'metadata.yaml').write_text('task: obb\n',encoding='utf-8')
            with patch('model.appearance_openvino.openvino_artifact',return_value=artifact) as export, \
                 patch('ultralytics.YOLO') as yolo:
                yolo.return_value.predict.return_value = [SimpleNamespace(names={},obb=[],boxes=None)]
                detector = YoloInference(dict(root_dir=directory,model=dict(
                    weights=weights.name,appearance_backend='openvino',render_predictions=False)))
                detector.predict(np.zeros((100,100,3),np.uint8))
                export.assert_called_once_with(weights,640)
                yolo.assert_called_once_with(str(artifact),task='obb')
                self.assertEqual(yolo.return_value.predict.call_args.kwargs['device'],'intel:cpu')

    def test_multiple_threshold_lines_switch_saved_scores_and_highlight_changes(self):
        from inspection.confidence_runs import reserve_run
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            for threshold,score in ((10,.30),(20,.31)):
                folder = parent/f'part_A_23fps_threshold_{threshold/100:.2f}_[5fps]'
                recorder = ConfidenceRecorder(folder,'A_23fps',threshold,'part.mp4',5,run=reserve_run(folder,1))
                recorder.add(1,np.zeros((100,100,3),np.uint8),[
                    dict(class_name='scratch',confidence=.15,xyxy=[10,10,30,30]),
                    dict(class_name='dent',confidence=score,xyxy=[50,50,80,80])])
                recorder.finish('completed')
            root = tk.Tk()
            root.withdraw()
            try:
                view = ThresholdReviewWindow(root,parent)
                view.window.update()
                self.assertEqual(view.thresholds,[10,20])
                def colors():
                    return [view.plot.itemcget(i,'fill') for i in view.plot.find_all()
                            if view.plot.type(i) == 'oval']
                self.assertEqual(colors(),['#dc2626','#eab308'])
                line_y = next(y for value,y,_ in view.threshold_hits if value == 20)
                view.click(SimpleNamespace(x=100,y=line_y))
                self.assertEqual(view.threshold,20)
                self.assertEqual(colors(),['#16a34a','#eab308'])
                self.assertIn('31.00%',view.details.get('1.0','end'))
                self.assertEqual(view.cpk_report['runs'],[1])
                view.wheel_plot(SimpleNamespace(delta=-120,x=300,y=300))
                self.assertEqual(view.plot_zoom,1.25)
                view.start_plot_drag(SimpleNamespace(x=300,y=300))
                before = view.plot_origin
                view.drag_plot(SimpleNamespace(x=330,y=330))
                view.finish_plot_drag(SimpleNamespace(x=330,y=330))
                self.assertNotEqual(view.plot_origin,before)
                view.reset_plot_view()
                self.assertEqual(view.plot_origin,(0.,0.))
                self.assertEqual(len(view.threshold_hits),2)
            finally:
                root.destroy()

    def test_model_records_at_one_percent_without_changing_judge_threshold(self):
        with patch.object(YoloInference,"_load_model"):
            model = YoloInference({"root_dir":".","model":{"confidence":.5},"record_confidence_points":True})
        model.model = Mock()
        model.model.predict.return_value = [SimpleNamespace(names={},boxes=None,plot=lambda:np.zeros((10,10,3),np.uint8))]
        model.predict(np.zeros((10,10,3),np.uint8))
        self.assertEqual(model.model.predict.call_args.kwargs["conf"],.01)
        self.assertEqual(model.confidence,.5)

    def test_records_zero_low_high_and_overwrites_only_owned_point_files(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            image = np.zeros((100,100,3),np.uint8)
            recorder = ConfidenceRecorder(root,"A_23fps",50,"full1.mp4",23)
            recorder.add(1,image,[])
            recorder.add(2,image,[{"class_name":"scratch","confidence":.3725,"xyxy":[10,10,40,40]}])
            recorder.add(3,image,[{"class_name":"dent","confidence":.8,"xyxy":[10,10,40,40]}])
            recorder.flush("completed")
            folder,data = load_review(root)
            self.assertEqual([item["result"] for item in data["frames"]],["OK","OK","NG"])
            self.assertTrue((folder/"frame_no_2_37.25.png").exists())
            keep = root/"other_result.png"
            keep.write_bytes(b"untouched")
            recorder = ConfidenceRecorder(root,"A_23fps",50,"full2.mp4",23)
            recorder.add(1,image,[])
            recorder.flush("completed")
            self.assertFalse((folder/"frame_no_2_37.25.png").exists())
            self.assertEqual(len(list(folder.glob("frame_no_*.png"))),1)
            self.assertEqual(keep.read_bytes(),b"untouched")
            self.assertEqual(load_review(root)[1]["source"],"full2.mp4")

    def test_plot_click_selects_matching_image_and_detection_details(self):
        with tempfile.TemporaryDirectory() as directory:
            recorder = ConfidenceRecorder(directory,"A_23fps",50,"full1.mp4",23)
            image = np.zeros((100,100,3),np.uint8)
            recorder.add(1,image,[])
            recorder.add(2,image,[{"class_name":"scratch","confidence":.7,"xyxy":[10,10,40,40]},
                                  {"class_name":"dent","confidence":.2,"xyxy":[60,60,85,85]}])
            recorder.flush("completed")
            root = tk.Tk()
            root.withdraw()
            try:
                view = ThresholdReviewWindow(root,directory)
                view.window.update()
                self.assertEqual(len(view.points),3)
                self.assertIn('#16a34a',[view.plot.itemcget(i,'fill') for i in view.plot.find_all()
                                         if view.plot.type(i) == 'oval'])
                x,y = view.points[2]
                view.click(SimpleNamespace(x=x,y=y))
                self.assertIn("20.00% | OK",view.caption.get())
                self.assertEqual(len(view.points),3)
                self.assertTrue(any(view.plot.itemcget(i,'outline') == '#eab308'
                                    for i in view.plot.find_all() if view.plot.type(i) == 'oval'))
                self.assertEqual(view.source_image.getpixel((10,30)),(0,0,0))
                self.assertGreater(view.source_image.getpixel((60,70))[1],0)
                view.visible_run.set('Per frame')
                view.change_run()
                self.assertEqual(view.point_indices,[(1,0),(1,1)])
                view.box_picker.current(2)
                view.change_box()
                self.assertEqual(view.point_indices,[(1,0)])
                self.assertEqual(view.source_image.getpixel((60,70)),(0,0,0))
                view.box_picker.current(1)
                view.change_box()
                self.assertEqual(view.point_indices,[(1,1)])
                view.wheel_image(SimpleNamespace(delta=-120,x=100,y=100))
                self.assertEqual(view.image_zoom,1.25)
                view.wheel_image(SimpleNamespace(delta=120,x=100,y=100))
                self.assertEqual(view.image_zoom,1.)
                view.box_picker.current(0)
                view.change_box()
                view.visible_run.set('All runs')
                view.select(0)
                x,y = view.points[1]
                view.click(SimpleNamespace(x=x,y=y))
                self.assertEqual(view.selected,1)
                self.assertIn("70.00%",view.caption.get())
                self.assertIn("scratch",view.details.get("1.0","end"))
                self.assertIsNotNone(view.photo)
                self.assertEqual(view.source_image.size,(100,100))
                view.select(0)
                self.assertIn("no detections",view.details.get("1.0","end"))
                view.window.destroy()
            finally:
                root.destroy()

    def test_inspection_scores_exclude_rule_failures_and_manual_except(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root/"source.avi"
            writer = cv2.VideoWriter(str(source),cv2.VideoWriter_fourcc(*"MJPG"),5,(100,100))
            for _ in range(3):
                writer.write(np.zeros((100,100,3),np.uint8))
            writer.release()
            predictions = iter([
                [{"class_name":"scratch","confidence":.99,"xyxy":[1,1,10,10]},
                 {"class_name":"scratch","confidence":.3,"xyxy":[40,40,50,50]}],
                [{"class_name":"dent","confidence":.8,"xyxy":[40,40,50,50]}],[]])
            detector = SimpleNamespace(model=True,predict=lambda frame:(next(predictions),frame))
            special = SimpleNamespace(references={"OCR":{}},check=lambda *args:[
                {"class_name":"OCR_mismatch","inspection_kind":"OCR","special_failure":True,"confidence":1.,"xyxy":[20,20,30,30]}])
            result_dir = root/"A_5fps_threshold_50"
            settings = {"result_directory":str(result_dir),"record_confidence_points":True,"special_group":"A_5fps",
                        "judge":{"min_confidence":.5,"ng_classes":["scratch","dent"]},
                        "except_regions":[{"box":[0,0,.2,.2]}]}
            with patch("model.yolo_inference.YoloInference",return_value=detector), \
                 patch("inspection.special_checks.SpecialInspector",return_value=special), \
                 patch("inspection.video_export.finalize_result_video"):
                inspect_video(source,settings,root/"result.mp4",show_window=False)
            result_dir = root/"source_A_5fps_threshold_50_[30fps]"
            _,data = load_review(result_dir)
            self.assertEqual([item["score_percent"] for item in data["frames"]],[30.,80.,0.])
            self.assertEqual([item["result"] for item in data["frames"]],["OK","NG","OK"])
            self.assertEqual(data["status"],"completed")
            low_predictions = iter([[{"class_name":"scratch","confidence":.3,"xyxy":[40,40,50,50]}],[],[]])
            detector.predict = lambda frame:(next(low_predictions),frame)
            special.references = {}
            special.check = lambda *args:[]
            with patch("model.yolo_inference.YoloInference",return_value=detector), \
                 patch("inspection.special_checks.SpecialInspector",return_value=special), \
                 patch("inspection.video_export.finalize_result_video"):
                output = inspect_video(source,settings,root/"low_result.mp4",show_window=False,status_suffix=True)
            self.assertTrue(output.name.endswith("_p_[30fps].mp4"))
            self.assertEqual([item["score_percent"] for item in load_review(result_dir)[1]["frames"]],[30.,0.,0.])

    def test_old_folder_without_point_data_is_not_inferred(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError,"Run a new appearance inspection"):
                load_review(directory)

    def test_video_overlays_only_checked_ocr_frames_and_hides_after_success(self):
        from inspection.annotation_details import preview_with_details
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root/"source.avi"
            writer = cv2.VideoWriter(str(source),cv2.VideoWriter_fourcc(*"MJPG"),5,(128,128))
            for _ in range(3):
                writer.write(np.zeros((128,128,3),np.uint8))
            writer.release()
            save_time(root/"info","A_5fps","OCR",0,.4)
            recorded = []
            delivered = []
            class Writer:
                def __init__(self,path,*args):
                    Path(path).touch()
                def isOpened(self):
                    return True
                def write(self,image):
                    recorded.append(image.copy())
                def release(self):
                    pass
            class Inspector:
                def __init__(self,settings):
                    self.references = {"OCR":{},"conn":{}}
                    self.annotation_frames = {kind:{n:{} for n in (1,2,3)} for kind in self.references}
                    self.detail_detections = []
                    self.status_lines = []
                def check_frame(self,image,number,total,**kwargs):
                    self.detail_detections = []
                    if "OCR" not in self.annotation_frames:
                        return []
                    item = dict(class_name="Text OK" if number == 2 else "OCR_mismatch",
                        confidence=1.,xyxy=[40,80,70,110],inspection_kind="OCR",expected_text="A",
                        recognized_text=["A" if number == 2 else "X"])
                    item["inspection_pass" if number == 2 else "special_failure"] = True
                    self.detail_detections = [item]
                    return [] if number == 2 else [item]
            detector = SimpleNamespace(model=None,predict=lambda image:([],image))
            settings = {"special_group":"A_5fps","special_info_dir":str(root/"info"),"test_fps":5,
                        "special_labeled_frames":True,"special_photo_inspection":True,"save_test_details":True}
            with patch("inspection.special_checks.SpecialInspector",Inspector), \
                 patch("model.yolo_inference.YoloInference",return_value=detector), \
                 patch("cv2.VideoWriter",Writer), \
                 patch("inspection.video_export.finalize_result_video"), \
                 patch("inspection.annotation_details.preview_with_details",wraps=preview_with_details) as panels:
                inspect_video(source,settings,root/"result.mp4",show_window=False,display_width=128,display_height=128,
                              result_callback=lambda image,number,total,statuses,details:delivered.append((image.copy(),number)))
            self.assertEqual(len(recorded),5)
            self.assertGreater(recorded[0][90,40,2],100)
            self.assertGreater(recorded[1][90,40,1],100)
            self.assertGreater(recorded[3][90,40,1],100)
            self.assertEqual(int(recorded[4][90,40].sum()),0)
            video_panels = [call for call in panels.call_args_list
                            if call.kwargs.get("status_lines",[""])[0].startswith("Frame ")]
            self.assertEqual(len(delivered),3)
            self.assertEqual([number for _,number in delivered],[1,2,3])
            self.assertGreater(delivered[0][0][90,40,2],100)
            self.assertGreater(delivered[1][0][90,40,1],100)
            self.assertEqual(int(delivered[2][0][90,40].sum()),0)
            self.assertEqual(len(video_panels),3)
            self.assertTrue(any(item.get("inspection_pass") for item in video_panels[1].args[1]))
            self.assertFalse(any(item.get("inspection_kind") == "OCR" for item in video_panels[2].args[1]))
            self.assertTrue(any("OCR: OK" in line for line in video_panels[2].kwargs["status_lines"]))
