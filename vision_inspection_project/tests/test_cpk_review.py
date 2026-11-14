import json
import math
import tempfile
import tkinter as tk
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock,patch

import cv2
import numpy as np

from inspection.confidence_runs import reserve_run,load_runs
from inspection.cpk_review import capability,grade,match_detections,build_cpk,export_cpk,show_cpk
from inspection.threshold_review import ConfidenceRecorder,load_review,ThresholdReviewWindow


def detection(center,confidence=.7,kind='scratch'):
    return dict(class_name=kind,confidence=confidence,xyxy=[center-5,20,center+5,30])


class CpkTests(unittest.TestCase):
    def test_comparison_runs_two_profiles_five_times_without_baseline(self):
        from video_inspection_gui import VideoInspectionApp
        from inspection import automatic_runtime
        app = VideoInspectionApp.__new__(VideoInspectionApp)
        app.root = SimpleNamespace(after=lambda _,fn:fn())
        app.status_var = Mock()
        app.append_train_log = Mock()
        app._finish_live_test = Mock()
        seen = []
        def inspect(*args,**kwargs):
            current = kwargs['batch_settings']
            seen.append((current['openvino_comparison_profile'],current['comparison_repetition']))
            return True
        app._inspect_worker = inspect
        with patch.object(automatic_runtime,'prepare') as prepare:
            app._batch_inspect_worker([Path('proto_1.mp4')],5,{'compare_openvino':True},None)
        self.assertEqual(seen,[(profile,n) for profile in ('jobs2_threads2','jobs1_threads4') for n in range(1,6)])
        self.assertEqual(prepare.call_count,2)
        app._finish_live_test.assert_called_once()
    def test_review_jumps_to_run_and_frame_and_rejects_missing_frame(self):
        with tempfile.TemporaryDirectory() as temporary:
            parent = Path(temporary)/'A_B_23fps_threshold_0.50'
            for _ in range(2):
                self.record(parent,[[detection(20)]]*3)
            root = tk.Tk()
            root.withdraw()
            try:
                view = ThresholdReviewWindow(root,parent)
                view.jump_run.set('2')
                view.jump_frame.set('2')
                view.jump_to_frame()
                self.assertEqual(view.selected,4)
                self.assertIn('2st | Frame 2',view.caption.get())
                self.assertEqual(view.visible_run.get(),'2st')
                view.visible_run.set('1st')
                view.change_run()
                self.assertEqual(view.selected,1)
                view.jump_run.set('2')
                view.jump_frame.set('999')
                with patch('tkinter.messagebox.showwarning') as warning:
                    view.jump_to_frame()
                    warning.assert_called_once()
                self.assertEqual(view.selected,1)
                self.assertEqual(view.visible_run.get(),'1st')
                view.window.destroy()
            finally:
                root.destroy()

    def test_direct_sixth_run_point_folder_uses_its_actual_run_number(self):
        with tempfile.TemporaryDirectory() as temporary:
            parent = Path(temporary)/'6st_A_B_23fps_threshold_0.50'
            recorder = ConfidenceRecorder(parent,'B_23fps',50,'A.mp4',23,run=reserve_run(parent,number=6))
            recorder.add(14,np.zeros((80,100,3),np.uint8),[detection(20)])
            recorder.finish('completed')
            root = tk.Tk()
            root.withdraw()
            try:
                view = ThresholdReviewWindow(root,recorder.directory)
                self.assertEqual(view.jump_run.get(),'6')
                self.assertEqual(view.jump_frame.get(),'14')
                self.assertIn('6st | Frame 14',view.caption.get())
                view.jump_to_frame()
                view.window.destroy()
            finally:
                root.destroy()

    def test_numbered_parent_accepts_single_prefix_and_legacy_double_prefix(self):
        with tempfile.TemporaryDirectory() as temporary:
            parent = Path(temporary)/'1st_A_B_threshold_0.50_[23fps]'
            recorder = self.record(parent,[[detection(20)]])
            self.assertFalse(recorder.directory.name.startswith('1st_1st_'))
            self.assertEqual(len(load_review(parent)[1]['frames']),1)
            legacy = recorder.directory.with_name('1st_'+recorder.directory.name)
            recorder.directory.rename(legacy)
            self.assertEqual(len(load_review(parent)[1]['frames']),1)

    def test_review_rejects_different_test_fps(self):
        with tempfile.TemporaryDirectory() as temporary:
            parent = Path(temporary)/'A_B_threshold_0.50_[15fps]'
            first = self.record(parent,[[detection(20)]])
            second = self.record(parent,[[detection(20)]])
            self.assertTrue(second.directory.name.endswith('_[15fps]'))
            path = second.directory/'index.json'
            data = json.loads(path.read_text())
            data['fps'] = 5
            path.write_text(json.dumps(data))
            with self.assertRaisesRegex(ValueError,'^different Test fps$'):
                load_review(parent)

    def test_percent_units_median_limits_sample_sigma_and_grades(self):
        result = capability([70,70.03,69.98])
        self.assertEqual(result['median'],70)
        self.assertAlmostEqual(result['lsl'],69.95)
        self.assertAlmostEqual(result['usl'],70.05)
        self.assertAlmostEqual(result['stddev'],.0251661148)
        self.assertEqual(result['verdict'],'불합격')
        self.assertEqual([grade(v) for v in (1.67,1.33,1, .99)],['우수','양호','주의','불합격'])
        self.assertEqual(capability([70])['verdict'],'계산 불가')
        self.assertEqual(capability([70,70])['verdict'],'변동 없음·합격')
        self.assertEqual(capability([70,70],center=50)['verdict'],'변동 없음·불합격')

    def test_nearest_conflict_does_not_fill_different_label_and_missing_is_zero(self):
        tracks = []
        match_detections(tracks,[detection(20),detection(35)],1)
        match_detections(tracks,[detection(21,.6),detection(23,.8)],2)
        self.assertEqual(tracks[0]['scores'][2],60)
        self.assertNotIn(2,tracks[1]['scores'])
        self.assertEqual(len(tracks),2)
        runs = [(Path('.'),dict(source='A.mp4',run_number=1,frames=[dict(frame_number=1,detections=[detection(20),detection(35)])])),
                (Path('.'),dict(source='A.mp4',run_number=2,frames=[dict(frame_number=1,detections=[detection(21,.6)])]))]
        report = build_cpk(runs)
        self.assertEqual(report['rows'][1]['scores'],[70,0])
        self.assertEqual(report['verdict'],'불합격')

    def record(self,parent,items,source='A.mp4',threshold=50,group='B_23fps'):
        run = reserve_run(parent)
        recorder = ConfidenceRecorder(parent,group,threshold,source,23,run=run)
        for number,detections in enumerate(items,1):
            recorder.add(number,np.zeros((80,100,3),np.uint8),detections)
        recorder.finish('completed')
        return recorder

    def test_runs_accumulate_finish_names_review_and_identity_validation(self):
        with tempfile.TemporaryDirectory() as temporary:
            parent = Path(temporary)/'A_B_23fps_threshold_0.50'
            first = self.record(parent,[[detection(20)],[]])
            second = self.record(parent,[[detection(20)],[]])
            self.assertTrue(first.directory.name.startswith('1st_'+parent.name+'_point_frame_'))
            self.assertTrue(second.directory.name.startswith('2st_'))
            self.assertTrue(first.directory.exists())
            _,data = load_review(parent)
            self.assertEqual(len(data['frames']),4)
            self.assertTrue(all((parent/f['image']).is_file() for f in data['frames']))
            report = build_cpk(load_runs(parent))
            self.assertEqual(report['verdict'],'변동 없음·합격')
            self.assertEqual(report['rows'][1]['label'],'검출 없음 #1')
            with self.assertRaisesRegex(ValueError,'same source'):
                self.record(parent,[[detection(20)],[]],group='WRONG')
            with self.assertRaisesRegex(ValueError,'same source'):
                load_runs(parent)
            third = next(path for path in parent.iterdir() if path.name.startswith('3st_'))
            third.rename(third.with_name('3st_OTHER_point_frame_2026-11-05_00-00-00'))
            with self.assertRaisesRegex(ValueError,'Inconsistent run folder'):
                load_runs(parent)

    def test_new_labels_in_later_runs_receive_zero_for_earlier_runs(self):
        runs = [(Path('.'),dict(source='A.mp4',frames=[dict(frame_number=1,detections=[])])),
                (Path('.'),dict(source='A.mp4',frames=[dict(frame_number=1,detections=[detection(20)])]))]
        self.assertEqual(build_cpk(runs)['rows'][0]['scores'],[0,70])
        runs[1][1]['frames'][0]['frame_number'] = 2
        with self.assertRaisesRegex(ValueError,'same inspected frame'):
            build_cpk(runs)

    def test_excel_and_popup_contain_actual_numeric_data(self):
        from openpyxl import load_workbook
        runs = [(Path('.'),dict(source='A.mp4',frames=[dict(frame_number=1,detections=[detection(20)])]))]*2
        report = build_cpk(runs)
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary)/'cpk.xlsx'
            export_cpk(report,path)
            book = load_workbook(path)
            self.assertEqual(book['Cpk']['D2'].value,70)
            self.assertEqual(book['Cpk']['E2'].value,70)
            self.assertEqual(book['Cpk'].freeze_panes,'D2')
            book.close()
        root = tk.Tk()
        root.withdraw()
        try:
            window = show_cpk(root,report)
            window.update_idletasks()
            window.destroy()
        finally:
            root.destroy()

    def test_review_cpk_minimum_only_and_download_defaults(self):
        with tempfile.TemporaryDirectory() as temporary:
            parent = Path(temporary)/'A_B_23fps_threshold_0.50'
            self.record(parent,[[detection(20)]])
            self.record(parent,[[detection(20)]])
            root = tk.Tk()
            root.withdraw()
            try:
                view = ThresholdReviewWindow(root,parent)
                view.window.update_idletasks()
                self.assertEqual(view.summary.get(),'변동 없음·합격')
                with patch('tkinter.filedialog.asksaveasfilename',return_value='') as dialog:
                    view.download_cpk()
                    self.assertEqual(dialog.call_args.kwargs['initialdir'],str(parent))
                    self.assertEqual(dialog.call_args.kwargs['initialfile'],parent.name+'_cpk.xlsx')
                view.window.destroy()
            finally:
                root.destroy()

    def test_batch_runs_a_then_b_and_does_not_unlock_between_runs(self):
        from video_inspection_gui import VideoInspectionApp
        app = VideoInspectionApp.__new__(VideoInspectionApp)
        app.root = SimpleNamespace(after=lambda _,fn:fn())
        app.status_var = Mock()
        app.append_train_log = Mock()
        app._inspect_worker = Mock(return_value=True)
        app._finish_live_test = Mock()
        app._batch_inspect_worker([Path('A.mp4'),Path('B.mp4')],3,{},None)
        self.assertEqual([call.args[0].name for call in app._inspect_worker.call_args_list],['A.mp4']*3+['B.mp4']*3)
        app._finish_live_test.assert_called_once()

    def test_inspection_history_keeps_outputs_logs_and_points_for_every_run(self):
        from main import inspect_video
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root/'source.avi'
            writer = cv2.VideoWriter(str(source),cv2.VideoWriter_fourcc(*'MJPG'),5,(100,80))
            for _ in range(2):
                writer.write(np.zeros((80,100,3),np.uint8))
            writer.release()
            settings = dict(root_dir=str(root),require_regular_model=False,special_group='A_5fps',test_fps=5,
                confidence_history=True,record_confidence_points=True,result_directory=str(root/'A_threshold_50'),
                judge=dict(min_confidence=.5,ng_classes=['scratch']))
            detector = SimpleNamespace(model=True,predict=lambda frame:([detection(20)],frame))
            inspector = SimpleNamespace(references={},check=lambda *args:[])
            with patch('model.yolo_inference.YoloInference',return_value=detector), \
                 patch('inspection.special_checks.SpecialInspector',return_value=inspector), \
                 patch('inspection.video_export.finalize_result_video'):
                outputs = [inspect_video(source,settings,root/'result.mp4',show_window=False,
                           log_dir=root/'logs',file_test_log=True) for _ in range(2)]
            parent = root/'source_A_5fps_threshold_0.50_[5fps]'
            self.assertTrue(all(path.parent == parent and path.is_file() for path in outputs))
            self.assertTrue(outputs[0].name.startswith('1st_'))
            self.assertTrue(outputs[1].name.startswith('2st_'))
            self.assertEqual(len(load_runs(parent)),2)
            self.assertEqual(len(list((root/'logs').glob('*.txt'))),2)
            recorded = json.loads((parent/'label_points.json').read_text(encoding='utf-8'))
            self.assertEqual(recorded['frames'][0]['scores'],[70,70])
