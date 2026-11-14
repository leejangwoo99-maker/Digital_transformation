import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock,patch

import cv2
import numpy as np

from inspection import automatic_runtime as automatic
from inspection.photo_inspection import save_photo_frame_range
from inspection.special_checks import SpecialInspector


class InlineFileTests(unittest.TestCase):
    def test_ocr_label_and_conn_execute_concurrently_in_real_frame_loop(self):
        import threading
        from main import inspect_video
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root/'sample.avi'
            writer = cv2.VideoWriter(str(source),cv2.VideoWriter_fourcc(*'MJPG'),5,(80,80))
            for _ in range(3):
                writer.write(np.zeros((80,80,3),np.uint8))
            writer.release()
            barrier = threading.Barrier(3)
            next_prediction_started = threading.Event()
            seen = []
            class Inspector:
                def __init__(self):
                    self.references = {kind:dict(regions=[]) for kind in ('OCR','label','conn')}
                    self.annotation_frames = {kind:{2:record} for kind,record in self.references.items()}
                    self.labeled_frames_only = True
                    self.status_lines,self.detail_detections = [],[]
                def begin_item_range(self,*args):
                    pass
                def check_frame(self,frame,number,total):
                    if number == 2:
                        barrier.wait(3)
                    self.detail_detections = []
                    return []
            special = Inspector()
            def predict(frame):
                seen.append(len(seen)+1)
                if len(seen) == 3:
                    next_prediction_started.set()
                return [],frame
            def present(image, number, *details):
                if number == 1:
                    self.assertEqual(seen,[1])
                if number == 2:
                    self.assertTrue(next_prediction_started.wait(3))
            detector = SimpleNamespace(model=True,predict=predict)
            def prepare(settings,*args):
                settings['_automatic_runtime'] = dict(parallel=True,special_workers=3,save_queue_capacity=3)
                return detector,special
            for kind in ('OCR','label'):
                save_photo_frame_range(root/'info','A_5fps',kind,2,2)
            settings = dict(root_dir=directory,special_group='A_5fps',special_info_dir=str(root/'info'),
                auto_runtime=True,test_fps=5,group_fps=5,special_photo_inspection=True,
                photo_frame_selection=True,special_labeled_frames=True,
                record_confidence_points=True,save_scatter_plot=False,result_directory=str(root/'results'))
            with patch.object(automatic,'prepare',side_effect=prepare),patch('inspection.video_export.finalize_result_video'),\
                 patch('inspection.threshold_review.ConfidenceRecorder') as recorder:
                inspect_video(source,settings,root/'result.mp4',show_window=False,file_test_log=True,
                              log_dir=root/'logs',result_callback=present)
                recorder.assert_not_called()
            report = json.loads(next((root/'logs').glob('*.pipeline.json')).read_text())
            self.assertFalse(report['performance']['scatter_plot_recording'])
            self.assertEqual(seen,[1,2,3])
            self.assertFalse(report['performance']['automatic_runtime']['appearance_prefetch'])
            self.assertTrue(report['performance']['automatic_runtime']['appearance_parallel'])
            self.assertEqual(report['performance']['automatic_runtime']['appearance_tail_lookahead_frames'],1)
            self.assertGreater(report['performance']['automatic_runtime']['special_parallel_frames'],0)
            self.assertEqual(report['photo_kind_results']['OCR'],'OK')
            self.assertEqual(report['photo_kind_results']['label'],'OK')

    def test_frames_are_presented_before_later_ocr_and_no_prepass(self):
        from main import inspect_video
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root/'sample.avi'
            writer = cv2.VideoWriter(str(source),cv2.VideoWriter_fourcc(*'MJPG'),5,(80,80))
            for _ in range(5):
                writer.write(np.zeros((80,80,3),np.uint8))
            writer.release()
            events = []
            special = SpecialInspector.__new__(SpecialInspector)
            record = dict(regions=[dict(role='text',text='A',box=[.1,.1,.5,.5])])
            special.references = {'OCR':record}
            special.annotation_frames = {'OCR':{n:record for n in range(1,6)}}
            special.times = {'OCR':(0,None)}
            special.labeled_frames_only = True
            special.ocr = SimpleNamespace(candidates=lambda image:(events.append('ocr') or ['A']))
            detector = SimpleNamespace(model=True,predict=lambda frame:([],frame))
            def prepare(settings,*args):
                settings['_automatic_runtime'] = dict(parallel=True,save_queue_capacity=3)
                return detector,special
            settings = dict(root_dir=directory,special_group='A_5fps',special_info_dir=str(root/'info'),
                            auto_runtime=True,test_fps=5,group_fps=5,special_photo_inspection=True,
                            photo_frame_selection=True,special_labeled_frames=True)
            save_photo_frame_range(root/'info','A_5fps','OCR',3,5)
            with patch.object(automatic,'prepare',side_effect=prepare),patch('inspection.video_export.finalize_result_video'),\
                 patch('inspection.photo_inspection.select_frame_range_photos',side_effect=AssertionError('No prepass')):
                inspect_video(source,settings,root/'result.mp4',show_window=False,
                              result_callback=lambda image,number,*rest:events.append(number),
                              file_test_log=True,log_dir=root/'logs')
            self.assertLess(events.index(1),events.index('ocr'))
            self.assertLess(events.index(2),events.index('ocr'))
            self.assertEqual([v for v in events if isinstance(v,int)],[1,2,3,4,5])
            report = json.loads(next((root/'logs').glob('*.pipeline.json')).read_text())
            milestones = report['performance']['milestones_seconds']
            self.assertLess(milestones['first_result_frame_ready'],milestones['first_special_inspection_started'])
            self.assertEqual(report['photo_kind_results']['OCR'],'OK')
            self.assertEqual(report['photo_inspections'][0]['frame_number'],3)
            timeline = report['performance']['verdict_timeline']
            tail = timeline['special_ready_to_txt']
            self.assertEqual(tail['start_seconds'],milestones['inline_photo_inspections_ready'])
            self.assertEqual(tail['end_seconds'],milestones['verdict_txt_published'])
            self.assertGreater(tail['stages']['appearance_inference']['span_count'],0)
            self.assertIn('verdict_txt_publish',tail['stages'])
            self.assertAlmostEqual(tail['wall_seconds'],sum(tail['exclusive_lane_wall_seconds'].values())
                                   +tail['overlap_wall_seconds']+tail['unmeasured_wall_seconds'])

    def test_tests_do_not_start_while_selection_preparation_is_busy(self):
        from video_inspection_gui import VideoInspectionApp
        app = VideoInspectionApp.__new__(VideoInspectionApp)
        app._runtime_preparing = True
        app.status_var = SimpleNamespace(set=Mock())
        with patch('video_inspection_gui.filedialog.askopenfilenames') as files:
            app.test_selected_file()
            files.assert_not_called()
        app.open_phone_camera(True)
        self.assertEqual(app.status_var.set.call_count,2)

    def test_selection_disables_testing_until_latest_preparation_finishes(self):
        from video_inspection_gui import VideoInspectionApp
        app = VideoInspectionApp.__new__(VideoInspectionApp)
        callbacks = []
        app.root = SimpleNamespace(after=lambda delay,callback:callbacks.append(callback))
        app.live_test_running = False
        app.active_group_name = 'A'
        app._settings_from_ui = lambda:{}
        app.status_var = SimpleNamespace(set=Mock())
        app.live_test_button = SimpleNamespace(config=Mock())
        app.phone_live_test_button = SimpleNamespace(config=Mock())
        def thread(**kwargs):
            return SimpleNamespace(start=kwargs['target'])
        with patch.object(automatic,'prepare',return_value=(None,SimpleNamespace(mark_model=None))),\
             patch('video_inspection_gui.threading.Thread',side_effect=thread):
            app.prepare_selected_runtime()
            self.assertTrue(app._runtime_preparing)
            app.live_test_button.config.assert_called_with(state='disabled')
            app.prepare_selected_runtime()
            callbacks[0]()
            self.assertTrue(app._runtime_preparing)
            callbacks[1]()
            self.assertFalse(app._runtime_preparing)
            app.live_test_button.config.assert_called_with(state='normal')
            app.phone_live_test_button.config.assert_called_with(state='normal')
