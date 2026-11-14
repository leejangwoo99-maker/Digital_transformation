import json
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock,patch

import cv2
import numpy as np
import torch

from inspection import automatic_runtime as automatic
from inspection.performance import Performance
from inspection.runtime_pipeline import OrderedSaver,AppearanceStream


class PipelineTests(unittest.TestCase):
    def test_background_media_error_is_reported(self):
        from threading import Event
        from inspection.runtime_pipeline import LiveMediaSaver
        done = Event()
        errors = []
        saver = LiveMediaSaver(Performance())
        def fail():
            raise OSError('disk full')
        saver.submit('save',fail)
        saver.close()
        saver.finish_background(lambda error:(errors.append(error),done.set()))
        self.assertTrue(done.wait(3))
        self.assertEqual(errors,['disk full'])

    def test_verdict_log_is_published_complete(self):
        from inspection.live_completion import publish_txt
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'verdict.txt'
            elapsed = publish_txt(path,['result_PASS'],time.monotonic(),3)
            self.assertGreaterEqual(elapsed,0)
            text = path.read_text()
            self.assertIn('result_PASS',text)
            self.assertIn('inspection_count: 3',text)
            self.assertFalse(path.with_name(path.name+'.pending').exists())

    def test_live_txt_published_before_media_encoding_finishes(self):
        from concurrent.futures import ThreadPoolExecutor
        from threading import Event
        from main import inspect_video
        from inspection.special_checks import SpecialInspector
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root/'sample.avi'
            writer = cv2.VideoWriter(str(source),cv2.VideoWriter_fourcc(*'MJPG'),5,(80,80))
            for _ in range(3):
                writer.write(np.zeros((80,80,3),np.uint8))
            writer.release()
            gate,done = Event(),Event()
            errors = []
            def encode(path):
                gate.wait(5)
            def finished(error):
                errors.append(error)
                done.set()
            special = SpecialInspector.__new__(SpecialInspector)
            special.references = {}
            special.annotation_frames = {}
            detector = SimpleNamespace(model=True,predict=lambda image:([],image))
            def prepare(settings,*args):
                settings['_automatic_runtime'] = dict(parallel=True,save_queue_capacity=3)
                return detector,special
            settings = dict(root_dir=str(root),special_group='A_5fps',special_info_dir=str(root/'info'),test_fps=5,
                auto_runtime=True,live_async_media=True,live_photo_inspection=True,
                live_clock_from_capture=True,
                live_expected_frames=3,live_inspection_count=2,live_log_timestamp='20261109_170548',
                _live_started_at=time.monotonic(),media_done_callback=finished)
            from inspection.runtime_pipeline import ReleaseOnce
            capture = ReleaseOnce(cv2.VideoCapture(str(source)))
            capture.started_at = time.monotonic()-5.
            try:
                with patch.object(automatic,'prepare',side_effect=prepare),patch('inspection.video_export.finalize_result_video',side_effect=encode),ThreadPoolExecutor(1) as pool:
                    task = pool.submit(inspect_video,source,settings,root/'result.mp4',
                        show_window=False,log_dir=root/'logs',file_test_log=True,capture_override=capture)
                    try:
                        result = task.result(timeout=3)
                        self.assertFalse(done.is_set())
                        log = Path(settings['_live_result_log'])
                        self.assertEqual(log.name,'A_5fps_live_test_threshold_50_result_p_[5fps]_20261109_170548.txt')
                        self.assertIn('pipeline time(sec)',log.read_text())
                        self.assertIn('inspection_count: 2',log.read_text())
                        self.assertTrue(log.with_suffix('.pipeline.json').is_file())
                        report = json.loads(log.with_suffix('.pipeline.json').read_text())
                        self.assertGreaterEqual(report['pipeline_time_seconds'],5.)
                        self.assertIn('Actual camera capture start',report['pipeline_time_boundary'])
                    finally:
                        gate.set()
                        if not task.exception():
                            self.assertTrue(done.wait(5))
                self.assertEqual(errors,[None])
                self.assertTrue(result.is_file())
            finally:
                gate.set()
                capture.release()

    def test_file_and_live_pipeline_keep_frame_results_with_async_save(self):
        from inspection.special_checks import SpecialInspector
        from inspection.photo_inspection import save_photo_frame_range
        from main import inspect_video
        for live in (False,True):
            with self.subTest(live=live),tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                source = root/'sample.avi'
                writer = cv2.VideoWriter(str(source),cv2.VideoWriter_fourcc(*'MJPG'),5,(80,80))
                for _ in range(5):
                    writer.write(np.zeros((80,80,3),np.uint8))
                writer.release()
                record = dict(regions=[dict(role='text',box=[.1,.1,.4,.4],text='A')])
                special = SpecialInspector.__new__(SpecialInspector)
                special.references = {'OCR':record}
                special.annotation_frames = {'OCR':{n:record for n in range(1,6)}}
                special.times = {'OCR':(0,None)}
                special.labeled_frames_only = True
                special.ocr = SimpleNamespace(candidates=lambda image:['A'])
                detector = SimpleNamespace(model=True,predict=lambda image:([],image))
                def prepare(settings,*args):
                    settings['_automatic_runtime'] = dict(parallel=True,save_queue_capacity=2)
                    return detector,special
                settings = dict(root_dir=str(root),special_group='A_5fps',special_info_dir=str(root/'info'),
                    auto_runtime=True,test_fps=5,group_fps=5,special_photo_inspection=True,
                    photo_frame_selection=True,special_labeled_frames=True,require_regular_model=True,
                    save_test_details=True,live_photo_inspection=live,live_expected_frames=5)
                save_photo_frame_range(root/'info','A_5fps','OCR',1,3)
                capture = cv2.VideoCapture(str(source)) if live else None
                with patch.object(automatic,'prepare',side_effect=prepare),patch('inspection.video_export.finalize_result_video'):
                    output = inspect_video(source,settings,root/'result.mp4',show_window=False,
                        display_width=80,display_height=80,log_dir=root/'logs',file_test_log=True,capture_override=capture)
                report = json.loads(next((root/'logs').glob('*.pipeline.json')).read_text(encoding='utf-8'))
                self.assertEqual(report['processed_frames'],5)
                self.assertEqual(report['photo_kind_results']['OCR'],'OK')
                self.assertIn('ocr_compile_cache_totals',report['performance'])
                self.assertIn('cache_hits',report['performance']['ocr_compile_cache_totals'])
                milestones = report['performance']['milestones_seconds']
                self.assertIn('first_result_frame_ready',milestones)
                self.assertLessEqual(milestones['models_ready'],milestones['first_result_frame_ready'])
                self.assertTrue(output.is_file())
                runtime = report['performance']['automatic_runtime']
                self.assertLessEqual(runtime['save_queue_peak'],2)
                self.assertFalse(runtime['appearance_prefetch'])
                self.assertTrue(runtime['appearance_parallel'])
                self.assertEqual(runtime['inline_file_photo_inspection'],not live)

    def test_saver_is_bounded_ordered_and_propagates_error(self):
        seen = []
        saver = OrderedSaver(Performance(),capacity=2)
        for n in range(7):
            saver.submit('save',lambda value:(time.sleep(.002),seen.append(value)),n)
        saver.close()
        self.assertEqual(seen,list(range(7)))
        self.assertEqual(saver.peak,2)
        bad = OrderedSaver(Performance())
        def failure():
            raise OSError('disk unavailable')
        bad.submit('save',failure)
        with self.assertRaisesRegex(OSError,'disk unavailable'):
            bad.close()

    def test_prefetch_keeps_sampling_order_and_closes_when_full(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'sample.avi'
            writer = cv2.VideoWriter(str(path),cv2.VideoWriter_fourcc(*'MJPG'),10,(64,48))
            for n in range(10):
                writer.write(np.full((48,64,3),n*20,np.uint8))
            writer.release()
            detector = SimpleNamespace(predict=lambda image:([round(float(image.mean()))],image))
            stream = AppearanceStream(path,5,detector,Performance(),capacity=1)
            try:
                self.assertEqual([stream.take(n)[0][0] for n in range(1,6)],[0,40,80,120,160])
            finally:
                stream.close()
            stream = AppearanceStream(path,5,detector,Performance(),capacity=1)
            time.sleep(.02)
            stream.close()
            self.assertFalse(stream.thread.is_alive())


class AutomaticTests(unittest.TestCase):
    def test_parallel_overrides_serial_profile_but_respects_resource_limits(self):
        selected = dict(parallel=False,cpu_threads=4,mark_workers=3,mark_spread=2)
        automatic.enable_parallel(selected,dict(parallel=True,cpu_budget=9))
        self.assertTrue(selected['parallel'])
        self.assertLessEqual(selected['cpu_threads']+selected['mark_workers']*selected['mark_spread'],9)
        automatic.enable_parallel(selected,dict(parallel=False,cpu_budget=9))
        self.assertFalse(selected['parallel'])
    def tearDown(self):
        automatic.shutdown()
        automatic.DETECTORS.clear()
        automatic.PREPARED.clear()
        automatic.WARMED.clear()
        torch.set_num_threads(4)

    def test_measures_real_callbacks_and_reuses_profile_and_detector(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root/'weights').mkdir()
            (root/'weights/A.pt').write_bytes(b'checkpoint')
            image = root/'frame.jpg'
            cv2.imwrite(str(image),np.full((80,80,3),100,np.uint8))
            class Detector:
                model = True
                confidence = .5
                def predict(self,frame):
                    time.sleep(.003 if torch.get_num_threads()==2 else .009)
                    return [],frame
            class Reader:
                def candidates_batch(self,requests):
                    time.sleep(.01)
                    return [['A'] for _ in requests]
            def inspector(settings):
                reference = dict(image=str(image),regions=[dict(role='text',text='A',box=[.1,.1,.8,.8])])
                return SimpleNamespace(references={'OCR':reference},trained_text_ocr={'OCR':Reader()},mark_model=None)
            factory = Mock(side_effect=lambda settings:Detector())
            limits = dict(cores=8,cpu_budget=6,available_bytes=4*1024**3,mark_workers=2,mark_spread=1,
                          cpu_threads=4,parallel=True,save_queue_capacity=2)
            settings = dict(root_dir=str(root),special_group='A_10fps',special_info_dir=str(root/'info'),
                            model={'weights':'weights/A.pt'},runtime_calibration=True)
            with patch.object(automatic,'allocation',return_value=limits),patch.object(automatic.psutil,'cpu_percent',return_value=10),patch.object(automatic.psutil,'virtual_memory',return_value=SimpleNamespace(total=16*1024**3,percent=40)):
                first,_ = automatic.prepare(settings,factory,inspector)
                self.assertTrue(settings['_automatic_runtime']['parallel'])
                self.assertTrue(automatic.profile_path(settings).is_file())
                second,_ = automatic.prepare(settings,factory,inspector)
                self.assertIs(first,second)
                factory.assert_called_once()
                self.assertTrue(settings['_automatic_runtime']['profile_reused'])
                self.assertFalse(settings['_automatic_runtime']['trials'])
                stored = json.loads(automatic.profile_path(settings).read_text())
                self.assertGreater(len(stored['trials']),1)
                self.assertEqual(stored['cpu_threads'],2)

    def test_test_start_never_runs_calibration(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            image = np.zeros((32,32,3),np.uint8)
            detector = SimpleNamespace(model=True,confidence=.5,predict=Mock(return_value=([],image)))
            special = SimpleNamespace(references={},trained_text_ocr={},mark_model=None)
            settings = dict(root_dir=str(root),special_group='A',special_info_dir=str(root/'info'))
            with patch.object(automatic,'samples',return_value=(image,{})),patch.object(automatic.psutil,'cpu_percent',return_value=0):
                automatic.prepare(settings,lambda settings:detector,lambda settings:special)
            self.assertEqual(settings['_automatic_runtime']['trials'],[])
            self.assertEqual(detector.predict.call_count,1)
            self.assertFalse(automatic.profile_path(settings).exists())

    def test_allocation_respects_memory_budget_and_cpu_count(self):
        memory = SimpleNamespace(total=16*1024**3,used=15*1024**3,percent=94)
        with patch.object(automatic.psutil,'cpu_count',return_value=2),patch.object(automatic.psutil,'virtual_memory',return_value=memory):
            limits = automatic.allocation()
            self.assertEqual(limits['mark_workers'],1)
            self.assertFalse(limits['parallel'])
            self.assertEqual(limits['save_queue_capacity'],1)
