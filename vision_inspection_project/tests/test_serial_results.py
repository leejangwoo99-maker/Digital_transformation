import json
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import cv2
import numpy as np

from inspection.serial_results import safe_serial,recognized_serial,plan_results


class SerialTests(unittest.TestCase):
    def test_sequence_continues_from_highest_run_inside_selected_file_folder(self):
        from inspection.serial_results import claim_result_folder
        with tempfile.TemporaryDirectory() as directory:
            container = Path(directory)/'proto_1'
            container.mkdir()
            (container/'5st_PREVIOUS_SETTINGS').mkdir()
            for number in range(6,11):
                target,actual = claim_result_folder(container/'SN_Proto_threshold_0.10_[5fps]',global_sequence=True)
                self.assertEqual(actual,number)
                self.assertTrue(target.name.startswith(f'{number}st_'))

    def test_safe_actual_parts_and_no_reading(self):
        record = dict(regions=[dict(role='qr',serial_order=2),dict(role='qr',serial_order=1)])
        inspector = SimpleNamespace(references={'label':record},serial_reads={2:'123',1:'A/B'})
        self.assertEqual(recognized_serial([inspector]),'A_B123')
        inspector.serial_reads.pop(1)
        self.assertEqual(recognized_serial([inspector]),'no_reading')
        self.assertEqual(recognized_serial([]),'no_reading')
        self.assertEqual(recognized_serial([SimpleNamespace(references={'label':dict(regions=[dict(role='text',serial_order=1)])},serial_reads={1:'OLD_TEXT'})]),'no_reading')
        self.assertEqual(safe_serial('CON'),'_CON')
        self.assertEqual(safe_serial('A:*?'),'A___')

    def test_serial_read_is_actual_not_expected_transcript(self):
        from inspection.special_checks import SpecialInspector
        inspector = SpecialInspector.__new__(SpecialInspector)
        inspector.references = {'label':dict(regions=[dict(role='qr',number=3,box=[0,0,1,1],serial_order=1)])}
        inspector.times = {'label':(0,None)}
        inspector.qr = SimpleNamespace(detectAndDecode=lambda image:('ACTUAL123',None,None))
        inspector.check(np.zeros((80,80,3),np.uint8),1)
        self.assertEqual(recognized_serial([inspector]),'ACTUAL123')

    def test_file_routing_preserves_repeats_and_point_history(self):
        from inspection.threshold_review import ConfidenceRecorder,load_review
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for repeat in range(1,4):
                stage = root/f'.inspection_{repeat}'
                stage.mkdir()
                output = stage/'old_result_p.mp4'
                output.write_bytes(b'video')
                photo = stage/'old_label_p.png'
                photo.write_bytes(b'photo')
                recorder = ConfidenceRecorder(stage,'A_23fps',30,'old.mp4',5)
                recorder.add(1,np.zeros((40,40,3),np.uint8),[])
                photos = [dict(kind='label',result='OK',output=str(photo))]
                settings = dict(special_group='A_23fps',confidence_history=True,inspection_timestamp='20261110_120000',judge={'min_confidence':.3})
                final,name,finish = plan_results(settings,stage,output,photos,'SN123',5,True,recorder)
                self.assertFalse(final.exists())
                finish()
                prefix = f'{repeat}st_'
                self.assertEqual(final.parent.name,prefix+'SN123_A_23fps_threshold_0.30_[5fps]')
                self.assertEqual(final.parent.parent.name,'old')
                self.assertTrue(final.name.startswith(f'{repeat}st_SN123_'))
                self.assertTrue(final.exists())
                self.assertTrue(Path(photos[0]['output']).exists())
                self.assertFalse(stage.exists())
            _,data = load_review(final.parent)
            self.assertEqual(len(data['frames']),1)
            self.assertEqual(len([p for p in (root/'old').iterdir() if p.is_dir()]),3)
            self.assertFalse(any('st_1st_' in p.name for p in final.parent.iterdir()))
            review_root,data = load_review(root/'old')
            self.assertEqual([item['run_number'] for item in data['frames']],[1,2,3])
            self.assertTrue(all((review_root/item['image']).is_file() for item in data['frames']))

    def test_live_prefix_and_no_phone_stream(self):
        with tempfile.TemporaryDirectory() as directory:
            stage = Path(directory)/'.inspection_test'
            stage.mkdir()
            output = stage/'phone_stream_result_f.mp4'
            output.write_bytes(b'video')
            settings = dict(special_group='A_23fps',live_photo_inspection=True,
                live_log_timestamp='20261110_120000',judge={'min_confidence':.3})
            final,name,finish = plan_results(settings,stage,output,[],'SN_123',5,False,None)
            finish()
            self.assertTrue(final.name.startswith('SN_123_20261110_120000_live_'))
            self.assertTrue(final.parent.name.startswith('SN_123_20261110_120000_'))
            self.assertTrue(name.startswith('SN_123_A_23fps_live_test_'))
            self.assertNotIn('phone_stream',str(final))

    def test_main_serial_and_cycle_log_before_media_complete(self):
        from main import inspect_video
        from inspection import automatic_runtime as automatic
        from inspection.special_checks import SpecialInspector
        from threading import Event
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root/'old.avi'
            writer = cv2.VideoWriter(str(source),cv2.VideoWriter_fourcc(*'MJPG'),5,(80,80))
            for _ in range(3):
                writer.write(np.zeros((80,80,3),np.uint8))
            writer.release()
            special = SpecialInspector.__new__(SpecialInspector)
            record = dict(image=str(source),regions=[dict(role='qr',number=3,box=[.1,.1,.4,.4],serial_order=1),
                dict(role='text',number=4,box=[.5,.5,.8,.8],text='SN42')])
            special.references = {'label':record}
            special.annotation_frames = {'label':{n:record for n in range(1,4)}}
            special.times = {'label':(0,None)}
            special.labeled_frames_only = True
            special.ocr = SimpleNamespace(candidates=lambda image:['SN42'])
            special.qr = SimpleNamespace(detectAndDecode=lambda image:('SN42',None,None))
            from inspection.photo_inspection import save_photo_frame_range
            save_photo_frame_range(root/'info','A_5fps','label',1,3)
            done = Event()
            errors = []
            detector = SimpleNamespace(model=True,predict=lambda frame:([],frame))
            def prepare(settings,*args):
                settings['_automatic_runtime'] = dict(parallel=True,save_queue_capacity=3)
                return detector,special
            settings = dict(root_dir=str(root),special_group='A_5fps',test_fps=5,
                auto_runtime=True,live_async_media=True,serial_naming=True,
                special_photo_inspection=True,photo_frame_selection=True,special_info_dir=str(root/'info'),save_test_details=True,
                _file_started_at=time.monotonic(),pipeline_start_date='2026-11-10_12:00:00',
                inspection_timestamp='20261110_120000',result_directory=str(root/'test'/'placeholder'),
                media_done_callback=lambda error:(errors.append(error),done.set()))
            with patch.object(automatic,'prepare',side_effect=prepare),patch('inspection.video_export.finalize_result_video'):
                result = inspect_video(source,settings,root/'old_result.mp4',show_window=False,
                    file_test_log=True,log_dir=root/'logs')
                self.assertTrue(done.wait(5))
            self.assertEqual(errors,[None])
            self.assertTrue(result.exists())
            self.assertIn('SN42',result.name)
            log = Path(settings['_live_result_log'])
            self.assertIn('pipeline date : ',log.read_text())
            report = json.loads(log.with_suffix('.pipeline.json').read_text())
            self.assertEqual(report['serial_number'],'SN42')
            self.assertTrue(Path(report['photo_inspections'][0]['output']).is_file())
            self.assertIn('pipeline_time_seconds',report)
