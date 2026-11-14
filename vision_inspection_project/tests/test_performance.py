import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from inspection.performance import Performance, result_directory
from inspection.live_results import name_live_results


class PerformanceTests(unittest.TestCase):
    def test_result_status_and_collision_keep_test_fps_last(self):
        from main import result_file_path
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory)/'trial_[15fps]_OCR.png'
            first = result_file_path(source,True)
            self.assertEqual(first.name,'trial_OCR_p_[15fps].png')
            first.touch()
            self.assertEqual(result_file_path(source,True).name,'trial_OCR_1_p_[15fps].png')

    def test_stage_totals_and_average(self):
        performance = Performance()
        with patch('inspection.performance.perf_counter',side_effect=[1.,1.25]):
            performance.call('OCR',lambda:None)
        performance.add('OCR',.75)
        report = performance.report()
        self.assertEqual(report['stages']['OCR']['count'],2)
        self.assertEqual(report['stages']['OCR']['mean_seconds'],.5)
        self.assertEqual(report['stages']['OCR']['max_seconds'],.75)
        self.assertEqual(report['slowest_stage'],'OCR')

    def test_folder_includes_source_group_and_only_one_fps(self):
        self.assertEqual(result_directory('test','full2','A_23fps',15,70).name,'full2_A_23fps_threshold_70_[15fps]')
        self.assertEqual(result_directory('test','full2','A',23,70).name,'full2_A_23fps_threshold_70_[23fps]')

    def test_live_rename_preserves_scores_and_updates_photo_and_logs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            folder = root/'old_group_threshold_50'
            points = folder/'confidence_point_frame'
            points.mkdir(parents=True)
            saved = folder/'stamp_live_test_p.mp4'
            saved.write_bytes(b'video')
            photo = folder/'stamp_live_test_OCR_p.png'
            photo.write_bytes(b'photo')
            (points/'index.json').write_text(json.dumps(dict(source='phone',frames=[dict(image='frame_no_1_0.00.png')])))
            logs = root/'logs'
            logs.mkdir()
            report = dict(output=str(saved),photo_inspections=[dict(output=str(photo))])
            (logs/'stamp_live_test_p.pipeline.json').write_text(json.dumps(report))
            (logs/'stamp_live_test_p.txt').write_text(str(saved))
            result = name_live_results(saved,'trial','A_23fps',23,50,logs,'stamp_live_test')
            self.assertTrue(result.exists())
            self.assertEqual(result.parent.name,'trial_A_23fps_threshold_50_[23fps]')
            data = json.loads((logs/'trial_live_test_p.pipeline.json').read_text())
            self.assertEqual(data['output'],str(result))
            self.assertTrue(Path(data['photo_inspections'][0]['output']).exists())
            self.assertEqual(json.loads((result.parent/'confidence_point_frame/index.json').read_text())['frames'][0]['image'],'frame_no_1_0.00.png')
