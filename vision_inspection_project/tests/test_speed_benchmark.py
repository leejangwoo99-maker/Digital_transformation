import copy
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from inspection.automatic_runtime import check_parallelism
from inspection.speed_benchmark import apply_condition, conditions, load_condition, save_condition
from video_inspection_gui import VideoInspectionApp


class SpeedBenchmarkTests(unittest.TestCase):
    def test_progress_counts_frames_but_finishes_only_after_last_run(self):
        from inspection.batch_progress import BatchProgress
        progress = BatchProgress(2,100.)
        self.assertEqual(progress.estimate(101.),(0.,None))
        progress.update_frames(5,10)
        self.assertEqual(progress.estimate(105.),(25.,15.))
        progress.finish_run()
        self.assertEqual(progress.estimate(110.),(50.,10.))
        progress.update_frames(10,10)
        self.assertLess(progress.estimate(119.)[0],100)
        progress.finish_run()
        self.assertEqual(progress.estimate(120.),(100.,0.))

    def test_scatter_thresholds_share_requested_runs_without_overwriting_current_settings(self):
        app = VideoInspectionApp.__new__(VideoInspectionApp)
        app.root = SimpleNamespace(after=lambda _,callback:callback())
        app.status_var = Mock()
        app.append_train_log = Mock()
        app._finish_live_test = Mock()
        app._inspect_worker = Mock(return_value=True)
        settings = dict(scatter_thresholds=[10,20],save_scatter_plot=True,
                        model=dict(confidence=.3),judge=dict(min_confidence=.3))
        app._batch_inspect_worker([Path('a.mp4')],2,settings,None)
        actual = [call.kwargs['batch_settings'] for call in app._inspect_worker.call_args_list]
        self.assertEqual([(row['model']['confidence'],row['judge']['min_confidence']) for row in actual],
                         [(.3,.3),(.3,.3)])
        self.assertEqual([row['scatter_thresholds'] for row in actual],[[10,20],[10,20]])
        self.assertEqual(settings['judge']['min_confidence'],.3)

    def test_candidates_use_cpu_memory_and_start_at_two(self):
        limits = dict(cores=12,cpu_budget=9,cpu_threads=4,parallel=True,
                      available_bytes=3*1024**3,memory_percent=75)
        plan = conditions(limits)
        self.assertEqual(len(plan),18)
        self.assertEqual([row['backend'] for row in plan],['pytorch']*9+['openvino']*9)
        self.assertEqual({(row['workers'],row['threads']) for row in plan},
                         {(w,t) for w in range(2,5) for t in range(2,5)})
        self.assertEqual(len(conditions(dict(limits,memory_percent=81))),2)
        self.assertEqual(conditions(dict(limits,memory_percent=86)),[])
        larger = conditions(dict(limits,cores=18,cpu_budget=14,available_bytes=5*1024**3))
        self.assertEqual({(row['workers'],row['threads']) for row in larger},
                         {(w,t) for w in range(4,7) for t in range(4,7)})

    def test_accepted_profile_is_scoped_to_group_and_pc(self):
        with tempfile.TemporaryDirectory() as directory:
            settings = dict(special_info_dir=directory,special_group='A')
            fixed = dict(backend='pytorch',workers=4,threads=3)
            with patch('inspection.speed_benchmark.platform.node',return_value='PC A'):
                self.assertIsNone(load_condition(settings))
                save_condition(settings,fixed)
                self.assertEqual(load_condition(settings),fixed)
                self.assertIsNone(load_condition(dict(settings,special_group='B')))
            with patch('inspection.speed_benchmark.platform.node',return_value='PC B'):
                self.assertIsNone(load_condition(settings))
            apply_condition(settings,fixed)
            limits = dict(cores=12,cpu_budget=9,cpu_threads=4,parallel=True)
            runtime = dict(limits)
            check_parallelism(runtime,limits,settings)
            self.assertEqual(settings['model']['appearance_backend'],'pytorch')
            self.assertEqual((runtime['ocr_job_workers'],runtime['ocr_inference_threads']),(4,3))

    def test_sweep_excludes_warmups_and_ranks_exact_saved_measurements(self):
        app = VideoInspectionApp.__new__(VideoInspectionApp)
        app.root = SimpleNamespace(after=lambda _,callback:callback())
        app.status_var = SimpleNamespace(set=Mock())
        app._confirm_speed_condition = Mock()
        cases = [dict(backend='pytorch',workers=2,threads=3),
                 dict(backend='openvino',workers=3,threads=2)]
        with tempfile.TemporaryDirectory() as directory:
            seen = []
            def run(source,*args,batch_settings,**kwargs):
                tag = copy.deepcopy(batch_settings['speed_benchmark'])
                case = tag['condition']
                self.assertFalse(batch_settings['save_scatter_plot'])
                seen.append((case['backend'],source.name,tag['warmup']))
                path = Path(directory)/f'{len(seen)}.pipeline.json'
                path.write_text(json.dumps(dict(speed_benchmark=tag,status='completed',
                    pipeline_time_seconds=.1 if tag['warmup'] else 10-tag['repetition']
                        +(case['backend']=='pytorch'),
                    performance=dict(appearance_backend=dict(name=case['backend']),
                    automatic_runtime=dict(ocr_job_workers=case['workers'],
                                           ocr_inference_threads=case['threads'])))),encoding='utf-8')
                return path
            app._inspect_worker = run
            app._run_speed_sweep([Path('a.mp4'),Path('b.mp4')],2,dict(speed_sweep=cases),None)
            self.assertEqual([backend for backend,_,_ in seen],['pytorch']*6+['openvino']*6)
            self.assertEqual(sum(warmup for _,_,warmup in seen),4)
            best = app._confirm_speed_condition.call_args.args[1][0]
            self.assertEqual((best['backend'],best['mean'],best['minimum'],best['maximum'],best['count']),
                             ('openvino',8.5,8,9,4))
