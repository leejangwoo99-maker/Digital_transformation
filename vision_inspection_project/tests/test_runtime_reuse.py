import tempfile
import unittest
from functools import lru_cache
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from inspection import automatic_runtime as automatic
from inspection import openvino_ocr as ocr
from inspection.special_checks import SpecialInspector


class RuntimeReuseTests(unittest.TestCase):
    def test_standard_runtime_selects_four_by_four_on_twelve_logical_cpus(self):
        selected = dict(cpu_threads=5,mark_workers=3,mark_spread=2)
        automatic.check_parallelism(selected,dict(cores=12,cpu_budget=9,parallel=True))
        self.assertEqual(selected['ocr_job_workers'],4)
        self.assertEqual(selected['ocr_inference_threads'],4)
        self.assertEqual(selected['mark_workers'],1)

    def test_ocr_square_configuration_scales_with_cpu_and_memory_budget(self):
        for cores,budget,ram,parallel,expected in (
            (6, 4, 8*1024**3, True, 2),
            (8, 6, 8*1024**3, True, 2),
            (12, 9, 8*1024**3, True, 4),
            (16, 12, 8*1024**3, True, 5),
            (24, 19, 8*1024**3, True, 8),
            (24, 19, 1024**3, True, 2),
            (24, 19, 0, False, 1),
        ):
            with self.subTest(budget=budget,ram=ram,parallel=parallel):
                selected = dict(cpu_threads=5,mark_workers=3,mark_spread=2)
                automatic.check_parallelism(selected,dict(cores=cores,cpu_budget=budget,available_bytes=ram,parallel=parallel))
                self.assertEqual((selected['ocr_job_workers'],selected['ocr_inference_threads']),
                                 (expected,expected))

    def test_memory_pressure_changes_ocr_configuration_between_runs(self):
        selected = dict(cpu_threads=4,mark_workers=1,mark_spread=1)
        for memory_percent, expected in ((75,6),(82,2),(86,1),(75,6)):
            automatic.check_parallelism(selected,dict(cores=24,cpu_budget=19,parallel=True,
                available_bytes=4*1024**3,memory_percent=memory_percent))
            self.assertEqual((selected['ocr_job_workers'],selected['ocr_inference_threads']),
                             (expected,expected))

    def test_twelve_logical_processors_allow_four_by_four_with_memory_headroom(self):
        selected = dict(cpu_threads=4,mark_workers=1,mark_spread=1)
        for ram, percent, expected in ((3*1024**3,75,4),(2*1024**3,75,3),(1024**3,75,2),
                                       (3*1024**3,81,2),(3*1024**3,85,1)):
            automatic.check_parallelism(selected,dict(cores=12,cpu_budget=9,parallel=True,
                available_bytes=ram,memory_percent=percent))
            self.assertEqual((selected['ocr_job_workers'],selected['ocr_inference_threads']),
                             (expected,expected))

    def test_recognition_prioritizes_cost_but_returns_original_order(self):
        from concurrent.futures import Future
        from inspection.supervised_text_ocr import run_recognition_jobs
        small = (0,128,False)
        wide = (0,512,False)
        batch = (0,256,False)
        jobs = {small:[None],wide:[None],batch:[None]*3}
        recognize = Mock(side_effect=lambda key,items:key)
        def submit(function,*args):
            future = Future()
            future.set_result(function(*args))
            return future
        with patch('concurrent.futures.ThreadPoolExecutor') as executor:
            executor.return_value.__enter__.return_value.submit.side_effect = submit
            self.assertEqual(run_recognition_jobs(jobs,2,recognize),list(jobs))
            executor.assert_called_once_with(max_workers=2,thread_name_prefix='ocr-batch')
        self.assertEqual([call.args[0] for call in recognize.call_args_list],[batch,wide,small])

    def test_comparison_changes_only_ocr_profile_and_freezes_other_thread_settings(self):
        limits = dict(cores=12,cpu_budget=9,parallel=True)
        for profile,jobs,threads in (('jobs2_threads2',2,2),('jobs1_threads4',1,4)):
            selected = dict(cpu_threads=5,mark_workers=3,mark_spread=2)
            automatic.check_parallelism(selected,limits,{'openvino_comparison_profile':profile})
            self.assertEqual(selected['ocr_job_workers'],jobs)
            self.assertEqual(selected['ocr_inference_threads'],threads)
            self.assertEqual(selected['cpu_threads'],2)
            self.assertEqual(selected['mark_workers'],1)
            self.assertEqual(selected['mark_spread'],1)
    def tearDown(self):
        automatic.shutdown()
        automatic.DETECTORS.clear()
        automatic.WARMED.clear()
        automatic.PREPARED.clear()

    def test_compile_cache_counts_hits_misses_and_eviction(self):
        stats = dict(compiles=0,recompiles=0,disk_cache_hits=0)
        seen = set()
        @lru_cache(maxsize=1)
        def compile_model(xml, stamp, width, threads):
            key = (xml,stamp,width,threads)
            stats['compiles'] += 1
            stats['recompiles'] += int(key in seen)
            stats['disk_cache_hits'] += int(key in seen)
            seen.add(key)
            return object()
        timing = {}
        with patch.object(ocr,'compiled_recognizer',compile_model),patch.object(ocr,'COMPILE_STATS',stats):
            first = ocr.lookup_recognizer('A',1,128,timing)
            self.assertIs(first,ocr.lookup_recognizer('A',1,128,timing))
            ocr.lookup_recognizer('A',1,256,timing)
            ocr.lookup_recognizer('A',1,128,timing)
        self.assertEqual(timing['openvino_cache_hits'],1)
        self.assertEqual(timing['openvino_cache_misses'],3)
        self.assertEqual(timing['openvino_compiles'],3)
        self.assertEqual(timing['openvino_recompiles'],1)
        self.assertEqual(timing['openvino_disk_cache_hits'],1)

    def test_completed_ocr_profile_uses_full_cache_key_and_reports_frequencies(self):
        from inspection.inference_telemetry import InferenceTelemetry
        telemetry = InferenceTelemetry()
        key = ('model.xml', 123, 128, 2)
        telemetry.record_model_use(key)
        telemetry.record_model_use(key)
        from collections import OrderedDict
        with patch.object(ocr,'USAGE_PROFILES',OrderedDict()):
            self.assertEqual(ocr.learn_cache_usage(telemetry,(2,2)),
                             [dict(model='model.xml',width=128,threads=2,calls=2)])
            self.assertEqual(ocr.learn_cache_usage(InferenceTelemetry(),(2,2)),[])
            self.assertEqual(ocr.USAGE_PROFILES[(2,2)],{key:2})

    def test_preparation_only_loads_missing_top_keys_from_matching_scope(self):
        with tempfile.TemporaryDirectory() as directory:
            xml = Path(directory)/'ocr.xml'
            xml.touch()
            stamp = xml.stat().st_mtime_ns
            hot, next_key, cold = [(str(xml),stamp,width,2) for width in (128,256,512)]
            old = (str(xml),stamp,128,1)
            resident = {hot}
            cache = Mock(side_effect=lambda *key:resident.add(key))
            cache.is_resident.side_effect = resident.__contains__
            cache.preparation_budget.return_value = 2
            with patch.object(ocr,'compiled_recognizer',cache),patch.object(ocr,'USAGE_PROFILES',
                    {(1,1):{old:10},(2,2):{hot:4,next_key:3,cold:1}}):
                report = ocr.prepare_profiled_models((2,2))
                cache.assert_called_once_with(*next_key)
                cache.set_usage_profile.assert_called_once_with({hot:4,next_key:3})
                self.assertEqual((report['profiled'],report['selected'],report['reused'],
                                  report['loaded'],report['resident']),(3,2,1,1,2))

    def test_fresh_product_has_no_previous_verdict_or_serial(self):
        with tempfile.TemporaryDirectory() as directory:
            settings = dict(root_dir=directory)
            template = SpecialInspector(settings)
            template.trained_text_ocr = {'label':SimpleNamespace(backend_counts={},runtime_seconds={},backend_errors=[])}
            first = template.fresh_run(settings)
            first.serial_reads = {1:'OLD'}
            first.window_results = object()
            first.component_reads = {'old':'value'}
            first.status_lines.append('OK')
            first.trained_text_ocr['label'].backend_counts['old'] = 1
            second = template.fresh_run(settings)
            self.assertFalse(hasattr(second,'serial_reads'))
            self.assertFalse(hasattr(second,'window_results'))
            self.assertFalse(hasattr(second,'component_reads'))
            self.assertEqual(second.status_lines,[])
            self.assertEqual(second.trained_text_ocr['label'].backend_counts,{})
            self.assertIsNot(first.shape_registration,second.shape_registration)
            self.assertIsNot(first.trained_text_ocr['label'],second.trained_text_ocr['label'])
            self.assertIs(first.text_image_models,second.text_image_models)

    def test_reuses_fixed_assets_and_does_not_repeat_warmup_samples(self):
        with tempfile.TemporaryDirectory() as directory:
            settings = dict(root_dir=directory,special_group='',special_info_dir=directory)
            detector = SimpleNamespace(confidence=.5,model=None)
            factory = Mock(side_effect=SpecialInspector)
            with patch.object(automatic,'samples',return_value=(None,{})) as samples:
                _,first = automatic.prepare(settings,lambda settings:detector,factory)
                _,second = automatic.prepare(settings,lambda settings:detector,factory)
            factory.assert_called_once()
            samples.assert_called_once()
            self.assertIsNot(first,second)
            self.assertTrue(settings['_automatic_runtime']['fixed_data_reused'])
            self.assertIn('fresh_run_state',settings['_automatic_runtime']['preparation_stages_seconds'])

    def test_condition_and_frame_changes_invalidate_assets(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            frame = root/'frame.png'
            frame.write_bytes(b'old')
            condition = root/'A_label_test_condition.txt'
            condition.write_text('start_seconds=0')
            settings = dict(root_dir=directory,special_group='A',special_info_dir=directory,
                            special_frame_paths=[str(frame)])
            first = automatic.fingerprint(settings)
            condition.write_text('start_seconds=100')
            second = automatic.fingerprint(settings)
            self.assertNotEqual(first,second)
            frame.write_bytes(b'new image')
            self.assertNotEqual(second,automatic.fingerprint(settings))

    def test_encoder_limits_threads_and_preserves_quality(self):
        from inspection.video_export import finalize_result_video
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory)/'result.mp4'
            source.write_bytes(b'old')
            def run(command,**kwargs):
                self.assertIn('-crf',command)
                self.assertEqual(command[command.index('-crf')+1],'18')
                self.assertEqual(command.count('-threads'),2)
                self.assertEqual(command[command.index('-threads')+1],'1')
                Path(command[-1]).write_bytes(b'encoded')
                return SimpleNamespace(returncode=0)
            with patch('inspection.video_export.subprocess.run',side_effect=run):
                finalize_result_video(source)
            self.assertEqual(source.read_bytes(),b'encoded')
