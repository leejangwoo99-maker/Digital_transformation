import unittest

from inspection.inference_telemetry import InferenceTelemetry


class InferenceTelemetryTests(unittest.TestCase):
    def test_concurrent_requests_measure_union_and_sum_separately(self):
        clock = [0.]
        telemetry = InferenceTelemetry(clock=lambda:clock[0])
        with telemetry.inference('OCR'):
            clock[0] = 1
            with telemetry.inference('label'):
                clock[0] = 3
            clock[0] = 4
        report = telemetry.report()
        self.assertEqual(report['peak_concurrent_openvino_requests'],2)
        self.assertEqual(report['openvino_busy_wall_seconds'],4)
        self.assertEqual(report['openvino_inference_seconds_sum'],6)
        self.assertEqual(report['mean_concurrency_while_busy'],1.5)
        self.assertEqual(report['active_openvino_requests'],0)

    def test_failure_clears_active_request_and_marks_failure(self):
        telemetry = InferenceTelemetry()
        with self.assertRaises(RuntimeError):
            with telemetry.inference('OCR'):
                raise RuntimeError('backend failure')
        self.assertEqual(telemetry.report()['openvino_failures'],1)
        self.assertEqual(telemetry.report()['active_openvino_requests'],0)

    def test_batch_trace_is_bounded_and_reports_truncation(self):
        telemetry = InferenceTelemetry()
        for _ in range(513):
            telemetry.batch(kind='OCR',width=128,batch_size=1,wall_seconds=.1)
        self.assertEqual(len(telemetry.report()['batches']),512)
        self.assertTrue(telemetry.report()['batches_truncated'])
