import unittest
from types import SimpleNamespace
from unittest.mock import patch

from inspection.resource_policy import ResourceGate,parallel_settings,resource_estimate


class ResourcePolicyTests(unittest.TestCase):
    def test_positive_settings_machine_limits_and_estimate_warnings(self):
        with patch('inspection.resource_policy.psutil.cpu_count',return_value=12):
            self.assertEqual(parallel_settings(12,2),(12,2))
            for workers,spread in ((0,1),(2,0),(62,1),(1,13),(1.5,1),(float('nan'),1)):
                with self.assertRaises(ValueError):
                    parallel_settings(workers,spread)
            memory = SimpleNamespace(total=16*1024**3,used=14*1024**3,available=2*1024**3,percent=87.5)
            with patch('inspection.resource_policy.psutil.virtual_memory',return_value=memory), \
                 patch('inspection.resource_policy.psutil.cpu_percent',return_value=85):
                result = resource_estimate(12,2)
            self.assertGreaterEqual(len(result['warnings']),3)

    def test_sustained_pressure_delays_without_skipping_then_recovers(self):
        clock = [0.]
        loads = [(90.,95.)]
        sleeps = []
        gate = ResourceGate(clock=lambda:clock[0],sleep=sleeps.append,sampler=lambda:loads[0])
        gate.before_submit()
        self.assertEqual(sleeps,[])
        clock[0] = 1.1
        gate.before_submit()
        self.assertEqual(len(sleeps),1)
        self.assertIn('no frames skipped',gate.message)
        loads[0] = (80.,90.)
        clock[0] = 1.5
        gate.before_submit()
        self.assertEqual(gate.message,'')
        self.assertEqual(gate.report()['delay_count'],1)
