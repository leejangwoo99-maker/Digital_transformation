import unittest
from types import SimpleNamespace
from unittest.mock import Mock

from inspection.adaptive_model_cache import AdaptiveModelCache


class AdaptiveCacheTests(unittest.TestCase):
    def test_previous_run_usage_protects_hot_models_without_disabling_pressure_limits(self):
        cache,state,clock,factory = self.cache()
        state.percent,state.used = 79,int(state.total*.79)
        cache.capacity = 2
        hot, warm = cache('hot'), cache('warm')
        cache.set_usage_profile({('hot',):5, ('warm',):1})
        cache('new')
        self.assertIs(cache('hot'), hot)
        self.assertIs(cache('warm'), warm)
        self.assertEqual(factory.call_count,3)
        cache.set_usage_profile({('hot',):5, ('new',):2})
        new = cache('new')
        self.assertIs(cache('new'), new)
        self.assertNotIn(('warm',),cache.values)
        state.percent,state.used = 91,int(state.total*.91)
        clock[0] += 2
        cache('hot')
        self.assertEqual(cache.cache_info().currsize,0)

    def test_shrink_trace_records_memory_and_before_after_sizes(self):
        cache,state,clock,_ = self.cache()
        for n in range(32):
            cache(n)
        baseline = cache.report()['event_sequence']
        state.percent,state.used = 85,int(state.total*.85)
        clock[0] += 2
        cache(31)
        clock[0] += 3.1
        cache(31)
        trace = cache.events_since(baseline)
        shrink = next(e for e in trace['events'] if e['evicted'])
        self.assertEqual(shrink['memory_percent'],85)
        self.assertEqual(shrink['reason'],'sustained_memory_target_80')
        self.assertGreater(shrink['entries_before'],shrink['entries_after'])
        self.assertEqual(cache.report()['shrink_delay_seconds'],3.)

    def test_initial_rss_spike_does_not_limit_cache_to_eleven_entries(self):
        state = SimpleNamespace(total=16*1024**3,used=int(16*1024**3*.64),percent=64)
        readings = iter([value for growth in [300*1024**2]+[32*1024**2]*31 for value in (0,growth)])
        cache = AdaptiveModelCache(lambda *args:object(),memory=lambda:state,rss=lambda:next(readings))
        for n in range(32):
            cache(n)
        self.assertEqual(cache.cache_info().currsize,32)
        self.assertEqual(cache.estimated_entry_bytes,32*1024**2)
        self.assertEqual(cache.report()['rss_growth_peak_bytes'],300*1024**2)

    def test_later_outlier_recovers_instead_of_becoming_permanent_maximum(self):
        state = SimpleNamespace(total=16*1024**3,used=int(16*1024**3*.64),percent=64)
        readings = iter([value for growth in (32,32,300,32,32) for value in (0,growth*1024**2)])
        cache = AdaptiveModelCache(lambda *args:object(),memory=lambda:state,rss=lambda:next(readings))
        for n in range(5):
            cache(n)
        self.assertEqual(cache.estimated_entry_bytes,32*1024**2)
        self.assertGreaterEqual(cache.cache_info().maxsize,32)

    def cache(self,percent=50):
        state = SimpleNamespace(percent=percent,total=16*1024**3,used=int(16*1024**3*percent/100))
        clock = [0.]
        factory = Mock(side_effect=lambda *key:object())
        cache = AdaptiveModelCache(factory,memory=lambda:state,rss=lambda:0,clock=lambda:clock[0])
        return cache,state,clock,factory

    def test_reuses_more_than_sixteen_models_with_memory_headroom(self):
        cache,_,_,factory = self.cache()
        values = [cache(n) for n in range(32)]
        for n,value in enumerate(values):
            self.assertIs(cache(n),value)
        self.assertEqual(factory.call_count,32)
        self.assertGreaterEqual(cache.cache_info().maxsize,32)
        self.assertEqual(cache.cache_info().hits,32)

    def test_pressure_evicts_and_ninety_percent_disables_retention(self):
        cache,state,clock,_ = self.cache()
        for n in range(32):
            cache(n)
        state.percent,state.used = 85,int(state.total*.85)
        clock[0] += 2
        cache(31)
        self.assertEqual(cache.cache_info().currsize,32)
        clock[0] += 3.1
        cache(31)
        self.assertLess(cache.cache_info().currsize,32)
        self.assertGreater(cache.report()['pressure_evictions'],0)
        state.percent,state.used = 91,int(state.total*.91)
        clock[0] += 2
        cache(100)
        self.assertEqual(cache.cache_info().currsize,0)
        self.assertEqual(cache.cache_info().maxsize,0)
        state.percent,state.used = 50,int(state.total*.5)
        clock[0] += 2
        cache(101)
        self.assertEqual(cache.cache_info().currsize,1)

    def test_model_change_does_not_return_old_model(self):
        cache,_,_,_ = self.cache()
        first = cache('model',1,128)
        self.assertIsNot(first,cache('model',2,128))

    def test_short_spikes_do_not_evict_and_pressure_recovery_resets_timer(self):
        cache,state,clock,_ = self.cache()
        for n in range(32):
            cache(n)
        for percent in (81,79,81,79):
            state.percent,state.used = percent,int(state.total*percent/100)
            clock[0] += 2
            cache(31)
        self.assertEqual(cache.cache_info().currsize,32)
        self.assertEqual(cache.report()['pressure_evictions'],0)

    def test_sustained_pressure_shrinks_in_steps_with_cooldown(self):
        cache,state,clock,_ = self.cache()
        for n in range(32):
            cache(n)
        state.percent,state.used = 85,int(state.total*.85)
        clock[0] = 2
        cache(31)
        clock[0] = 5.1
        cache(31)
        self.assertEqual(cache.cache_info().currsize,28)
        clock[0] = 6.2
        cache(31)
        self.assertEqual(cache.cache_info().currsize,28)
        clock[0] = 7.3
        cache(31)
        self.assertEqual(cache.cache_info().currsize,25)

    def test_estimate_change_does_not_evict_when_actual_memory_is_healthy(self):
        cache,_,clock,_ = self.cache()
        for n in range(40):
            cache(n)
        cache.estimated_entry_bytes = 96*1024**2
        clock[0] += 2
        cache(39)
        self.assertEqual(cache.cache_info().currsize,40)
        self.assertEqual(cache.report()['pressure_evictions'],0)

    def test_lru_eviction_preserves_recent_model(self):
        cache,state,_,_ = self.cache()
        first = cache(1)
        capacity = cache.cache_info().maxsize
        for n in range(2,capacity+1):
            cache(n)
        self.assertIs(first,cache(1))
        cache(999)
        self.assertIs(first,cache(1))
        self.assertGreater(cache.report()['evictions'],0)
