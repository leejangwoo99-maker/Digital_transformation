"""Memory-aware LRU for prepared models, never for product recognition results."""
from collections import OrderedDict, namedtuple, deque
from statistics import median
from math import ceil
from threading import RLock
from time import monotonic

import psutil

CacheInfo = namedtuple('CacheInfo','hits misses maxsize currsize')


class AdaptiveModelCache:
    def __init__(self, factory, memory=None, rss=None, clock=monotonic):
        self.factory = factory
        self.memory = memory or psutil.virtual_memory
        self.rss = rss or (lambda:psutil.Process().memory_info().rss)
        self.clock = clock
        self.lock = RLock()
        self.values = OrderedDict()
        self.usage_profile = {}
        self.capacity = 16
        self.estimated_entry_bytes = 64*1024**2
        self.growth_samples = deque(maxlen=16)
        self.load_count = 0
        self.rss_growth_peak_bytes = 0
        self.hits = self.misses = self.evictions = self.pressure_evictions = 0
        self.resizes = self.peak_entries = 0
        self.last_sample = float('-inf')
        self.memory_percent = 0.
        self.events = deque(maxlen=512)
        self.event_sequence = 0
        self.pressure_since = None
        self.last_soft_shrink = float('-inf')
        self.shrink_delay_seconds = 3.
        self.shrink_interval_seconds = 2.

    def _event(self, reason, capacity_before, entries_before, evicted=0):
        self.event_sequence += 1
        self.events.append(dict(sequence=self.event_sequence,monotonic_seconds=self.clock(),reason=reason,
            capacity_before=capacity_before,capacity_after=self.capacity,entries_before=entries_before,
            entries_after=len(self.values),evicted=evicted,memory_percent=self.memory_percent,
            estimated_entry_bytes=self.estimated_entry_bytes,
            pressure_seconds=0. if self.pressure_since is None else max(0.,self.clock()-self.pressure_since)))

    def set_usage_profile(self, usage):
        with self.lock:
            self.usage_profile = dict(usage)

    def preparation_budget(self):
        with self.lock:
            self._resize(force=True)
            if self.memory_percent >= 80:
                return 0
            return self.capacity

    def is_resident(self, key):
        with self.lock:
            return key in self.values

    def _eviction_key(self):
        # OrderedDict iteration keeps the LRU tie-break for equally useful models.
        return min(self.values, key=lambda key: self.usage_profile.get(key, 0))

    def _resize(self, force=False):
        now = self.clock()
        if not force and now-self.last_sample < 1.:
            return
        self.last_sample = now
        memory = self.memory()
        self.memory_percent = memory.percent
        size = self.estimated_entry_bytes
        headroom = int(memory.total*.8-memory.used)
        budget = max(0,min(int(memory.total*.2),len(self.values)*size+headroom))
        target = min(128,budget//size)
        pressure_started = False
        if memory.percent >= 90:
            capacity,reason = 0,'emergency_memory_90'
            self.pressure_since = None
        elif memory.percent >= 80:
            if self.pressure_since is None:
                self.pressure_since = now
                pressure_started = True
            if now-self.pressure_since < self.shrink_delay_seconds:
                capacity,reason = max(self.capacity,len(self.values)),'soft_pressure_grace'
            elif now-self.last_soft_shrink < self.shrink_interval_seconds:
                capacity,reason = max(self.capacity,len(self.values)),'soft_shrink_cooldown'
            else:
                step = max(1,ceil(len(self.values)*.1))
                capacity = min(self.capacity,max(target,len(self.values)-step))
                reason = 'sustained_memory_target_80'
                self.last_soft_shrink = now
        else:
            self.pressure_since = None
            # Estimated per-model costs must not evict useful entries while actual RAM is healthy.
            capacity = max(len(self.values),target if memory.percent < 78 else min(self.capacity,target))
            reason = 'available_headroom_or_entry_estimate' if memory.percent < 78 else 'growth_hysteresis_78'
        previous,entries = self.capacity,len(self.values)
        if capacity != self.capacity:
            self.resizes += 1
            self.capacity = capacity
        while len(self.values) > self.capacity:
            del self.values[self._eviction_key()]
            self.evictions += 1
            self.pressure_evictions += 1
        if previous != self.capacity or entries != len(self.values) or pressure_started:
            self._event(reason,previous,entries,entries-len(self.values))
            self.events[-1].update(memory_used_bytes=memory.used,memory_total_bytes=memory.total,
                                   headroom_bytes=headroom,budget_bytes=budget)

    def __call__(self, *key):
        with self.lock:
            self._resize()
            if key in self.values:
                self.hits += 1
                self.values.move_to_end(key)
                return self.values[key]
            self.misses += 1
            before = self.rss()
            value = self.factory(*key)
            growth = self.rss()-before
            self.load_count += 1
            self.rss_growth_peak_bytes = max(self.rss_growth_peak_bytes,growth)
            # Initial plugin allocations and unrelated lanes are not per-model costs.
            # System-memory feedback remains the safety guard, not this noisy estimate.
            if self.load_count > 1 and growth > 0:
                self.growth_samples.append(growth)
                self.estimated_entry_bytes = max(16*1024**2,min(96*1024**2,int(median(self.growth_samples))))
            self._resize(force=True)
            if self.capacity:
                while len(self.values) >= self.capacity:
                    victim = self._eviction_key()
                    if self.usage_profile.get(victim, 0) > self.usage_profile.get(key, 0):
                        return value
                    entries = len(self.values)
                    del self.values[victim]
                    self.evictions += 1
                    self._event('capacity_lru_replacement',self.capacity,entries,1)
                self.values[key] = value
                self.peak_entries = max(self.peak_entries,len(self.values))
            return value

    def cache_info(self):
        with self.lock:
            return CacheInfo(self.hits,self.misses,self.capacity,len(self.values))

    def cache_clear(self):
        with self.lock:
            self.values.clear()
            self.usage_profile.clear()
            self.hits = self.misses = self.evictions = self.pressure_evictions = 0
            self.resizes = self.peak_entries = 0
            self.last_sample = float('-inf')
            self.growth_samples.clear()
            self.load_count = self.rss_growth_peak_bytes = 0
            self.estimated_entry_bytes = 64*1024**2
            self.events.clear()
            self.event_sequence = 0
            self.pressure_since = None
            self.last_soft_shrink = float('-inf')

    def events_since(self, sequence):
        with self.lock:
            events = [dict(event) for event in self.events if event['sequence'] > sequence]
            return dict(events=events,truncated=bool(events and events[0]['sequence'] > sequence+1))

    def report(self):
        with self.lock:
            return dict(evictions=self.evictions,pressure_evictions=self.pressure_evictions,
                        event_sequence=self.event_sequence,sampled_at_monotonic=self.clock(),
                        sampling_interval_seconds=1.,shrink_delay_seconds=self.shrink_delay_seconds,
                        shrink_interval_seconds=self.shrink_interval_seconds,max_soft_eviction_fraction=.1,
                        growth_recovery_percent=78,policy_name='usage_ranked_adaptive_lru',
                        profiled_models=len(self.usage_profile),
                        capacity_changes=self.resizes,peak_entries=self.peak_entries,
                        estimated_entry_bytes=self.estimated_entry_bytes,
                        estimate_strategy='rolling median; first load excluded; 16..96 MiB estimate; actual system-memory guard',
                        rss_growth_samples=len(self.growth_samples),rss_growth_peak_bytes=self.rss_growth_peak_bytes,
                        estimated_cache_bytes=len(self.values)*self.estimated_entry_bytes,
                        memory_percent=self.memory_percent,target_memory_percent=80,
                        emergency_memory_percent=90,max_entries=128)
