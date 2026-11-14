"""Soft admission control. Never drop frames or kill in-flight inference."""
import math
import os
import time

import psutil


def parallel_settings(workers, spread):
    values = [float(workers),float(spread)]
    if any(not math.isfinite(v) or not v.is_integer() or v < 1 for v in values):
        raise ValueError('Worker and spread must be positive integers.')
    workers,spread = map(int,values)
    if workers > 61:
        raise ValueError('Worker limit: 61 processes (Windows process-pool limit).')
    cores = psutil.cpu_count() or 1
    if spread > cores:
        raise ValueError(f'Spread limit: {cores} logical CPU threads per process.')
    return workers,spread


def resource_estimate(workers,spread,model_bytes=0):
    workers,spread = parallel_settings(workers,spread)
    memory = psutil.virtual_memory()
    cores = psutil.cpu_count() or 1
    # Each spawned interpreter owns its model plus a bounded 256 MiB variant cache.
    per_worker = 320*1024**2+256*1024**2+model_bytes
    budget = max(0,int(memory.total*.9-memory.used))
    cpu = psutil.cpu_percent(interval=None)
    warnings = []
    if workers*spread > max(1,math.floor(cores*.8)):
        warnings.append(f'CPU budget warning: {workers} x {spread} threads; {cores} logical cores, target 80%.')
    if workers*per_worker > budget:
        warnings.append(f'Memory budget warning: estimated {workers*per_worker/1024**3:.2f} GiB; available below 90%: {budget/1024**3:.2f} GiB.')
    if cpu > 80 or memory.percent > 90:
        warnings.append(f'Current load: CPU {cpu:.1f}%, memory {memory.percent:.1f}%.')
    return dict(workers=workers,spread=spread,cores=cores,available_gib=memory.available/1024**3,
                estimated_worker_gib=per_worker/1024**3,memory_worker_limit=budget//per_worker,
                cpu_percent=cpu,memory_percent=memory.percent,warnings=warnings)


class ResourceGate:
    def __init__(self, enabled=True, sustained=1., clock=time.monotonic, sleep=time.sleep, sampler=None,status_callback=None):
        self.enabled,self.sustained = enabled,sustained
        self.clock,self.sleep = clock,sleep
        self.sampler = sampler or (lambda:(psutil.cpu_percent(interval=None),psutil.virtual_memory().percent))
        self.last_sample = -math.inf
        self.high_since = None
        self.cpu = self.memory = 0.
        self.peak_cpu = self.peak_memory = 0.
        self.samples = 0
        self.cpu_total = self.memory_total = 0.
        self.wait_seconds = 0.
        self.delays = 0
        self.message = ''
        self.status_callback = status_callback
        self.last_notice = -math.inf

    def before_submit(self):
        if not self.enabled:
            return
        now = self.clock()
        if now-self.last_sample >= .25:
            self.cpu,self.memory = self.sampler()
            self.last_sample = now
            self.peak_cpu = max(self.peak_cpu,self.cpu)
            self.peak_memory = max(self.peak_memory,self.memory)
            self.samples += 1
            self.cpu_total += self.cpu
            self.memory_total += self.memory
        high = self.cpu > 80 or self.memory > 90
        if not high:
            self.high_since = None
            self.message = ''
            return
        if self.high_since is None:
            self.high_since = now
        if now-self.high_since >= self.sustained:
            delay = min(1.,.1+max(0,self.cpu-80)/50+max(0,self.memory-90)/20)
            self.message = f'Resource delay | CPU {self.cpu:.1f}% / memory {self.memory:.1f}% | no frames skipped'
            if self.status_callback and now-self.last_notice >= 1:
                self.last_notice = now
                try:
                    self.status_callback(self.message)
                except Exception:
                    pass
            self.sleep(delay)
            self.wait_seconds += delay
            self.delays += 1

    def report(self):
        return dict(cpu_limit_percent=80,memory_limit_percent=90,soft_limits=True,
                    sustained_seconds=self.sustained,delay_count=self.delays,wait_seconds=self.wait_seconds,
                    last_cpu_percent=self.cpu,last_memory_percent=self.memory,
                    peak_cpu_percent=self.peak_cpu,peak_memory_percent=self.peak_memory,
                    mean_cpu_percent=self.cpu_total/max(1,self.samples),mean_memory_percent=self.memory_total/max(1,self.samples),
                    note='Admission delays only; OS usage is not a hard cap; no frame dropping.')
