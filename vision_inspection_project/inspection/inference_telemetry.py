"""Per-inspection diagnostics; aggregate elapsed work is distinct from wall time."""
from collections import Counter
from contextlib import contextmanager
from threading import Lock
from time import perf_counter


class InferenceTelemetry:
    def __init__(self, clock=perf_counter):
        self.clock = clock
        self.started = clock()
        self.lock = Lock()
        self.active = self.peak = self.calls = self.failures = 0
        self.busy_started = None
        self.busy_seconds = self.inference_seconds = 0.
        self.kinds = {}
        self.batches = []
        self.dispatches = []
        self.batch_count = self.dispatch_count = 0
        self.configurations = set()
        self.model_usage = Counter()

    def record_model_use(self, key):
        with self.lock:
            self.model_usage[key] += 1

    def model_profile(self):
        with self.lock:
            return self.model_usage.copy()

    def configuration(self, kind, requested_threads, effective_threads):
        with self.lock:
            self.configurations.add((kind,requested_threads,effective_threads))

    @contextmanager
    def inference(self, kind):
        started = self.clock()
        with self.lock:
            row = self.kinds.setdefault(kind,dict(active=0,peak=0,calls=0,failures=0,inference_seconds_sum=0.))
            if self.active == 0:
                self.busy_started = started
            self.active += 1
            self.peak = max(self.peak,self.active)
            row['active'] += 1
            row['peak'] = max(row['peak'],row['active'])
        failed = True
        try:
            yield
            failed = False
        finally:
            elapsed = self.clock()-started
            with self.lock:
                self.active -= 1
                self.calls += 1
                self.failures += int(failed)
                self.inference_seconds += elapsed
                row['active'] -= 1
                row['calls'] += 1
                row['failures'] += int(failed)
                row['inference_seconds_sum'] += elapsed
                if self.active == 0:
                    self.busy_seconds += self.clock()-self.busy_started
                    self.busy_started = None

    def batch(self, **row):
        with self.lock:
            self.batch_count += 1
            if len(self.batches) < 512:
                self.batches.append(dict(row))

    def dispatch(self, **row):
        with self.lock:
            self.dispatch_count += 1
            if len(self.dispatches) < 512:
                self.dispatches.append(dict(row))

    def report(self):
        with self.lock:
            busy = self.busy_seconds+(self.clock()-self.busy_started if self.active else 0.)
            return dict(scope='inspection only; excludes selection-time preparation',
                openvino_calls=self.calls,openvino_failures=self.failures,active_openvino_requests=self.active,
                peak_concurrent_openvino_requests=self.peak,openvino_busy_wall_seconds=busy,
                openvino_inference_seconds_sum=self.inference_seconds,
                mean_concurrency_while_busy=self.inference_seconds/busy if busy and not self.active else None,
                kinds={key:dict(value) for key,value in self.kinds.items()},
                native_thread_configurations=[dict(kind=kind,requested_threads=requested,effective_threads=effective)
                    for kind,requested,effective in sorted(self.configurations,key=lambda row:(row[0],row[1],str(row[2])))],
                batches=list(self.batches),batch_count=self.batch_count,batches_truncated=self.batch_count>512,
                dispatches=list(self.dispatches),dispatch_count=self.dispatch_count,dispatches_truncated=self.dispatch_count>512,
                note='Batch/individual inference elapsed times overlap across workers. Do not sum them to estimate CT.')
