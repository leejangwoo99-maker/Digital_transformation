from contextlib import contextmanager
from time import perf_counter


_TIMELINE_LANES = {
    'appearance_inference': 'appearance',
    'special_frame_parallel_wall': 'special_checks',
    'essential_shape_check': 'special_checks',
    'photo_selection_including_inspection': 'special_checks',
    'conn_result_finalize': 'special_result_finalize',
    'frame_read_decode_wait': 'frame_input',
    'appearance_lookahead_input_wait': 'frame_input',
    'resource_admission_wait': 'resource_wait',
    'appearance_result_wait': 'appearance_wait',
    'frame_alignment': 'alignment',
    'frame_annotation': 'render',
    'result_video_composition': 'render',
    'inspection_photo_composition': 'render',
    'embedded_result_prepare': 'preview',
    'preview_display': 'preview',
    'progress_callback': 'preview',
    'save_queue_backpressure': 'media_wait',
    'media_cleanup_wait': 'media_wait',
    'pipeline_cleanup': 'cleanup',
    'result_video_write': 'media_write',
    'inspection_photo_save': 'media_write',
    'confidence_frame_save': 'media_write',
    'confidence_flush': 'media_write',
    'result_video_release': 'media_write',
    'result_video_rename': 'media_write',
    'result_video_encoding': 'media_write',
    'serial_result_planning': 'result_finalize',
    'ocr_cache_profile_finalize': 'result_finalize',
    'verdict_txt_prepare': 'txt_prepare',
    'verdict_txt_publish': 'txt_publish',
}


def timeline_lane(stage):
    if stage.startswith('inline_photo_inspection.'):
        return 'special_checks'
    return _TIMELINE_LANES.get(stage)


def summarize_timeline(spans, start, end):
    """Partition elapsed wall time into disjoint sets of active lanes."""
    from collections import Counter, defaultdict
    edges = defaultdict(Counter)
    edges[start]
    edges[end]
    stages = defaultdict(lambda: dict(span_count=0, elapsed_seconds=0.))
    for stage, left, right in spans:
        left, right = max(left,start), min(right,end)
        if right <= left:
            continue
        lane = timeline_lane(stage)
        edges[left][lane] += 1
        edges[right][lane] -= 1
        stages[stage]['span_count'] += 1
        stages[stage]['elapsed_seconds'] += right-left
    active, lane_seconds, combinations = Counter(), Counter(), Counter()
    previous = start
    for edge, changes in sorted(edges.items()):
        if edge > previous:
            lanes = tuple(sorted(lane for lane,count in active.items() if count > 0))
            elapsed = edge-previous
            combinations[lanes] += elapsed
            for lane in lanes:
                lane_seconds[lane] += elapsed
        active.update(changes)
        previous = edge
    return dict(start_seconds=start,end_seconds=end,wall_seconds=end-start,
                stages=dict(stages),lane_wall_seconds=dict(lane_seconds),
                exclusive_lane_wall_seconds={lane:combinations[(lane,)] for lane in lane_seconds},
                overlap_wall_seconds=sum(value for lanes,value in combinations.items() if len(lanes)>1),
                unmeasured_wall_seconds=combinations[()],
                overlap_combinations=[dict(lanes=list(lanes),wall_seconds=value)
                                      for lanes,value in sorted(combinations.items()) if len(lanes)>1])


class Performance:
    def __init__(self):
        from threading import Lock
        self.lock = Lock()
        self.started = perf_counter()
        self.stages = {}
        self.milestones = {}
        self.spans = []
        self.active_spans = {}

    def mark(self, name):
        with self.lock:
            self.milestones.setdefault(name,perf_counter()-self.started)

    def add(self, name, seconds):
        with self.lock:
            row = self.stages.setdefault(name, dict(count=0, total_seconds=0., max_seconds=0.))
            row['count'] += 1
            row['total_seconds'] += seconds
            row['max_seconds'] = max(row['max_seconds'], seconds)

    def add_since(self, name, started_at):
        end = perf_counter()
        self.add(name,end-started_at)
        if timeline_lane(name):
            with self.lock:
                self.spans.append((name,started_at-self.started,end-self.started))

    @contextmanager
    def measure(self, name):
        start = perf_counter()
        token = None
        if timeline_lane(name):
            token = object()
            with self.lock:
                self.active_spans[token] = (name,start-self.started)
        try:
            yield
        finally:
            end = perf_counter()
            if token is not None:
                with self.lock:
                    stage, left = self.active_spans.pop(token)
                    self.spans.append((stage,left,end-self.started))
            self.add(name,end-start)

    def verdict_timeline(self):
        with self.lock:
            end = self.milestones.get('verdict_txt_published')
            if end is None:
                return None
            ready = self.milestones.get('inline_photo_inspections_ready',
                                        self.milestones.get('file_photo_inspections_ready'))
            spans = self.spans.copy()
            spans.extend((name,start,end) for name,start in self.active_spans.values())
        return dict(clock='seconds since pipeline entry',
                    special_ready_boundary='OCR/Label results selected; Conn finalization may follow',
                    note='Lane totals overlap. Overlap combinations are disjoint wall intervals. Unmeasured time is not necessarily idle.',
                    pipeline_to_txt=summarize_timeline(spans,0.,end),
                    special_ready_to_txt=summarize_timeline(spans,ready,end) if ready is not None else None)

    def call(self, name, function, *args, **kwargs):
        with self.measure(name):
            return function(*args, **kwargs)

    def items(self, values):
        for kind, value in values:
            with self.measure('inspection.'+kind):
                yield kind, value

    def report(self):
        with self.lock:
            stages = {name: dict(row, mean_seconds=row['total_seconds']/row['count'])
                      for name, row in self.stages.items()}
            milestones = dict(self.milestones)
        return dict(total_wall_seconds=perf_counter()-self.started, stages=stages,milestones_seconds=milestones,
                    slowest_stage=max((name for name in stages if name != 'photo_selection_including_inspection'),
                        key=lambda name: stages[name]['total_seconds'],default=None),
                    note='Stage totals overlap: inspection kinds include reference correction; capture, appearance and saving may run concurrently. Do not sum stage totals or subtract asynchronous save time from wall time.')


def result_directory(base, source_name, group, fps, threshold, fraction=False):
    import re
    from pathlib import Path
    def clean(value):
        return re.sub(r'[<>:"/\\|?*\x00-\x1f]', '_', str(value)).strip(' .') or 'result'
    group = clean(group)
    fps_part = '' if re.search(r'\d+(?:\.\d+)?fps', group, re.I) else f'_{float(fps):g}fps'
    value = f'{float(threshold)/100:.2f}' if fraction else f'{float(threshold):g}'
    return Path(base) / f'{clean(source_name)}_{group}{fps_part}_threshold_{value}_[{float(fps):g}fps]'
