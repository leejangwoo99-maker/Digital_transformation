"""File-test sweep and machine-local accepted speed conditions."""
import hashlib
import json
import math
import platform
from pathlib import Path
from statistics import mean


def conditions(limits):
    from inspection.automatic_runtime import check_parallelism
    selected = dict(limits)
    check_parallelism(selected,limits)
    workers,threads = selected['ocr_job_workers'],selected['ocr_inference_threads']
    return [dict(backend=backend,workers=w,threads=t)
            for backend in ('pytorch','openvino')
            for w in range(max(2,workers-2),workers+1)
            for t in range(max(2,threads-2),threads+1)]


def valid_condition(value):
    return (isinstance(value,dict) and value.get('backend') in ('pytorch','openvino')
            and all(type(value.get(key)) is int and value[key] >= 2 for key in ('workers','threads')))


def profile_path(settings):
    from inspection.special_checks import group_key
    machine = hashlib.sha256(platform.node().encode()).hexdigest()[:16]
    return Path(settings['special_info_dir'])/f"{group_key(settings['special_group'])}_speed_{machine}.json"


def load_condition(settings):
    path = profile_path(settings)
    if not path.is_file():
        return None
    value = json.loads(path.read_text(encoding='utf-8'))
    return value if valid_condition(value) else None


def save_condition(settings, condition):
    if not valid_condition(condition):
        raise ValueError('Invalid speed condition')
    path = profile_path(settings)
    path.parent.mkdir(parents=True,exist_ok=True)
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(condition,indent=2),encoding='utf-8')
    temporary.replace(path)


def apply_condition(settings, condition):
    settings['speed_condition'] = dict(condition)
    settings.setdefault('model',{})['appearance_backend'] = condition['backend']


def rank_conditions(paths, benchmark_id, expected):
    groups = {}
    for path in paths:
        data = json.loads(Path(path).read_text(encoding='utf-8'))
        tag = data.get('speed_benchmark',{})
        if tag.get('id') != benchmark_id or tag.get('warmup') or data.get('status') != 'completed':
            continue
        condition = tag['condition']
        performance = data['performance']
        runtime = performance['automatic_runtime']
        if (performance['appearance_backend']['name'],runtime['ocr_job_workers'],runtime['ocr_inference_threads']) != (
                condition['backend'],condition['workers'],condition['threads']):
            continue
        ct = data['pipeline_time_seconds']
        if isinstance(ct,(int,float)) and math.isfinite(ct) and ct > 0:
            key = (condition['backend'],condition['workers'],condition['threads'])
            groups.setdefault(key,[]).append(ct)
    rows = [dict(backend=backend,workers=workers,threads=threads,mean=mean(values),
                 minimum=min(values),maximum=max(values),count=len(values))
            for (backend,workers,threads),values in groups.items() if len(values) == expected]
    return sorted(rows,key=lambda row:(row['mean'],row['maximum']))
