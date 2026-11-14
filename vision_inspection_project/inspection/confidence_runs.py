"""Immutable per-run confidence history under a source/group/threshold folder."""
import json
import math
import re
from datetime import datetime
from pathlib import Path


def unnumbered_name(name):
    return re.sub(r'^(?:\d+st_)+','',name)


def reserve_run(parent, number=None):
    parent = Path(parent).resolve()
    parent.mkdir(parents=True,exist_ok=True)
    numbers = [int(m[1]) for path in parent.iterdir()
               if (m := re.match(r'\.?([0-9]+)st_',path.name))]
    number = number if number is not None else max(numbers,default=0)+1
    while True:
        directory = parent/f'.{number}st_pending'
        try:
            directory.mkdir()
            return directory,number
        except FileExistsError:
            number += 1


def finish_run(directory, number):
    directory = Path(directory)
    now = datetime.now()
    suffix = re.search(r'_\[\d+(?:\.\d+)?fps\]$',directory.parent.name)
    fps_suffix = suffix.group() if suffix else ''
    name = f'{number}st_{unnumbered_name(directory.parent.name)}_point_frame_{now:%Y-%m-%d_%H-%M-%S}'
    target = directory.with_name(name+fps_suffix)
    if target.exists():
        target = directory.with_name(f'{name}_{now.microsecond//1000:03}{fps_suffix}')
    directory.rename(target)
    return target


def load_runs(folder, _depth=0, *, allow_mixed_thresholds=False):
    parent = Path(folder).resolve()
    names = '|'.join(re.escape(name) for name in {parent.name,unnumbered_name(parent.name)})
    pattern = re.compile(r'([0-9]+)st_(?:'+names+r')_point_frame_\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}(?:_\d{3})?(?:_\[\d+(?:\.\d+)?fps\])?')
    records = []
    identity = None
    for directory in parent.iterdir():
        if not directory.is_dir() or directory.name.startswith('.'):
            continue
        if directory.name == 'confidence_point_frame':
            continue
        match = pattern.fullmatch(directory.name)
        if directory.is_symlink():
            raise ValueError(f'Inconsistent run folder: {directory.name}')
        if not match:
            is_threshold_folder = re.search(r'_threshold_\d+(?:\.\d+)?(?:_\[\d+(?:\.\d+)?fps\])?$',directory.name)
            if ((_depth == 0 and re.match(r'^\d+st_.+_threshold_',directory.name))
                    or (allow_mixed_thresholds and _depth < 2 and is_threshold_folder)):
                legacy = directory/'confidence_point_frame'
                if allow_mixed_thresholds and (legacy/'index.json').is_file():
                    if legacy.is_symlink():
                        raise ValueError(f'Inconsistent run folder: {legacy.name}')
                    data = json.loads((legacy/'index.json').read_text(encoding='utf-8'))
                    prefix = re.match(r'^(\d+)st_',directory.name)
                    nested = [(legacy,dict(data,run_number=data.get('run_number',int(prefix[1]) if prefix else 1)))]
                else:
                    nested = load_runs(directory,_depth=_depth+1,allow_mixed_thresholds=allow_mixed_thresholds)
            else:
                raise ValueError(f'Inconsistent run folder: {directory.name}')
        else:
            data = json.loads((directory/'index.json').read_text(encoding='utf-8'))
            if data.get('run_number') != int(match[1]) or data.get('parent_name') != parent.name:
                raise ValueError(f'Invalid run identity: {directory.name}')
            nested = [(directory,data)]
        for run_folder,data in nested:
            current = (data.get('original_source',data['source']),data['group'],float(data['threshold_percent']),float(data['fps']))
            if not math.isfinite(current[2]) or not 0 < current[2] <= 100 or not math.isfinite(current[3]) or current[3] <= 0:
                raise ValueError('Invalid threshold or FPS in run records')
            comparable = current[:2]+current[3:] if allow_mixed_thresholds else current
            if identity is not None and comparable != identity:
                if comparable[-1] != identity[-1]:
                    raise ValueError('different Test fps')
                raise ValueError('All runs must have the same source, frame group and FPS.' if allow_mixed_thresholds
                                 else 'All runs must have the same source, frame group, FPS and threshold.')
            identity = comparable
            if data.get('status') != 'completed':
                raise ValueError(f'Incomplete inspection run: {run_folder.name}')
            records.append((run_folder,data))
    records.sort(key=lambda pair:pair[1]['run_number'])
    if len({(float(data['threshold_percent']) if allow_mixed_thresholds else None,data['run_number'])
            for _,data in records}) != len(records):
        raise ValueError('Duplicate run numbers')
    if not records:
        raise ValueError('No completed confidence runs. Run a new appearance inspection first.')
    return records
