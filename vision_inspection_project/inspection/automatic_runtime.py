"""Measured CPU scheduling, cached by hardware/model generation, with soft limits."""
import copy
import functools
import hashlib
import json
import math
import os
import platform
from pathlib import Path
from threading import RLock
from time import perf_counter
from concurrent.futures import ThreadPoolExecutor

import psutil

LOCK = RLock()
DETECTORS = {}
MARKS = {}
PREPARED = set()
WARMED = set()
SPECIALS = {}
VERSION = 5
COMPARISON_PROFILES = {'jobs2_threads2':(2,2),'jobs1_threads4':(1,4)}


@functools.lru_cache(maxsize=1024)
def annotation_images(path, size, modified_ns):
    data = json.loads(Path(path).read_text(encoding='utf-8'))
    image = data.get('image')
    return (image,) if image else ()


def serialized(function):
    @functools.wraps(function)
    def wrapped(*args,**kwargs):
        settings = args[1] if len(args)>1 else kwargs['settings']
        if not settings.get('auto_runtime'):
            return function(*args,**kwargs)
        waiting = perf_counter()
        with LOCK:
            settings['_runtime_lock_wait_seconds'] = perf_counter()-waiting
            settings['_runtime_resources'] = []
            try:
                return function(*args,**kwargs)
            finally:
                import sys
                failed = sys.exc_info()[0] is not None
                errors = []
                for resource in reversed(settings.pop('_runtime_resources',[])):
                    try:
                        if hasattr(resource,'finish_background'):
                            if failed:
                                resource.abort()
                            else:
                                resource.finish_background(settings.get('media_done_callback'))
                        elif hasattr(resource,'close'):
                            resource.close()
                        else:
                            resource.shutdown(wait=True,cancel_futures=True)
                    except Exception as exc:
                        errors.append(exc)
                if errors and not failed:
                    raise errors[0]
    return wrapped


def retain(settings,resource):
    settings.setdefault('_runtime_resources',[]).append(resource)
    return resource


def allocation():
    cores = psutil.cpu_count() or 1
    memory = psutil.virtual_memory()
    budget = max(1,math.floor(cores*.8))
    ram = max(0,int(memory.total*.9-memory.used))
    workers = max(1,min(3,budget//2,ram//(640*1024**2)))
    if memory.percent < 89 and MARKS:
        workers = max(workers,min(3,budget//2,max(model.workers for model in MARKS.values())))
    return dict(cores=cores,cpu_budget=budget,available_bytes=ram,memory_percent=memory.percent,
                mark_workers=int(workers),mark_spread=1,
                cpu_threads=max(1,min(4,budget)),parallel=budget>=4 and ram>=1024**3,
                save_queue_capacity=max(1,min(3,ram//(64*1024**2))))


def fingerprint(settings):
    root = Path(settings['root_dir'])
    group = settings.get('special_group','')
    files = list((root/'weights').glob(f'{group}*'))
    files += list((root/'weights/openvino_ocr').glob(f'{group}*'))
    annotations = list((root/'dataset/special_labels'/group).rglob('*.json'))
    files += annotations
    for annotation in annotations:
        try:
            stat = annotation.stat()
            files += [Path(image) for image in annotation_images(str(annotation),stat.st_size,stat.st_mtime_ns)]
        except (OSError,ValueError):
            pass
    files += [p for p in Path(settings['special_info_dir']).glob(f'{group}*')
              if p.is_file() and 'runtime_profile' not in p.name]
    files += [Path(p) for p in settings.get('special_frame_paths',[])]
    if settings.get('alignment_reference'):
        files.append(Path(settings['alignment_reference']))
    stamps = [(str(p),p.stat().st_size,p.stat().st_mtime_ns) for p in files if p.is_file()]
    weights = root/settings.get('model',{}).get('weights','weights/best.pt')
    if weights.is_file():
        stamps.append((str(weights),weights.stat().st_size,weights.stat().st_mtime_ns))
    data = dict(version=VERSION,processor=platform.processor(),cores=psutil.cpu_count(),
                memory=psutil.virtual_memory().total,files=sorted(stamps),
                model=settings.get('model',{}),options={key:settings.get(key) for key in
                    ('special_frame_ranges','special_labeled_frames','conn_inspection_frame',
                     'alignment_enabled','alignment_reference','ocr_backend','reference_tracking','openvino_comparison_profile')},
                frame_paths=[str(p) for p in settings.get('special_frame_paths',[])])
    return hashlib.sha256(json.dumps(data,sort_keys=True).encode()).hexdigest()


def enable_parallel(selected,limits):
    # Keep the appearance lane available even if an older tuning profile chose serial.
    selected['parallel'] = bool(limits['parallel'])
    remaining = max(1,limits['cpu_budget']-(selected['cpu_threads'] if selected['parallel'] else 0))
    selected['mark_workers'] = min(selected['mark_workers'],remaining)
    selected['mark_spread'] = min(selected['mark_spread'],max(1,remaining//selected['mark_workers']))


def check_parallelism(selected, limits, settings=None):
    budget = limits['cpu_budget']
    workers = 3 if budget >= 8 else 2 if budget >= 6 else 1
    if not limits['parallel']:
        workers = 1
    # This maps 12 logical CPUs to 4x4, not to a system-wide thread limit.
    cpu_side = max(1,min(budget,limits['cores']//3))
    ram = limits.get('available_bytes')
    memory_side = (2+max(0,ram-1024**3)//(640*1024**2)) if ram is not None else cpu_side
    memory_percent = limits.get('memory_percent',0)
    if memory_percent >= 85:
        memory_side = 1
    elif memory_percent >= 80:
        memory_side = min(memory_side,2)
    side = min(cpu_side,memory_side) if limits['parallel'] else 1
    selected.update(special_workers=workers,ocr_job_workers=side,ocr_inference_threads=side,
                    cpu_threads=min(selected['cpu_threads'],2),mark_workers=1,mark_spread=1)
    selected.update(ocr_cpu_policy='logical_processors_divided_by_3',
                    ocr_cpu_candidate=cpu_side,ocr_memory_candidate=int(memory_side))
    profile = (settings or {}).get('openvino_comparison_profile')
    if profile:
        if profile not in COMPARISON_PROFILES:
            raise ValueError(f'Unknown OpenVINO comparison profile: {profile}')
        jobs,threads = COMPARISON_PROFILES[profile]
        selected.update(ocr_job_workers=jobs,ocr_inference_threads=threads,
                        cpu_threads=min(2,limits['cpu_budget']),mark_workers=1,mark_spread=1,
                        comparison_profile=profile)
    fixed = (settings or {}).get('speed_condition')
    if fixed is not None:
        from inspection.speed_benchmark import valid_condition
        if not valid_condition(fixed) or max(fixed['workers'],fixed['threads']) > limits['cores']:
            raise ValueError('Saved speed condition is not valid for this CPU.')
        selected.update(ocr_job_workers=fixed['workers'],ocr_inference_threads=fixed['threads'],
                        ocr_cpu_policy='fixed_speed_condition')


def profile_path(settings):
    return Path(settings['special_info_dir'])/f"{settings['special_group']}_runtime_profile.json"


def samples(special):
    import cv2
    from inspection.text_image_model import text_crop
    from inspection.region_masks import mask_regions
    requests = {}
    image = None
    for kind,model in special.trained_text_ocr.items():
        record = special.references[kind]
        source = cv2.imread(record['image'])
        if source is None:
            continue
        if image is None:
            image = source
        masked = mask_regions(source,[r for r in record['regions'] if r['role']=='exclude'])
        rows = [dict(r,image=text_crop(masked,r),line_count=len(r.get('text','').split('\n')))
                for r in record['regions'] if r['role']=='text']
        requests[kind] = rows
        area = next((r for r in record['regions'] if r['role']=='registration'),None)
        if area:
            special.shape_registration.fit(source,record['image'],area)
    if image is None and special.references:
        image = cv2.imread(next(iter(special.references.values()))['image'])
    return image,requests


def tune_marks(model,requests,worker_limit,cpu_budget):
    rows = []
    expected = None
    try:
        for workers in sorted({1,min(2,worker_limit),worker_limit}):
            for spread in ((1,) if workers == 1 and not getattr(model,'allow_global_thread_limits',True) else (1,2)):
                if workers*spread > cpu_budget:
                    continue
                model.close()
                model.workers,model.spread = workers,spread
                try:
                    model.predict_many(requests)
                    started = perf_counter()
                    result = [r['verdict'] for r in model.predict_many(requests)]
                    elapsed = perf_counter()-started
                    if expected is None:
                        expected = result
                    rows.append(dict(workers=workers,spread=spread,seconds=elapsed,
                                     consistent=result == expected))
                except Exception as exc:
                    rows.append(dict(workers=workers,spread=spread,error=str(exc),consistent=False))
    finally:
        model.close()
    valid = [r for r in rows if r.get('consistent')]
    best = min(valid,key=lambda r:r['seconds']) if valid else dict(workers=1,spread=1)
    return best,rows


def prepare(settings,detector_factory=None,special_factory=None):
    import cv2
    import torch
    from inspection.shape_registration import ShapeRegistration
    if detector_factory is None:
        from model.yolo_inference import YoloInference
        detector_factory = YoloInference
    if special_factory is None:
        from inspection.special_checks import SpecialInspector
        special_factory = SpecialInspector
    started = perf_counter()
    preparation_stages = {}
    def measured(name, function, *args):
        tick = perf_counter()
        try:
            return function(*args)
        finally:
            preparation_stages[name] = preparation_stages.get(name,0.)+perf_counter()-tick
    with LOCK:
        limits = allocation()
        signature = fingerprint(settings)
        path = profile_path(settings)
        profile = None
        try:
            stored = json.loads(path.read_text(encoding='utf-8'))
            valid = (isinstance(stored,dict) and isinstance(stored.get('parallel'),bool)
                     and all(type(stored.get(k)) is int and stored[k] >= 1
                             for k in ('cpu_threads','mark_workers','mark_spread'))
                     and stored['cpu_threads'] <= limits['cores'] and stored['mark_workers'] <= 61
                     and stored['mark_spread'] <= limits['cores'])
            if valid and stored.get('signature') == signature:
                profile = stored
        except (OSError,ValueError):
            pass
        selected = dict(limits)
        if profile:
            selected.update({k:profile[k] for k in ('cpu_threads','parallel','mark_workers','mark_spread')})
        selected['cpu_threads'] = min(selected['cpu_threads'],limits['cpu_budget'])
        selected['mark_workers'] = min(selected['mark_workers'],limits['mark_workers'])
        enable_parallel(selected,limits)
        check_parallelism(selected,limits,settings)
        settings.update(cpu_threads=selected['cpu_threads'],mark_workers=selected['mark_workers'],
                        mark_spread=selected['mark_spread'])
        torch.set_num_threads(selected['cpu_threads'])
        cv2.setNumThreads(1)
        detector_key = signature
        cached = detector_key in DETECTORS
        if not cached:
            DETECTORS.clear()
            DETECTORS[detector_key] = measured('appearance_model_loading',detector_factory,dict(settings,model=dict(settings.get('model',{}),render_predictions=False)))
        detector = DETECTORS[detector_key]
        detector.recording_confidence = .01 if settings.get('record_confidence_points') else detector.confidence
        special_key = (signature,special_factory)
        special_cached = special_key in SPECIALS
        if not special_cached:
            template = measured('fixed_assets_loading',special_factory,settings)
            if hasattr(template,'fresh_run'):
                SPECIALS.clear()
                SPECIALS[special_key] = template
        else:
            template = SPECIALS[special_key]
        special = measured('fresh_run_state',template.fresh_run,settings) if hasattr(template,'fresh_run') else template
        for model in special.trained_text_ocr.values():
            model.job_workers = selected['ocr_job_workers']
            model.inference_threads = selected['ocr_inference_threads']
        if getattr(special,'mark_model',None):
            special.mark_model.allow_global_thread_limits = not selected['parallel']
        trials = []
        if signature not in PREPARED and (signature not in WARMED or settings.get('runtime_calibration',False)):
            image,requests = measured('reference_sample_preparation',samples,special)
            if image is None:
                paths = settings.get('special_frame_paths',[])
                image = cv2.imread(str(paths[0])) if paths else None
            if signature not in WARMED:
                if image is not None and detector.model is not None:
                    measured('appearance_warmup',detector.predict,image)
                for kind,rows in requests.items():
                    if rows:
                        measured('ocr_warmup.'+kind,special.trained_text_ocr[kind].candidates_batch,rows)
                WARMED.clear()
                WARMED.add(signature)
            def ocr_probe():
                for kind,rows in requests.items():
                    if rows:
                        special.trained_text_ocr[kind].candidates_batch(rows[:4])
            can_measure = (settings.get('runtime_calibration',False) and profile is None and image is not None and detector.model is not None
                           and psutil.cpu_percent(interval=.1)<80 and psutil.virtual_memory().percent<85)
            if can_measure:
                # Tune throughput using real models/crops, not synthetic CPU utilization.
                candidates = sorted({1,min(2,limits['cpu_budget']),min(4,limits['cpu_budget']),
                                     max(1,min(8,limits['cpu_budget']-4))})
                for threads in candidates:
                    torch.set_num_threads(threads)
                    for parallel in (False,True) if requests and limits['parallel'] and threads+4<=limits['cpu_budget'] else (False,):
                        durations = []
                        scores = []
                        appearance_times = []
                        def appearance_probe():
                            started = perf_counter()
                            for _ in range(4):
                                detector.predict(image)
                            return (perf_counter()-started)/4
                        with ThreadPoolExecutor(max_workers=2) as pool:
                            for _ in range(2):
                                tick = perf_counter()
                                if parallel:
                                    a,b = pool.submit(appearance_probe),pool.submit(ocr_probe)
                                    appearance_time = a.result()
                                    b.result()
                                else:
                                    appearance_time = appearance_probe()
                                    ocr_probe()
                                durations.append(perf_counter()-tick)
                                appearance_times.append(appearance_time)
                                # OCR is a short burst; appearance continues for the rest of a clip.
                                scores.append(durations[-1]+60*appearance_time)
                        trials.append(dict(cpu_threads=threads,parallel=parallel,seconds=sum(scores)/len(scores),
                            measured_probe_seconds=sum(durations)/len(durations),
                            appearance_frame_seconds=sum(appearance_times)/len(appearance_times),
                            weighted_appearance_frames=64))
                best = min(trials,key=lambda row:row['seconds'])
                selected.update(cpu_threads=best['cpu_threads'],parallel=best['parallel'])
                enable_parallel(selected,limits)
                profile = dict(signature=signature,version=VERSION,**{k:selected[k] for k in
                    ('cpu_threads','parallel','mark_workers','mark_spread')},trials=trials,
                    allocation_note='Measured CPU and Mark settings; appearance/special parallel lane enabled when resource budget allows, irrespective of serial trial winner.')
                path.parent.mkdir(parents=True,exist_ok=True)
                temporary = path.with_suffix('.tmp')
                temporary.write_text(json.dumps(profile,indent=2),encoding='utf-8')
                temporary.replace(path)
            if profile:
                PREPARED.add(signature)
        settings.update(cpu_threads=selected['cpu_threads'],mark_workers=selected['mark_workers'],mark_spread=selected['mark_spread'])
        check_parallelism(selected,limits,settings)
        settings['cpu_threads'] = selected['cpu_threads']
        torch.set_num_threads(selected['cpu_threads'])
        if getattr(special,'mark_model',None):
            if trials:
                record = special.references.get('OCR')
                if record:
                    from inspection.text_image_model import text_crop
                    source = cv2.imread(record['image'])
                    marks = [(text_crop(source,r),r) for r in record['regions'] if r['role']=='mark'][:8] if source is not None else []
                    if marks:
                        remaining = max(1,limits['cpu_budget']-(selected['cpu_threads'] if selected['parallel'] else 0))
                        best,mark_trials = tune_marks(special.mark_model,marks,selected['mark_workers'],remaining)
                        selected.update(mark_workers=best['workers'],mark_spread=best['spread'])
                        profile.update(mark_workers=best['workers'],mark_spread=best['spread'],mark_trials=mark_trials)
                        temporary = path.with_suffix('.tmp')
                        temporary.write_text(json.dumps(profile,indent=2),encoding='utf-8')
                        temporary.replace(path)
            special.mark_model.workers = selected['mark_workers']
            special.mark_model.spread = selected['mark_spread']
            special.mark_model.allow_global_thread_limits = not selected['parallel']
            model = special.mark_model
            stat = Path(model.path).stat()
            key = (model.path,stat.st_mtime_ns,stat.st_size,model.workers,model.spread)
            if key not in MARKS:
                for previous in MARKS.values():
                    previous.close()
                MARKS.clear()
                from inspection.text_image_model import text_crop
                record = special.references.get('OCR')
                if record:
                    source = cv2.imread(record['image'])
                    if source is not None:
                        measured('mark_warmup',model.predict_many,[(text_crop(source,r),r) for r in record['regions'] if r['role']=='mark'])
                MARKS[key] = model
            special.mark_model = MARKS[key]
            special.mark_model.allow_global_thread_limits = not selected['parallel']
            special.mark_model.resource_gate = settings.get('_resource_gate',special.mark_model.resource_gate)
            special.mark_model.parallel_batches = special.mark_model.serial_batches = 0
            special.cached_mark_runtime = True
        special.shape_registration = ShapeRegistration(tracking=settings.get('reference_tracking',True))
        for model in special.trained_text_ocr.values():
            model.backend_counts,model.runtime_seconds,model.backend_errors = {},{},[]
            model.job_workers = selected['ocr_job_workers']
            model.inference_threads = selected['ocr_inference_threads']
        settings['_automatic_runtime'] = dict(selected,preparation_seconds=perf_counter()-started,
            preparation_stages_seconds=preparation_stages,
            lock_wait_before_pipeline_seconds=settings.get('_runtime_lock_wait_seconds',0.),
            detector_reused=cached,fixed_data_reused=special_cached,
            media_policy='below-normal save thread; below-normal single-thread video encoding',
            profile_reused=bool(profile and not trials),profile_file=str(path),trials=trials,
            parallel_policy='enabled when CPU/RAM budget allows; serial profile does not disable it')
        return detector,special


def shutdown():
    for model in MARKS.values():
        model.close()
    MARKS.clear()
    SPECIALS.clear()
    annotation_images.cache_clear()


import atexit
atexit.register(shutdown)
