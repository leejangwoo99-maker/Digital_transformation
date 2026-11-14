"""Optional, validated FP32 OpenVINO backend for the existing CTC recognizer."""
import hashlib
import json
from collections import OrderedDict
from contextlib import nullcontext
from pathlib import Path
from threading import RLock
from time import perf_counter

import numpy as np
from inspection.adaptive_model_cache import AdaptiveModelCache

COMPILE_LOCK = RLock()
COMPILED_KEYS = set()
COMPILE_STATS = dict(compiles=0,recompiles=0,disk_cache_hits=0)
USAGE_PROFILES = OrderedDict()


def cache_snapshot():
    with COMPILE_LOCK:
        info = compiled_recognizer.cache_info()
        policy = compiled_recognizer.report() if hasattr(compiled_recognizer,'report') else {}
        return dict(COMPILE_STATS,cache_hits=info.hits,cache_misses=info.misses,
                    evictions=policy.get('evictions',0),pressure_evictions=policy.get('pressure_evictions',0),
                    capacity_changes=policy.get('capacity_changes',0),policy=policy,
                    entries=info.currsize,capacity=info.maxsize)


def cache_report(before):
    current = cache_snapshot()
    result = {name:current[name]-before[name] for name in
              ('cache_hits','cache_misses','compiles','recompiles','disk_cache_hits',
               'evictions','pressure_evictions','capacity_changes')}
    calls = result['cache_hits']+result['cache_misses']
    result.update(hit_rate=result['cache_hits']/calls if calls else None,
                  entries=current['entries'],capacity=current['capacity'],memory_policy=current['policy'])
    changes = (compiled_recognizer.events_since(before['policy'].get('event_sequence',0))
               if hasattr(compiled_recognizer,'events_since') else dict(events=[],truncated=False))
    baseline = before['policy'].get('sampled_at_monotonic',0.)
    result['events'] = [dict(event,seconds_since_run_cache_snapshot=event['monotonic_seconds']-baseline)
                        for event in changes['events']]
    result['events_truncated'] = changes['truncated']
    return result


def learn_cache_usage(telemetry, scope=None):
    usage = telemetry.model_profile()
    if usage:
        with COMPILE_LOCK:
            USAGE_PROFILES[scope] = usage
            USAGE_PROFILES.move_to_end(scope)
            if len(USAGE_PROFILES) > 32:
                USAGE_PROFILES.popitem(last=False)
    return [dict(model=Path(xml).name, width=width, threads=threads, calls=calls)
            for (xml, _, width, threads), calls in usage.most_common()]


def prepare_profiled_models(scope):
    started = perf_counter()
    with COMPILE_LOCK:
        cache = compiled_recognizer
        usage = USAGE_PROFILES.get(scope, {})
        candidates = {}
        for key, calls in sorted(usage.items(), key=lambda item: -item[1]):
            xml, stamp, _, _ = key
            try:
                if Path(xml).stat().st_mtime_ns == stamp:
                    candidates[key] = calls
            except OSError:
                continue
        budget = cache.preparation_budget()
        selected = dict(list(candidates.items())[:budget])
        cache.set_usage_profile(selected if budget else candidates)
        reused = loaded = errors = 0
        before = dict(COMPILE_STATS)
        for index, key in enumerate(selected):
            if cache.is_resident(key):
                reused += 1
                continue
            if index >= cache.preparation_budget():
                break
            try:
                cache(*key)
            except (OSError, RuntimeError, ValueError):
                errors += 1
                continue
            loaded += int(cache.is_resident(key))
        return dict(profiled=len(usage), valid=len(candidates), budget=budget,
                    selected=len(selected), reused=reused, loaded=loaded, errors=errors,
                    resident=sum(cache.is_resident(key) for key in selected),
                    compiles=COMPILE_STATS['compiles']-before['compiles'],
                    recompiles=COMPILE_STATS['recompiles']-before['recompiles'],
                    seconds=perf_counter()-started)


def identity(checkpoint):
    path = Path(checkpoint).resolve()
    stat = path.stat()
    return {'source':str(path),'size':stat.st_size,'modified_ns':stat.st_mtime_ns}


def numeric_checkpoint(reader):
    directory = getattr(reader,'model_storage_directory',None)
    if directory is None:
        return None
    path = Path(directory)/'latin_g2.pth'
    return path if path.is_file() else None


def default_numeric_checkpoint():
    from easyocr.config import MODULE_PATH
    path = Path(MODULE_PATH)/'model'/'latin_g2.pth'
    return path if path.is_file() else None


def decoder(checkpoint):
    """Decode IR logits without loading a duplicate PyTorch model."""
    target = artifact(checkpoint)
    metadata = target.with_suffix('.json')
    if not target.is_file() or not metadata.is_file():
        return None
    import openvino as ov
    data = json.loads(metadata.read_text(encoding='utf-8'))
    if (not data.get('validated') or data.get('openvino') != ov.__version__
            or not data.get('characters') or any(data.get(k) != v for k,v in identity(checkpoint).items())):
        return None
    from easyocr.utils import CTCLabelConverter
    return CTCLabelConverter(''.join(data['characters'][1:]))


def artifact(checkpoint):
    source = identity(checkpoint)
    digest = hashlib.sha256(json.dumps(source,sort_keys=True).encode()).hexdigest()[:16]
    return Path(checkpoint).parent/'openvino_ocr'/f'{Path(checkpoint).stem}_{digest}.xml'


def export_recognizer(reader, checkpoint):
    import openvino as ov
    import torch

    class Recognizer(torch.nn.Module):
        def __init__(self, model):
            super().__init__()
            self.model = model

        def forward(self, image):
            return self.model(image,torch.zeros((image.shape[0],1),dtype=torch.long))

    target = artifact(checkpoint)
    metadata = target.with_suffix('.json')
    wrapper = Recognizer(reader.recognizer).eval()
    sample = torch.zeros((1,1,64,128))
    model = ov.convert_model(wrapper,example_input=sample,input=[ov.PartialShape([-1,1,64,-1])])
    compiled = ov.Core().compile_model(model,'CPU',{
        'PERFORMANCE_HINT':'LATENCY','INFERENCE_PRECISION_HINT':'f32',
        'NUM_STREAMS':1,'INFERENCE_NUM_THREADS':4})
    errors = []
    generator = torch.Generator().manual_seed(2718)
    for shape in ((1,1,64,128),(2,1,64,256),(1,1,64,512)):
        tensor = torch.rand(shape,generator=generator)*2-1
        with torch.inference_mode():
            expected = wrapper(tensor).numpy()
        actual = compiled([tensor.numpy()])[0]
        np.testing.assert_allclose(actual,expected,rtol=2e-3,atol=2e-3)
        if not np.array_equal(actual.argmax(2),expected.argmax(2)):
            raise ValueError('OpenVINO CTC argmax differs during conversion validation.')
        errors.append(float(np.max(np.abs(actual-expected))))
    target.parent.mkdir(parents=True,exist_ok=True)
    ov.save_model(model,target,compress_to_fp16=False)
    metadata.write_text(json.dumps(dict(identity(checkpoint),openvino=ov.__version__,
        precision='f32',validated=True,max_abs_errors=errors,
        characters=list(reader.converter.character) if hasattr(reader,'converter') else None),indent=2),encoding='utf-8')
    return target


@AdaptiveModelCache
def compiled_recognizer(xml, modified_ns, width, threads=4):
    import openvino as ov
    core = ov.Core()
    core.set_property({'CACHE_DIR':str(Path(xml).parent/'cpu_cache')})
    model = core.read_model(xml)
    model.reshape(ov.PartialShape([-1,1,64,width]))
    compiled = core.compile_model(model,'CPU',{
        'PERFORMANCE_HINT':'LATENCY','INFERENCE_PRECISION_HINT':'f32',
        'NUM_STREAMS':1,'INFERENCE_NUM_THREADS':threads})
    key = (xml,modified_ns,width,threads)
    COMPILE_STATS['compiles'] += 1
    COMPILE_STATS['recompiles'] += int(key in COMPILED_KEYS)
    COMPILED_KEYS.add(key)
    try:
        COMPILE_STATS['disk_cache_hits'] += int(bool(compiled.get_property('LOADED_FROM_CACHE')))
    except (RuntimeError,AttributeError):
        pass
    return compiled


def lookup_recognizer(xml, modified_ns, width, timings, threads=4):
    # Serialize cache misses so simultaneous readers do not compile the same key twice.
    with COMPILE_LOCK:
        before = compiled_recognizer.cache_info()
        counts = dict(COMPILE_STATS)
        compiled = compiled_recognizer(xml,modified_ns,width,threads)
        after = compiled_recognizer.cache_info()
        if timings is not None:
            for name,value in dict(cache_hits=after.hits-before.hits,
                                   cache_misses=after.misses-before.misses,
                                   **{key:COMPILE_STATS[key]-counts[key] for key in counts}).items():
                field = 'openvino_'+name
                timings[field] = timings.get(field,0)+value
        return compiled


def infer(checkpoint, tensors, timings=None, threads=4, telemetry=None, kind='unknown'):
    """Return None when no validated artifact exists; never convert inside Live."""
    target = artifact(checkpoint)
    metadata = target.with_suffix('.json')
    if not target.is_file() or not metadata.is_file():
        return None
    import openvino as ov
    data = json.loads(metadata.read_text(encoding='utf-8'))
    if (not data.get('validated') or data.get('openvino') != ov.__version__
            or any(data.get(k) != v for k,v in identity(checkpoint).items())):
        return None
    from time import perf_counter
    started = perf_counter()
    key = (str(target), target.stat().st_mtime_ns, int(tensors.shape[-1]), threads)
    compiled = lookup_recognizer(*key[:3], timings, threads=threads)
    ready = perf_counter()
    if telemetry is not None:
        telemetry.record_model_use(key)
        try:
            effective_threads = int(compiled.get_property('INFERENCE_NUM_THREADS'))
        except (RuntimeError,AttributeError,TypeError,ValueError):
            effective_threads = None
        telemetry.configuration(kind,threads,effective_threads)
    request_started = perf_counter()
    request = compiled.create_infer_request()
    request_ready = perf_counter()
    inputs = [np.ascontiguousarray(tensors.detach().cpu().numpy())]
    native_started = perf_counter()
    with telemetry.inference(kind) if telemetry is not None else nullcontext():
        request.infer(inputs)
    native_finished = perf_counter()
    result = request.get_output_tensor(0).data.copy()
    if timings is not None:
        timings['openvino_compile_lookup'] = timings.get('openvino_compile_lookup',0.)+ready-started
        timings['openvino_inference'] = timings.get('openvino_inference',0.)+perf_counter()-ready
        timings['openvino_request_creation'] = timings.get('openvino_request_creation',0.)+request_ready-request_started
        timings['openvino_native_inference'] = timings.get('openvino_native_inference',0.)+native_finished-native_started
    return result
