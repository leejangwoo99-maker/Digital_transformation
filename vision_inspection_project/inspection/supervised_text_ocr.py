"""Fine-tune EasyOCR's CTC character prediction head on entered transcripts."""
import json
from pathlib import Path
from functools import lru_cache
from threading import RLock

import cv2
import numpy as np

TORCH_OCR_LOCK = RLock()


def run_recognition_jobs(jobs, workers, recognize):
    ordered = list(jobs.items())
    # Width times crop count estimates the work without changing batch inputs.
    priority = sorted(range(len(ordered)),
                      key=lambda i: ordered[i][0][1]*len(ordered[i][1]),reverse=True)
    results = [None]*len(ordered)
    if workers > 1:
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=workers,thread_name_prefix='ocr-batch') as pool:
            futures = {i:pool.submit(recognize,*ordered[i]) for i in priority}
            for i in range(len(ordered)):
                results[i] = futures[i].result()
    else:
        for i in priority:
            results[i] = recognize(*ordered[i])
    return results


@lru_cache(maxsize=4)
def cached_reader(path, modified_ns, size, languages):
    import easyocr
    import torch
    reader = easyocr.Reader(list(languages),gpu=False,detector=False,quantize=False,verbose=False)
    reader.recognizer.load_state_dict(torch.load(path,map_location="cpu",weights_only=True))
    reader.recognizer.eval()
    return reader


@lru_cache(maxsize=1)
def numeric_reader():
    import easyocr
    reader = easyocr.Reader(["en","fr"],gpu=False,detector=False,quantize=False,verbose=False)
    reader.recognizer.eval()
    return reader


def oriented(crop, rotation):
    return np.ascontiguousarray(np.rot90(crop, rotation // 90)) if rotation else crop


def recognition_tensor(crop):
    from PIL import Image
    from easyocr.recognition import AlignCollate
    gray = cv2.cvtColor(crop,cv2.COLOR_BGR2GRAY) if crop.ndim == 3 else crop
    edge = np.concatenate((gray[0],gray[-1],gray[:,0],gray[:,-1]))
    pad = max(2,round(gray.shape[0]*.12))
    gray = cv2.copyMakeBorder(gray,pad,pad,pad,pad,cv2.BORDER_CONSTANT,value=float(np.median(edge)))
    width = min(2048,max(128,int(np.ceil(gray.shape[1]*64/gray.shape[0]/64))*64))
    return AlignCollate(imgH=64,imgW=width)([Image.fromarray(gray)])


def contextual_features(model, tensor):
    features = model.FeatureExtraction(tensor)
    features = model.AdaptiveAvgPool(features.permute(0,3,1,2)).squeeze(3)
    return model.SequenceModeling(features).contiguous()


def line_images(crop, text):
    lines = text.split("\n")
    height = crop.shape[0]
    return [(crop[round(i*height/len(lines)):round((i+1)*height/len(lines))], line)
            for i,line in enumerate(lines) if line]


def training_transcript(sample):
    return sample.get("refer", "") if sample.get("check") else sample.get("text", "")


def train_recognizers(samples, output):
    import time
    import easyocr
    import torch
    from PIL import Image
    from easyocr.recognition import AlignCollate
    from inspection.text_image_model import text_crop
    torch.set_num_threads(4)
    manifest = []
    generation = time.time_ns()
    remaining = {i for i,sample in enumerate(samples) if training_transcript(sample)}
    supported_alphabets = []
    dynamic_count = sum(bool(sample.get("check")) for sample in samples)
    for language in (["en", "fr"], ["ch_sim", "en"], ["ch_tra", "en"]):
        if not remaining:
            break
        reader = easyocr.Reader(language, gpu=False, detector=False, quantize=False, verbose=False)
        alphabet = set(reader.converter.character[1:])
        supported_alphabets.append(alphabet)
        selected = [i for i in sorted(remaining) if set(training_transcript(samples[i]).replace("\n", "")) <= alphabet]
        if not selected:
            continue
        model = reader.recognizer
        for parameter in model.parameters():
            parameter.requires_grad_(False)
        for parameter in model.Prediction.parameters():
            parameter.requires_grad_(True)
        optimizer = torch.optim.Adam(model.Prediction.parameters(), lr=1e-4)
        criterion = torch.nn.CTCLoss(blank=0, zero_infinity=True)
        prepared = []
        routes = []
        crop_directory = output.parent/"text_training_crops"/output.stem/str(generation)
        crop_directory.mkdir(parents=True,exist_ok=True)
        model.eval()
        previous_path,previous_image = None,None
        for i in selected:
            sample = samples[i]
            transcript_text = training_transcript(sample)
            if sample["image"] != previous_path:
                previous_path = sample["image"]
                previous_image = cv2.imread(previous_path)
            image = previous_image
            if image is None:
                raise ValueError(f"Cannot read OCR sample: {sample['image']}")
            from inspection.region_masks import mask_regions
            crop = text_crop(mask_regions(image,sample.get("masks",[])), sample)
            if min(crop.shape[:2]) < 3 or float(crop.std()) < 1:
                raise ValueError(f"Empty/too-small OCR crop: {sample['image']} {transcript_text!r}")
            # Supervised orientation selection uses the transcript during TRAIN only.
            losses = []
            for rotation in (0,90,180,270):
                with torch.no_grad():
                    total_loss = 0.
                    for line,transcript in line_images(oriented(crop,rotation),transcript_text):
                        tensor = recognition_tensor(line)
                        logits = model(tensor,torch.zeros((1,1),dtype=torch.long))
                        targets,lengths = reader.converter.encode([transcript])
                        required = len(transcript)+sum(a==b for a,b in zip(transcript,transcript[1:]))
                        if logits.shape[1] < required:
                            total_loss = float("inf")
                            break
                        total_loss += float(criterion(logits.log_softmax(2).permute(1,0,2),targets,
                            torch.tensor([logits.shape[1]],dtype=torch.int32),lengths))
                    losses.append((total_loss,rotation))
            orientation_loss,rotation = min(losses)
            if not np.isfinite(orientation_loss):
                raise ValueError(f"OCR crop cannot contain this transcript: {transcript_text!r}")
            routes.append({"number":sample.get("number"),"box":sample["box"],"rotation":rotation,
                           "check":bool(sample.get("check")),
                           "orientation_loss":orientation_loss})
            normalized = oriented(crop,rotation)
            if not cv2.imwrite(str(crop_directory/f"sample_{i:05d}_{rotation}deg.png"),normalized):
                raise ValueError("Could not save OCR training crop")
            for line, transcript in line_images(normalized, transcript_text):
                if not line.size:
                    continue
                targets, lengths = reader.converter.encode([transcript])
                # The recognizer body is frozen: cache its features once, not every epoch.
                for variant in (line,cv2.convertScaleAbs(line,alpha=.9,beta=8),cv2.GaussianBlur(line,(3,3),.5)):
                    with torch.no_grad():
                        features = contextual_features(model,recognition_tensor(variant))
                    prepared.append((features, targets, lengths))
        if not prepared:
            continue
        model.eval()  # Freeze feature statistics on small industrial datasets.
        for epoch in range(5):
            loss_total = 0.
            for features, targets, lengths in prepared:
                optimizer.zero_grad()
                logits = model.Prediction(features)
                loss = criterion(logits.log_softmax(2).permute(1,0,2), targets,
                                 torch.tensor([logits.shape[1]], dtype=torch.int32), lengths)
                if not torch.isfinite(loss):
                    raise ValueError("Nonfinite OCR training loss")
                loss.backward()
                torch.nn.utils.clip_grad_norm_(model.Prediction.parameters(), 5)
                optimizer.step()
                loss_total += float(loss.detach())
            print(f"Text CTC {language}: epoch {epoch+1}/5, loss {loss_total/len(prepared):.4f}")
        checkpoint = output.with_name(output.stem + f"_{generation}_" + language[0] + ".pt")
        temporary = checkpoint.with_suffix(".tmp")
        torch.save(model.state_dict(), temporary)
        temporary.replace(checkpoint)
        try:
            from inspection.openvino_ocr import export_recognizer
            print(f'OpenVINO OCR FP32: {export_recognizer(reader,checkpoint)}')
        except (ImportError,RuntimeError,ValueError,OSError,AssertionError) as exc:
            print(f'OpenVINO preparation unavailable; trained PyTorch model retained: {exc}')
        manifest.append({"languages":language, "file":checkpoint.name,
                         "training_lines":len(prepared), "epochs":5,"preprocessing_version":2,
                         "routes":routes,"training_crops":str(crop_directory)})
        remaining.difference_update(selected)
    if remaining:
        import unicodedata
        supported = set().union(*supported_alphabets)
        details = []
        for index in sorted(remaining):
            sample = samples[index]
            transcript = training_transcript(sample)
            unsupported = sorted(set(transcript.replace("\n", "")) - supported)
            characters = ", ".join(
                f"{character!r} U+{ord(character):04X} ({unicodedata.name(character, 'UNKNOWN')})"
                for character in unsupported)
            reason = characters or "Mixed scripts: no single OCR alphabet supports this entire field"
            details.append(f"{sample.get('image', '')} / field {sample.get('number', index+1)}: "
                           f"{transcript!r}\n{reason}")
        raise ValueError("OCR training alphabet mismatch. Latin accented letters such as e-acute "
                         "are already supported by the French/Latin model.\n\n" + "\n\n".join(details))
    if not manifest and dynamic_count:
        manifest.append({"languages":["en","fr"],"pretrained":True,"preprocessing_version":2})
    if dynamic_count:
        print(f"Dynamic check fields: {dynamic_count}; refer used for TRAIN only; TEST compares actual OCR + format + QR.")
    manifest_path = output.with_suffix(".ocr.json")
    temporary_manifest = manifest_path.with_suffix(".tmp")
    temporary_manifest.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    temporary_manifest.replace(manifest_path)
    return manifest_path


class TrainedTextOCR:
    def __init__(self, manifest_path):
        self.path = Path(manifest_path)
        self.manifest = json.loads(self.path.read_text(encoding="utf-8"))
        self.readers = None
        self.backend_counts = {}
        self.backend_errors = []

    def candidates(self, crop, line_count=1):
        return self.candidates_batch([{"image":crop,"line_count":line_count}])[0]

    def reader(self, index):
        import easyocr
        import torch
        if index == -1 or self.manifest[index].get("pretrained"):
            return numeric_reader()
        if self.readers is None:
            self.readers = {}
        if index not in self.readers:
            item = self.manifest[index]
            path = (self.path.parent/item["file"]).resolve()
            stat = path.stat()
            self.readers[index] = cached_reader(str(path),stat.st_mtime_ns,stat.st_size,tuple(item["languages"]))
        return self.readers[index]

    def routes_for(self, request):
        numeric = request.get("check") and request.get("pattern") and all(value == "num" for value in request["pattern"])
        options = []
        if "box" in request:
            center = np.asarray(request["box"]).reshape(2,2).mean(axis=0)
            for index,item in enumerate(self.manifest):
                for route in item.get("routes",[]):
                    if request.get("check") and not route.get("check"):
                        continue
                    if route["number"] == request.get("number"):
                        distance = np.linalg.norm(center-np.asarray(route["box"]).reshape(2,2).mean(axis=0))
                        if distance < .08:
                            options.append((float(distance),index,route["rotation"]))
        if options:
            _,index,rotation = min(options)
            return [(index,[rotation]),(-1,[rotation])] if numeric else [(index,[rotation])]
        if numeric:
            return [(-1,[0,90,180,270])]
        # Script routing chooses an engine, never inserts expected characters into output.
        text = request.get("text", "")
        has_chinese = any("\u3400" <= c <= "\u9fff" for c in text)
        choices = [i for i,item in enumerate(self.manifest) if
                   (any(lang.startswith("ch_") for lang in item["languages"]) if has_chinese else
                    not any(lang.startswith("ch_") for lang in item["languages"]))]
        if not choices:
            choices = list(range(len(self.manifest)))
        crop = request["image"]
        rotations = [90,270] if crop.shape[0] > crop.shape[1]*1.5 else [0]
        return [(i,rotations) for i in choices]

    def candidates_batch(self, requests):
        import itertools
        from time import perf_counter
        import torch
        from PIL import Image
        from easyocr.recognition import AlignCollate
        jobs = {}
        if not hasattr(self,'runtime_seconds'):
            self.runtime_seconds = {}
        started = perf_counter()
        output = [[[] for _ in range(request.get("line_count",1))] for request in requests]
        for number,request in enumerate(requests):
            crop = request["image"]
            if not crop.size:
                continue
            for reader_index,rotations in self.routes_for(request):
                for rotation in rotations:
                    image = oriented(crop,rotation)
                    count = request.get("line_count",1)
                    for line_index in range(count):
                        line = image[round(line_index*image.shape[0]/count):round((line_index+1)*image.shape[0]/count)]
                        if not line.size:
                            continue
                        if reader_index == -1 or self.manifest[reader_index].get("preprocessing_version",1) >= 2:
                            tensor = recognition_tensor(line)
                        else:
                            width = min(2048,max(128,round(line.shape[1]*64/line.shape[0])))
                            tensor = AlignCollate(imgH=64,imgW=width)([Image.fromarray(cv2.cvtColor(line,cv2.COLOR_BGR2GRAY))])
                        numeric = bool(request.get("check") and request.get("pattern") and all(v == "num" for v in request["pattern"]))
                        jobs.setdefault((reader_index,tensor.shape[-1],numeric),[]).append((number,line_index,tensor))
        self.runtime_seconds['preprocessing'] = self.runtime_seconds.get('preprocessing',0.)+perf_counter()-started
        workers = min(max(1,int(getattr(self,'job_workers',1))),len(jobs))
        telemetry = getattr(self,'telemetry',None)
        if telemetry is not None:
            telemetry.dispatch(kind=getattr(self,'telemetry_kind','unknown'),fields=len(requests),
                job_groups=len(jobs),configured_workers=getattr(self,'job_workers',1),dispatch_workers=workers,
                inference_threads=getattr(self,'inference_threads',4),
                scheduling='largest_estimated_job_first',
                start_seconds=perf_counter()-telemetry.started)
        results = run_recognition_jobs(jobs,workers,self._recognize_job)
        self.backend_counts = getattr(self,'backend_counts',{})
        self.backend_errors = getattr(self,'backend_errors',[])
        for assignments,times,counts,errors in results:
            for name,value in times.items():
                self.runtime_seconds[name] = self.runtime_seconds.get(name,0.)+value
            for name,value in counts.items():
                self.backend_counts[name] = self.backend_counts.get(name,0)+value
            for message in errors:
                if message not in self.backend_errors:
                    self.backend_errors.append(message)
                    print(message)
            for number,line_index,value in assignments:
                if value not in output[number][line_index]:
                    output[number][line_index].append(value)
        return [["\n".join(values) for values in itertools.islice(itertools.product(*lines),32)]
                if all(lines) else [] for lines in output]

    def _recognize_job(self, key, items):
        import torch
        from time import perf_counter
        from inspection.openvino_ocr import default_numeric_checkpoint,decoder,infer
        from types import SimpleNamespace
        reader_index,_,numeric = key
        started = perf_counter()
        times,counts,errors,assignments = {},{},[],[]
        item = self.manifest[reader_index] if reader_index != -1 else {}
        checkpoint = (self.path.parent/item['file'] if item.get('file') else
                      default_numeric_checkpoint() if reader_index == -1 or item.get('pretrained') else None) if hasattr(self,'path') else None
        converter = None
        if checkpoint is not None and not getattr(self,'force_torch',False):
            try:
                converter = decoder(checkpoint)
            except (ImportError,RuntimeError,ValueError,OSError):
                pass
        if converter is None:
            with TORCH_OCR_LOCK:
                reader = self.reader(reader_index)
        else:
            reader = SimpleNamespace(converter=converter)
        times['reader_loading'] = perf_counter()-started
        with torch.inference_mode():
            for start in range(0,len(items),8):
                batch_started = perf_counter()
                before = dict(times)
                batch = items[start:start+8]
                tensors = torch.cat([job[2] for job in batch])
                logits = None
                if checkpoint is not None and not getattr(self,'force_torch',False):
                    try:
                        values = infer(checkpoint,tensors,times,threads=getattr(self,'inference_threads',4),
                                       telemetry=getattr(self,'telemetry',None),kind=getattr(self,'telemetry_kind','unknown'))
                        if values is not None:
                            logits = torch.from_numpy(values)
                    except (ImportError,RuntimeError,ValueError,OSError) as exc:
                        errors.append(f'OpenVINO fallback to PyTorch: {exc}')
                backend = 'openvino' if logits is not None else 'pytorch'
                counts[backend] = counts.get(backend,0)+1
                if logits is None:
                    started = perf_counter()
                    with TORCH_OCR_LOCK:
                        if not hasattr(reader,'recognizer'):
                            reader = self.reader(reader_index)
                        logits = reader.recognizer(tensors,torch.zeros((len(batch),1),dtype=torch.long))
                    times['pytorch_inference'] = times.get('pytorch_inference',0.)+perf_counter()-started
                if numeric:
                    keep = [0]+[i for i,char in enumerate(reader.converter.character) if char in '0123456789']
                    allowed = torch.zeros(logits.shape[-1],dtype=torch.bool)
                    allowed[keep] = True
                    logits = logits.masked_fill(~allowed,float('-inf'))
                values = reader.converter.decode_greedy(logits.argmax(2).reshape(-1).numpy(),
                    torch.IntTensor([logits.shape[1]]*len(batch)))
                assignments.extend((number,line_index,value) for (number,line_index,_),value in zip(batch,values))
                telemetry = getattr(self,'telemetry',None)
                if telemetry is not None:
                    telemetry.batch(kind=getattr(self,'telemetry_kind','unknown'),reader_index=reader_index,
                        model=checkpoint.name if checkpoint is not None else None,width=int(tensors.shape[-1]),
                        batch_size=len(batch),numeric=bool(numeric),backend=backend,
                        inference_threads=getattr(self,'inference_threads',4),
                        start_seconds=batch_started-telemetry.started,wall_seconds=perf_counter()-batch_started,
                        compile_lookup_seconds=times.get('openvino_compile_lookup',0.)-before.get('openvino_compile_lookup',0.),
                        inference_seconds=times.get('openvino_inference',0.)-before.get('openvino_inference',0.),
                        request_creation_seconds=times.get('openvino_request_creation',0.)-before.get('openvino_request_creation',0.),
                        native_inference_seconds=times.get('openvino_native_inference',0.)-before.get('openvino_native_inference',0.),
                        cache_hits=int(times.get('openvino_cache_hits',0)-before.get('openvino_cache_hits',0)),
                        cache_misses=int(times.get('openvino_cache_misses',0)-before.get('openvino_cache_misses',0)))
        return assignments,times,counts,errors
