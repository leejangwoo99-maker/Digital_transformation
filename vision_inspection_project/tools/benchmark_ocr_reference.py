"""Compare actual video crops and reference fits, without operating camera/Bot."""
import argparse
import json
import sys
from pathlib import Path
from time import perf_counter

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))


def main():
    import cv2
    import numpy as np
    import torch
    from inspection.special_checks import labeled_frame_schedule
    from inspection.shape_registration import ShapeRegistration
    from inspection.supervised_text_ocr import TrainedTextOCR
    from inspection.text_image_model import text_crop,model_path
    from inspection.region_masks import mask_regions
    from inspection.test_sampling import TestSampling

    parser = argparse.ArgumentParser()
    parser.add_argument('video',type=Path)
    parser.add_argument('--group',default='Proto_23fps')
    parser.add_argument('--output',type=Path,required=True)
    args = parser.parse_args()
    torch.set_num_threads(4)
    cv2.setNumThreads(1)
    images = sorted((ROOT/'dataset/images/train').glob(f'{args.group}_*.jpg'))
    schedules = {kind:labeled_frame_schedule(ROOT,args.group,kind,images) for kind in ('OCR','label')}
    sampling = TestSampling(23,10)
    cap = cv2.VideoCapture(str(args.video))
    frames = {}
    source_fps = cap.get(cv2.CAP_PROP_FPS)
    try:
        for n in range(5,14):
            cap.set(cv2.CAP_PROP_POS_FRAMES,int(np.ceil((n-1)*source_fps/10-1e-9)))
            ok,image = cap.read()
            if not ok:
                raise ValueError(f'Cannot decode test frame {n}')
            frames[n] = image
    finally:
        cap.release()
    runs = {}
    crops = {}
    matrices = {}
    for pass_index,tracking in enumerate((False,False,True)):
        matcher = ShapeRegistration(tracking=tracking)
        results = []
        for n,image in frames.items():
            for kind,schedule in schedules.items():
                original = sampling.annotation_number(n)
                if original not in schedule:
                    continue
                reference = schedule[original]
                area = next(r for r in reference['regions'] if r['role'] == 'registration')
                start = perf_counter()
                result = matcher.fit(image,reference['image'],area)
                seconds = perf_counter()-start
                row = dict(frame=n,kind=kind,seconds=seconds,found=result is not None)
                if result is not None:
                    matrix,shape = result
                    if not tracking:
                        matrices[n,kind] = matrix
                    elif (n,kind) in matrices:
                        corners = np.float32([[[0,0],[shape[1],0],[shape[1],shape[0]],[0,shape[0]]]])
                        delta = cv2.perspectiveTransform(corners,matrix)-cv2.perspectiveTransform(corners,matrices[n,kind])
                        row['max_corner_difference_px'] = float(np.linalg.norm(delta,axis=2).max())
                    if n == 5 and not tracking:
                        aligned = cv2.warpPerspective(image,np.linalg.inv(matrix),(shape[1],shape[0]))
                        aligned = mask_regions(aligned,[r for r in reference['regions'] if r['role'] == 'exclude'])
                        crops[kind] = [dict(r,image=text_crop(aligned,r),line_count=len(r.get('text','').split('\n')))
                                       for r in reference['regions'] if r['role'] == 'text']
                results.append(row)
        runs['reference_cache_cold' if pass_index == 0 else 'tracking' if tracking else 'full_sift'] = dict(
            seconds=sum(r['seconds'] for r in results),stats=matcher.stats,frames=results)
    recognition = {}
    for kind,requests in crops.items():
        model = TrainedTextOCR(model_path(ROOT,args.group,kind).with_suffix('.ocr.json'))
        measured = {}
        baseline = None
        for backend in ('pytorch','openvino'):
            model.force_torch = backend == 'pytorch'
            start = perf_counter()
            values = model.candidates_batch(requests)
            cold = perf_counter()-start
            timings = []
            for _ in range(3):
                start = perf_counter()
                values = model.candidates_batch(requests)
                timings.append(perf_counter()-start)
            if baseline is None:
                baseline = values
            measured[backend] = dict(first_seconds=cold,warm_seconds=timings,
                mean_seconds=sum(timings)/len(timings),same_candidates=values == baseline,
                candidates=values)
        recognition[kind] = dict(requests=len(requests),backends=model.backend_counts,measurements=measured)
    report = dict(video=str(args.video),group=args.group,reference=runs,recognition=recognition,
        note='Component benchmark on actual test video; not an end-to-end Live latency prediction.')
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
    print(args.output,flush=True)
    print(json.dumps(dict(reference={k:v['seconds'] for k,v in runs.items()},recognition={
        k:{b:m['mean_seconds'] for b,m in r['measurements'].items()} for k,r in recognition.items()})),flush=True)


if __name__ == '__main__':
    main()
