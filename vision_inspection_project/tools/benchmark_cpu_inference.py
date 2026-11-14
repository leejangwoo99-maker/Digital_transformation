"""Compare CPU thread budgets without changing image size or model precision."""
import json
import subprocess
import sys
from pathlib import Path
from time import perf_counter

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))


def worker(threads):
    import cv2
    import torch
    torch.set_num_threads(threads)
    cv2.setNumThreads(1)
    from inspection.special_checks import labeled_records
    from model.yolo_inference import YoloInference
    record = next(data for _,data in labeled_records(ROOT,'full_1-4_merged_23fps','OCR'))
    image = cv2.imread(record['image'])
    model = YoloInference(dict(root_dir=str(ROOT),model=dict(
        weights='weights/full_1-4_merged_23fps_algorith.pt',image_size=640,
        confidence=.3,iou=.45,render_predictions=False)))
    if model.model is None:
        raise RuntimeError(model.model_error)
    model.predict(image)
    samples = []
    counts = []
    for _ in range(8):
        start = perf_counter()
        detections,_ = model.predict(image)
        samples.append(perf_counter()-start)
        counts.append(len(detections))
    return dict(threads=threads,mean_seconds=sum(samples)/len(samples),samples=samples,detection_counts=counts)


if __name__ == '__main__':
    if len(sys.argv)>1:
        print(json.dumps(worker(int(sys.argv[1]))))
    else:
        results = []
        for count in (1,2,4,6,10):
            run = subprocess.run([sys.executable,str(Path(__file__).resolve()),str(count)],capture_output=True,text=True,check=True)
            value = json.loads(run.stdout.splitlines()[-1])
            results.append(value)
            print(value['threads'],value['mean_seconds'],flush=True)
        path = Path(r'C:\vision_inspection\testlog\cpu_inference_benchmark.json')
        path.write_text(json.dumps(dict(note='Single training frame, warm CPU inference, eight iterations; no live throughput guarantee.',results=results),indent=2))
        print(path,flush=True)
