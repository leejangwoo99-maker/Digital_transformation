"""Export and reuse an appearance model without replacing its source checkpoint."""
import hashlib
import shutil
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Lock


_EXPORT_LOCK = Lock()


def openvino_artifact(weights, image_size):
    from importlib.metadata import version
    from ultralytics import YOLO

    weights = Path(weights).resolve()
    stat = weights.stat()
    fingerprint = (str(weights),stat.st_size,stat.st_mtime_ns,image_size,
                   version('ultralytics'),version('openvino'),'dynamic-fp32-v1')
    digest = hashlib.sha256(repr(fingerprint).encode()).hexdigest()[:16]
    root = weights.parent/'openvino_appearance'
    target = root/f'{weights.stem}_{digest}_openvino_model'
    with _EXPORT_LOCK:
        if not target.is_dir():
            root.mkdir(parents=True,exist_ok=True)
            with TemporaryDirectory(prefix='.export_',dir=root) as temporary:
                checkpoint = Path(temporary)/weights.name
                shutil.copy2(weights,checkpoint)
                exported = Path(YOLO(str(checkpoint)).export(format='openvino',imgsz=image_size,
                                dynamic=True,half=False,int8=False,nms=False,batch=1,device='cpu'))
                exported.rename(target)
    return target
