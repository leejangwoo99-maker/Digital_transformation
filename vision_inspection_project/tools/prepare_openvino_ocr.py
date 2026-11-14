"""Convert existing trained CTC checkpoints without modifying training weights."""
import argparse
import sys
from pathlib import Path

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))


def main():
    import torch
    from inspection.supervised_text_ocr import TrainedTextOCR,numeric_reader
    from inspection.openvino_ocr import export_recognizer,numeric_checkpoint
    parser = argparse.ArgumentParser()
    parser.add_argument('manifests',nargs='+',type=Path)
    args = parser.parse_args()
    torch.set_num_threads(4)
    for path in args.manifests:
        model = TrainedTextOCR(path)
        for index,item in enumerate(model.manifest):
            if item.get('file'):
                print(export_recognizer(model.reader(index),path.parent/item['file']),flush=True)
    reader = numeric_reader()
    checkpoint = numeric_checkpoint(reader)
    if checkpoint:
        print(export_recognizer(reader,checkpoint),flush=True)


if __name__ == '__main__':
    main()
