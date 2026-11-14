from __future__ import annotations

import argparse
import shutil
from pathlib import Path
from typing import Any

import yaml


ROOT_DIR = Path(__file__).resolve().parent
CONFIG_PATH = ROOT_DIR / "config" / "settings.yaml"
DATASET_YAML_PATH = ROOT_DIR / "dataset" / "data.yaml"


def load_settings(path: Path = CONFIG_PATH) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as file:
        settings = yaml.safe_load(file) or {}

    settings["root_dir"] = str(ROOT_DIR)
    return settings


def ensure_dataset_structure() -> None:
    for split in ("train", "val", "test"):
        (ROOT_DIR / "dataset" / "images" / split).mkdir(parents=True, exist_ok=True)
        (ROOT_DIR / "dataset" / "labels" / split).mkdir(parents=True, exist_ok=True)


def validate_dataset(data_yaml: Path) -> None:
    with data_yaml.open("r", encoding="utf-8") as file:
        data = yaml.safe_load(file) or {}

    dataset_path = Path(data.get("path", data_yaml.parent))
    if not dataset_path.is_absolute():
        dataset_path = (data_yaml.parent / dataset_path).resolve()

    missing_paths = []
    for split in ("train", "val"):
        image_dir = dataset_path / data.get(split, f"images/{split}")
        if not image_dir.exists():
            missing_paths.append(str(image_dir))

    if missing_paths:
        joined = "\n".join(missing_paths)
        raise FileNotFoundError(f"Dataset folders do not exist:\n{joined}")

    train_label_dir = dataset_path / "labels" / "train"
    val_label_dir = dataset_path / "labels" / "val"
    label_files = list(train_label_dir.glob("*.txt")) + list(val_label_dir.glob("*.txt"))
    object_count = 0
    for label_file in label_files:
        object_count += sum(1 for line in label_file.read_text(encoding="utf-8").splitlines() if line.strip())

    if object_count == 0:
        raise RuntimeError(
            "No YOLO labels found in dataset/labels/train or dataset/labels/val.\n"
            "Open revision/open labeler, draw boxes, then save labels before training."
        )


def copy_best_weight(run_dir: Path, target_path: Path) -> None:
    trained_best = run_dir / "weights" / "best.pt"
    if not trained_best.exists():
        raise RuntimeError(f"Training did not produce best.pt; existing model preserved: {trained_best}")

    target_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(trained_best, target_path)
    print(f"Copied trained weight to: {target_path}")


def train(args: argparse.Namespace) -> None:
    ensure_dataset_structure()

    settings = load_settings()
    train_settings = settings.get("train", {})
    model_settings = settings.get("model", {})

    data_yaml = Path(args.data or train_settings.get("data", DATASET_YAML_PATH))
    if not data_yaml.is_absolute():
        data_yaml = ROOT_DIR / data_yaml

    validate_dataset(data_yaml)

    from ultralytics import YOLO

    base_model = args.model or train_settings.get("base_model", "yolov8n.pt")
    data_config = yaml.safe_load(data_yaml.read_text(encoding="utf-8")) or {}
    task = getattr(args, "task", None) or data_config.get("task", "detect")
    if task == "obb" and "obb" not in str(base_model):
        base_model = "yolov8n-obb.pt"
    epochs = int(args.epochs or train_settings.get("epochs", 50))
    image_size = int(args.imgsz or model_settings.get("image_size", 640))
    batch = int(args.batch or train_settings.get("batch", 8))
    device = args.device if args.device is not None else train_settings.get("device", "cpu")
    project = Path(args.project or train_settings.get("project", ROOT_DIR / "runs" / "train"))
    name = args.name or train_settings.get("name", "product_inspection")

    model = YOLO(str(base_model), task=task)
    result = model.train(
        data=str(data_yaml),
        epochs=epochs,
        imgsz=image_size,
        batch=batch,
        device=device,
        project=str(project),
        name=name,
        **({"mosaic": 0.0, "mixup": 0.0, "scale": 0.1, "translate": 0.05, "workers": 0}
           if getattr(args, "conn_crop", False) else {}),
    )

    save_best = bool(train_settings.get("copy_best_to_weights", True))
    if save_best:
        target_setting = getattr(args, "target_weights", None)
        target = Path(target_setting) if target_setting else ROOT_DIR / model_settings.get("weights", "weights/best.pt")
        if not target.is_absolute():
            target = ROOT_DIR / target
        copy_best_weight(Path(result.save_dir), target)
        import json
        manifest = {"conn_crop": bool(getattr(args, "conn_crop", False)),
                    "conn_reflection_augmentation": bool(getattr(args, "conn_reflection_augmentation", False)),
                    "conn_pin_point": bool(getattr(args, "conn_pin_point", False)), "image_size": image_size,
                    "task": task,
                    "data": str(data_yaml), "run_dir": str(result.save_dir),
                    "metrics": {key: float(value) for key, value in result.results_dict.items()}}
        target.with_suffix(".json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        recall = manifest["metrics"].get("metrics/recall(B)", 0)
        print(f"Validation recall: {recall:.3f}")
        if recall < 0.5:
            print("WARNING: Low validation recall. Saved model is not ready for reliable inspection.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Train Ultralytics YOLO for product inspection")
    parser.add_argument("--data", type=Path, help="Path to YOLO data.yaml")
    parser.add_argument("--model", help="Base model weight, for example yolov8n.pt")
    parser.add_argument("--epochs", type=int, help="Training epochs")
    parser.add_argument("--imgsz", type=int, help="Training image size")
    parser.add_argument("--batch", type=int, help="Batch size")
    parser.add_argument("--device", help="cpu, 0, 0,1, or mps")
    parser.add_argument("--project", type=Path, help="Training output directory")
    parser.add_argument("--name", help="Training run name")
    parser.add_argument("--target-weights", type=Path, help="Copy trained best.pt to this path")
    args = parser.parse_args()
    train(args)


if __name__ == "__main__":
    main()
