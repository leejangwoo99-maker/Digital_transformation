import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import cv2
import numpy as np
import yaml

from inspection.region_masks import filter_training_scratches
from inspection.special_checks import conn_position_failures
from video_inspection_gui import VideoInspectionApp


class TrainingExceptionsConnTest(unittest.TestCase):
    def test_appearance_uses_only_ng_labeled_frames_not_unlabeled_or_control_only(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            images,labels = root/"images",root/"labels"
            images.mkdir()
            labels.mkdir()
            paths = []
            texts = ["2 0.5 0.5 0.1 0.1\n"]*3 + [None,"", "7 0.5 0.5 0.1 0.1\n10 0.5 0.5 0.1 0.1\n",
                     "0 0.5 0.5 0.1 0.1\n", "12 0.5 0.5 0.8 0.8\n2 0.5 0.5 0.1 0.1\n"]
            for index,text in enumerate(texts):
                path = images/f"A_{index:06d}.jpg"
                cv2.imwrite(str(path),np.zeros((48,48,3),np.uint8))
                paths.append(path)
                if text is not None:
                    (labels/f"{path.stem}.txt").write_text(text)
            data_path = root/"data.yaml"
            data_path.write_text(yaml.safe_dump({"names":{0:"ok",2:"scratch",7:"auto_exclude",10:"roi",12:"except"}}))
            app = VideoInspectionApp.__new__(VideoInspectionApp)
            app.active_image_paths = paths
            app.active_algorithm_name = "A_algorith"
            app._active_label_paths = lambda:(images,labels)
            with patch("video_inspection_gui.ROOT_DIR",root),patch("video_inspection_gui.DATASET_YAML_PATH",data_path):
                result,message = app._build_algorithm_dataset()
                included = {path.name for path in (result.parent/"images").rglob("*.jpg")}
                self.assertEqual(included,{path.name for path in paths[:3]})
                self.assertIn("skipped 5 frames",message)
                self.assertEqual(len(list((result.parent/"labels").rglob("*.txt"))),3)
                sentinel = result.parent/"preserve.txt"
                sentinel.write_text("previous dataset")
                app.active_image_paths = paths[:2]
                with self.assertRaisesRegex(RuntimeError,"at least 3"):
                    app._build_algorithm_dataset()
                self.assertTrue(sentinel.exists())
                app.active_image_paths = paths[3:7]
                self.assertFalse(app._has_regular_labels())
            for path,text in zip(paths,texts):
                if text is not None:
                    self.assertEqual((labels/f"{path.stem}.txt").read_text(),text)

    def test_scratch_only_fully_contained_is_removed(self):
        text = ("12 0.5 0.5 0.4 0.4\n2 0.5 0.5 0.2 0.2\n"
                "2 0.7 0.5 0.2 0.2\n2 0.9 0.9 0.1 0.1\n1 0.5 0.5 0.2 0.2\n")
        filtered, count = filter_training_scratches(text, {12: "except", 2: "scratch", 1: "defect"})
        self.assertEqual(count, 1)
        self.assertNotIn("2 0.5 0.5 0.2 0.2", filtered.splitlines())
        self.assertIn("2 0.7 0.5", filtered)
        self.assertIn("2 0.9 0.9", filtered)
        self.assertIn("1 0.5 0.5", filtered)
        self.assertIn("12 0.5 0.5", filtered)

    def test_no_except_preserves_scratch(self):
        text = "2 0.5 0.5 0.2 0.2\n"
        self.assertEqual(filter_training_scratches(text, {2: "scratch"}), (text, 0))

    def test_dataset_filters_all_splits_without_editing_source(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            images, labels = root / "images", root / "labels"
            images.mkdir()
            labels.mkdir()
            source = "12 0.5 0.5 0.4 0.4\n2 0.5 0.5 0.2 0.2\n2 0.7 0.5 0.2 0.2\n"
            paths = []
            for index in range(3):
                path = images / f"A_{index:06d}.jpg"
                cv2.imwrite(str(path), np.zeros((48, 48, 3), dtype=np.uint8))
                (labels / f"{path.stem}.txt").write_text(source)
                paths.append(path)
            data_path = root / "data.yaml"
            data_path.write_text(yaml.safe_dump({"names": {2: "scratch", 12: "except"}}))
            app = VideoInspectionApp.__new__(VideoInspectionApp)
            app.active_image_paths = paths
            app.active_algorithm_name = "A_algorith"
            app._active_label_paths = lambda: (images, labels)
            with patch("video_inspection_gui.ROOT_DIR", root), patch("video_inspection_gui.DATASET_YAML_PATH", data_path):
                result, message = app._build_algorithm_dataset()
            self.assertIn("Excluded 3 scratches", message)
            for split in ("train", "val", "test"):
                label = next((result.parent / "labels" / split).glob("*.txt"))
                self.assertNotIn("2 0.5 0.5 0.2 0.2", label.read_text().splitlines())
                self.assertIn("2 0.7 0.5", label.read_text())
            self.assertTrue(all(path.read_text() == source for path in labels.glob("*.txt")))

    def test_eight_pins_in_one_connector_are_ok_when_positions_match(self):
        slots = [(0, 0, 100, 100)]
        positions = [(10+index*20, 10+row*40, 25+index*20, 25+row*40)
                     for row in range(2) for index in range(4)]
        pins = [(x1+2, y1+2, x2-2, y2-2) for x1, y1, x2, y2 in positions]
        self.assertEqual(conn_position_failures(slots, positions, pins), [])
        self.assertIn("conn_pin_8_count_0", conn_position_failures(slots, positions, pins[:-1]))
        self.assertIn("conn_pin_1_count_2", conn_position_failures(slots, positions, pins + pins[:1]))
        boundary = [(positions[0][0], pins[0][1], pins[0][2], pins[0][3])] + pins[1:]
        self.assertEqual(conn_position_failures(slots, positions, boundary), [])
        oversized = [(0, 0, 35, 35)] + pins[1:]
        self.assertEqual(conn_position_failures(slots, positions, oversized), [])
        self.assertIn("conn_outside_connector_or_boundary",
                      conn_position_failures(slots, positions, pins + [(110, 110, 115, 115)]))
        self.assertIn("conn_outside_or_boundary",
                      conn_position_failures(slots, positions, pins + [(91, 70, 95, 80)]))


if __name__ == "__main__":
    unittest.main()
