import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import cv2
import numpy as np

from inspection.special_checks import (
    SpecialInspector, annotation_dir, compose_ocr, conn_failures, labeled_records,
    load_reference, load_time, save_time, write_json,
    matches_format,
)
from inspection.region_masks import auto_exclude_regions, mask_regions


class SpecialChecksTest(unittest.TestCase):
    def test_pin_count_boundary_and_outside(self):
        slots = [(10, 10, 40, 40), (50, 10, 80, 40)]
        valid = [(15, 15, 35, 35), (55, 15, 75, 35)]
        self.assertEqual(conn_failures(slots, valid), [])
        self.assertTrue(conn_failures(slots, valid[:1]))
        self.assertTrue(conn_failures(slots, valid + [valid[0]]))
        self.assertTrue(conn_failures(slots, [(10, 15, 35, 35), valid[1]]))
        self.assertTrue(conn_failures(slots, [(5, 15, 35, 35), valid[1]]))
        self.assertTrue(conn_failures(slots, valid + [(90, 10, 95, 30)]))

    def test_time_persistence_and_group_isolation(self):
        with tempfile.TemporaryDirectory() as temporary:
            info = Path(temporary)
            self.assertEqual(load_time(info, "A", "conn"), (0, None))
            save_time(info, "A", "conn", 1.5, 3.5)
            self.assertEqual(load_time(info, "A", "conn"), (1.5, 3.5))
            self.assertEqual(load_time(info, "B", "conn"), (0, None))
            self.assertEqual(load_time(info, "A", "label"), (0, None))
            for start, end in ((-1, 4), (3, 3), (4, 3), (float("nan"), 4)):
                with self.assertRaises(ValueError):
                    save_time(info, "A", "conn", start, end)

    def test_reference_and_unicode_round_trip(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            directory = annotation_dir(root, "A", "OCR")
            value = {"image": "A_000000.jpg", "regions": [{"role": "text", "box": [0, 0, 1, 1],
                                                        "text": "Été 汉字 漢字 ',:/_\nA"}]}
            write_json(directory / "A_000000.json", value)
            write_json(directory / "A_000001.json", {"regions": []})
            self.assertEqual(len(labeled_records(root, "A", "OCR")), 1)
            self.assertEqual(load_reference(root, "A", "OCR"), value)
            self.assertIsNone(load_reference(root, "B", "OCR"))
            write_json(directory / "reference.json", value)
            self.assertEqual(len(labeled_records(root, "A", "OCR")), 1)

    def test_ocr_geometry_keeps_symbols_spaces_and_newlines(self):
        def result(x, y, width, text):
            return ([[x, y], [x + width, y], [x + width, y + 10], [x, y + 10]], text, 0.99)
        self.assertEqual(compose_ocr([result(0, 0, 40, "A,_/"), result(60, 0, 20, "B:"),
                                      result(0, 15, 20, "C'")]), "A,_/  B:\nC'")

    def inspector(self, kind, regions):
        inspector = SpecialInspector.__new__(SpecialInspector)
        inspector.references = {kind: {"regions": regions}}
        inspector.times = {kind: (0, None)}
        inspector.qr = SimpleNamespace(detectAndDecode=lambda crop: ("AB", None, None))
        inspector.ocr = SimpleNamespace(candidates=lambda crop: ["AB"])
        return inspector

    def test_ocr_exact_matching_and_unreadable(self):
        frame = np.zeros((100, 100, 3), dtype=np.uint8)
        region = {"role": "text", "box": [0, 0, 1, 1], "text": "AB"}
        inspector = self.inspector("OCR", [region])
        self.assertEqual(inspector.check(frame, 0), [])
        for text in ("ab", "A B", "AB\n", "AB_", ""):
            inspector.ocr.candidates = lambda crop, text=text: [text]
            self.assertTrue(inspector.check(frame, 0))
        inspector.ocr.candidates = lambda crop: []
        self.assertTrue(inspector.check(frame, 0))

    def test_vertical_ocr_rotates_crops_and_records_actual_mismatch(self):
        from inspection.special_checks import MultilingualOCR
        ocr = MultilingualOCR.__new__(MultilingualOCR)
        reader = Mock()
        reader.readtext.return_value = [([[0, 0], [40, 0], [40, 10], [0, 10]], "ABC", 0.9)]
        ocr.readers = [reader]
        self.assertEqual(ocr.candidates(np.zeros((80, 12, 3), dtype=np.uint8)), ["ABC"])
        self.assertEqual(reader.readtext.call_count, 2)
        self.assertEqual(reader.readtext.call_args.args[0].shape, (12, 80, 3))
        inspector = self.inspector("OCR", [{"role": "text", "box": [0, 0, 1, 1], "text": "ABC."}])
        inspector.ocr = ocr
        failures = inspector.check(np.zeros((80, 12, 3), dtype=np.uint8), 0)
        self.assertEqual(failures[0]["expected_text"], "ABC.")
        self.assertEqual(failures[0]["recognized_text"], ["ABC"])

    def test_qr_multi_regions_order_and_missing_partner(self):
        frame = np.zeros((100, 100, 3), dtype=np.uint8)
        regions = [{"role": "qr", "number": 1, "box": [0, 0, 0.5, 0.5]},
                   {"role": "qr", "number": 1, "box": [0.5, 0, 1, 0.5]},
                   {"role": "text", "number": 2, "box": [0, 0.5, 1, 1], "text": "ABAB"}]
        inspector = self.inspector("label", regions)
        inspector.ocr.candidates = lambda crop: ["ABAB"]
        self.assertEqual(inspector.check(frame, 0), [])
        inspector.qr.detectAndDecode = lambda crop: ("", None, None)
        self.assertTrue(inspector.check(frame, 0))
        inspector.references["label"]["regions"] = regions[:2]
        self.assertTrue(inspector.check(frame, 0))
        inspector.times["label"] = (1.5, 3.5)
        self.assertEqual(inspector.check(frame, 1), [])
        self.assertTrue(inspector.check(frame, 2))
        self.assertEqual(inspector.check(frame, 4), [])

    def test_label_text_without_qr_requires_exact_answer_for_all_numbers(self):
        frame = np.zeros((100, 100, 3), np.uint8)
        expected = "Ab ',:/_\nX"
        for number in (None, 2, 4, 6, 8, 9, 0, *range(10, 21)):
            with self.subTest(number=number):
                region = {"role":"text", "number":number, "box":[0,0,1,1], "text":expected}
                inspector = self.inspector("label", [region])
                inspector.ocr.candidates = lambda crop: [expected]
                self.assertEqual(inspector.check(frame, 0), [])
                for observed in (expected.lower(), expected.replace(" ", ""), expected.replace("\n", " "), ""):
                    inspector.ocr.candidates = lambda crop, value=observed: [value]
                    failures = inspector.check(frame, 0)
                    self.assertEqual(failures[0]["class_name"], f"label_text_{number}_mismatch")

    def test_label_qr_and_ocr_must_both_match(self):
        frame = np.zeros((100,100,3), np.uint8)
        for odd in (1,3,5,7):
            inspector = self.inspector("label", [
                {"role":"qr", "number":odd, "box":[0,0,.5,1]},
                {"role":"text", "number":odd+1, "box":[.5,0,1,1], "text":"AB"}])
            self.assertEqual(inspector.check(frame,0), [])
            inspector.qr.detectAndDecode = lambda crop: ("AC",None,None)
            self.assertTrue(inspector.check(frame,0))
            inspector.qr.detectAndDecode = lambda crop: ("AB",None,None)
            inspector.ocr.candidates = lambda crop: ["AC"]
            self.assertTrue(inspector.check(frame,0))

    def test_dynamic_date_compares_actual_digits_to_qr_not_training_image(self):
        frame = np.zeros((100,100,3),np.uint8)
        regions = [{"role":"qr","number":1,"box":[0,0,.5,1]}] + [
            {"role":"text","number":2,"box":[.5,0,1,1],"text":"","check":True,
             "pattern":["num","num"],"verdict":"PASS"} for _ in range(3)]
        inspector = self.inspector("label",regions)
        inspector.text_image_models = {"label":SimpleNamespace(
            predict=Mock(side_effect=AssertionError("No fixed-image comparison for variable dates")))}
        for values in (("11","08","23"),("12","09","24")):
            inspector.qr.detectAndDecode = lambda crop,values=values:("".join(values),None,None)
            inspector.trained_text_ocr = {"label":SimpleNamespace(candidates_batch=lambda requests,values=values:[[v] for v in values])}
            self.assertEqual(inspector.check(frame,0),[])
        inspector.trained_text_ocr = {"label":SimpleNamespace(candidates_batch=lambda requests:[["12"],["9"],["24"]])}
        self.assertTrue(inspector.check(frame,0))

    def test_standalone_text_numbers_have_independent_window_items(self):
        inspector = self.inspector("label", [
            {"role":"text", "number":number, "box":[0,0,1,1], "text":"AB"} for number in (9,0,*range(10,21))])
        self.assertEqual(inspector.check_frame(np.zeros((100,100,3),np.uint8),1,1), [])
        self.assertIn(("label", "text_9"), inspector.window_results.successes)
        self.assertIn(("label", "text_0"), inspector.window_results.successes)
        for number in range(10,21):
            self.assertIn(("label", f"text_{number}"), inspector.window_results.successes)

    def test_ng_display_independent_of_defect_threshold(self):
        from main import draw_ng_detections
        frame = np.zeros((100, 100, 3), dtype=np.uint8)
        failures = [{"class_name": "OCR_mismatch", "confidence": 1.0, "xyxy": [10, 10, 40, 40],
                     "special_failure": True}]
        _, detections = draw_ng_detections(frame, failures, {"judge": {"ng_classes": ["defect"], "min_confidence": 1}})
        self.assertEqual(detections, failures)

    def test_real_qr_decoder_preserves_payload(self):
        if not hasattr(cv2, "QRCodeEncoder_create"):
            self.skipTest("QR encoder not available")
        payload = "AB ',:/_"
        qr = cv2.QRCodeEncoder_create().encode(payload)
        qr = cv2.copyMakeBorder(qr, 4, 4, 4, 4, cv2.BORDER_CONSTANT, value=255)
        qr = cv2.resize(qr, None, fx=10, fy=10, interpolation=cv2.INTER_NEAREST)
        self.assertEqual(cv2.QRCodeDetector().detectAndDecode(qr)[0], payload)

    def test_conn_training_uses_only_labeled_frames_and_pin_class(self):
        import video_inspection_gui as gui
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            images = root / "images"
            images.mkdir()
            for index in range(5):
                image = images / f"A_{index:06d}.jpg"
                cv2.imwrite(str(image), np.zeros((80, 80, 3), dtype=np.uint8))
                if index < 3:
                    write_json(annotation_dir(root, "A", "conn") / f"{image.stem}.json",
                               {"image": str(image), "regions": [
                                   {"role": "slot", "box": [0.1, 0.1, 0.6, 0.6]},
                                   {"role": "pin", "box": [0.15, 0.15, 0.55, 0.55]},
                                   {"role": "pin_point", "box": [0.2, 0.2, 0.5, 0.5]}]})
            app = gui.VideoInspectionApp.__new__(gui.VideoInspectionApp)
            app.active_group_name = "A"
            with patch.object(gui, "ROOT_DIR", root), patch.object(gui, "train") as trainer:
                app._train_conn(SimpleNamespace())
            args = trainer.call_args.args[0]
            self.assertEqual(args.target_weights.name, "A_conn_algorith.pt")
            self.assertTrue(args.conn_crop)
            self.assertTrue(args.conn_pin_point)
            dataset = args.data.parent
            self.assertTrue(args.conn_reflection_augmentation)
            self.assertEqual(len(list((dataset / "images").rglob("*.jpg"))), 6)
            self.assertEqual(len(list((dataset / "images" / "train").glob("*reflection*.jpg"))), 3)
            for split in ("val", "test"):
                self.assertEqual(list((dataset / "images" / split).glob("*reflection*.jpg")), [])
            for image_path in (dataset / "images").rglob("*.jpg"):
                self.assertLess(cv2.imread(str(image_path)).shape[0], 80)
            for label in (dataset / "labels").rglob("*.txt"):
                lines = label.read_text().splitlines()
                self.assertEqual(len(lines), 1)
                self.assertTrue(lines[0].startswith("0 "))

    def test_special_result_video_and_log(self):
        from main import inspect_video
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            source = directory / "source.avi"
            writer = cv2.VideoWriter(str(source), cv2.VideoWriter_fourcc(*"MJPG"), 10, (64, 48))
            for _ in range(3):
                writer.write(np.zeros((48, 64, 3), dtype=np.uint8))
            writer.release()
            failure = {"class_name": "OCR_mismatch", "xyxy": [10, 10, 30, 30],
                       "confidence": 1, "special_failure": True}
            detector = SimpleNamespace(model=None, predict=lambda frame: ([], frame))
            special = SimpleNamespace(references={"OCR": {}}, check=lambda frame, seconds: [failure])
            with patch("model.yolo_inference.YoloInference", return_value=detector), \
                    patch("inspection.special_checks.SpecialInspector", return_value=special):
                output = inspect_video(source, {"root_dir": str(directory)}, directory / "test.mp4",
                                       show_window=False, status_suffix=True, log_dir=directory / "logs")
                self.assertEqual(output.name, "test_f.mp4")
                self.assertIn("OCR_mismatch", (directory / "logs" / "test_f.txt").read_text())
                cap = cv2.VideoCapture(str(output))
                ok, frame = cap.read()
                cap.release()
                self.assertTrue(ok)
                self.assertEqual(frame.shape[:2], (48, 64))
                special.check = lambda frame, seconds: []
                output = inspect_video(source, {"root_dir": str(directory)}, directory / "test.mp4",
                                       show_window=False, status_suffix=True, log_dir=directory / "logs")
                self.assertEqual(output.name, "test_p.mp4")

    def test_background_is_not_automatically_excluded(self):
        image = np.full((400, 400, 3), 255, dtype=np.uint8)
        image[100:300, 120:280] = 30
        regions = auto_exclude_regions(image)
        self.assertEqual(regions, [])
        masked = mask_regions(image, regions)
        self.assertTrue(np.array_equal(masked[100:300, 120:280], image[100:300, 120:280]))
        self.assertEqual(int(masked[0, 0, 0]), 255)
        self.assertEqual(auto_exclude_regions(np.full_like(image, 255)), [])

    def test_exceptions_only_suppress_contained_defects(self):
        from main import draw_ng_detections
        frame = np.zeros((100, 100, 3), dtype=np.uint8)
        settings = {"judge": {"ng_classes": ["defect"], "min_confidence": 0.1},
                    "except_regions": [{"box": [0.1, 0.1, 0.6, 0.6]}]}
        inside = {"class_name": "defect", "confidence": 0.9, "xyxy": [20, 20, 40, 40]}
        crossing = {"class_name": "defect", "confidence": 0.9, "xyxy": [50, 50, 70, 70]}
        outside = {"class_name": "defect", "confidence": 0.9, "xyxy": [80, 80, 90, 90]}
        _, detections = draw_ng_detections(frame, [inside, crossing, outside], settings)
        self.assertEqual(detections, [crossing, outside])

    def test_check_mode_requires_qr_ocr_equality_and_format(self):
        frame = np.zeros((100, 100, 3), dtype=np.uint8)
        regions = [{"role": "qr", "number": 1, "box": [0, 0, 0.5, 0.5]},
                   {"role": "text", "number": 2, "box": [0, 0.5, 1, 1],
                    "text": "unused", "check": True, "pattern": ["num", "text"]}]
        inspector = self.inspector("label", regions)
        for text, valid in (("1L", True), ("2M", True), ("3_", True), ("4é", True),
                            ("L1", False), ("12", False), ("1LM", False), ("", False)):
            inspector.qr.detectAndDecode = lambda crop, text=text: (text, None, None)
            inspector.ocr.candidates = lambda crop, text=text: [text]
            self.assertEqual(not inspector.check(frame, 0), valid, text)
        inspector.qr.detectAndDecode = lambda crop: ("1L", None, None)
        inspector.ocr.candidates = lambda crop: ["2M"]
        self.assertTrue(inspector.check(frame, 0))
        self.assertTrue(matches_format("1/", ["num", "text"]))
        self.assertFalse(matches_format("1", ["num", "text"]))

    def test_newest_labeled_frame_used_background_only_frames_ignored(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            directory = annotation_dir(root, "A", "OCR")
            region = {"role": "text", "box": [0, 0, 1, 1], "text": "first"}
            write_json(directory / "A_000000.json", {"saved_at_ns": 1, "regions": [region]})
            region = {**region, "text": "last"}
            write_json(directory / "A_000001.json", {"saved_at_ns": 2, "regions": [region]})
            write_json(directory / "A_000002.json", {"saved_at_ns": 3, "regions": [{"role": "auto_exclude", "box": [0, 0, 1, 1]}]})
            self.assertEqual(len(labeled_records(root, "A", "OCR")), 2)
            self.assertEqual(load_reference(root, "A", "OCR")["regions"][0]["text"], "last")


if __name__ == "__main__":
    unittest.main()
