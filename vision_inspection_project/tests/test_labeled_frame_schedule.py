import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import numpy as np

from inspection.special_checks import SpecialInspector, annotation_dir, labeled_frame_schedule, write_json


class LabeledFrameScheduleTest(unittest.TestCase):
    def test_conn_range_inspects_unlabeled_frames_using_nearest_conditions(self):
        inspector = SpecialInspector.__new__(SpecialInspector)
        inspector.labeled_frames_only = True
        first,second = {"regions":[],"name":"first"},{"regions":[],"name":"second"}
        inspector.references = {"conn":first}
        inspector.annotation_frames = {"conn":{1:first,9:second}}
        inspector.conn_inspection_range = (3,7)
        seen = []
        def check(frame,number,mask):
            reference = inspector.references.get("conn")
            seen.append((number,reference))
            return [{"class_name":"conn_pin_1_missing","inspection_kind":"conn","pin_number":1}] if reference else []
        inspector.check = check
        results = [inspector.check_frame(None,n,10) for n in range(1,11)]
        self.assertFalse(results[2])
        self.assertIn("conn_pin_1_missing",[f["class_name"] for f in results[3]])
        self.assertEqual([n for n,r in seen if r],[3,4,5,6,7])
        self.assertIs(seen[4][1],first)
        self.assertIs(seen[5][1],second)
        self.assertEqual(sorted(inspector.annotation_frames["conn"]),[1,9])
        self.assertIn("OUTSIDE RANGE",inspector.status_lines[-1])

    def test_conn_inspection_range_roundtrip_and_missing_file(self):
        from inspection.photo_inspection import save_photo_frame_range,load_photo_frame_range,frame_path
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaises(ValueError):
                load_photo_frame_range(root,"A_23fps","conn")
            save_photo_frame_range(root,"A_23fps","conn",10,50)
            self.assertEqual(frame_path(root,"A_23fps","conn").name,"A_23fps_conn_inspection_frame.txt")
            self.assertEqual(load_photo_frame_range(root,"A_23fps","conn"),(10,50))

    def test_frame_number_is_group_order_not_original_video_index(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            paths = [root / f"A_10fps_{index:06d}.jpg" for index in (0, 3, 6, 9)]
            for kind in ("OCR", "conn", "label"):
                write_json(annotation_dir(root, "A_10fps", kind) / f"{paths[2].stem}.json",
                           {"image": str(paths[2]), "regions": [{"role": "text", "text": "A", "box": [0, 0, 1, 1]}]})
                schedule = labeled_frame_schedule(root, "A_10fps", kind, paths)
                self.assertEqual(list(schedule), [3])

    def test_only_labeled_frames_run_with_their_own_regions_and_answers(self):
        inspector = SpecialInspector.__new__(SpecialInspector)
        inspector.labeled_frames_only = True
        first = {"regions": [{"role": "text", "text": "A", "box": [0, 0, .5, 1]}]}
        second = {"regions": [{"role": "text", "text": "B", "box": [.5, 0, 1, 1]}]}
        inspector.references = {"OCR": second}
        inspector.annotation_frames = {"OCR": {2: first, 4: second}}
        inspector.times = {"OCR": (0, None)}
        read = Mock(side_effect=[["A"], ["wrong"]])
        inspector.ocr = SimpleNamespace(candidates=read)
        frame = np.zeros((80, 80, 3), dtype=np.uint8)
        self.assertEqual(inspector.check_frame(frame, 1, 4), [])
        read.assert_not_called()
        self.assertIn("SKIPPED", inspector.status_lines[0])
        self.assertEqual(inspector.check_frame(frame, 2, 4), [])
        self.assertIn("OK", inspector.status_lines[0])
        self.assertEqual(inspector.check_frame(frame, 3, 4), [])
        failures = inspector.check_frame(frame, 4, 4)
        self.assertEqual(failures[0]["expected_text"], "B")
        self.assertEqual(failures[0]["xyxy"], (40, 0, 80, 80))
        self.assertEqual(read.call_count, 2)
        self.assertIn("NG", inspector.status_lines[0])
        self.assertIs(inspector.references["OCR"], second)

    def test_all_three_functions_are_scheduled_independently(self):
        inspector = SpecialInspector.__new__(SpecialInspector)
        inspector.labeled_frames_only = True
        inspector.references = {kind: {"regions": []} for kind in ("OCR", "conn", "label")}
        inspector.annotation_frames = {kind: {i: inspector.references[kind]} for i, kind in enumerate(inspector.references, 1)}
        called = []
        inspector.check = lambda frame, number, mask: called.append(list(inspector.references)) or []
        for number in range(1, 5):
            inspector.check_frame(None, number, 4)
        self.assertEqual(called, [["OCR"], ["conn"], ["label"], []])
