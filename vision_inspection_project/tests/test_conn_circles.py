import unittest
import tempfile
from pathlib import Path
from types import SimpleNamespace
import numpy as np
from inspection.conn_geometry import circle_area, area_geometry, center_inside, annotation_status
from inspection.special_checks import conn_position_failures
from inspection.conn_geometry import gap_failures, validate_gap_limits, load_gap_limits, save_gap_limits
from inspection.conn_geometry import boundary_outside_indices, shift_region_group


class ConnCircleTests(unittest.TestCase):
    def test_boundary_counts_and_axis_group_copy(self):
        regions = [{"role": "conn_boundary", "box": [.2, .2, .8, .8]}]
        self.assertEqual(boundary_outside_indices([(20, 20), (80, 80), (50, 50)], regions, (100, 100)), [])
        self.assertEqual(boundary_outside_indices([(10, 50), (90, 50), (50, 50)], regions, (100, 100)), [0, 1])
        point = {"role": "pin_point", "box": [.4, .4, .5, .5]}
        area = circle_area((.45, .45), (.55, .45), (100, 100))
        shifted = shift_region_group([point, area], 10, 20, "y", (100, 100))
        for original, new in zip([point, area], shifted):
            self.assertEqual(new["box"][0], original["box"][0])
            self.assertAlmostEqual(new["box"][1]-original["box"][1], .2)
        self.assertEqual(point["box"], [.4, .4, .5, .5])

    def test_runtime_boundary_one_outside_passes_two_outside_fails(self):
        from inspection.special_checks import SpecialInspector
        inspector = SpecialInspector.__new__(SpecialInspector)
        regions = [circle_area((x/100, .5), ((x+7)/100, .5), (100, 100)) for x in (20, 50, 80)]
        boundary = {"role": "conn_boundary", "box": [.3, .3, .7, .7]}
        regions.append(boundary)
        inspector.references = {"conn": {"regions": regions}}
        inspector.times = {"conn": (0, None)}
        inspector.pin_crop_model = inspector.original_resolution = False
        inspector.confidence, inspector.iou, inspector.image_size = .1, .45, 640
        result = SimpleNamespace(boxes=[SimpleNamespace(xyxy=np.array([[x-2, 48, x+2, 52]])) for x in (20, 50, 80)])
        inspector.pin_model = SimpleNamespace(predict=lambda *args, **kwargs: [result])
        inspector.conn_limits = {"max_horizontal_px": 1}
        frame = np.zeros((100, 100, 3), np.uint8)
        failures = inspector.check(frame, 0)
        self.assertEqual([item["class_name"] for item in failures], ["conn_boundary_outside_pins"])
        self.assertEqual(failures[0]["outside_count"], 2)
        boundary["box"] = [.1, .3, .7, .7]
        self.assertEqual(inspector.check(frame, 0), [])
        annotated = inspector.draw_conn_guides(frame, 1)
        self.assertGreater(annotated[:, :, 2].sum(), 0)
        self.assertEqual(frame.sum(), 0)

    def test_all_pins_maximum_gap_and_boundary(self):
        limits = {"max_horizontal_px": 30, "max_vertical_px": 10}
        self.assertEqual(gap_failures([(0, 0), (30, 10), (15, 5)], limits), [])
        failures = gap_failures([(0, 0), (31, 11), (15, 5)], limits)
        self.assertEqual([item["axis"] for item in failures], ["horizontal", "vertical"])
        self.assertEqual(gap_failures([(0, 0), (1000, 1000)], {}), [])
        self.assertEqual(gap_failures([(0, 0)], limits), [])
        for bad in (-1, float("nan"), float("inf")):
            with self.assertRaises(ValueError):
                validate_gap_limits({"max_horizontal_px": bad})

    def test_limits_persist_per_group(self):
        with tempfile.TemporaryDirectory() as folder:
            a, b = Path(folder) / "A", Path(folder) / "B"
            save_gap_limits(a, {"max_horizontal_px": 30, "max_vertical_px": ""})
            self.assertEqual(load_gap_limits(a), {"max_horizontal_px": 30, "max_vertical_px": None})
            self.assertEqual(load_gap_limits(b), {"max_horizontal_px": None, "max_vertical_px": None})

    def test_runtime_ignores_removed_numeric_gap_limits(self):
        from inspection.special_checks import SpecialInspector
        inspector = SpecialInspector.__new__(SpecialInspector)
        regions = [circle_area((.35, .5), (.45, .5), (100, 100)),
                   circle_area((.65, .5), (.75, .5), (100, 100))]
        inspector.references = {"conn": {"regions": regions}}
        inspector.times = {"conn": (0, None)}
        inspector.pin_crop_model = inspector.original_resolution = False
        inspector.confidence, inspector.iou, inspector.image_size = .1, .45, 640
        result = SimpleNamespace(boxes=[SimpleNamespace(xyxy=np.array([[33, 48, 37, 52]])),
                                        SimpleNamespace(xyxy=np.array([[63, 48, 67, 52]]))])
        inspector.pin_model = SimpleNamespace(predict=lambda *args, **kwargs: [result])
        inspector.conn_limits = {"max_horizontal_px": 25, "max_vertical_px": 10}
        failures = inspector.check(np.zeros((100, 100, 3), np.uint8), 0)
        self.assertEqual(failures, [])
        inspector.conn_limits["max_horizontal_px"] = 30
        self.assertEqual(inspector.check(np.zeros((100, 100, 3), np.uint8), 0), [])

    def test_true_circle_on_non_square_image(self):
        region = circle_area((.5, .5), (.6, .5), (100, 200))
        self.assertEqual(region["shape"], "circle")
        x1, y1, x2, y2 = region["box"]
        self.assertAlmostEqual((x2-x1)*200, (y2-y1)*100)
        geometry = area_geometry(region, (100, 200))
        self.assertTrue(center_inside((100, 50), geometry))
        self.assertTrue(center_inside((120, 50), geometry))
        self.assertFalse(center_inside((119, 69), geometry))

    def test_slot_limits_runtime_pins_and_expected_areas(self):
        from inspection.special_checks import SpecialInspector
        inspector = SpecialInspector.__new__(SpecialInspector)
        regions = [{"role": "slot", "box": [.2, .2, .5, .8]},
                   circle_area((.35, .5), (.45, .5), (100, 100)),
                   circle_area((.8, .5), (.9, .5), (100, 100)),
                   {"role": "conn_boundary", "box": [.2, .2, .5, .8]}]
        inspector.references = {"conn": {"regions": regions}}
        inspector.times = {"conn": (0, None)}
        inspector.pin_crop_model = inspector.original_resolution = False
        inspector.confidence, inspector.iou, inspector.image_size = .1, .45, 640
        result = SimpleNamespace(boxes=[SimpleNamespace(xyxy=np.array([box])) for box in
                                       ((33,48,37,52), (78,48,82,52), (88,48,92,52))])
        inspector.pin_model = SimpleNamespace(predict=lambda *args, **kwargs: [result])
        self.assertEqual(inspector.check(np.zeros((100,100,3), np.uint8), 0), [])
        self.assertEqual(len(inspector.conn_visuals["pins"]), 1)
        result.boxes = []
        failures = inspector.check(np.zeros((100,100,3), np.uint8), 0)
        self.assertEqual([f["class_name"] for f in failures], ["conn_pin_1_count_0"])

    def test_multiple_slots_include_union_and_rotated_scope(self):
        from inspection.conn_geometry import inside_slots
        regions = [{"role":"slot", "box":[.1,.1,.3,.3]},
                   {"role":"slot", "box":[.6,.6,.8,.8], "angle":45}]
        self.assertTrue(inside_slots((20,20), regions, (100,100)))
        self.assertTrue(inside_slots((70,70), regions, (100,100)))
        self.assertFalse(inside_slots((50,50), regions, (100,100)))
        self.assertTrue(inside_slots((50,50), [], (100,100)))

    def test_slot_not_required_and_center_only(self):
        geometry = {"center": (50, 50), "radius": 10}
        self.assertEqual(conn_position_failures([], [geometry], [(0, 0, 100, 100)]), [])
        self.assertIn("conn_pin_1_count_0", conn_position_failures([], [geometry], []))
        self.assertIn("conn_pin_1_count_0", conn_position_failures([], [geometry], [(58, 58, 60, 60)]))
        self.assertIn("conn_outside_or_boundary", conn_position_failures([], [geometry], [(70, 70, 80, 80)]))
        self.assertIn("conn_pin_1_count_2", conn_position_failures([], [geometry], [(48, 48, 52, 52)]*2))

    def test_annotation_status_changes_with_pin_position(self):
        point = {"role": "pin_point", "box": [.4, .4, .6, .6]}
        area = circle_area((.5, .5), (.7, .5), (100, 100))
        self.assertEqual(annotation_status([point, area], (100, 100)), ([True], [True]))
        moved = dict(point, box=[.8, .8, .9, .9])
        self.assertEqual(annotation_status([moved, area], (100, 100)), ([False], [False]))
        self.assertEqual(annotation_status([area], (100, 100)), ([], [False]))
        self.assertEqual(annotation_status([point], (100, 100)), ([False], []))


if __name__ == "__main__":
    unittest.main()
