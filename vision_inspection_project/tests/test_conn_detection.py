import unittest
from types import SimpleNamespace
from unittest.mock import Mock

import numpy as np

from inspection.conn_detection import verify_external_pins, deduplicate_pins


def result(*boxes):
    return SimpleNamespace(boxes=[SimpleNamespace(xyxy=np.asarray([box], dtype=float)) for box in boxes])


class ConnDetectionTests(unittest.TestCase):
    def test_inside_crop_reflection_is_rejected_without_local_confirmation(self):
        model = Mock()
        model.predict.return_value = [result()]
        self.assertEqual(verify_external_pins(model, np.zeros((200,200,3), np.uint8),
            (10,10,110,110), [(40,40,50,50)], .1,.45,640, include_inside=True), [])
        self.assertEqual(model.predict.call_count, 1)

    def test_real_extra_pin_inside_crop_is_preserved_when_locally_confirmed(self):
        model = Mock()
        model.predict.return_value = [result((45,45,55,55))]
        self.assertEqual(verify_external_pins(model, np.zeros((200,200,3), np.uint8),
            (10,10,110,110), [(40,40,50,50)], .1,.45,640, include_inside=True), [(40,40,50,50)])

    def test_slot_resize_does_not_change_training_crop(self):
        from inspection.special_checks import conn_crop_bounds
        pins = [{"role":"pin_point", "box":[.3,.3,.4,.4]}]
        small = pins + [{"role":"slot", "box":[.2,.2,.5,.5]}]
        large = pins + [{"role":"slot", "box":[0,0,1,1]}]
        self.assertEqual(conn_crop_bounds(small,(200,200)), conn_crop_bounds(large,(200,200)))
        self.assertEqual(conn_crop_bounds(pins,(200,200)), conn_crop_bounds(large,(200,200)))

    def test_observed_pin_2_duplicate_is_suppressed(self):
        boxes = [(67.412628,811.803772,81.252670,827.504272),
                 (61.489426,814.251694,74.044479,829.897751)]
        self.assertEqual(deduplicate_pins(boxes, [.32284,.28471]), boxes[:1])

    def test_distinct_pins_are_not_merged_just_because_they_share_an_area(self):
        boxes = [(10,10,20,20), (22,10,32,20), (10,24,20,34)]
        self.assertEqual(deduplicate_pins(boxes), boxes)
        self.assertEqual(len(deduplicate_pins([(10,10,20,20), (19,10,29,20)])), 2)

    def test_high_confidence_duplicate_survives_and_two_outside_pins_stay_two(self):
        duplicate = [(10,10,20,20), (11,11,21,21)]
        self.assertEqual(deduplicate_pins(duplicate, [.2,.9]), duplicate[1:])
        self.assertEqual(len(deduplicate_pins([(100,100,110,110), (150,150,160,160)])), 2)

    def test_coarse_false_positives_are_not_counted_without_local_confirmation(self):
        model = Mock()
        model.predict.return_value = [result()]
        frame = np.zeros((200, 200, 3), np.uint8)
        found = verify_external_pins(model, frame, (10, 10, 60, 60), [(100,100,120,120)], .1, .45, 640)
        self.assertEqual(found, [])
        self.assertEqual(model.predict.call_args.args[0].shape, (50, 50, 3))
        self.assertEqual(model.predict.call_args.kwargs["imgsz"], 640)

    def test_two_real_external_pins_remain_two_after_verification(self):
        model = Mock()
        model.predict.return_value = [result((20,20,30,30))]
        found = verify_external_pins(model, np.zeros((200,200,3), np.uint8), (10,10,60,60),
                                     [(100,100,110,110), (150,150,160,160)], .1, .45, 640)
        self.assertEqual(found, [(100,100,110,110), (150,150,160,160)])
        self.assertEqual(model.predict.call_count, 2)

    def test_coarse_candidates_inside_connector_are_not_verified_again(self):
        model = Mock()
        self.assertEqual(verify_external_pins(model, np.zeros((100,100,3), np.uint8), (10,10,60,60),
                                             [(20,20,30,30)], .1, .45, 640), [])
        model.predict.assert_not_called()

    def test_guides_show_pin_areas_and_actual_pin_points_with_stable_numbers(self):
        import cv2
        from inspection.special_checks import SpecialInspector
        from inspection.conn_geometry import circle_area
        inspector = SpecialInspector.__new__(SpecialInspector)
        inspector.references = {"conn": {"regions": [circle_area((.5,.5),(.7,.5),(100,100)),
                                                       {"role":"pin_point", "box":[.45,.45,.55,.55]},
                                                       {"role":"conn_boundary", "box":[.2,.2,.8,.8]}]}}
        inspector.conn_visuals = {"pins":[(46,46,54,54)], "numbers":[1]}
        frame = np.zeros((100,100,3),np.uint8)
        with unittest.mock.patch("cv2.putText", wraps=cv2.putText) as labels:
            annotated = inspector.draw_conn_guides(frame, 1)
        self.assertIn("pin area 1", [call.args[1] for call in labels.call_args_list])
        self.assertIn("pin point 1", [call.args[1] for call in labels.call_args_list])
        self.assertGreater(annotated.sum(),0)
        self.assertEqual(frame.sum(),0)


if __name__ == "__main__":
    unittest.main()
