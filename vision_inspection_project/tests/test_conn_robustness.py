import unittest
import numpy as np

from inspection.conn_robustness import ConnConfirmation, reflection_samples


class RobustnessTests(unittest.TestCase):
    def failure(self, pin=1):
        return dict(inspection_kind="conn", class_name=f"conn_pin_{pin}_count_0", pin_number=pin)

    def test_two_consecutive_same_pin_required(self):
        tracker = ConnConfirmation()
        self.assertEqual(tracker.update([self.failure()], 1), [])
        self.assertEqual(len(tracker.pending), 1)
        self.assertEqual(len(tracker.update([self.failure()], 2)), 1)
        self.assertEqual(tracker.update([], 3), [])
        self.assertEqual(tracker.update([self.failure()], 4), [])

    def test_different_pins_and_frame_gaps_do_not_confirm(self):
        tracker = ConnConfirmation()
        tracker.update([self.failure()], 1)
        self.assertEqual(tracker.update([self.failure(2)], 2), [])
        self.assertEqual(tracker.update([self.failure(2)], 4), [])

    def test_different_reflections_do_not_confirm(self):
        tracker = ConnConfirmation()
        failure = dict(inspection_kind="conn", class_name="conn_outside_or_boundary",
                       evidence_centers=[(100, 100)])
        tracker.update([failure], 1)
        other = dict(failure, evidence_centers=[(200, 200)])
        self.assertEqual(tracker.update([other], 2), [])
        self.assertEqual(len(tracker.update([other], 3)), 1)

    def test_ocr_is_not_delayed(self):
        failure = dict(inspection_kind="OCR", class_name="OCR_mismatch")
        self.assertEqual(ConnConfirmation().update([failure], 1), [failure])

    def test_samples_preserve_dimensions_and_source(self):
        image = np.full((64, 96, 3), 100, np.uint8)
        before = image.copy()
        samples = list(reflection_samples(image))
        self.assertEqual(len(samples), 3)
        np.testing.assert_array_equal(image, before)
        for _, sample in samples:
            self.assertEqual(sample.shape, image.shape)
            self.assertEqual(sample.dtype, np.uint8)
            self.assertFalse(np.array_equal(sample, image))
