import unittest

import numpy as np

from inspection.region_masks import auto_exclude_regions, mask_regions


class ManualExclusionOnlyTests(unittest.TestCase):
    def test_background_never_generates_exclusions(self):
        image = np.full((100, 100, 3), 255, dtype=np.uint8)
        image[30:70, 30:70] = 20
        self.assertEqual(auto_exclude_regions(image), [])

    def test_legacy_automatic_masks_ignored_manual_masks_retained(self):
        image = np.full((100, 100, 3), 255, dtype=np.uint8)
        output = mask_regions(image, [
            {"role": "auto_exclude", "box": [0, 0, .5, 1]},
            {"role": "exclude", "box": [.7, .7, .9, .9]},
        ])
        self.assertTrue(np.all(output[:, :50] == 255))
        self.assertTrue(np.all(output[70:90, 70:90] == 0))
        self.assertTrue(np.all(image == 255))
