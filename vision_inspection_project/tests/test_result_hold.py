import unittest

from main import result_hold_frames


class ResultHoldTests(unittest.TestCase):
    def test_success_is_held_for_at_least_half_a_second(self):
        for fps,expected in ((23,12),(30,15),(10,5),(1,1)):
            self.assertEqual(result_hold_frames(fps,["OCR"]),expected)

    def test_non_success_frames_are_not_duplicated(self):
        self.assertEqual(result_hold_frames(23,[]),1)
