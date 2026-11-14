import unittest
from unittest.mock import patch

from inspection.performance import Performance, summarize_timeline


class VerdictTimelineTests(unittest.TestCase):
    def test_clips_at_boundaries_and_does_not_double_count_nested_lanes(self):
        result = summarize_timeline([
            ('appearance_inference',1,6),
            ('special_frame_parallel_wall',2,4),
            ('result_video_write',3,7),
            ('inspection_photo_save',4,5),
            ('preview_display',7,8),
            ('verdict_txt_publish',9,12),
        ],3,10)
        self.assertEqual(result['wall_seconds'],7)
        self.assertEqual(result['lane_wall_seconds']['appearance'],3)
        self.assertEqual(result['lane_wall_seconds']['media_write'],4)
        self.assertEqual(result['overlap_wall_seconds'],3)
        self.assertEqual(result['unmeasured_wall_seconds'],1)
        self.assertEqual(result['exclusive_lane_wall_seconds']['media_write'],1)
        self.assertEqual(result['stages']['verdict_txt_publish']['elapsed_seconds'],1)
        self.assertEqual(sum(row['wall_seconds'] for row in result['overlap_combinations']),3)

    def test_in_flight_media_is_measured_only_until_txt_publication(self):
        with patch('inspection.performance.perf_counter',side_effect=[10,11,12,14,18]):
            performance = Performance()
            with performance.measure('result_video_encoding'):
                performance.mark('inline_photo_inspections_ready')
                performance.mark('verdict_txt_published')
                during = performance.verdict_timeline()
            after = performance.verdict_timeline()
        self.assertEqual(during,after)
        self.assertEqual(after['special_ready_to_txt']['lane_wall_seconds']['media_write'],2)
