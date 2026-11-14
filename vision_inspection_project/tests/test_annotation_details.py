import unittest
from unittest.mock import patch

import numpy as np

from inspection.annotation_details import draw_failure_details, draw_inspection_labels, failure_detail_lines, preview_with_details
from main import draw_ng_detections


class AnnotationDetailsTest(unittest.TestCase):
    def test_hidden_photo_boxes_preserve_verdict_and_render_only_visible_boxes(self):
        frame = np.zeros((180, 200, 3), dtype=np.uint8)
        scratch = dict(class_name='scratch',confidence=.9,xyxy=[40,90,70,130])
        failure = dict(class_name='OCR_mismatch',inspection_kind='OCR',special_failure=True,
                       confidence=1.,xyxy=[110,90,150,130])
        settings = {'judge':{'ng_classes':['scratch']}}
        expected, _ = draw_ng_detections(frame,[scratch],settings)
        annotated, verdict = draw_ng_detections(frame,[scratch,failure],settings,hidden_kinds={'OCR'})
        self.assertEqual(verdict,[scratch,failure])
        np.testing.assert_array_equal(annotated,expected)
        expected_with_detail = draw_inspection_labels(expected,[failure])
        self.assertIs(draw_inspection_labels(annotated,[failure],copy_frame=False),annotated)
        np.testing.assert_array_equal(annotated,expected_with_detail)
        self.assertFalse(frame.any())

    def test_hidden_predicted_exception_still_applies_to_verdict_only(self):
        frame = np.zeros((180, 200, 3), dtype=np.uint8)
        scratch = dict(class_name='scratch',confidence=.9,xyxy=[40,90,70,130])
        hidden_exception = dict(class_name='except',inspection_kind='label',xyxy=[0,0,200,180])
        settings = {'judge':{'ng_classes':['scratch']},'allow_predicted_except':True}
        expected, _ = draw_ng_detections(frame,[scratch],settings)
        annotated, verdict = draw_ng_detections(frame,[scratch,hidden_exception],settings,hidden_kinds={'label'})
        self.assertEqual(verdict,[])
        np.testing.assert_array_equal(annotated,expected)

    def test_preview_details_are_on_right_without_changing_video(self):
        video = np.full((600, 400, 3), 255, dtype=np.uint8)
        failure = {"class_name": "OCR_mismatch", "expected_text": "A.", "recognized_text": ["A"]}
        preview = preview_with_details(video, [failure])
        self.assertEqual(preview.shape, (600, 880, 3))
        self.assertTrue(np.array_equal(preview[:, :400], video))
        self.assertTrue(np.any(preview[:, 400:] != 20))

    def test_details_do_not_cover_video(self):
        frame = np.full((600, 800, 3), 255, dtype=np.uint8)
        failure = {"class_name": "conn_pin_1_count_0", "display_label": "pin 1 NG",
                   "pin_number": 1, "confidence": 1.0, "xyxy": [20, 40, 50, 70],
                   "special_failure": True, "expected_text": "1 pin", "recognized_text": "0 pins"}
        annotated, _ = draw_ng_detections(frame, [failure], {})
        self.assertTrue(np.array_equal(annotated[300:], frame[300:]))
        self.assertEqual(annotated.shape, frame.shape)

    def test_conn_ng_pin_number_is_drawn_on_video(self):
        failure = {"class_name": "conn_pin_8_count_0", "display_label": "pin 8 NG",
                   "confidence": 1.0, "xyxy": [20, 40, 50, 70], "special_failure": True}
        with patch("cv2.putText") as put_text:
            _, kept = draw_ng_detections(np.zeros((100, 100, 3), dtype=np.uint8), [failure], {})
        self.assertEqual(kept, [failure])
        self.assertIn("pin 8 NG", [call.args[1] for call in put_text.call_args_list])

    def test_except_requires_full_containment_on_every_edge(self):
        settings = {"judge": {"ng_classes": ["scratch"], "min_confidence": .1},
                    "except_regions": [{"box": [.2, .2, .6, .6]}]}
        inside = {"class_name": "scratch", "confidence": .9, "xyxy": [20, 20, 60, 60]}
        for box in ([19.9, 30, 40, 40], [30, 19.9, 40, 40], [30, 30, 60.1, 40],
                    [30, 30, 40, 60.1], [30, 55, 40, 75]):
            crossing = dict(inside, xyxy=box)
            _, kept = draw_ng_detections(np.zeros((100, 100, 3), dtype=np.uint8), [inside, crossing], settings)
            self.assertEqual(kept, [crossing])

    def test_model_predicted_except_cannot_enlarge_manual_area(self):
        settings = {"judge": {"ng_classes": ["scratch"], "min_confidence": .1},
                    "except_regions": [{"box": [.2, .2, .6, .599]}]}
        scratch = {"class_name": "scratch", "confidence": .9, "xyxy": [30, 50, 40, 59.95]}
        prediction = {"class_name": "except", "confidence": .9, "xyxy": [0, 0, 100, 100]}
        _, kept = draw_ng_detections(np.zeros((100, 100, 3), dtype=np.uint8), [scratch, prediction], settings)
        self.assertEqual(kept, [scratch])

    def test_unicode_expected_observed_spaces_and_newlines_visible_as_data(self):
        failure = {"class_name": "OCR_mismatch", "expected_text": "中文 A.\nB", "recognized_text": ["中文A B"]}
        lines = failure_detail_lines([failure])
        self.assertIn('Expected: "中文 A.\\nB"', lines)
        self.assertIn('Read: ["中文A B"]', lines)
        image = np.full((600, 800, 3), 255, dtype=np.uint8)
        output = draw_failure_details(image.copy(), [failure])
        self.assertEqual(output.shape, image.shape)
        self.assertTrue(np.array_equal(output[:300], image[:300]))
        self.assertFalse(np.array_equal(output, image))

    def test_label_shows_code_and_ocr_and_dynamic_format(self):
        failure = {"class_name": "label_pair_1_2_mismatch", "expected_text": "",
                   "expected_format": [{"pattern": ["num", "text"]}],
                   "recognized_text": {"code": ["1L"], "text": [["1I"]]}}
        lines = failure_detail_lines([failure])
        self.assertTrue(any("num" in line and "text" in line for line in lines))
        self.assertIn('Code: ["1L"]', lines)
        self.assertIn('OCR: [["1I"]]', lines)

    def test_no_text_failure_does_not_add_panel(self):
        image = np.zeros((100, 100, 3), dtype=np.uint8)
        self.assertIs(draw_failure_details(image, [{"class_name": "scratch"}]), image)


if __name__ == "__main__":
    unittest.main()
