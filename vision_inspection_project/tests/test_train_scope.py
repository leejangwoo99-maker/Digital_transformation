import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock,patch

from video_inspection_gui import VideoInspectionApp


class TrainScopeTests(unittest.TestCase):
    def test_only_selected_model_is_updated(self):
        for scope in ('ALL','open_labeler','conn','label','OCR'):
            app = VideoInspectionApp.__new__(VideoInspectionApp)
            app.root = SimpleNamespace(after=lambda _,fn:fn())
            app.clear_train_log = Mock()
            app.append_train_log = Mock()
            app.status_var = Mock()
            app.active_image_paths = [Path('A_23fps_000000.jpg')]
            app.active_group_name = 'A_23fps'
            app.active_algorithm_name = 'A_23fps_algorith'
            app.active_algorithm_weights = Path('A_23fps_algorith.pt')
            app._has_regular_labels = Mock(return_value=True)
            app._build_algorithm_dataset = Mock(return_value=(Path('data.yaml'),'split'))
            app._train_conn = Mock()
            with patch('video_inspection_gui.train') as appearance, \
                 patch('inspection.text_image_model.train_text_images') as text, \
                 patch('inspection.mark_model.train_marks') as marks, \
                 patch('video_inspection_gui.messagebox.showerror') as errors:
                app._train_worker(scope)
            errors.assert_not_called()
            self.assertEqual(appearance.call_count,int(scope in ('ALL','open_labeler')))
            self.assertEqual(app._train_conn.call_count,int(scope in ('ALL','conn')))
            self.assertEqual(marks.call_count,int(scope in ('ALL','OCR')))
            expected = ['OCR','label'] if scope == 'ALL' else [scope] if scope in ('OCR','label') else []
            self.assertEqual([call.args[2] for call in text.call_args_list],expected)
            self.assertEqual(app._build_algorithm_dataset.call_count,int(scope in ('ALL','open_labeler')))
