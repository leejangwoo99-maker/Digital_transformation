import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from camera.live_settings import resolve_live_settings
from camera.record_conditions import load_conditions,save_conditions


class LiveGroupSettingsTests(unittest.TestCase):
    def test_group_condition_resolution_and_missing_or_mismatched_errors(self):
        with tempfile.TemporaryDirectory() as directory:
            info = Path(directory)
            settings = {"special_info_dir":str(info),"root_dir":str(info),"test_fps":23}
            app = SimpleNamespace(active_group_name="OCR_23fps",active_image_paths=["one.jpg"],
                settings_for_frame_group=lambda *args:dict(settings))
            with self.assertRaisesRegex(ValueError,"Record condition missing"):
                resolve_live_settings(app)
            path = info/"OCR_record_condition.txt"
            data = load_conditions(info/"missing.txt",default_fps=23)
            save_conditions(data,path)
            with patch("inspection.pipeline_audit.audit_pipeline",return_value={"errors":[]}), \
                    patch("inspection.special_checks.load_reference",return_value=None):
                resolved,_ = resolve_live_settings(app)
                self.assertEqual(resolved["record_condition"],str(path))
                self.assertTrue(resolved["live_photo_inspection"])
                self.assertFalse(resolved["record_confidence_points"])
                self.assertEqual(resolved["live_expected_frames"],230)
                settings.update(group_fps=23,test_fps=10)
                resolved,capture_conditions = resolve_live_settings(app)
                self.assertEqual(resolved['live_expected_frames'],100)
                self.assertEqual(capture_conditions['fps'],23)
                settings.update(test_fps=23)
                data["fps"] = 10
                save_conditions(data,path)
                with self.assertRaisesRegex(ValueError,"must match group FPS"):
                    resolve_live_settings(app)
            app.active_group_name = None
            with self.assertRaisesRegex(ValueError,"Select a frame group"):
                resolve_live_settings(app)
