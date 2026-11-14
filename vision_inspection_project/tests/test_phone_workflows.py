import json
import os
import tempfile
import threading
import tkinter as tk
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import cv2
import numpy as np

from camera.phone_control_gui import PhoneControlWindow
from camera.record_conditions import condition_files, load_conditions, named_condition_path, save_conditions
from main import inspect_video


class NamedConditionTests(unittest.TestCase):
    def test_automatic_install_precedes_usb_connect(self):
        ready = threading.Event()
        order = []
        window = PhoneControlWindow.__new__(PhoneControlWindow)
        window.connecting = window.connected = window.closed = False
        window.post = lambda callback: callback()
        window.camera_ready = lambda status: (order.append("ready"), ready.set())
        window.connection_failed = lambda message: self.fail(message)
        window.client = SimpleNamespace(connect=lambda: (order.append("connect"), {"ready": True})[1])
        with patch("camera.phone_control_gui.adb", side_effect=lambda *args: order.append("install")):
            window.setup_camera()
            self.assertTrue(ready.wait(3))
        self.assertEqual(order, ["install", "connect", "ready"])

    def test_suffix_and_latest_order(self):
        with tempfile.TemporaryDirectory() as folder:
            directory = Path(folder)
            first = named_condition_path("A", directory)
            second = named_condition_path("B_record_condition.txt", directory)
            self.assertEqual(first.name, "A_record_condition.txt")
            self.assertEqual(second.name, "B_record_condition.txt")
            data = load_conditions(directory / "missing.txt")
            save_conditions(data, first)
            save_conditions(data, second)
            os.utime(first, ns=(100, 100))
            os.utime(second, ns=(200, 200))
            self.assertEqual(condition_files(directory), [first, second])
            for name in ("../bad", "CON", "A:B", ""):
                with self.assertRaises(ValueError):
                    named_condition_path(name, directory)


class PhoneWindowTests(unittest.TestCase):
    def test_actual_capture_clock_excludes_preparation(self):
        window = self.window(live=True)
        window.busy = self.app.live_test_running = True
        with patch('camera.phone_control_gui.time.monotonic',return_value=110.):
            window.refresh_elapsed()
        self.assertEqual(window.result_view.pipeline_time.get(),'pipeline time(sec) : 0.0000000')
        window.capture = SimpleNamespace(started_at=100.,duration=None,count=0,fps=10,
            stop_requested=threading.Event(),finished=threading.Event())
        with patch('camera.phone_control_gui.time.monotonic',return_value=103.25):
            window.refresh_elapsed()
        self.assertEqual(window.result_view.pipeline_time.get(),'pipeline time(sec) : 3.2500000')
        window.busy = self.app.live_test_running = False
        window.capture = None

    def test_repeat_prompt_once_then_automatic_next_and_stop(self):
        window = self.window(live=True)
        window.connected = True
        window.live_settings.update(test_fps=10,group_fps=10)
        with patch.object(window,'read_conditions',return_value=window.conditions), \
             patch('inspection.pipeline_audit.audit_pipeline',return_value={'errors':[]}), \
             patch('camera.phone_control_gui.messagebox.showerror',side_effect=AssertionError('Unexpected settings error')), \
             patch('camera.phone_control_gui.simpledialog.askinteger',return_value=3) as prompt, \
             patch('camera.phone_control_gui.threading.Thread'):
            window.start()
            self.assertEqual(window.repeat_remaining,2)
            self.assertEqual(window.inspection_count,1)
            self.assertIsNone(window.pipeline_started_at)
            window.finish(None,self.path/'result.mp4',None,'now')
            self.assertIsNotNone(window.repeat_after)
            window.window.after_cancel(window.repeat_after)
            window.start_next()
            self.assertEqual(window.repeat_remaining,1)
            self.assertEqual(window.inspection_count,2)
            self.assertEqual(prompt.call_count,1)
            window.stop()
            window.finish(None,self.path/'result.mp4',None,'now')
            self.assertEqual(window.repeat_remaining,0)
            self.assertIsNone(window.repeat_after)
            self.assertEqual(window.start_button['state'],'normal')

    def test_cancel_repeat_prompt_does_not_start(self):
        window = self.window(live=True)
        window.connected = True
        window.live_settings.update(test_fps=10,group_fps=10)
        with patch.object(window,'read_conditions',return_value=window.conditions), \
             patch('inspection.pipeline_audit.audit_pipeline',return_value={'errors':[]}), \
             patch('camera.phone_control_gui.messagebox.showerror',side_effect=AssertionError('Unexpected settings error')), \
             patch('camera.phone_control_gui.simpledialog.askinteger',return_value=None), \
             patch('camera.phone_control_gui.threading.Thread') as worker:
            window.start()
        self.assertEqual(window.inspection_count,0)
        self.assertFalse(window.busy)
        worker.assert_not_called()

    def test_live_timer_and_finish_automatically_enables_next_start(self):
        window = self.window(live=True)
        window.busy = self.app.live_test_running = True
        window.pipeline_started_at = 10.
        window.inspection_count = 2
        window.inspection_count_var.set('Inspection count : 2')
        with patch('camera.phone_control_gui.time.monotonic',return_value=12.3456789):
            window.refresh_elapsed()
        self.assertEqual(window.result_view.pipeline_time.get(),'pipeline time(sec) : 2.3456789')
        window.pipeline_seconds = 3.5
        with patch('camera.phone_control_gui.simpledialog.askstring',side_effect=AssertionError('No naming dialog')):
            window.finish(None,self.path/'result.mp4',None,'timestamp')
        self.assertFalse(window.busy)
        self.assertFalse(self.app.live_test_running)
        self.assertEqual(window.start_button['state'],'normal')
        self.assertEqual(window.result_view.pipeline_time.get(),'pipeline time(sec) : 3.5000000')

    def setUp(self):
        self.root = tk.Tk()
        self.root.withdraw()
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name)
        self.app = SimpleNamespace(root=self.root, target_fps_var=tk.DoubleVar(value=10),
                                   active_group_name=None, live_test_running=False,
                                   trained_frame_groups=lambda: {"A_10fps": []})
        self.setup_patch = patch.object(PhoneControlWindow, "setup_camera")
        self.setup_patch.start()
        self.windows = []

    def tearDown(self):
        for window in self.windows:
            window.close()
        self.setup_patch.stop()
        for timer in self.root.tk.call("after", "info"):
            self.root.after_cancel(timer)
        self.root.destroy()
        self.directory.cleanup()

    def window(self, live=False, profiles=None):
        data = load_conditions(self.path/"missing.txt",default_fps=10)
        settings = {"special_group":"A_10fps","record_condition":str(self.path/"A_record_condition.txt")}
        with patch("camera.phone_control_gui.condition_files", return_value=profiles or []), \
                patch("camera.live_settings.resolve_live_settings",return_value=(settings,data)):
            window = PhoneControlWindow(self.app, live_mode=live)
        self.windows.append(window)
        self.root.update()
        return window

    def test_live_result_is_separate_from_camera_and_keeps_latest_frame(self):
        window = self.window(live=True)
        source = np.zeros((100,100,3),np.uint8)
        window.receive_result(source,1,161,["OCR: PENDING"])
        window.receive_result(np.full_like(source,255),2,161,["OCR: OK"])
        window.refresh_result()
        self.assertIn("2/161",window.result_view.caption.get())
        self.assertIn("OCR: OK",window.result_view.caption.get())
        self.assertEqual(window.result_view.image.getpixel((0,0)),(255,255,255))
        from PIL import Image
        window.show_preview(Image.new("RGB",(100,100),"red"))
        self.assertEqual(window.result_view.image.getpixel((0,0)),(255,255,255))
        self.assertIsNone(window.result_mailbox.take())
        window.receive_result(source,3,161,["OCR: NG"],[dict(class_name="OCR_mismatch",expected_text="ABC",recognized_text=["ABD"])])
        window.refresh_result()
        self.assertIn('ABC',window.result_view.details.get('1.0','end'))
        self.assertEqual(window.result_view.image.size,(100,100))
        window.result_view.change_zoom(2)
        self.assertEqual(window.result_view.zoom,2)
        window.result_view.begin_pan(SimpleNamespace(x=10,y=10))
        window.result_view.drag_pan(SimpleNamespace(x=30,y=30))
        window.result_view.fit()
        self.assertEqual(window.result_view.zoom,1)
        self.assertEqual(window.result_view.pan,[0.,0.])
        window.result_view.show(Image.new('RGB',(720,1280),'white'),4,161,[])
        self.root.update()
        canvas = window.result_view.canvas
        scale = min(canvas.winfo_width()/720,canvas.winfo_height()/1280)
        self.assertAlmostEqual(window.result_view.photo.width(),720*scale,delta=1)
        self.assertAlmostEqual(window.result_view.photo.height(),1280*scale,delta=1)
        window.result_view.reset()
        self.assertEqual(window.result_view.details.get('1.0','end').strip(),'')

    def test_switchbot_settings_roundtrip_add_remove_and_saved_address(self):
        window = self.window()
        window.bot_enabled.set(True)
        window.bot_choices = {'Test Bot [AA:BB:CC:DD:EE:FF]':'AA:BB:CC:DD:EE:FF'}
        window.bot_address.set('Test Bot [AA:BB:CC:DD:EE:FF]')
        window.add_bot_interval(dict(stop=3,go=4))
        window.add_bot_interval(dict(stop=6,go=7))
        data = window.read_conditions()
        self.assertEqual(data['switchbot']['address'],'AA:BB:CC:DD:EE:FF')
        window.apply_conditions(data)
        self.assertEqual(len(window.bot_rows),2)
        self.assertEqual(window.bot_address.get(),'AA:BB:CC:DD:EE:FF')
        self.assertEqual(window.read_conditions()['switchbot'],data['switchbot'])
        window.remove_bot_interval(window.bot_rows[-1])
        self.assertEqual(len(window.read_conditions()['switchbot']['intervals']),1)

    def test_alignment_guides_drag_in_both_phone_modes_without_changing_source(self):
        from PIL import Image
        for live in (False,True):
            window = self.window(live=live)
            image = Image.new("RGB",(320,240),"white")
            before = image.tobytes()
            window.show_preview(image)
            preview = window.preview_label
            left,top,right,bottom = preview.image_bounds
            x,y = (left+right)/2,(top+bottom)/2
            preview.begin_drag(SimpleNamespace(x=x,y=y))
            preview.drag(SimpleNamespace(x=left+.7*(right-left),y=top+.3*(bottom-top)))
            preview.end_drag(None)
            self.assertAlmostEqual(preview.guide_x,.7)
            self.assertAlmostEqual(preview.guide_y,.3)
            preview.begin_drag(SimpleNamespace(x=left+.7*(right-left),y=top+.8*(bottom-top)))
            preview.drag(SimpleNamespace(x=left+.6*(right-left),y=bottom))
            preview.end_drag(None)
            self.assertAlmostEqual(preview.guide_x,.6)
            self.assertAlmostEqual(preview.guide_y,.3)
            preview.begin_drag(SimpleNamespace(x=left+.1*(right-left),y=top+.3*(bottom-top)))
            preview.drag(SimpleNamespace(x=left,y=top+.4*(bottom-top)))
            preview.end_drag(None)
            window.show_preview(image)
            self.assertAlmostEqual(preview.guide_x,.6)
            self.assertAlmostEqual(preview.guide_y,.4)
            self.assertEqual(image.tobytes(),before)
            self.assertEqual(preview.source.tobytes(),before)
            self.assertEqual(preview.itemcget("horizontal","fill"),"#ff2222")

    def test_removed_install_connect_and_latest_selection(self):
        data = load_conditions(self.path / "missing.txt", default_fps=10)
        old, latest = self.path / "old_record_condition.txt", self.path / "new_record_condition.txt"
        save_conditions(data, old)
        data["total_time"] = 12
        save_conditions(data, latest)
        window = self.window(profiles=[old, latest])
        self.assertEqual(window.condition_name.get(), latest.stem)
        self.assertEqual(window.total_time.get(), 12)
        def texts(parent):
            values = []
            for child in parent.winfo_children():
                if "text" in child.keys():
                    values.append(child.cget("text"))
                values.extend(texts(child))
            return values
        self.assertNotIn("install phone app", texts(window.window))
        self.assertNotIn("connect USB", texts(window.window))

    def test_normal_record_cancel_discards_temporary_video(self):
        window = self.window()
        temporary = tempfile.TemporaryDirectory()
        path = Path(temporary.name)
        (path / "recording.mp4").write_bytes(b"temporary")
        window.busy = self.app.live_test_running = True
        with patch("camera.phone_control_gui.filedialog.asksaveasfilename", return_value=""):
            window.finish(temporary, None, None, "now")
        self.assertFalse(path.exists())
        self.assertEqual(window.state.get(), "Recording discarded.")
        self.assertFalse(self.app.live_test_running)

    def test_normal_record_save_uses_chosen_name(self):
        window = self.window()
        temporary = tempfile.TemporaryDirectory()
        (Path(temporary.name) / "recording.mp4").write_bytes(b"video")
        destination = self.path / "chosen.mp4"
        with patch("camera.phone_control_gui.filedialog.asksaveasfilename", return_value=str(destination)):
            window.finish(temporary, None, None, "now")
        self.assertEqual(destination.read_bytes(), b"video")

    def test_live_uses_main_group_without_selection_controls(self):
        window = self.window(live=True)
        self.assertEqual(window.start_button.cget("text"), "start")
        self.assertFalse(hasattr(window,"group_name"))
        self.assertFalse(window.condition_combo.winfo_ismapped())
        with patch("camera.live_settings.resolve_live_settings",side_effect=ValueError("record_condition missing")), \
                self.assertRaisesRegex(ValueError, "record_condition"):
            window.read_conditions()

    def test_profile_overwrite_requires_confirmation(self):
        window = self.window()
        target = self.path / "example_record_condition.txt"
        target.write_text("original")
        with patch("camera.phone_control_gui.simpledialog.askstring", return_value="example"), \
                patch("camera.phone_control_gui.named_condition_path", return_value=target), \
                patch("camera.phone_control_gui.messagebox.askyesno", return_value=False) as question:
            window.save_settings()
        question.assert_called_once()
        self.assertEqual(target.read_text(), "original")


class LiveOutputTests(unittest.TestCase):
    def test_deferred_camera_start_and_both_log_formats(self):
        order = []
        class Capture:
            count = 0
            def isOpened(self): return True
            def get(self, prop):
                return {cv2.CAP_PROP_FPS: 10, cv2.CAP_PROP_FRAME_WIDTH: 32,
                        cv2.CAP_PROP_FRAME_HEIGHT: 24, cv2.CAP_PROP_FRAME_COUNT: 0}.get(prop, 0)
            def read(self):
                self.count += 1
                return (True, np.zeros((24, 32, 3), np.uint8)) if self.count <= 2 else (False, None)
            def release(self): pass
        def detector(settings):
            order.append("model")
            return SimpleNamespace(model=True, predict=lambda frame: ([], frame))
        def factory():
            order.append("capture")
            return Capture()
        special = SimpleNamespace(references={}, annotation_frames={}, check=lambda *args: [], check_frame=lambda *args, **kwargs: [])
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            settings = {"root_dir": str(root), "test_fps": 10, "live_expected_frames": 2,
                        "save_test_details": True, "special_labeled_frames": True,
                        "record_condition": "A_record_condition.txt"}
            with patch("model.yolo_inference.YoloInference", side_effect=detector), \
                    patch("inspection.special_checks.SpecialInspector", return_value=special), \
                    patch("inspection.pipeline_audit.audit_pipeline", return_value={"group": "A_10fps", "messages": [], "errors": []}), \
                    patch("inspection.video_export.finalize_result_video") as finalize:
                output = inspect_video("http://127.0.0.1/usb-phone", settings, root / "test/result_live_test.mp4",
                                       show_window=False, log_dir=root / "logs", file_test_log=True,
                                       capture_factory=factory)
            self.assertEqual(order, ["model", "capture"])
            self.assertTrue(output.exists())
            text = (root / "logs/result_live_test_p.txt").read_text()
            self.assertIn("result_PASS", text)
            self.assertNotIn("playback_file_", text)
            self.assertFalse(list((root / "test").glob("*_playback*.mp4")))
            finalize.assert_called_once_with(output)
            report = json.loads((root / "logs/result_live_test_p.pipeline.json").read_text())
            self.assertEqual(report["record_condition"], "A_record_condition.txt")
            self.assertEqual(report["processed_frames"], 2)


if __name__ == "__main__":
    unittest.main()
