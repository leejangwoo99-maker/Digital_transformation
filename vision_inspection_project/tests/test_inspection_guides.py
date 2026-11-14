import copy
import json
import tempfile
import tkinter as tk
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from unittest.mock import Mock

import numpy as np
from PIL import Image

from camera.alignment_preview import AlignmentPreview
from camera.inspection_guides import common_frames,guide_layers,projected_guides
from camera.phone_control_gui import PhoneControlWindow
from camera.record_conditions import load_conditions


class GuideTests(unittest.TestCase):
    def test_only_shared_original_frames_are_offered(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = [root/f'frame_{n}.png' for n in range(1,5)]
            for path in paths:
                Image.new('RGB',(200,100)).save(path)
            for kind,numbers in (('OCR',(1,2,4)),('label',(2,3,4))):
                folder = root/'dataset/special_labels/G'/kind
                folder.mkdir(parents=True)
                for number in numbers:
                    (folder/f'{number}.json').write_text(json.dumps(dict(image=str(paths[number-1]),
                        regions=[dict(role='text',box=[.1,.2,.5,.6],text='A')])) )
            settings = dict(root_dir=directory,special_group='G',special_frame_paths=list(map(str,paths)),test_fps=5,group_fps=23)
            frames = common_frames(settings)
            self.assertEqual(list(frames),[2,4])
            self.assertEqual(set(frames[2]),{'OCR','label'})
            self.assertEqual(guide_layers(frames[2])[0]['size'],(200,100))

    def test_original_aspect_ratio_and_rotation_are_preserved(self):
        layers = [dict(kind='OCR',size=(200,100),regions=[dict(role='text',box=[.1,.2,.5,.6])])]
        original = copy.deepcopy(layers)
        points = projected_guides(layers,(400,400))[0]['points']
        np.testing.assert_allclose(points,[[40,140],[200,140],[200,220],[40,220]])
        self.assertEqual(layers,original)
        layers[0]['regions'][0]['angle'] = 90
        rotated = projected_guides(layers,(400,400))[0]['points']
        self.assertAlmostEqual(np.ptp(rotated[:,0]),80)
        self.assertAlmostEqual(np.ptp(rotated[:,1]),160)


class GuideUITests(unittest.TestCase):
    def setUp(self):
        self.root = tk.Tk()
        self.root.withdraw()

    def tearDown(self):
        for timer in self.root.tk.call('after','info'):
            self.root.after_cancel(timer)
        self.root.destroy()

    def test_canvas_guides_hide_restore_without_changing_camera_pixels(self):
        preview = AlignmentPreview(self.root)
        preview.set_inspection_guides([dict(kind='label',size=(200,100),
            regions=[dict(role='qr',box=[.1,.2,.5,.6],number=1)])])
        source = Image.new('RGB',(400,400),'white')
        preview.show(source)
        self.assertEqual(len(preview.find_withtag('inspection-guide')),1)
        preview.show_inspection_guides(False)
        self.assertEqual(len(preview.find_withtag('inspection-guide')),0)
        preview.show(source)
        self.assertEqual(len(preview.find_withtag('inspection-guide')),0)
        preview.show_inspection_guides(True)
        self.assertEqual(len(preview.find_withtag('inspection-guide')),1)
        self.assertEqual(preview.source.tobytes(),source.tobytes())

    def test_live_guide_selection_and_idle_running_visibility(self):
        app = SimpleNamespace(root=self.root,target_fps_var=tk.DoubleVar(value=10),live_test_running=False)
        with tempfile.TemporaryDirectory() as directory:
            image = Path(directory)/'frame.png'
            Image.new('RGB',(200,100),'white').save(image)
            record = dict(image=str(image),regions=[dict(role='text',box=[.1,.2,.5,.6],text='A')])
            data = load_conditions(Path(directory)/'missing.txt',default_fps=10)
            settings = dict(special_group='G',record_condition=str(Path(directory)/'G_record_condition.txt'))
            with patch.object(PhoneControlWindow,'setup_camera'),\
                 patch('camera.live_settings.resolve_live_settings',return_value=(settings,data)),\
                 patch('camera.inspection_guides.common_frames',return_value={23:{'OCR':record,'label':record}}):
                window = PhoneControlWindow(app,live_mode=True)
            try:
                window.show_preview(Image.new('RGB',(400,400),'white'))
                self.assertEqual(window.guide_frame.get(),'Frame 23')
                self.assertEqual(len(window.preview_label.find_withtag('inspection-guide')),2)
                window.busy = True
                window.update_guide_visibility()
                self.assertFalse(window.preview_label.guides_visible)
                self.assertEqual(window.live_zoom_controls.winfo_manager(),'')
                window.busy = False
                window.update_guide_visibility()
                self.assertTrue(window.preview_label.guides_visible)
                self.assertEqual(window.live_zoom_controls.winfo_manager(),'pack')
                self.assertEqual(window.guide_frame.get(),'Frame 23')
                window.guide_frame.set('Off')
                window.select_guide_frame()
                self.assertEqual(window.preview_label.inspection_guides,[])
            finally:
                window.close()

    def test_live_zoom_controls_camera_but_keeps_recording_conditions(self):
        app = SimpleNamespace(root=self.root,target_fps_var=tk.DoubleVar(value=10),live_test_running=False)
        data = load_conditions(default_fps=10)
        original_zoom = data['initial']['zoom']
        settings = dict(special_group='G',record_condition='G_record_condition.txt')
        with patch.object(PhoneControlWindow,'setup_camera'),\
             patch('camera.live_settings.resolve_live_settings',return_value=(settings,data)):
            window = PhoneControlWindow(app,live_mode=True)
        try:
            operations = []
            window.background = lambda operation:operations.append(operation)
            window.client.configure = Mock()
            window.camera_ready(dict(width=200,height=100,zoom_min=1,zoom_max=8))
            self.assertEqual(window.preview_zoom_limits,(1,8))
            operations.pop()()
            window.client.configure.reset_mock()
            window.zoom.set(3.5)
            window.apply_live_zoom()
            operations.pop()()
            self.assertEqual(window.client.configure.call_args.args[0]['zoom'],3.5)
            self.assertEqual(data['initial']['zoom'],original_zoom)
            with patch('camera.live_settings.resolve_live_settings',return_value=(settings,data)):
                saved = window.read_conditions()
            self.assertEqual(saved['initial']['zoom'],original_zoom)
            window.client.configure.reset_mock()
            window.zoom.set(4)
            window.apply_live_zoom()
            window.zoom.set(5)
            window.apply_live_zoom()
            operations.pop(0)()
            window.client.configure.assert_not_called()
            window.busy = True
            operations.pop(0)()
            window.client.configure.assert_not_called()
        finally:
            window.busy = False
            window.close()
