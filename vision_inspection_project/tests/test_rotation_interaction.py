"""Manual: open each labeler (YOLO, OCR, conn, label), press o/O, enclose
one then several labels, drag the single yellow handle. Centers must stay fixed.
Try partial enclosure, edge rejection, U, zoom/pan, N/P, save/reopen, brackets,
and R ROI in YOLO. Confirm angles survive save and training OBB conversion.
"""
import copy
import math
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

import numpy as np

from rotation_interaction import RotationInteraction
from inspection.rotated_regions import region_polygon, appearance_obb_labels
from label_images import YoloBoxLabeler


class RotationInteractionTests(unittest.TestCase):
    def test_conn_maximum_boundary_is_selected_on_partial_overlap_and_rotates(self):
        self.regions = [{"role":"conn_boundary","box":[.2,.3,.3,.6]}]
        self.interaction.begin((115,100),self.regions,self.shape,1)
        self.interaction.finish((130,130),self.regions,self.shape)
        self.assertEqual(self.interaction.selected,[0])
        moved,point = self.turn(15)
        self.assertTrue(moved)
        self.assertAlmostEqual(self.regions[0]["angle"],15)

    shape = (200, 400)

    def setUp(self):
        self.regions = [{"box": [.2, .3, .3, .6], "angle": 10},
                        {"box": [.6, .3, .7, .6], "angle": 30}]
        self.interaction = RotationInteraction()

    def select(self):
        self.interaction.begin((20, 10), self.regions, self.shape, 1)
        self.interaction.finish((350, 170), self.regions, self.shape)

    def turn(self, degrees):
        _, center, handle = self.interaction.geometry(self.regions, self.shape, 1)
        self.interaction.begin(handle, self.regions, self.shape, 1)
        radians = math.radians(degrees)
        matrix = np.array([[math.cos(radians), -math.sin(radians)],
                           [math.sin(radians), math.cos(radians)]])
        point = center + matrix @ (handle - center)
        return self.interaction.move(point, self.regions, self.shape), point

    def test_group_rotation_uses_each_own_center(self):
        self.select()
        self.assertEqual(self.interaction.selected, [0, 1])
        before = copy.deepcopy(self.regions)
        accepted, point = self.turn(40)
        self.assertTrue(accepted)
        for old, new in zip(before, self.regions):
            self.assertEqual(old["box"], new["box"])
            self.assertAlmostEqual(new["angle"] - old["angle"], 40)
            np.testing.assert_allclose(region_polygon(old, self.shape).mean(0),
                                       region_polygon(new, self.shape).mean(0), atol=1e-5)
        self.assertEqual(self.interaction.finish(point, self.regions, self.shape), {0: 10, 1: 30})

    def test_selection_accepts_partial_rotated_polygon_overlap(self):
        self.interaction.begin((80, 60), self.regions, self.shape, 1)
        self.interaction.finish((120, 120), self.regions, self.shape)
        self.assertEqual(self.interaction.selected, [0])
        self.regions[0]["role"] = "auto_exclude"
        self.select()
        self.assertEqual(self.interaction.selected, [1])

    def test_overlap_selection_accepts_roi_edge_touch_and_reverse_drag(self):
        regions = [{"box":[.2,.3,.3,.6]},
                   {"role":"label_roi","points":[[.5,.3],[.7,.3],[.6,.6]]}]
        for start,end,expected in (((125,90),(120,80),[0]),
                                   ((250,130),(235,100),[1]),
                                   ((350,180),(360,190),[])):
            self.interaction.clear()
            self.interaction.begin(start,regions,self.shape,1)
            self.interaction.finish(end,regions,self.shape)
            self.assertEqual(self.interaction.selected,expected)

    def test_rotation_crossing_frame_is_clipped_not_rejected(self):
        self.regions[1] = {"box": [.0, .1, .1, .9], "angle": 0}
        self.interaction.selected = [0, 1]
        before = copy.deepcopy(self.regions)
        accepted, _ = self.turn(90)
        self.assertTrue(accepted)
        self.assertNotEqual(before,self.regions)
        polygon = region_polygon(self.regions[1],self.shape)
        self.assertTrue(self.regions[1]["clip_to_frame"])
        self.assertTrue((polygon >= 0).all())
        self.assertLessEqual(polygon[:,0].max(),self.shape[1]-1)
        self.assertLessEqual(polygon[:,1].max(),self.shape[0]-1)

    def test_clear_removes_drag_and_selection(self):
        self.select()
        self.turn(20)
        self.interaction.clear()
        self.assertEqual(self.interaction.selected, [])
        self.assertIsNone(self.interaction.baseline)

    def test_yolo_mouse_rotation_save_and_frame_clear(self):
        import cv2
        import json
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            image_path = path / "frame.png"
            cv2.imwrite(str(image_path), np.zeros((*self.shape, 3), np.uint8))
            labeler = YoloBoxLabeler(path, path, {0: "defect", 1: "roi"}, image_paths=[image_path])
            labeler._load_current_image()
            labeler.boxes = [(0, 80, 60, 120, 120), (0, 240, 60, 280, 120)]
            original = list(labeler.boxes)
            labeler._set_mode("rotation")
            def mouse(event, point):
                x, y = np.asarray(point) * labeler.scale + [labeler.offset_x, labeler.offset_y]
                labeler._on_mouse(event, round(x), round(y), 0, None)
            mouse(cv2.EVENT_LBUTTONDOWN, (20, 10))
            mouse(cv2.EVENT_LBUTTONUP, (350, 170))
            self.assertEqual(labeler.rotation_selection.selected, [0, 1])
            _, center, handle = labeler.rotation_selection.geometry(labeler._rotation_regions(), self.shape, labeler.scale)
            mouse(cv2.EVENT_LBUTTONDOWN, handle)
            mouse(cv2.EVENT_LBUTTONUP, center + [40, 0])
            self.assertEqual(original, labeler.boxes)
            self.assertEqual(len(labeler.rotation_undo), 1)
            angles = json.loads((path / "frame.rotation.json").read_text())["angles"]
            self.assertAlmostEqual(angles[0], angles[1])
            self.assertEqual(len(appearance_obb_labels((path / "frame.txt").read_text(), angles, self.shape).splitlines()), 2)
            labeler._load_current_image()
            self.assertEqual(labeler.rotation_selection.selected, [])
            self.assertEqual(len(labeler.rotations), 2)
            labeler._set_mode("roi")
            self.assertTrue(labeler.roi_mode)
            self.assertFalse(labeler.rotation_mode)

    def test_special_modes_rotation_undo_save_and_reload(self):
        import tkinter as tk
        from PIL import Image
        from special_labeler import SpecialLabeler
        root = tk.Tk()
        root.withdraw()
        try:
            with tempfile.TemporaryDirectory() as directory:
                path = Path(directory)
                frame = path / "frame.png"
                Image.new("RGB", (400, 200), "black").save(frame)
                for kind in ("OCR", "conn", "label"):
                    with self.subTest(kind=kind):
                        ui = SpecialLabeler(root, path, "A", kind, [frame])
                        root.update()
                        ui.regions = [dict(region, role="pin_point" if kind == "conn" else "text")
                                      for region in copy.deepcopy(self.regions)]
                        before = copy.deepcopy(ui.regions)
                        ui.key(SimpleNamespace(keysym="Escape", state=0, widget=ui.canvas))
                        def event(point):
                            x, y = np.asarray(point) * ui.scale + ui.offset
                            return SimpleNamespace(x=x, y=y)
                        ui.begin(event((20, 10)))
                        ui.end(event((350, 170)))
                        self.assertEqual(ui.editor.selected, [0, 1])
                        _,handles = ui.editor.geometry(ui.rotation_regions(),self.shape,ui.scale)
                        center,handle = handles["move"],handles["rotate"]
                        ui.begin(event(handle))
                        ui.drag(event(center + [50, 0]))
                        ui.end(event(center + [50, 0]))
                        after = copy.deepcopy(ui.regions)
                        self.assertNotEqual(before, after)
                        for old, new in zip(before, after):
                            self.assertEqual(old["box"], new["box"])
                        self.assertAlmostEqual((after[0]["angle"] - before[0]["angle"]) % 360,
                                               (after[1]["angle"] - before[1]["angle"]) % 360)
                        ui.key(SimpleNamespace(keysym="u", state=0, widget=ui.canvas))
                        self.assertEqual(ui.regions, before)
                        ui.regions = after
                        ui.dirty = True
                        ui.save()
                        ui.load()
                        self.assertEqual(ui.regions, after)
                        self.assertEqual(ui.rotation_selection.selected, [])
                        ui.close()
        finally:
            root.destroy()


if __name__ == "__main__":
    unittest.main()
