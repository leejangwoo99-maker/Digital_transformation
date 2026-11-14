import tempfile
import tkinter as tk
import unittest
from pathlib import Path
from types import SimpleNamespace

import cv2
import numpy as np
from PIL import Image

from appearance_labeler_gui import AppearanceLabelerWindow,render_viewport
from label_images import YoloBoxLabeler


class AppearanceLabelerTests(unittest.TestCase):
    def test_viewport_uses_original_pixels_without_intermediate_low_resolution(self):
        source = Image.fromarray(np.random.default_rng(2).integers(0,256,(300,500,3),np.uint8))
        view = render_viewport(source,(160,100),1,(-80,-40))
        np.testing.assert_array_equal(np.asarray(view),np.asarray(source)[40:140,80:240])
        enlarged = render_viewport(source,(400,300),2,(-200,-100))
        self.assertEqual(enlarged.size,(400,300))
        self.assertEqual(source.size,(500,300))

    def test_native_controls_zoom_edit_save_and_frame_navigation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = tk.Tk()
            root.withdraw()
            ui = None
            try:
                folder = Path(directory)
                paths = [folder/f"sample_{i:06d}.png" for i in range(3)]
                image = np.full((480,640,3),220,np.uint8)
                cv2.putText(image,"SCRATCH",(140,240),cv2.FONT_HERSHEY_SIMPLEX,1,(0,0,0),2)
                for path in paths:
                    cv2.imwrite(str(path),image)
                classes = {0:"ok",1:"scratch",2:"auto_exclude",3:"roi",4:"exclude",5:"except",13:"Essential"}
                c = YoloBoxLabeler(folder,folder,classes,image_paths=paths)
                ui = AppearanceLabelerWindow(c,root)
                root.update()
                self.assertIsInstance(ui.frame_entry,tk.Entry)
                self.assertEqual(c.header_height,0)
                self.assertEqual(ui.photo.width(),ui.canvas.winfo_width())
                np.testing.assert_array_equal(c.image,image)
                ui.class_number.set(1)
                ui.select_class()
                def event(x,y):
                    return SimpleNamespace(x=round(c.offset_x+x*c.scale),y=round(c.offset_y+y*c.scale),state=0)
                ui.mouse(event(200,180),cv2.EVENT_LBUTTONDOWN)
                ui.mouse(event(300,280),cv2.EVENT_LBUTTONUP)
                self.assertTrue(any(box[0] == 1 for box in c.boxes))
                def press(key):
                    ui.key(SimpleNamespace(keysym=key,state=0,widget=ui.canvas))
                press("f")
                self.assertEqual(ui.class_number.get(),13)
                ui.mouse(event(320,180),cv2.EVENT_LBUTTONDOWN)
                ui.mouse(event(420,280),cv2.EVENT_LBUTTONUP)
                self.assertEqual(c.boxes[-1][0],13)
                press("t")
                self.assertEqual(ui.class_number.get(),5)
                press("c")
                self.assertTrue(c.copy_mode)
                press("d")
                self.assertTrue(c.delete_mode)
                ui.key(SimpleNamespace(keysym="Escape",state=0,widget=ui.canvas))
                self.assertTrue(c.edit_mode)
                old_scale = c.scale
                wheel = event(250,240)
                wheel.delta = 120
                ui.wheel(wheel)
                self.assertGreater(c.scale,old_scale)
                ui.frame_number.set("2")
                ui.jump()
                self.assertEqual(c.index,1)
                (folder/f"{paths[1].stem}.txt").write_text("4 0.5 0.5 0.1 0.1\n",encoding="utf-8")
                (folder/f"{paths[2].stem}.txt").write_text("1 0.5 0.5 0.1 0.1\n",encoding="utf-8")
                c.index = 0
                c._load_current_image()
                ui.next_labeled()
                self.assertEqual(c.index,2)
                ui.close()
                self.assertTrue((folder/f"{paths[0].stem}.txt").exists())
            finally:
                if ui is not None and not ui.closed:
                    ui.close()
                root.destroy()
