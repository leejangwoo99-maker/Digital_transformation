import tempfile
import tkinter as tk
import unittest
from pathlib import Path
from types import SimpleNamespace
from tkinter import ttk
from unittest.mock import patch

from PIL import Image, ImageDraw

from inspection.special_checks import labeled_records, load_reference
from special_labeler import SpecialLabeler


def descendants(widget):
    output = []
    for child in widget.winfo_children():
        output.append(child)
        output.extend(descendants(child))
    return output


class SpecialLabelerUITest(unittest.TestCase):
    def test_ocr_and_label_no_longer_offer_indicate(self):
        for kind in ('OCR','label'):
            labeler = self.make_labeler(kind)
            controls = [widget.cget('text') for widget in descendants(labeler.window)
                        if isinstance(widget,(tk.Radiobutton,tk.Button))]
            self.assertIn('Flexible',controls)
            self.assertTrue({'Indicate','Apply frame','Clear start'}.isdisjoint(controls))
            labeler.close()

    def test_flexible_click_toggle_persistence_and_conn_exclusion(self):
        for kind in ('OCR','label','conn'):
            labeler = self.make_labeler(kind)
            radios = [w.cget('text') for w in descendants(labeler.window) if isinstance(w,tk.Radiobutton)]
            self.assertEqual('Flexible' in radios,kind != 'conn')
            if kind != 'conn':
                labeler.regions = [dict(role='text',number=11,text='12',verdict='PASS',box=[.1,.1,.3,.3]),
                                   dict(role='qr',number=1,box=[.5,.5,.7,.7])]
                labeler.select_flexible()
                labeler.begin(self.event_at(labeler,80,80))
                self.assertTrue(labeler.regions[0]['flexible'])
                self.assertEqual(labeler.regions[0]['verdict'],'PASS')
                labeler.begin(self.event_at(labeler,240,240))
                self.assertTrue(labeler.regions[1]['flexible'])
                labeler.save()
                labeler.load()
                self.assertTrue(labeler.regions[0]['flexible'])
                labeler.begin(self.event_at(labeler,80,80))
                self.assertNotIn('flexible',labeler.regions[0])
            labeler.close()

    def test_serial_click_order_toggle_and_persistence(self):
        labeler = self.make_labeler('label')
        labeler.regions = [dict(role='qr',number=1,box=[.1,.1,.3,.3]),
                           dict(role='qr',number=3,box=[.5,.5,.7,.7])]
        labeler.select_serial()
        def click(x,y):
            labeler.begin(SimpleNamespace(x=labeler.offset[0]+x*labeler.image.width*labeler.scale,
                                          y=labeler.offset[1]+y*labeler.image.height*labeler.scale))
        click(.6,.6)
        click(.2,.2)
        self.assertEqual([r['serial_order'] for r in labeler.regions],[2,1])
        click(.6,.6)
        self.assertNotIn('serial_order',labeler.regions[1])
        labeler.save()
        labeler.load()
        self.assertEqual(labeler.regions[0]['serial_order'],2)
        labeler.close()

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.directory = Path(self.temporary.name)
        self.image_path = self.directory / "A_000000.jpg"
        image = Image.new("RGB", (400, 400), "white")
        ImageDraw.Draw(image).rectangle((120, 100, 280, 300), fill=(30, 30, 30))
        image.save(self.image_path)
        self.root = tk.Tk()
        self.root.withdraw()

    def tearDown(self):
        self.root.destroy()
        self.temporary.cleanup()

    def make_labeler(self, kind):
        labeler = SpecialLabeler(self.root, self.directory, "A", kind, [self.image_path])
        self.root.update()
        # Legacy creation-tool tests explicitly enter drawing mode.
        labeler.mode = "draw"
        return labeler

    def test_multiple_inspection_range_controls_only_ocr_and_label(self):
        for kind in ('OCR','label','conn'):
            with patch('inspection.photo_inspection.load_photo_frame_ranges',return_value=[(1,1)]):
                labeler = self.make_labeler(kind)
            buttons = [w.cget('text') for w in descendants(labeler.window) if isinstance(w,tk.Button)]
            self.assertEqual('+ range' in buttons,kind != 'conn')
            if kind != 'conn':
                labeler.paths = [self.image_path]*10
                labeler.add_inspection_range(4,8)
                with patch('inspection.photo_inspection.save_photo_frame_ranges') as save:
                    labeler.save_inspection_time()
                    self.assertEqual(save.call_args.args[-1],[(1.,1.),(4.,8.)])
                labeler.remove_inspection_range(labeler.inspection_rows[0])
                self.assertEqual(len(labeler.inspection_rows),1)
                labeler.remove_inspection_range(labeler.inspection_rows[0])
                self.assertEqual(len(labeler.inspection_rows),1)
            labeler.close()

    def test_delete_inner_box_keeps_surrounding_box(self):
        for kind in ('OCR','label','conn'):
            labeler = self.make_labeler(kind)
            role = 'slot' if kind == 'conn' else 'text'
            outer = {'role':role,'box':[.1,.1,.9,.9]}
            inner = {'role':role,'box':[.4,.4,.5,.5]}
            labeler.regions = [outer,inner]
            labeler.mode = 'delete'
            labeler.begin(self.event_at(labeler,150,150))
            labeler.end(self.event_at(labeler,210,210))
            self.assertEqual(labeler.regions,[outer])
            labeler.close()

    def test_copy_inner_box_does_not_select_surrounding_box_or_roi(self):
        for kind in ('OCR','label','conn'):
            labeler = self.make_labeler(kind)
            role = 'slot' if kind == 'conn' else 'text'
            outer = {'role':role,'box':[.1,.1,.9,.9]}
            inner = {'role':role,'box':[.4,.4,.5,.5]}
            labeler.regions = [outer,inner]
            labeler.rois = [{'role':'label_roi','points':[[.1,.1],[.9,.1],[.9,.9],[.1,.9]]}]
            labeler.mode = 'copy'
            labeler.begin(self.event_at(labeler,150,150))
            labeler.end(self.event_at(labeler,210,210))
            self.assertEqual(labeler.clipboard,[inner])
            self.assertEqual(labeler.clipboard_rois,[])
            labeler.close()

    def test_appearance_copy_uses_rotated_border_not_containment(self):
        import numpy as np
        from label_images import YoloBoxLabeler
        labeler = YoloBoxLabeler.__new__(YoloBoxLabeler)
        labeler.image = np.zeros((400,400,3),np.uint8)
        outer,inner = (1,40,40,360,360),(2,160,160,200,200)
        labeler.boxes = [outer,inner]
        labeler.rotations = {inner:15}
        labeler.auto_exclude_class_id = None
        labeler._copy_overlapping_boxes(145,145,215,215)
        self.assertEqual(labeler.clipboard_boxes,[inner])
        self.assertEqual(labeler.clipboard_rotations,{inner:15})

    def test_reference_area_can_be_drawn_saved_and_edited_for_all_kinds(self):
        for kind in ("OCR","label","conn"):
            labeler = self.make_labeler(kind)
            labeler.select_reference_area()
            labeler.begin(self.event_at(labeler,100,100))
            labeler.drag(self.event_at(labeler,220,220))
            labeler.end(self.event_at(labeler,220,220))
            regions = [r for r in labeler.regions if r["role"] == "registration"]
            self.assertEqual(len(regions),1)
            for actual,expected in zip(regions[0]["box"],[.25,.25,.55,.55]):
                self.assertAlmostEqual(actual,expected)
            labeler.save()
            labeler.load()
            self.assertEqual(labeler.regions[0]["role"],"registration")
            labeler.close()

    def test_default_editor_never_creates_and_resizes_from_opposite_side_without_o(self):
        import copy
        import numpy as np
        from inspection.rotated_regions import region_polygon
        for kind in ("OCR","label","conn"):
            labeler = SpecialLabeler(self.root,self.directory,"edit_"+kind,kind,[self.image_path])
            self.root.update()
            self.assertEqual(labeler.mode,"edit")
            labeler.regions = []
            labeler.rois = []
            labeler.begin(self.event_at(labeler,70,70))
            labeler.end(self.event_at(labeler,250,250))
            self.assertEqual(labeler.regions,[])
            labeler.regions = [{"role":"pin_point" if kind == "conn" else "text","text":"AB","box":[.2,.3,.3,.5]},
                {"role":"pin" if kind == "conn" else "text","text":"CD","shape":"circle" if kind == "conn" else "rectangle","box":[.5,.3,.6,.4]}]
            labeler.rois = [{"role":"label_roi","points":[[.7,.3],[.8,.3],[.75,.5]]}]
            original = copy.deepcopy(labeler.data())
            labeler.begin(self.event_at(labeler,60,80))
            labeler.end(self.event_at(labeler,340,240))
            self.assertEqual(labeler.editor.selected,[0,1,2])
            labeler.key(SimpleNamespace(keysym="o",state=0))
            self.assertEqual(labeler.mode,"edit")
            _,handles = labeler.editor.geometry(labeler.rotation_regions(),(400,400),labeler.scale)
            handle = handles["e"]
            labeler.begin(self.event_at(labeler,*handle))
            labeler.end(self.event_at(labeler,handle[0]+12,handle[1]))
            for before,after in zip(original["regions"]+original["rois"],labeler.rotation_regions()):
                a,b = region_polygon(before,(400,400)),region_polygon(after,(400,400))
                self.assertGreater(((b.min(0)+b.max(0))/2)[0],((a.min(0)+a.max(0))/2)[0])
                self.assertGreater(np.ptp(b[:,0]),np.ptp(a[:,0]))
            labeler.key(SimpleNamespace(keysym="u",state=0))
            self.assertEqual(labeler.regions,original["regions"])
            self.assertEqual(labeler.rois,original["rois"])
            labeler.key(SimpleNamespace(keysym="e",state=0))
            labeler.begin(self.event_at(labeler,20,20))
            labeler.key(SimpleNamespace(keysym="Escape",state=0))
            labeler.end(self.event_at(labeler,40,40))
            self.assertEqual(labeler.mode,"edit")
            self.assertEqual(labeler.regions,original["regions"])
            labeler.close()

    def test_escape_cancels_tools_and_restores_in_progress_translation(self):
        import copy
        for kind in ("OCR","conn","label"):
            labeler = self.make_labeler(kind)
            labeler.regions = [{"role":"slot","box":[.3,.3,.5,.5]}]
            before = copy.deepcopy(labeler.regions)
            labeler.translation.selected = [0]
            labeler.begin(self.event_at(labeler,160,160))
            labeler.drag(self.event_at(labeler,190,180))
            self.assertNotEqual(labeler.regions,before)
            labeler.key(SimpleNamespace(keysym="Escape",state=0))
            self.assertEqual(labeler.regions,before)
            for key in ("c","d","e","o"):
                labeler.key(SimpleNamespace(keysym=key,state=0))
                labeler.key(SimpleNamespace(keysym="Escape",state=0))
                self.assertEqual(labeler.mode,"edit")
                self.assertIsNone(labeler.start)
                self.assertTrue(labeler.window.winfo_exists())
            labeler.close()

    def test_exclude_drag_is_purple_persists_and_next_button_skips_exclude_only(self):
        from inspection.special_checks import write_json
        for kind in ("OCR","label","conn"):
            paths = [self.directory/f"nav_{kind}_{i:06d}.jpg" for i in range(4)]
            for path in paths:
                Image.open(self.image_path).save(path)
            labeler = SpecialLabeler(self.root,self.directory,f"nav_{kind}",kind,paths)
            self.root.update()
            labeler.key(SimpleNamespace(keysym="e",state=0))
            labeler.begin(self.event_at(labeler,80,80))
            labeler.drag(self.event_at(labeler,120,120))
            self.assertEqual(labeler.canvas.itemcget(labeler.canvas.find_withtag("drag")[-1],"outline"),"#b040ff")
            labeler.end(self.event_at(labeler,120,120))
            excluded = next(r for r in labeler.regions if r["role"] == "exclude")
            self.assertNotIn("number",excluded)
            labeler.save()
            labeler.load()
            self.assertTrue(any(r["role"] == "exclude" for r in labeler.regions))
            write_json(labeler.directory/f"{paths[1].stem}.json",{"regions":[excluded]})
            write_json(labeler.directory/f"{paths[3].stem}.json",{
                "regions":[{"role":"pin_point" if kind == "conn" else "text","text":"A","box":[.3,.3,.4,.4]}]})
            button = next(w for w in descendants(labeler.window) if isinstance(w,tk.Button) and w.cget("text") == ">>")
            button.invoke()
            self.assertEqual(labeler.index,3)
            button.invoke()
            self.assertEqual(labeler.index,3)
            labeler.close()

    def test_center_handle_moves_selected_group_for_each_special_kind_and_undo(self):
        import copy
        for kind in ("OCR","label","conn"):
            labeler = self.make_labeler(kind)
            labeler.regions = [{"role":"pin_point" if kind == "conn" else "text",
                "box":[.3,.3,.5,.5],"text":"A","angle":5},
                {"role":"slot" if kind == "conn" else "mark" if kind == "OCR" else "text",
                 "box":[.6,.3,.8,.5],"text":"B"}]
            original = copy.deepcopy(labeler.regions)
            labeler.draw()
            for x,shift in ((130,False),(270,True)):
                event = self.event_at(labeler,x,140)
                event.state = int(shift)
                labeler.begin(event)
                labeler.end(event)
            self.assertEqual(labeler.translation.selected,[0,1])
            _,center = labeler.translation.geometry(labeler.regions,(400,400))
            labeler.begin(self.event_at(labeler,*center))
            labeler.end(self.event_at(labeler,center[0]+20,center[1]+30))
            for before,after in zip(original,labeler.regions):
                self.assertAlmostEqual(after["box"][0]-before["box"][0],.05,places=5)
                self.assertAlmostEqual(after["box"][1]-before["box"][1],.075,places=5)
            labeler.key(SimpleNamespace(keysym="u",state=0))
            self.assertEqual(labeler.regions,original)
            labeler.close()

    def test_frame_jump_saves_current_labels_for_all_special_modes(self):
        second = self.directory / "A_000001.jpg"
        Image.open(self.image_path).save(second)
        for kind in ("OCR", "conn", "label"):
            labeler = SpecialLabeler(self.root, self.directory, "A", kind, [self.image_path, second])
            labeler.regions.append({"role": "slot" if kind == "conn" else "text",
                                    "box": [0.3, 0.3, 0.5, 0.5], "text": "A"})
            labeler.dirty = True
            labeler.frame_number.set("2")
            self.assertEqual(labeler.jump_to_frame(), "break")
            self.assertEqual(labeler.index, 1)
            self.assertTrue((labeler.directory / "A_000000.json").exists())
            labeler.close()

    def test_auto_exclude_only_frames_not_saved_for_each_kind(self):
        for kind in ("OCR", "conn", "label"):
            labeler = self.make_labeler(kind)
            self.assertEqual(labeler.regions, [])
            self.assertFalse(any(isinstance(widget, tk.Button) and widget.cget("text") == "use for test"
                                 for widget in descendants(labeler.window)))
            labeler.close()
            self.assertEqual(labeled_records(self.directory, "A", kind), [])

    def event_at(self, labeler, x, y):
        return SimpleNamespace(x=labeler.offset[0] + x * labeler.scale,
                               y=labeler.offset[1] + y * labeler.scale)

    def test_manual_and_shortcuts_for_all_three_modes(self):
        for kind in ("OCR", "conn", "label"):
            labeler = self.make_labeler(kind)
            manual = labeler.manual.cget("text")
            for text in ("Ctrl+V=paste", "arrows=pan", "space=play/pause", "d then drag=delete", "c=copy"):
                self.assertIn(text, manual)
            before = list(labeler.offset)
            labeler.key(SimpleNamespace(keysym="Right", state=0))
            self.assertEqual(labeler.offset[0], before[0] - 80)
            zoom = labeler.zoom
            labeler.wheel(SimpleNamespace(x=200, y=200, delta=120))
            self.assertGreater(labeler.zoom, zoom)
            labeler.wheel(SimpleNamespace(x=200, y=200, delta=-120))
            self.assertAlmostEqual(labeler.zoom, zoom)
            region = {"role": "pin_point" if kind == "conn" else "text", "box": [0.4, 0.4, 0.6, 0.6], "text": "ABC"}
            labeler.regions = [region]
            labeler.key(SimpleNamespace(keysym="C", state=0))
            labeler.begin(self.event_at(labeler, 150, 150))
            labeler.end(self.event_at(labeler, 250, 250))
            self.assertEqual(labeler.clipboard, [region])
            labeler.paste()
            self.assertEqual(len(labeler.regions), 2)
            labeler.key(SimpleNamespace(keysym="U", state=0))
            self.assertEqual(len(labeler.regions), 1)
            labeler.key(SimpleNamespace(keysym="D", state=0))
            labeler.begin(self.event_at(labeler, 150, 150))
            labeler.end(self.event_at(labeler, 250, 250))
            self.assertEqual(labeler.regions, [])
            labeler.playing = True
            labeler.tick()
            self.assertFalse(labeler.playing)
            self.assertEqual(labeler.index, 0)
            labeler.close()

    def test_pin_rectangle_and_circle_area_show_pass_and_save(self):
        labeler = self.make_labeler("conn")
        self.assertEqual(labeler.role.get(), "pin_point")
        radios = [widget.cget("text") for widget in descendants(labeler.window) if isinstance(widget, tk.Radiobutton)]
        self.assertEqual(radios, ["pin point", "pin area", "slot"])
        labeler.begin(self.event_at(labeler, 180, 190))
        labeler.end(self.event_at(labeler, 220, 210))
        self.assertIn("FAIL", labeler.info.get())
        labeler.key(SimpleNamespace(keysym="2", state=0))
        event = self.event_at(labeler, 200, 200)
        labeler.begin(event)
        labeler.drag(self.event_at(labeler, 230, 200))
        with patch("special_labeler.simpledialog.askfloat", return_value=60):
            labeler.end(self.event_at(labeler, 230, 200))
        pin = labeler.regions[-1]
        self.assertEqual(pin["role"], "pin")
        self.assertEqual(pin["shape"], "circle")
        self.assertAlmostEqual(pin["box"][0], .425)
        self.assertAlmostEqual(pin["box"][2], .575)
        self.assertIn("Conn PASS", labeler.info.get())
        self.assertTrue(any(labeler.canvas.type(item) == "oval" for item in labeler.canvas.find_all()))
        labeler.close()
        self.assertEqual(load_reference(self.directory, "A", "conn")["regions"][-1], pin)

    def test_conn_selection_copies_points_and_areas_together_on_release(self):
        import copy
        from inspection.conn_geometry import circle_area
        labeler = self.make_labeler("conn")
        labeler.regions = [{"role": "pin_point", "box": [.45, .45, .55, .55]},
                           circle_area((.5, .5), (.6, .5), (400, 400))]
        original = copy.deepcopy(labeler.regions)
        buttons = [widget.cget("text") for widget in descendants(labeler.window) if isinstance(widget, tk.Button)]
        self.assertNotIn("copy (C)", buttons)
        self.assertNotIn("paste", buttons)
        labeler.key(SimpleNamespace(keysym="C", state=0))
        labeler.begin(self.event_at(labeler, 140, 140))
        labeler.end(self.event_at(labeler, 260, 260))
        self.assertEqual(labeler.copy_selected, [0, 1])
        labeler.begin(self.event_at(labeler, 200, 200))
        labeler.drag(self.event_at(labeler, 280, 215))
        self.assertEqual(labeler.regions, original)
        labeler.end(self.event_at(labeler, 280, 215))
        self.assertEqual(labeler.regions[:2], original)
        self.assertEqual(len(labeler.regions), 4)
        for old, new in zip(original, labeler.regions[2:]):
            self.assertAlmostEqual(new["box"][0]-old["box"][0], .2)
            self.assertEqual(new["box"][1], old["box"][1])
            self.assertEqual(old["role"], new["role"])
        labeler.key(SimpleNamespace(keysym="u", state=0))
        self.assertEqual(labeler.regions, original)
        labeler.close()

    def test_m_boundary_is_red_persists_and_requires_two_outside_centers(self):
        from inspection.conn_geometry import circle_area
        labeler = self.make_labeler("conn")
        labeler.regions = []
        for x in (90, 200, 310):
            labeler.regions.append({"role": "pin_point", "box": [(x-5)/400, .4875, (x+5)/400, .5125]})
            labeler.regions.append(circle_area((x/400, .5), ((x+15)/400, .5), (400, 400)))
        labeler.key(SimpleNamespace(keysym="M", state=0))
        labeler.begin(self.event_at(labeler, 100, 100))
        labeler.end(self.event_at(labeler, 300, 300))
        self.assertIn("Conn FAIL", labeler.info.get())
        self.assertIn("outside boundary: 2", labeler.info.get())
        self.assertTrue(any(labeler.canvas.itemcget(item, "outline") == "#ff2222"
                            for item in labeler.canvas.find_all() if labeler.canvas.type(item) == "rectangle"))
        labeler.key(SimpleNamespace(keysym="m", state=0))
        labeler.begin(self.event_at(labeler, 100, 100))
        labeler.end(self.event_at(labeler, 390, 300))
        self.assertIn("Conn PASS", labeler.info.get())
        self.assertIn("outside boundary: 1", labeler.info.get())
        self.assertEqual(sum(region["role"] == "conn_boundary" for region in labeler.regions), 1)
        labeler.close()
        reopened = self.make_labeler("conn")
        self.assertEqual(sum(region["role"] == "conn_boundary" for region in reopened.regions), 1)
        reopened.close()

    def test_pin_point_axis_copy_and_c_paste(self):
        labeler = self.make_labeler("conn")
        point = {"role": "pin_point", "box": [.45, .475, .55, .525]}
        labeler.regions = [point]
        for x, y in ((280, 215), (215, 280)):
            labeler.begin(self.event_at(labeler, 200, 200))
            labeler.end(self.event_at(labeler, x, y))
            box = labeler.regions[-1]["box"]
            self.assertAlmostEqual(box[2]-box[0], .1)
            self.assertAlmostEqual(box[3]-box[1], .05)
            self.assertAlmostEqual((box[1]+box[3])/2 if x > y else (box[0]+box[2])/2, .5)
        event = self.event_at(labeler, 200, 200)
        with patch.object(labeler.canvas, "winfo_pointerx", return_value=labeler.canvas.winfo_rootx()+event.x), \
                patch.object(labeler.canvas, "winfo_pointery", return_value=labeler.canvas.winfo_rooty()+event.y):
            labeler.key(SimpleNamespace(keysym="C", state=0))
        self.assertEqual(labeler.clipboard, [point])
        labeler.paste()
        self.assertEqual(len(labeler.regions), 4)
        self.assertEqual(labeler.regions[-1], point)
        self.assertIsNot(labeler.regions[-1], point)
        labeler.close()

    def test_pin_area_snaps_to_nearby_point_and_uses_pixel_diameter(self):
        labeler = self.make_labeler("conn")
        labeler.regions = [{"role": "pin_point", "box": [.45, .45, .55, .55]}]
        labeler.role.set("pin")
        labeler.begin(self.event_at(labeler, 203, 202))
        with patch("special_labeler.simpledialog.askfloat", return_value=40) as dialog:
            labeler.end(self.event_at(labeler, 240, 220))
        dialog.assert_called_once()
        area = labeler.regions[-1]
        self.assertAlmostEqual((area["box"][0]+area["box"][2])/2, .5)
        self.assertAlmostEqual((area["box"][1]+area["box"][3])/2, .5)
        self.assertAlmostEqual((area["box"][2]-area["box"][0])*400, 40)
        labeler.close()

    def test_removed_gap_controls_and_legacy_conditions_are_inactive(self):
        from inspection.conn_geometry import circle_area, save_gap_limits, load_gap_limits
        labeler = self.make_labeler("conn")
        save_gap_limits(labeler.directory, {"max_horizontal_px": 1, "max_vertical_px": 1})
        labeler.regions = [{"role": "pin_point", "box": [.35, .45, .45, .55]},
                           {"role": "pin_point", "box": [.55, .45, .65, .55]},
                           circle_area((.4, .5), (.45, .5), (400, 400)),
                           circle_area((.6, .5), (.65, .5), (400, 400))]
        labeler.draw()
        self.assertIn("Conn PASS", labeler.info.get())
        texts = [widget.cget("text") for widget in descendants(labeler.window) if "text" in widget.keys()]
        self.assertFalse(any("max horizontal gap" in text or "max vertical gap" in text for text in texts))
        labeler.close()
        reopened = self.make_labeler("conn")
        self.assertFalse(hasattr(reopened, "gap_vars"))
        self.assertEqual(load_gap_limits(reopened.directory)["max_horizontal_px"], 1)
        reopened.close()

    def test_c_selection_copies_m_boundary_together_with_pins_and_areas(self):
        import copy
        from inspection.conn_geometry import circle_area
        labeler = self.make_labeler("conn")
        labeler.regions = [{"role": "pin_point", "box": [.45, .45, .55, .55]},
                           circle_area((.5, .5), (.6, .5), (400, 400)),
                           {"role": "conn_boundary", "box": [.2, .2, .7, .7]}]
        original = copy.deepcopy(labeler.regions)
        labeler.key(SimpleNamespace(keysym="C", state=0))
        labeler.begin(self.event_at(labeler, 70, 70))
        labeler.end(self.event_at(labeler, 290, 290))
        self.assertEqual(labeler.copy_selected, [0, 1, 2])
        labeler.begin(self.event_at(labeler, 200, 200))
        labeler.drag(self.event_at(labeler, 280, 210))
        self.assertEqual(labeler.regions, original)
        labeler.end(self.event_at(labeler, 280, 210))
        self.assertEqual(labeler.regions[:3], original)
        self.assertEqual(len(labeler.regions), 6)
        self.assertEqual(labeler.regions[-1]["role"], "conn_boundary")
        for before, after in zip(original, labeler.regions[3:]):
            self.assertAlmostEqual(after["box"][0]-before["box"][0], .2)
            self.assertEqual(after["box"][1], before["box"][1])
        labeler.close()
        reopened = self.make_labeler("conn")
        self.assertEqual(sum(region["role"] == "conn_boundary" for region in reopened.regions), 2)
        reopened.close()

    def test_c_selection_copies_m_boundary_alone(self):
        labeler = self.make_labeler("conn")
        labeler.regions = [{"role": "conn_boundary", "box": [.2, .2, .7, .7]}]
        labeler.key(SimpleNamespace(keysym="c", state=0))
        labeler.begin(self.event_at(labeler, 70, 70))
        labeler.end(self.event_at(labeler, 290, 290))
        self.assertEqual(labeler.copy_selected, [0])
        labeler.begin(self.event_at(labeler, 180, 180))
        labeler.end(self.event_at(labeler, 185, 250))
        self.assertEqual(len(labeler.regions), 2)
        self.assertEqual(labeler.regions[0]["box"][0], labeler.regions[1]["box"][0])
        self.assertAlmostEqual(labeler.regions[1]["box"][1] - labeler.regions[0]["box"][1], .175)
        labeler.close()

    def test_m_boundary_partial_overlap_and_edge_touch_are_selected(self):
        labeler = self.make_labeler("conn")
        labeler.regions = [{"role": "pin_point", "box": [.45, .45, .55, .55]},
                           {"role": "conn_boundary", "box": [.2, .2, .7, .7]}]
        for start, end, expected in (((260, 260), (300, 300), [1]),
                                     ((280, 260), (300, 300), [1]),
                                     ((290, 290), (320, 320), []),
                                     ((170, 170), (205, 205), [0])):
            labeler.copy_selected = []
            labeler.key(SimpleNamespace(keysym="c", state=0))
            labeler.begin(self.event_at(labeler, *start))
            labeler.end(self.event_at(labeler, *end))
            self.assertEqual(labeler.copy_selected, expected)
        labeler.close()

    def test_slot_draw_partial_overlap_copy_and_reload(self):
        labeler = self.make_labeler("conn")
        labeler.regions = []
        labeler.key(SimpleNamespace(keysym="3", state=0))
        self.assertEqual(labeler.role.get(), "slot")
        labeler.begin(self.event_at(labeler, 80, 80))
        labeler.end(self.event_at(labeler, 280, 280))
        self.assertEqual(labeler.regions[0]["role"], "slot")
        labeler.key(SimpleNamespace(keysym="c", state=0))
        labeler.begin(self.event_at(labeler, 270, 270))
        labeler.end(self.event_at(labeler, 300, 300))
        self.assertEqual(labeler.copy_selected, [0])
        labeler.begin(self.event_at(labeler, 180, 180))
        labeler.end(self.event_at(labeler, 220, 190))
        self.assertEqual(len(labeler.regions), 2)
        self.assertEqual(labeler.regions[1]["role"], "slot")
        self.assertEqual(labeler.regions[0]["box"][1], labeler.regions[1]["box"][1])
        labeler.save()
        labeler.load()
        self.assertEqual(sum(r["role"] == "slot" for r in labeler.regions), 2)
        labeler.close()

    def test_circle_area_axis_copy_preserves_diameter_and_can_undo(self):
        labeler = self.make_labeler("conn")
        labeler.role.set("pin")
        original = {"role": "pin_point", "box": [0.45, 0.475, 0.55, 0.525]}
        labeler.regions = [original]
        labeler.draw()
        labeler.begin(self.event_at(labeler, 200, 200))
        with patch("special_labeler.simpledialog.askfloat", return_value=60):
            labeler.end(self.event_at(labeler, 200, 200))
        for x, y in ((280, 215), (215, 280)):
            labeler.begin(self.event_at(labeler, 200, 200))
            labeler.end(self.event_at(labeler, x, y))
            box = labeler.regions[-1]["box"]
            self.assertAlmostEqual(box[2] - box[0], .15)
            self.assertAlmostEqual(box[3] - box[1], .15)
            if x > y:
                self.assertAlmostEqual((box[1]+box[3])/2, .5)
            else:
                self.assertAlmostEqual((box[0]+box[2])/2, .5)
            self.assertEqual(labeler.regions[0], original)
        labeler.key(SimpleNamespace(keysym="u", state=0))
        self.assertEqual(len(labeler.regions), 3)
        labeler.close()

    def test_circle_can_be_drawn_without_pin_point_and_zero_radius_is_ignored(self):
        labeler = self.make_labeler("conn")
        labeler.role.set("pin")
        original = {"role": "pin_point", "box": [0.45, 0.475, 0.55, 0.525]}
        labeler.regions = [original]
        labeler.draw()
        event = self.event_at(labeler, 200, 200)
        labeler.begin(event)
        with patch("special_labeler.simpledialog.askfloat", return_value=None):
            labeler.end(event)
        self.assertEqual(len(labeler.regions), 1)
        labeler.begin(event)
        with patch("special_labeler.simpledialog.askfloat", return_value=400):
            labeler.end(self.event_at(labeler, 600, 210))
        box = labeler.regions[-1]["box"]
        self.assertAlmostEqual(box[2], 1)
        self.assertAlmostEqual(box[2] - box[0], 1)
        labeler.regions = []
        labeler.begin(self.event_at(labeler, 100, 100))
        labeler.end(self.event_at(labeler, 130, 100))
        self.assertEqual(len(labeler.regions), 1)
        self.assertEqual(labeler.regions[0]["shape"], "circle")
        self.assertAlmostEqual(sum(labeler.regions[0]["box"][::2]) / 2, .25)
        self.assertIn("Conn FAIL", labeler.info.get())
        labeler.close()

    def test_circle_away_from_pin_center_shows_fail_then_pass(self):
        labeler = self.make_labeler("conn")
        labeler.regions = [{"role": "pin_point", "box": [.45, .45, .55, .55]}]
        labeler.role.set("pin")
        labeler.begin(self.event_at(labeler, 250, 200))
        labeler.end(self.event_at(labeler, 270, 200))
        self.assertIn("Conn FAIL", labeler.info.get())
        labeler.regions = [region for region in labeler.regions if region["role"] == "pin_point"]
        labeler.begin(self.event_at(labeler, 250, 200))
        labeler.end(self.event_at(labeler, 310, 200))
        self.assertIn("Conn PASS", labeler.info.get())
        self.assertEqual(len(labeler.regions), 2)
        labeler.close()

    def test_ocr_double_click_edits_text_and_escape_saves_latest_value(self):
        labeler = self.make_labeler("OCR")
        labeler.remember()
        labeler.regions.append({"role": "text", "box": [0.4, 0.4, 0.6, 0.6], "text": "old_secret_text"})
        labeler.draw()
        visible_text = [labeler.canvas.itemcget(item, "text") for item in labeler.canvas.find_all()
                        if labeler.canvas.type(item) == "text"]
        self.assertFalse(any("old_secret_text" in text for text in visible_text))
        def update_dialog():
            dialog = next(widget for widget in labeler.window.winfo_children() if isinstance(widget, tk.Toplevel))
            entry = next(widget for widget in descendants(dialog) if isinstance(widget, tk.Text))
            self.assertEqual(entry.get("1.0", "end-1c"), "old_secret_text")
            entry.delete("1.0", "end")
            entry.insert("1.0", "new_secret_text\nA")
            next(widget for widget in descendants(dialog) if isinstance(widget, tk.Button) and widget.cget("text") == "OK").invoke()
        self.root.after(50, update_dialog)
        event = SimpleNamespace(x=labeler.offset[0] + 200 * labeler.scale,
                                y=labeler.offset[1] + 200 * labeler.scale)
        labeler.begin(event)
        labeler.end(event)
        self.assertEqual(labeler.translation.selected,[len(labeler.regions)-1])
        labeler.add_roi_vertex(event)
        self.assertEqual(labeler.regions[-1]["text"], "new_secret_text\nA")
        labeler.key(SimpleNamespace(keysym="Escape", state=0))
        self.assertTrue(labeler.window.winfo_exists())
        labeler.close()
        self.assertEqual(load_reference(self.directory, "A", "OCR")["regions"][-1]["text"], "new_secret_text\nA")

    def test_label_check_dialog_persists_two_position_pattern(self):
        labeler = self.make_labeler("label")
        def configure_dialog():
            dialog = next(widget for widget in labeler.window.winfo_children() if isinstance(widget, tk.Toplevel))
            widgets = descendants(dialog)
            next(widget for widget in widgets if isinstance(widget, tk.Checkbutton)).invoke()
            digits = next(widget for widget in widgets if isinstance(widget, tk.Spinbox))
            self.assertEqual(digits.get(), "2")
            choices = [widget for widget in widgets if isinstance(widget, ttk.Combobox)]
            self.assertEqual(len(choices), 2)
            choices[1].set("text")
            entry = next(widget for widget in widgets if isinstance(widget, tk.Entry))
            entry.insert(0,"1L")
            next(widget for widget in widgets if isinstance(widget, tk.Button) and widget.cget("text") == "OK").invoke()
        self.root.after(50, configure_dialog)
        result = labeler.enter_text()
        self.assertTrue(result["check"])
        self.assertEqual(result["pattern"], ["num", "text"])
        self.assertEqual(result["refer"],"1L")
        labeler.close()

    def test_ocr_check_dialog_supports_flexible_format(self):
        labeler = self.make_labeler('OCR')
        def configure_dialog():
            dialog = next(w for w in labeler.window.winfo_children() if isinstance(w,tk.Toplevel))
            widgets = descendants(dialog)
            next(w for w in widgets if isinstance(w,tk.Checkbutton)).invoke()
            next(w for w in widgets if isinstance(w,tk.Entry)).insert(0,'12')
            next(w for w in widgets if isinstance(w,tk.Button) and w.cget('text') == 'OK').invoke()
        self.root.after(50,configure_dialog)
        result = labeler.enter_text()
        self.assertTrue(result['check'])
        self.assertEqual(result['pattern'],['num','num'])
        self.assertEqual(result['refer'],'12')
        labeler.close()


    def test_ocr_uppercase_c_copies_hovered_box_and_text(self):
        labeler = self.make_labeler("OCR")
        region = {"role": "text", "box": [0.4, 0.4, 0.6, 0.6], "text": "ABC\n123"}
        labeler.regions.append(region)
        event = self.event_at(labeler, 200, 200)
        with patch.object(labeler.canvas, "winfo_pointerx", return_value=event.x + labeler.canvas.winfo_rootx()), \
                patch.object(labeler.canvas, "winfo_pointery", return_value=event.y + labeler.canvas.winfo_rooty()):
            labeler.key(SimpleNamespace(keysym="C", state=1))
        self.assertEqual(labeler.clipboard, [region])
        self.assertIsNot(labeler.clipboard[0], region)
        labeler.paste()
        self.assertEqual(labeler.regions[-1]["text"], "ABC\n123")
        labeler.close()

    def test_ocr_text_mark_radio_draw_saves_mark_without_text_dialog(self):
        labeler = self.make_labeler("OCR")
        labeler.regions = []
        choices = {widget.cget("text"):widget for widget in descendants(labeler.window) if isinstance(widget,tk.Radiobutton)}
        self.assertIn("Text",choices)
        self.assertIn("Mark",choices)
        choices["Mark"].invoke()
        self.assertEqual(labeler.role.get(),"mark")
        labeler.begin(self.event_at(labeler,100,100))
        with patch.object(labeler,"enter_text",side_effect=AssertionError("No text entry for mark")):
            labeler.end(self.event_at(labeler,180,180))
        self.assertEqual(labeler.regions[0]["role"],"mark")
        self.assertTrue(labeler.regions[0]["mark_id"])
        labeler.save()
        labeler.load()
        self.assertEqual(next(r for r in labeler.regions if r["role"] == "mark")["role"],"mark")
        choices["Text"].invoke()
        self.assertEqual(labeler.role.get(),"text")
        labeler.close()

    def test_pin_numbering_starts_at_one_independent_of_other_regions(self):
        labeler = self.make_labeler("conn")
        labeler.regions.extend([
            {"role": "slot", "box": [0.2, 0.2, 0.8, 0.8]},
            {"role": "pin", "box": [0.3, 0.3, 0.4, 0.4]},
            {"role": "pin", "box": [0.6, 0.3, 0.7, 0.4]},
        ])
        labeler.draw()
        texts = [labeler.canvas.itemcget(item, "text") for item in labeler.canvas.find_all()
                 if labeler.canvas.type(item) == "text"]
        self.assertIn("slot 1", texts)
        self.assertIn("pin area 1 FAIL", texts)
        self.assertIn("pin area 2 FAIL", texts)
        labeler.close()

    def test_label_nine_and_zero_are_text(self):
        labeler = self.make_labeler("label")
        choices = {widget.cget("text"): widget for widget in descendants(labeler.window)
                   if isinstance(widget, tk.Radiobutton)}
        for number in range(10,21):
            choices[str(number)].invoke()
            self.assertEqual(labeler.number, number)
            self.assertEqual(labeler.role.get(), "text")
        for number in (9,0,2,4,6,8):
            labeler.key(SimpleNamespace(keysym=str(number), state=0))
            self.assertEqual(labeler.number, number)
            self.assertEqual(labeler.role.get(), "text")
        for number in (1,3,5,7):
            labeler.key(SimpleNamespace(keysym=str(number), state=0))
            self.assertEqual(labeler.role.get(), "qr")
        labeler.close()

    def test_multiple_polygon_rois_vertices_and_text_verdict_persist(self):
        labeler = self.make_labeler("label")
        labeler.regions = []
        for start,end in (((40,40),(180,180)),((220,220),(350,350))):
            labeler.start_polygon()
            labeler.begin(self.event_at(labeler,*start))
            labeler.drag(self.event_at(labeler,*end))
            labeler.end(self.event_at(labeler,*end))
        labeler.add_roi_vertex(self.event_at(labeler,110,40))
        self.assertEqual([len(r["points"]) for r in labeler.rois],[5,4])
        labeler.begin(self.event_at(labeler,40,40))
        labeler.drag(self.event_at(labeler,50,50))
        labeler.end(self.event_at(labeler,50,50))
        self.assertAlmostEqual(labeler.rois[0]["points"][0][0],.125)
        labeler.regions.append({"role":"text", "text":"AC", "box":[.225,.225,.325,.275]})
        labeler.text_verdict.set("FAIL")
        labeler.select_text_verdict()
        self.assertEqual(labeler.number_var.get(),-1)
        self.assertIsNone(labeler.number)
        labeler.begin(self.event_at(labeler,80,80))
        with patch.object(labeler,"enter_text",side_effect=AssertionError("No new text dialog")):
            labeler.end(self.event_at(labeler,140,120))
        self.assertEqual(labeler.regions[-1]["verdict"],"FAIL")
        self.assertNotIn("number",labeler.regions[-1])
        labeler.save()
        labeler.load()
        self.assertEqual(len(labeler.rois),2)
        self.assertEqual(next(r["text"] for r in labeler.regions if r["role"] == "text"),"AC")
        labeler.close()

    def test_verdict_drag_updates_enclosed_text_only_preserves_numbers_and_undo(self):
        labeler = self.make_labeler("label")
        labeler.regions = [
            {"role":"text","number":2,"text":"AB","box":[.2,.2,.3,.3]},
            {"role":"text","number":12,"text":"CD","box":[.4,.2,.5,.3]},
            {"role":"text","number":4,"text":"EF","box":[.65,.2,.8,.3]},
            {"role":"qr","number":1,"box":[.2,.4,.3,.5]}]
        import copy
        before = copy.deepcopy(labeler.regions)
        for verdict in ("PASS","FAIL"):
            labeler.text_verdict.set(verdict)
            labeler.select_text_verdict()
            labeler.begin(self.event_at(labeler,60,60))
            with patch.object(labeler,"enter_text",side_effect=AssertionError("No text dialog")):
                labeler.end(self.event_at(labeler,280,240))
            self.assertEqual(len(labeler.regions),4)
            for index in (0,1):
                self.assertEqual(labeler.regions[index],dict(before[index],verdict=verdict))
            self.assertEqual(labeler.regions[2:],before[2:])
        labeler.key(SimpleNamespace(keysym="u",state=0))
        self.assertEqual(labeler.regions[0]["verdict"],"PASS")
        labeler.close()

    def test_roi_partial_overlap_delete_and_undo(self):
        labeler = self.make_labeler("label")
        labeler.start_polygon()
        labeler.begin(self.event_at(labeler,100,100))
        labeler.end(self.event_at(labeler,200,200))
        for start,end,count in (((150,150),(230,230),0),((80,80),(220,220),0)):
            labeler.key(SimpleNamespace(keysym="d",state=0))
            labeler.begin(self.event_at(labeler,*start))
            labeler.end(self.event_at(labeler,*end))
            self.assertEqual(len(labeler.rois),count)
            labeler.key(SimpleNamespace(keysym="u",state=0))
            self.assertEqual(len(labeler.rois),1)
        labeler.close()

    def test_roi_rotates_with_boxes_and_undo_restores_both(self):
        import copy
        import numpy as np
        labeler = self.make_labeler("label")
        labeler.regions = [{"role":"text","text":"A","number":9,"box":[.4,.4,.5,.5]}]
        labeler.rois = [{"role":"label_roi","points":[[.2,.2],[.3,.2],[.3,.3],[.2,.3]]}]
        before = copy.deepcopy(labeler.data())
        labeler.key(SimpleNamespace(keysym="Escape",state=0))
        labeler.begin(self.event_at(labeler,60,60))
        labeler.end(self.event_at(labeler,240,240))
        self.assertEqual(labeler.editor.selected,[0,1])
        _,handles = labeler.editor.geometry(labeler.rotation_regions(),(400,400),labeler.scale)
        center,handle = handles["move"],handles["rotate"]
        angle = np.deg2rad(15)
        matrix = np.array([[np.cos(angle),-np.sin(angle)],[np.sin(angle),np.cos(angle)]])
        target = center + matrix@(handle-center)
        labeler.begin(self.event_at(labeler,*handle))
        labeler.end(self.event_at(labeler,*target))
        self.assertAlmostEqual(labeler.regions[0]["angle"],15,places=4)
        self.assertNotEqual(labeler.rois,before["rois"])
        np.testing.assert_allclose(np.mean(labeler.rois[0]["points"],axis=0),[.25,.25],atol=1e-6)
        rotated_regions,rotated_rois = copy.deepcopy(labeler.regions),copy.deepcopy(labeler.rois)
        labeler.key(SimpleNamespace(keysym="u",state=0))
        self.assertEqual(labeler.regions,before["regions"])
        self.assertEqual(labeler.rois,before["rois"])
        labeler.regions,labeler.rois = rotated_regions,rotated_rois
        labeler.save()
        labeler.load()
        self.assertNotEqual(labeler.rois,before["rois"])
        labeler.close()

    def test_roi_and_text_copy_together_to_next_frame_and_undo(self):
        import copy
        labeler = self.make_labeler("label")
        labeler.regions = [{"role":"text","number":9,"text":"AB","verdict":"PASS","box":[.3,.3,.4,.4]}]
        labeler.rois = [{"role":"label_roi","points":[[.2,.2],[.5,.2],[.6,.4],[.5,.6],[.2,.6]]}]
        original = copy.deepcopy(labeler.rois)
        labeler.key(SimpleNamespace(keysym="c",state=0))
        labeler.begin(self.event_at(labeler,60,60))
        labeler.end(self.event_at(labeler,260,260))
        self.assertEqual(labeler.clipboard_rois,original)
        labeler.regions = []
        labeler.rois = []
        labeler.paste()
        self.assertEqual(labeler.rois,original)
        self.assertEqual(labeler.regions[0]["text"],"AB")
        self.assertIsNot(labeler.rois[0],labeler.clipboard_rois[0])
        labeler.key(SimpleNamespace(keysym="u",state=0))
        self.assertEqual(labeler.rois,[])
        self.assertEqual(labeler.regions,[])
        labeler.close()

    def test_pass_fail_selection_is_independent_of_all_number_buttons(self):
        labeler = self.make_labeler("label")
        for number in (0,*range(1,21)):
            labeler.number_var.set(number)
            labeler.select_number()
            for verdict in ("PASS","FAIL"):
                labeler.text_verdict.set(verdict)
                labeler.select_text_verdict()
                self.assertEqual(labeler.number_var.get(),-1)
                self.assertIsNone(labeler.number)
                self.assertEqual(labeler.role.get(),"text")
        labeler.close()


if __name__ == "__main__":
    unittest.main()
