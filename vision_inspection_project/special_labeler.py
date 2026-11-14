from __future__ import annotations

import copy
import tkinter as tk
import time
from pathlib import Path
from tkinter import messagebox, ttk, simpledialog
from types import SimpleNamespace

from PIL import Image, ImageTk

from inspection.special_checks import annotation_dir, read_json, write_json
from rotation_interaction import RotationInteraction
from translation_interaction import TranslationInteraction
from edit_interaction import EditInteraction


COLORS = {"text": "#00a878", "mark": "#e6a100", "slot": "#f49d37", "pin": "#ac49cf", "pin_point": "#00c9e8", "qr": "#168de2", "auto_exclude": "#a0a0a0", "conn_boundary": "#ff2222"}
COLORS["exclude"] = "#b040ff"
COLORS["registration"] = "#00bfff"
MANUALS = {
    "OCR": "drag=text  double-click text box=edit",
    "conn": "1=pin point (rectangle)  2=pin area (near pin center=snap + diameter in pixels; elsewhere=free circle)  3=slot (inspection region)\n"
            "C+drag=touch label borders to select; drag selected center=axis-locked copy on release\n"
            "M+drag=red boundary; 2 or more pin centers outside=FAIL; pin area checks remain separate",
    "label": "1/3/5/7=QR  2/4/6/8/9/0/10-20=text (QR optional)  drag=add box  double-click text box=edit\n"
             "multiple boxes=join in draw order",
}
COMMON_MANUAL = ("Ctrl+V=paste  wheel=zoom  arrows=pan  space=play/pause (end=first frame)\n"
                 "n=next  p=prev  u=undo  d then drag=delete  c=copy (hover box or drag)  ESC=cancel; Alt+F4/X=save close\n"
                 "hover box + [ / ]=rotate 5deg (Shift=1deg)\n"
                 "ESC=edit; select overlapping boxes/ROI; center=move; edge=resize; upper handle=rotate; U=undo")


class SpecialLabeler:
    def __init__(self, parent, root_dir: Path, group: str, kind: str, image_paths: list[Path], *, sampling=None):
        if not image_paths:
            raise ValueError("No frames selected.")
        self.root_dir, self.group, self.kind = root_dir, group, kind
        self.paths = image_paths
        self.sampling = sampling
        self.directory = annotation_dir(root_dir, group, kind)
        self.index = 0
        self.regions = []
        self.rotation_selection = RotationInteraction()
        self.translation = TranslationInteraction()
        self.history = []
        self.clipboard = []
        self.clipboard_rois = []
        self.number = 1
        self.mode = "edit"
        self.editor = EditInteraction()
        self.zoom = 1.0
        self.start = None
        self.pin_copy = None
        self.pin_axis = None
        self.pin_dimensions = (20.0, 20.0)
        self.pin_diameter = 20.0
        self.snapped_pin = False
        self.copy_selected = []
        self.group_copy = None
        self.playing = False
        self.dirty = False
        self.window = tk.Toplevel(parent)
        self.window.title(f"{group}_{kind}")
        self.window.geometry("1100x800")
        self.window.protocol("WM_DELETE_WINDOW", self.close)
        self.window.bind("<Alt-F4>",lambda event:(self.close(),"break")[1])
        toolbar = tk.Frame(self.window)
        toolbar.pack(fill="x")
        self.frame_number = tk.StringVar(value="1")
        self.frame_entry = tk.Entry(toolbar, textvariable=self.frame_number, width=7)
        self.frame_entry.pack(side="left", padx=(8, 0))
        self.frame_entry.bind("<Return>", self.jump_to_frame)
        tk.Label(toolbar, text=f"/{len(self.paths)}").pack(side="left")
        tk.Button(toolbar,text=">>",command=self.next_labeled_frame).pack(side="left",padx=4)
        tk.Button(toolbar,text="Reference area",command=self.select_reference_area).pack(side="left",padx=4)
        self.info = tk.StringVar()
        tk.Label(toolbar, textvariable=self.info).pack(side="left", padx=8)
        if kind in ("OCR", "label", "conn"):
            from inspection.photo_inspection import load_photo_frame_ranges
            try:
                inspection_ranges = load_photo_frame_ranges(Path("C:/vision_inspection/test/info"), group, kind)
            except ValueError:
                inspection_ranges = [(1,1)]
            self.inspection_rows = []
            self.inspection_controls = tk.Frame(self.window)
            self.inspection_controls.pack(fill='x')
            tk.Label(self.inspection_controls,text='Inspection frames').pack(side='left')
            self.inspection_rows_frame = tk.Frame(self.inspection_controls)
            self.inspection_rows_frame.pack(side='left', fill='x')
            for start,end in inspection_ranges:
                self.add_inspection_range(start,end)
            self.inspection_frame,self.inspection_frame_end = self.inspection_rows[0][1:]
            if kind in ('OCR','label'):
                tk.Button(self.inspection_controls,text='+ range',command=self.add_inspection_range).pack(side='left')
            tk.Button(self.inspection_controls,text='save ranges',command=self.save_inspection_time).pack(side='left')
        self.role = tk.StringVar(value="text" if kind == "OCR" else "pin_point" if kind == "conn" else "qr")
        if kind == "OCR":
            for role,title in (("text","Text"),("mark","Mark")):
                tk.Radiobutton(toolbar,text=title,variable=self.role,value=role,
                               command=self.select_ocr_role).pack(side="left")
        if kind == "conn":
            for role, title in (("pin_point", "pin point"), ("pin", "pin area"), ("slot", "slot")):
                tk.Radiobutton(toolbar, text=title, variable=self.role, value=role,
                               command=self.select_conn_role).pack(side="left")
        elif kind == "label":
            self.number_var = tk.IntVar(value=1)
            numbers = tk.Frame(self.window)
            numbers.pack(fill="x")
            for row, values in enumerate(((*range(1, 10), 0), range(10, 21))):
                for column, number in enumerate(values):
                    tk.Radiobutton(numbers, text=str(number), variable=self.number_var, value=number,
                                   command=self.select_number).grid(row=row, column=column, sticky="w")
        if kind in ("OCR", "label"):
            controls = tk.Frame(self.window)
            controls.pack(fill="x")
            self.text_verdict = tk.StringVar(value="")
            for verdict in ("PASS", "FAIL"):
                tk.Radiobutton(controls, text=verdict, variable=self.text_verdict, value=verdict,
                               command=self.select_text_verdict).pack(side="left")
            tk.Radiobutton(controls,text='Flexible',variable=self.text_verdict,value='Flexible',
                           command=self.select_flexible).pack(side='left')
            if kind == 'label':
                tk.Radiobutton(controls,text='Serial',variable=self.text_verdict,value='Serial',
                               command=self.select_serial).pack(side='left')
            tk.Button(controls, text="ROI polygon", command=self.start_polygon).pack(side="left")
            tk.Label(controls, text="Drag=ROI; drag vertices=edit; double-click edge=add vertex; D+drag=overlap ROI to delete").pack(side="left")
        self.manual = tk.Label(self.window, text=MANUALS[kind] + "\n" + COMMON_MANUAL,
                               bg="black", fg="white", justify="left", anchor="w", font=("Segoe UI", 10))
        self.manual.pack(fill="x")
        self.manual.bind("<Configure>", lambda event: self.manual.configure(wraplength=max(1, event.width - 16)))
        self.canvas = tk.Canvas(self.window, bg="black", highlightthickness=0)
        self.canvas.pack(fill="both", expand=True)
        self.canvas.bind("<Configure>", lambda event: self.draw())
        self.canvas.bind("<Button-1>", self.begin)
        self.canvas.bind("<B1-Motion>", self.drag)
        self.canvas.bind("<ButtonRelease-1>", self.end)
        self.canvas.bind("<Button-3>", self.delete_polygon)
        self.canvas.bind("<Double-Button-1>", self.add_roi_vertex)
        self.canvas.bind("<MouseWheel>", self.wheel)
        self.window.bind("<KeyPress>", self.key)
        self.window.bind("<Control-v>", self.paste)
        self.window.bind("<Control-V>", self.paste)
        self.load()
        self.canvas.focus_set()
        self._tick_after = self.window.after(100, self.tick)

    def run(self):
        self.window.grab_set()
        self.window.wait_window()

    def save_inspection_time(self):
        from inspection.photo_inspection import save_photo_frame_ranges
        try:
            rows = getattr(self,'inspection_rows',[(None,self.inspection_frame,self.inspection_frame_end)])
            ranges = [(float(start.get()),float(end.get())) for _,start,end in rows]
            if any(end > len(self.paths) for _,end in ranges):
                raise ValueError(f"Enter a frame range from 1 to {len(self.paths)}.")
            save_photo_frame_ranges(Path("C:/vision_inspection/test/info"),self.group,self.kind,ranges)
        except (ValueError,OSError) as exc:
            messagebox.showerror("Inspection frame",str(exc),parent=self.window)
            return
        self.info.set("Inspection frame range saved")

    def add_inspection_range(self, start=1, end=1):
        row = tk.Frame(self.inspection_rows_frame)
        row.pack(fill='x')
        first,last = tk.StringVar(value=str(start)),tk.StringVar(value=str(end))
        tk.Entry(row,textvariable=first,width=7).pack(side='left')
        tk.Label(row,text='~').pack(side='left')
        tk.Entry(row,textvariable=last,width=7).pack(side='left')
        item = (row,first,last)
        self.inspection_rows.append(item)
        if self.kind in ('OCR','label'):
            tk.Button(row,text='-',command=lambda:self.remove_inspection_range(item)).pack(side='left')
            if self.sampling is not None:
                self.add_inspection_sample_groups(row, first, last)

    def add_inspection_sample_groups(self, row, first, last):
        label = tk.Label(row, anchor='w', justify='left', wraplength=520)
        label.pack(side='left', padx=6)
        sampling = self.sampling

        def refresh(*_):
            try:
                start, end = int(first.get()), int(last.get())
            except ValueError:
                label.configure(text='')
                return
            if not 1 <= start <= end <= len(self.paths):
                label.configure(text='')
                return
            try:
                groups = ' / '.join(
                    '[' + ','.join(map(str, group)) + ']'
                    for group in sampling.start_groups((start, end))
                )
            except ValueError:
                groups = 'No test frames'
            label.configure(text=f'{sampling.test_fps:g} FPS: {groups}')

        first.trace_add('write', refresh)
        last.trace_add('write', refresh)
        refresh()

    def remove_inspection_range(self, item):
        if len(self.inspection_rows) > 1:
            self.inspection_rows.remove(item)
            item[0].destroy()

    def select_number(self):
        self.number = self.number_var.get()
        if hasattr(self, "text_verdict"):
            self.text_verdict.set("")
        self.role.set("qr" if self.number in (1, 3, 5, 7) else "text")
        self.mode = "draw"
        self.canvas.focus_set()

    def select_reference_area(self):
        self.cancel_action()
        self.mode = "registration"
        self.canvas.focus_set()

    def select_text_verdict(self):
        if self.kind == "label":
            self.number = None
            self.number_var.set(-1)
        if self.kind != "OCR":
            self.role.set("text")
        self.mode = "verdict"
        self.canvas.focus_set()

    def select_serial(self):
        self.cancel_action()
        self.text_verdict.set('Serial')
        self.number_var.set(-1)
        self.mode = 'serial'
        self.canvas.focus_set()

    def select_flexible(self):
        self.cancel_action()
        self.text_verdict.set('Flexible')
        if self.kind == 'label':
            self.number_var.set(-1)
        self.mode = 'flexible'
        self.canvas.focus_set()

    def select_ocr_role(self):
        self.mode = "draw"
        self.text_verdict.set("")
        self.canvas.focus_set()

    def start_polygon(self):
        self.mode = "roi_drag"
        self.start = None
        self.roi_pending = []
        self.canvas.focus_set()

    def add_roi_vertex(self, event):
        if self.kind not in ("OCR", "label") or self.mode not in ("draw", "roi_drag", "edit"):
            return
        import numpy as np
        point = np.asarray(self.point(event))
        scale = np.asarray([self.image.width,self.image.height])*self.scale
        for roi in reversed(self.rois):
            points = np.asarray(roi["points"])
            for i,a in enumerate(points):
                b = points[(i+1)%len(points)]
                delta = (b-a)*scale
                t = np.clip(np.dot((point-a)*scale,delta)/max(np.dot(delta,delta),1e-9),0,1)
                projected = a+t*(b-a)
                if .05 < t < .95 and np.linalg.norm((point-projected)*scale) <= 12:
                    self.remember_roi()
                    roi["points"].insert(i+1,projected.tolist())
                    self.start = None
                    self.draw()
                    return "break"

        if self.mode in ("draw","edit"):
            self.edit_text(event)

    def delete_polygon(self, event):
        if self.kind not in ("OCR", "label"):
            return
        import cv2
        import numpy as np
        point = self.point(event)
        for i in reversed(range(len(self.rois))):
            if cv2.pointPolygonTest(np.asarray(self.rois[i]["points"], np.float32), point, False) >= 0:
                self.remember_roi()
                self.rois.pop(i)
                self.dirty = True
                self.draw()
                break

    def select_conn_role(self):
        self.mode = "draw"
        self.canvas.focus_set()

    def path(self):
        return self.directory / f"{self.paths[self.index].stem}.json"

    def load(self):
        self.editor = EditInteraction()
        self.translation = TranslationInteraction()
        self.rotation_selection.clear()
        self.copy_selected = []
        self.group_copy = None
        self.image = Image.open(self.paths[self.index]).convert("RGB")
        data = read_json(self.path(), {})
        self.regions = [region for region in data.get("regions", []) if region.get("role") != "auto_exclude"]
        self.rois = data.get("rois", [])
        self.roi_pending = []
        self.roi_vertex = None
        self.history = []
        self.dirty = False
        self.zoom = 1.0
        self.offset = None
        self.start = None
        self.pin_copy = None
        self.pin_axis = None
        self.draw()

    def data(self):
        return {"version": 1, "group": self.group, "kind": self.kind,
                "frame_number": self.index + 1,
                "image": str(self.paths[self.index]), "size": list(self.image.size), "regions": self.regions,
                "rois": self.rois,
                "saved_at_ns": time.time_ns()}

    def save(self):
        if not self.dirty:
            return
        data = self.data()
        has_labels = any(region["role"] in ("text", "mark", "qr", "pin", "slot", "pin_point", "conn_boundary", "exclude", "registration") for region in self.regions)
        if has_labels or self.rois:
            write_json(self.path(), data)
        elif self.path().exists():
            self.path().unlink()
        reference_path = self.directory / "reference.json"
        if reference_path.exists():
            reference_path.unlink()
        self.dirty = False

    def remember(self):
        self.history.append(copy.deepcopy(self.regions))
        self.dirty = True

    def select_copy(self):
        self.mode = "copy"
        self.canvas.focus_set()
        self.draw()

    def rotation_regions(self):
        return self.regions + self.rois

    def draw(self):
        if not hasattr(self, "image"):
            return
        cw, ch = max(1, self.canvas.winfo_width()), max(1, self.canvas.winfo_height())
        width, height = self.image.size
        self.scale = min(cw / width, ch / height) * self.zoom
        if self.offset is None or getattr(self, "viewport_size", None) != (cw, ch):
            self.offset = [(cw - width * self.scale) / 2, (ch - height * self.scale) / 2]
        self.viewport_size = (cw, ch)
        ox, oy = self.offset
        # Render only the visible viewport to avoid huge allocations at high zoom.
        view = self.image.transform((cw, ch), Image.Transform.AFFINE,
                                    (1 / self.scale, 0, -ox / self.scale, 0, 1 / self.scale, -oy / self.scale),
                                    resample=Image.Resampling.BILINEAR)
        self.photo = ImageTk.PhotoImage(view)
        self.canvas.delete("all")
        self.canvas.create_image(0, 0, anchor="nw", image=self.photo)
        for roi in getattr(self, "rois", []):
            pixels = [(ox+x*width*self.scale, oy+y*height*self.scale) for x,y in roi["points"]]
            self.canvas.create_polygon(*[v for p in pixels for v in p], fill="", outline="#ffcf00", width=2)
            for i, (x,y) in enumerate(pixels):
                self.canvas.create_oval(x-5,y-5,x+5,y+5,fill="#ffcf00")
                self.canvas.create_text(x+9,y-9,text=str(i+1),fill="#ffcf00")
        pending = getattr(self, "roi_pending", [])
        if pending:
            pixels = [(ox+x*width*self.scale, oy+y*height*self.scale) for x,y in pending]
            if len(pixels) > 1:
                self.canvas.create_line(*[v for p in pixels for v in p],fill="#ffcf00",width=2)
            for x,y in pixels:
                self.canvas.create_oval(x-5,y-5,x+5,y+5,fill="#ffcf00")
        role_counts = {}
        conn_states = None
        boundary_outside = []
        if self.kind == "conn":
            from inspection.conn_geometry import annotation_status
            conn_states = annotation_status(self.regions, (height, width))
            from inspection.conn_geometry import pin_center
            from inspection.conn_geometry import boundary_outside_indices
            boundary_outside = boundary_outside_indices([pin_center(region, (height, width)) for region in self.regions
                                                         if region["role"] == "pin_point"], self.regions, (height, width))
        for index, region in enumerate(self.regions):
            role = region["role"]
            if role == "auto_exclude":
                continue
            role_counts[role] = role_counts.get(role, 0) + 1
            x1, y1, x2, y2 = region["box"]
            coords = (ox + x1 * width * self.scale, oy + y1 * height * self.scale,
                      ox + x2 * width * self.scale, oy + y2 * height * self.scale)
            color = COLORS[region["role"]]
            if role in ("text", "mark") and region.get("verdict") in ("PASS", "FAIL"):
                color = "#00a878" if region["verdict"] == "PASS" else "#ff4545"
            if region.get("shape") == "circle":
                if region.get("clip_to_frame"):
                    import numpy as np
                    from inspection.rotated_regions import clip_polygon
                    cx,cy = (x1+x2)*width/2,(y1+y2)*height/2
                    radius = (x2-x1)*width/2
                    angles = np.linspace(0,2*np.pi,96,endpoint=False)
                    polygon = clip_polygon(np.column_stack((cx+radius*np.cos(angles),cy+radius*np.sin(angles))),(height,width))
                    if len(polygon) >= 3:
                        self.canvas.create_polygon(*(polygon*self.scale+[ox,oy]).ravel(),fill="",outline=color,width=2)
                else:
                    self.canvas.create_oval(*coords, outline=color, width=2)
            elif region.get("angle", 0) or region.get("clip_to_frame"):
                from inspection.rotated_regions import region_polygon
                polygon = region_polygon(region, (height, width)) * self.scale + [ox, oy]
                self.canvas.create_polygon(*polygon.ravel(), fill="", outline=color, width=2)
            else:
                self.canvas.create_rectangle(*coords, outline=color, width=2)
            if region["role"] in ("pin", "pin_point"):
                cx, cy = (coords[0] + coords[2]) / 2, (coords[1] + coords[3]) / 2
                self.canvas.create_oval(cx - 5, cy - 5, cx + 5, cy + 5, outline=color, width=2)
                self.canvas.create_line(cx - 8, cy, cx + 8, cy, fill=color, width=2)
                self.canvas.create_line(cx, cy - 8, cx, cy + 8, fill=color, width=2)
            title = {"pin": "pin area", "pin_point": "pin point"}.get(role, role)
            label = (f"{title} {role_counts[role]}" if self.kind == "conn"
                     else f"{region.get('number', '')}:{role} {index + 1}")
            if role in ("text", "mark") and region.get("verdict"):
                label += " " + region["verdict"]
            if role == 'qr' and region.get('serial_order'):
                label += f" Serial {region['serial_order']}"
            if role in ('text','qr') and region.get('flexible'):
                label += ' Flexible'
            if conn_states is not None and role in ("pin_point", "pin"):
                states = conn_states[0 if role == "pin_point" else 1]
                passed = states[role_counts[role] - 1]
                label += " PASS" if passed else " FAIL"
                color = "#00c853" if passed else "#ff4545"
            self.canvas.create_text(coords[0], coords[1] - 3, text=label, fill=color, anchor="sw", font=("Segoe UI", 11))
        if self.kind == "conn":
            from inspection.rotated_regions import region_polygon
            self.copy_selected = [i for i in self.copy_selected if i < len(self.regions)]
            for i in self.copy_selected:
                region = self.regions[i]
                polygon = region_polygon(region, (height, width)) * self.scale + [ox, oy]
                self.canvas.create_polygon(*polygon.ravel(), fill="", outline="#ffff00", width=3)
                if region["role"] in ("conn_boundary", "slot"):
                    cx, cy = pin_center(region, (height, width))
                    cx, cy = ox+cx*self.scale, oy+cy*self.scale
                    self.canvas.create_oval(cx-5, cy-5, cx+5, cy+5, outline="#ffff00", width=2)
        if self.mode == "rotation":
            polygons, center, handle = self.rotation_selection.geometry(
                self.rotation_regions(), (height, width), self.scale)
            for polygon in polygons:
                self.canvas.create_polygon(*(polygon * self.scale + [ox, oy]).ravel(),
                                           fill="", outline="#ffff00", width=4)
            if handle is not None:
                cx, cy = center * self.scale + [ox, oy]
                hx, hy = handle * self.scale + [ox, oy]
                self.canvas.create_line(cx, cy, hx, hy, fill="#ffff00", width=2)
                self.canvas.create_oval(hx - 9, hy - 9, hx + 9, hy + 9,
                                        fill="#ffff00", outline="black", width=2)
        if self.mode == "rotation":
            self.translation.selected = list(self.rotation_selection.selected)
        if self.mode in ("draw","rotation"):
            polygons,center = self.translation.geometry(self.rotation_regions() if self.mode == "rotation" else self.regions,(height,width))
            for polygon in polygons:
                self.canvas.create_polygon(*(polygon*self.scale+[ox,oy]).ravel(),fill="",outline="#ffff00",width=3)
            if center is not None:
                cx,cy = center*self.scale+[ox,oy]
                self.canvas.create_oval(cx-6,cy-6,cx+6,cy+6,fill="#00ffff",outline="black",width=2)
        if self.mode == "edit":
            polygons,handles = self.editor.geometry(self.rotation_regions(),(height,width),self.scale)
            for polygon in polygons:
                self.canvas.create_polygon(*(polygon*self.scale+[ox,oy]).ravel(),fill="",outline="#ffff00",width=3)
            if handles:
                a,b = handles["move"]*self.scale+[ox,oy],handles["rotate"]*self.scale+[ox,oy]
                self.canvas.create_line(*a,*b,fill="#ffff00",width=2)
            for name,point in handles.items():
                x,y = point*self.scale+[ox,oy]
                if name in ("move","rotate"):
                    self.canvas.create_oval(x-6,y-6,x+6,y+6,fill="#00ffff" if name == "move" else "#ffff00",outline="black")
                else:
                    self.canvas.create_rectangle(x-4,y-4,x+4,y+4,fill="white",outline="black")
        self.info.set(f"{self.index + 1}/{len(self.paths)}  {self.paths[self.index].name}  {self.zoom:.2f}x  {self.mode}")
        if conn_states is not None and any(conn_states):
            passed = bool(conn_states[0] and conn_states[1]) and all(conn_states[0]) and all(conn_states[1]) and len(boundary_outside) < 2
            self.info.set(self.info.get() + ("  Conn PASS" if passed else "  Conn FAIL"))
            if any(region["role"] == "conn_boundary" for region in self.regions):
                self.info.set(self.info.get() + f"  outside boundary: {len(boundary_outside)} (FAIL >=2)")
        if self.window.focus_get() is not self.frame_entry:
            self.frame_number.set(str(self.index + 1))

    def point(self, event):
        width, height = self.image.size
        return ((event.x - self.offset[0]) / self.scale / width, (event.y - self.offset[1]) / self.scale / height)

    def begin(self, event):
        self.canvas.focus_set()
        self.playing = False
        point = self.point(event)
        if self.mode == 'flexible':
            from inspection.rotated_regions import point_in_region,region_polygon
            for region in reversed(self.regions):
                if region['role'] in ('text','qr') and point_in_region(
                        (point[0]*self.image.width,point[1]*self.image.height),
                        region_polygon(region,(self.image.height,self.image.width))):
                    self.remember()
                    if region.get('flexible'):
                        region.pop('flexible')
                    else:
                        region['flexible'] = True
                    self.dirty = True
                    self.draw()
                    break
            return
        if self.mode == 'serial':
            from inspection.rotated_regions import point_in_region,region_polygon
            for region in reversed(self.regions):
                if region['role'] == 'qr' and point_in_region(
                        (point[0]*self.image.width,point[1]*self.image.height),
                        region_polygon(region,(self.image.height,self.image.width))):
                    self.remember()
                    if region.get('serial_order'):
                        region.pop('serial_order')
                    else:
                        region['serial_order'] = max((r.get('serial_order',0) for r in self.regions if r['role']=='qr'),default=0)+1
                    self.dirty = True
                    self.draw()
                    break
            return
        if self.mode == "edit":
            self.editor.begin((point[0]*self.image.width,point[1]*self.image.height),self.rotation_regions(),
                              (self.image.height,self.image.width),self.scale,bool(getattr(event,"state",0)&1))
            self.start = None
            self.draw()
            return
        if self.mode == "rotation":
            self.translation.selected = list(self.rotation_selection.selected)
        if self.mode in ("draw","rotation") and self.translation.begin(
                (point[0]*self.image.width,point[1]*self.image.height),self.rotation_regions() if self.mode == "rotation" else self.regions,
                (self.image.height,self.image.width),self.scale):
            self._translation_before = {"regions":copy.deepcopy(self.regions),"rois":copy.deepcopy(self.rois)}
            self.start = None
            return
        if self.kind in ("OCR", "label"):
            if not (0 <= point[0] <= 1 and 0 <= point[1] <= 1):
                return
            if self.mode == "polygon":
                self.start = None
                if len(self.roi_pending) >= 3 and self.vertex_near(point, self.roi_pending[0]):
                    import cv2
                    import numpy as np
                    polygon = np.asarray(self.roi_pending, np.float32)
                    if abs(cv2.contourArea(polygon)) * self.image.width * self.image.height < 16:
                        messagebox.showwarning("ROI", "ROI polygon is too small or invalid.", parent=self.window)
                        return
                    self.remember_roi()
                    self.rois.append({"role":"label_roi", "points":copy.deepcopy(self.roi_pending)})
                    self.roi_pending = []
                    self.dirty = True
                    self.mode = "draw"
                else:
                    self.roi_pending.append(point)
                self.draw()
                return
            if self.mode == "draw":
                for i, roi in enumerate(self.rois):
                    for j, vertex in enumerate(roi["points"]):
                        if self.vertex_near(point, vertex):
                            self.remember_roi()
                            self.roi_vertex = (i,j)
                            return
        if self.mode == "rotation":
            self.start = None
            self.rotation_selection.begin((point[0] * self.image.width, point[1] * self.image.height),
                                          self.rotation_regions(), (self.image.height, self.image.width), self.scale)
            self._rotation_before = {"regions":copy.deepcopy(self.regions),"rois":copy.deepcopy(self.rois)}
            self.draw()
            return
        self.pin_copy = None
        self.pin_axis = None
        self.snapped_pin = False
        self.group_copy = None
        if 0 <= point[0] <= 1 and 0 <= point[1] <= 1:
            self.start = point
            if self.kind == "conn" and self.mode in ("draw", "copy") and self.copy_selected:
                from inspection.conn_geometry import pin_center
                for i in self.copy_selected:
                    if i >= len(self.regions):
                        continue
                    cx, cy = pin_center(self.regions[i], (self.image.height, self.image.width))
                    if ((cx-point[0]*self.image.width)**2 + (cy-point[1]*self.image.height)**2) * self.scale**2 <= 100:
                        self.group_copy = copy.deepcopy([self.regions[j] for j in self.copy_selected if j < len(self.regions)])
                        return
            if self.kind == "conn" and self.mode == "draw":
                from inspection.conn_geometry import pin_center
                shape = (self.image.height, self.image.width)
                for region in reversed(self.regions):
                    if region["role"] != self.role.get():
                        continue
                    cx, cy = pin_center(region, shape)
                    distance = ((point[0]*self.image.width-cx)**2 + (point[1]*self.image.height-cy)**2) * self.scale**2
                    if distance <= 100:
                        self.pin_copy = copy.deepcopy(region)
                        break
                if self.pin_copy is None and self.role.get() == "pin":
                    points = [pin_center(region, shape) for region in self.regions if region["role"] == "pin_point"]
                    if points:
                        cx, cy = min(points, key=lambda center: (point[0]*self.image.width-center[0])**2 + (point[1]*self.image.height-center[1])**2)
                        if ((point[0]*self.image.width-cx)**2 + (point[1]*self.image.height-cy)**2) * self.scale**2 <= 144:
                            self.start = (cx/self.image.width, cy/self.image.height)
                            self.snapped_pin = True

    def shifted_pin(self, point):
        dx = (point[0] - self.start[0]) * self.image.width
        dy = (point[1] - self.start[1]) * self.image.height
        if self.pin_axis is None:
            if max(abs(dx), abs(dy)) * self.scale < 6:
                return None
            self.pin_axis = "x" if abs(dx) >= abs(dy) else "y"
        x1, y1, x2, y2 = self.pin_copy["box"]
        if self.pin_axis == "x":
            shift = min(1 - x2, max(-x1, dx / self.image.width))
            box = [x1 + shift, y1, x2 + shift, y2]
        else:
            shift = min(1 - y2, max(-y1, dy / self.image.height))
            box = [x1, y1 + shift, x2, y2 + shift]
        candidate = dict(copy.deepcopy(self.pin_copy), box=box)
        if candidate.get("angle", 0) and candidate.get("shape") != "circle":
            from inspection.rotated_regions import region_polygon
            polygon = region_polygon(candidate, (self.image.height, self.image.width))
            if not ((polygon >= 0).all() and (polygon[:, 0] < self.image.width).all() and (polygon[:, 1] < self.image.height).all()):
                return None
        return candidate

    def shifted_group(self, point):
        dx = (point[0]-self.start[0])*self.image.width
        dy = (point[1]-self.start[1])*self.image.height
        if self.pin_axis is None:
            if max(abs(dx), abs(dy))*self.scale < 6:
                return []
            self.pin_axis = "x" if abs(dx) >= abs(dy) else "y"
        from inspection.conn_geometry import shift_region_group
        return shift_region_group(self.group_copy, dx, dy, self.pin_axis, (self.image.height, self.image.width))

    def drag(self, event):
        if self.mode == "edit":
            point = self.point(event)
            self.editor.move((point[0]*self.image.width,point[1]*self.image.height),self.rotation_regions(),(self.image.height,self.image.width))
            self.draw()
            if self.editor.action == "select":
                x,y = self.editor.start*self.scale+self.offset
                self.canvas.create_rectangle(x,y,event.x,event.y,outline="#ffff00",width=2,tags="drag")
            return
        if self.translation.baseline is not None:
            point = self.point(event)
            self.translation.move((point[0]*self.image.width,point[1]*self.image.height),
                                  self.rotation_regions() if self.mode == "rotation" else self.regions,(self.image.height,self.image.width))
            self.draw()
            return
        if getattr(self, "roi_vertex", None) is not None:
            point = self.point(event)
            i,j = self.roi_vertex
            self.rois[i]["points"][j] = [min(1,max(0,value)) for value in point]
            self.dirty = True
            self.draw()
            return
        if self.mode == "polygon":
            return
        if self.mode == "rotation":
            point = self.point(event)
            self.rotation_selection.move((point[0] * self.image.width, point[1] * self.image.height),
                                         self.rotation_regions(), (self.image.height, self.image.width))
            self.draw()
            if self.rotation_selection.start is not None:
                x, y = self.rotation_selection.start
                self.canvas.create_rectangle(self.offset[0] + x * self.scale,
                                             self.offset[1] + y * self.scale, event.x, event.y,
                                             outline="#ffff00", width=2, tags="drag")
            return
        if self.start is None:
            return
        self.canvas.delete("drag")
        width, height = self.image.size
        if self.mode in ("exclude", "registration"):
            x,y = self.start
            self.canvas.create_rectangle(self.offset[0]+x*width*self.scale,self.offset[1]+y*height*self.scale,
                                         event.x,event.y,outline=COLORS[self.mode],width=2,tags="drag")
            return
        if self.group_copy is not None:
            for region in self.shifted_group(self.point(event)):
                x1, y1, x2, y2 = region["box"]
                draw = self.canvas.create_oval if region.get("shape") == "circle" else self.canvas.create_rectangle
                draw(self.offset[0]+x1*width*self.scale, self.offset[1]+y1*height*self.scale,
                     self.offset[0]+x2*width*self.scale, self.offset[1]+y2*height*self.scale,
                     outline="#ffff00", width=2, dash=(4, 2), tags="drag")
            return
        if self.mode == "boundary":
            x, y = self.start
            self.canvas.create_rectangle(self.offset[0]+x*width*self.scale, self.offset[1]+y*height*self.scale,
                                         event.x, event.y, outline=COLORS["conn_boundary"], width=3, tags="drag")
            return
        if self.pin_copy is not None:
            region = self.shifted_pin(self.point(event))
            if region is not None:
                x1, y1, x2, y2 = region["box"]
                draw = self.canvas.create_oval if region.get("shape") == "circle" else self.canvas.create_rectangle
                draw(self.offset[0] + x1 * width * self.scale,
                                             self.offset[1] + y1 * height * self.scale,
                                             self.offset[0] + x2 * width * self.scale,
                                             self.offset[1] + y2 * height * self.scale,
                                             outline=COLORS["pin"], width=2, dash=(4, 2), tags="drag")
            return
        if self.kind == "conn" and self.mode == "draw" and self.role.get() == "pin":
            from inspection.conn_geometry import circle_area
            region = circle_area(self.start, self.point(event), (height, width))
            if region is not None:
                x1, y1, x2, y2 = region["box"]
                self.canvas.create_oval(self.offset[0]+x1*width*self.scale, self.offset[1]+y1*height*self.scale,
                                        self.offset[0]+x2*width*self.scale, self.offset[1]+y2*height*self.scale,
                                        outline=COLORS["pin"], width=2, tags="drag")
            return
        x, y = self.start
        self.canvas.create_rectangle(self.offset[0] + x * width * self.scale,
                                     self.offset[1] + y * height * self.scale, event.x, event.y,
                                     outline=COLORS[self.role.get()], width=2, tags="drag")

    def end(self, event):
        if self.mode == "edit":
            if self.editor.action is None:
                return
            point = self.point(event)
            before = self.editor.finish((point[0]*self.image.width,point[1]*self.image.height),self.rotation_regions(),(self.image.height,self.image.width))
            if before is not None:
                self.history.append({"regions":before[:len(self.regions)],"rois":before[len(self.regions):]})
                self.dirty = True
            self.draw()
            return
        if self.translation.baseline is not None:
            self.drag(event)
            baseline = self.translation.finish()
            if self._translation_before != {"regions":self.regions,"rois":self.rois}:
                self.history.append(self._translation_before)
                self.dirty = True
            self.draw()
            return
        if self.mode == "draw" and self.start is not None and not getattr(self,"snapped_pin",False):
            point = self.point(event)
            if max(abs(point[0]-self.start[0])*self.image.width,
                   abs(point[1]-self.start[1])*self.image.height)*self.scale < 3:
                self.translation.select((point[0]*self.image.width,point[1]*self.image.height),
                    self.regions,(self.image.height,self.image.width),bool(getattr(event,"state",0)&1))
                self.start = None
                self.draw()
                return
        if getattr(self, "roi_vertex", None) is not None:
            self.roi_vertex = None
            return
        if self.mode == "polygon":
            return
        if self.mode == "rotation":
            point = self.point(event)
            pixel = (point[0] * self.image.width, point[1] * self.image.height)
            shape = (self.image.height, self.image.width)
            if self.rotation_selection.baseline is not None:
                self.rotation_selection.move(pixel, self.rotation_regions(), shape)
            baseline = self.rotation_selection.finish(pixel, self.rotation_regions(), shape)
            if baseline is not None and {"regions":self.regions,"rois":self.rois} != self._rotation_before:
                self.history.append(self._rotation_before)
                self.dirty = True
            self.draw()
            return
        if self.start is None:
            return
        point = self.point(event)
        if self.group_copy is not None:
            copies = self.shifted_group(point)
            if copies and copies != self.group_copy:
                self.remember()
                self.regions.extend(copies)
            self.group_copy = self.pin_axis = self.start = None
            self.mode = "draw"
            self.draw()
            return
        if self.pin_copy is not None:
            region = self.shifted_pin(point)
            if region is not None and region["box"] != self.pin_copy["box"]:
                self.remember()
                self.regions.append(region)
            self.pin_copy = self.pin_axis = self.start = None
            self.draw()
            return
        if self.kind == "conn" and self.mode == "draw" and self.role.get() == "pin":
            from inspection.conn_geometry import circle_area
            center = self.start
            region = self.enter_pin_diameter(center) if self.snapped_pin else circle_area(center, point, (self.image.height, self.image.width))
            self.start = None
            if region is not None:
                self.remember()
                existing = next((i for i, old in enumerate(self.regions) if old["role"] == "pin"
                                 and abs((old["box"][0]+old["box"][2])/2-center[0]) < 1e-6
                                 and abs((old["box"][1]+old["box"][3])/2-center[1]) < 1e-6), None)
                if existing is not None:
                    self.regions[existing] = region
                else:
                    self.regions.append(region)
            self.draw()
            return
        x1, x2 = sorted((self.start[0], min(1, max(0, point[0]))))
        y1, y2 = sorted((self.start[1], min(1, max(0, point[1]))))
        self.start = None
        self.canvas.delete("drag")
        if (x2 - x1) * self.image.width < 2 or (y2 - y1) * self.image.height < 2:
            if self.mode == "draw":
                self.edit_text(event)
            return
        box = [x1, y1, x2, y2]
        if self.mode == "verdict":
            from inspection.rotated_regions import region_polygon
            selected = []
            for region in self.regions:
                if region["role"] not in (("text", "mark") if self.kind == "OCR" else ("text",)):
                    continue
                polygon = region_polygon(region, (self.image.height, self.image.width))
                if ((polygon >= [x1*self.image.width, y1*self.image.height]).all()
                        and (polygon <= [x2*self.image.width, y2*self.image.height]).all()):
                    selected.append(region)
            if selected:
                self.remember()
                for region in selected:
                    region["verdict"] = self.text_verdict.get()
            self.draw()
            return
        if self.mode == "roi_drag":
            self.remember_roi()
            self.rois.append({"role":"label_roi", "points":[[x1,y1],[x2,y1],[x2,y2],[x1,y2]]})
            self.mode = "draw"
            self.draw()
            return
        if self.kind == "conn" and self.mode == "boundary":
            self.remember()
            self.regions = [region for region in self.regions if region["role"] != "conn_boundary"]
            self.regions.append({"role": "conn_boundary", "box": box})
            self.copy_selected = []
            self.mode = "draw"
            self.draw()
            return
        overlaps = [region for region in self.regions if region["box"][0] < x2 and x1 < region["box"][2]
                    and region["box"][1] < y2 and y1 < region["box"][3]]
        if self.mode == "copy":
            from inspection.rotated_regions import region_polygon,polygon_border_overlaps_rectangle
            selection = [x1*self.image.width,y1*self.image.height,x2*self.image.width,y2*self.image.height]
            selected_indices = [i for i,region in enumerate(self.regions) if polygon_border_overlaps_rectangle(
                region_polygon(region,(self.image.height,self.image.width)),selection)]
            self.clipboard_rois = []
            if self.kind == "conn":
                self.copy_selected = selected_indices
                self.clipboard = copy.deepcopy([self.regions[i] for i in self.copy_selected])
            else:
                self.clipboard = copy.deepcopy([self.regions[i] for i in selected_indices])
                self.clipboard_rois = copy.deepcopy([roi for roi in self.rois if polygon_border_overlaps_rectangle(roi['points'],box)])
        elif self.mode == "delete":
            self.translation = TranslationInteraction()
            self.remember_roi()
            from inspection.rotated_regions import polygon_border_overlaps_rectangle,region_polygon
            self.rois = [roi for roi in self.rois if not polygon_border_overlaps_rectangle(roi["points"],box)]
            self.regions = [region for region in self.regions if not polygon_border_overlaps_rectangle(
                region_polygon(region,(self.image.height,self.image.width)),
                [x1*self.image.width,y1*self.image.height,x2*self.image.width,y2*self.image.height])]
            self.copy_selected = []
        else:
            region = {"role": self.mode if self.mode in ("exclude", "registration") else self.role.get(), "box": box}
            if region["role"] == "mark":
                import uuid
                region["mark_id"] = uuid.uuid4().hex
            if self.kind == "label" and region["role"] not in ("exclude", "registration"):
                if self.number is not None:
                    region["number"] = self.number
            if region["role"] == "text":
                value = self.enter_text()
                if value is None:
                    self.draw()
                    return
                region.update(value)
                if self.text_verdict.get() in ("PASS", "FAIL"):
                    region["verdict"] = self.text_verdict.get()
            self.remember()
            if region["role"] == "registration":
                self.regions = [old for old in self.regions if old["role"] != "registration"]
            self.regions.append(region)
        self.mode = "draw"
        self.draw()

    def edit_text(self, event):
        self.start = None
        x, y = self.point(event)
        for region in reversed(self.regions):
            x1, y1, x2, y2 = region["box"]
            if region["role"] == "text" and x1 <= x <= x2 and y1 <= y <= y2:
                value = self.enter_text(region.get("text", ""), region.get("check", False), region.get("pattern", []), region.get("refer", ""))
                if value is not None:
                    self.remember()
                    region.update(value)
                    if self.text_verdict.get() in ("PASS", "FAIL"):
                        region["verdict"] = self.text_verdict.get()
                    self.draw()
                break

    def vertex_near(self, point, vertex):
        return (((point[0]-vertex[0])*self.image.width*self.scale)**2
                + ((point[1]-vertex[1])*self.image.height*self.scale)**2) <= 100

    def remember_roi(self):
        self.history.append({"rois":copy.deepcopy(self.rois), "regions":copy.deepcopy(self.regions)})
        self.dirty = True

    def enter_pin_diameter(self, center):
        import math
        width, height = self.image.size
        maximum = 2 * min(center[0]*width, center[1]*height, (1-center[0])*width, (1-center[1])*height)
        if maximum < 4:
            return None
        diameter = simpledialog.askfloat("Pin area diameter", "Diameter (original image pixels)",
                                        initialvalue=min(self.pin_diameter, maximum), minvalue=4,
                                        maxvalue=maximum, parent=self.window)
        if diameter is None or not math.isfinite(diameter) or not 4 <= diameter <= maximum:
            return None
        from inspection.conn_geometry import circle_area
        self.pin_diameter = diameter
        return circle_area(center, (center[0] + diameter/2/width, center[1]), (height, width))

    def enter_pin_dimensions(self, center):
        dialog = tk.Toplevel(self.window)
        dialog.title("Pin box dimensions - original pixels")
        dialog.transient(self.window)
        variables = [tk.StringVar(value=f"{size:g}") for size in self.pin_dimensions]
        for row, (name, variable) in enumerate(zip(("width (px)", "height (px)"), variables)):
            tk.Label(dialog, text=name).grid(row=row, column=0, padx=12, pady=8)
            tk.Entry(dialog, textvariable=variable, width=12).grid(row=row, column=1, padx=12)
        result = [None]
        def accept(event=None):
            try:
                import math
                width, height = [float(variable.get()) for variable in variables]
                if not all(math.isfinite(size) and size >= 1 for size in (width, height)):
                    raise ValueError("Width and height must be finite and at least 1 pixel.")
                cx, cy = center
                half_w, half_h = width / self.image.width / 2, height / self.image.height / 2
                box = [cx - half_w, cy - half_h, cx + half_w, cy + half_h]
                if min(box) < 0 or max(box) > 1:
                    raise ValueError("The box must fit within the image. Choose smaller dimensions.")
            except ValueError as exc:
                messagebox.showerror("Invalid pin dimensions", str(exc), parent=dialog)
                return "break"
            self.pin_dimensions = (width, height)
            result[0] = box
            dialog.destroy()
            return "break"
        tk.Button(dialog, text="OK", command=accept).grid(row=2, column=1, pady=12)
        dialog.bind("<Return>", accept)
        dialog.bind("<Escape>", lambda event: dialog.destroy())
        dialog.grab_set()
        dialog.wait_window()
        self.window.grab_set()
        self.canvas.focus_set()
        return result[0]

    def enter_text(self, initial="", check=False, pattern=None, refer=""):
        dialog = tk.Toplevel(self.window)
        dialog.title("Expected text - English / French / Simplified / Traditional / symbols")
        dialog.transient(self.window)
        entry = tk.Text(dialog, width=54, height=5, font=("Segoe UI", 12))
        entry.pack(padx=12, pady=12)
        entry.insert("1.0", initial)
        check_var = tk.BooleanVar(value=check)
        digits_var = tk.StringVar(value=str(len(pattern) if pattern else 2))
        position_vars = []
        refer_var = tk.StringVar(value=refer)
        form = tk.Frame(dialog)
        if self.kind in ("OCR", "label"):
            form.pack(fill="x", padx=12)
            controls = tk.Frame(form)
            controls.pack(fill="x")
            refer_frame = tk.Frame(form)
            tk.Label(refer_frame,text="refer").pack(side="left")
            tk.Entry(refer_frame,textvariable=refer_var,width=48).pack(side="left",padx=8)
            grid_canvas = tk.Canvas(form, height=140, highlightthickness=0)
            scrollbar = tk.Scrollbar(form, orient="vertical", command=grid_canvas.yview)
            grid_canvas.configure(yscrollcommand=scrollbar.set)
            position_frame = tk.Frame(grid_canvas)
            grid_canvas.create_window((0, 0), window=position_frame, anchor="nw")
            position_frame.bind("<Configure>", lambda event: grid_canvas.configure(scrollregion=grid_canvas.bbox("all")))
            def rebuild(event=None):
                try:
                    count = int(digits_var.get())
                    if not 1 <= count <= 512:
                        return
                except ValueError:
                    return
                previous = [variable.get() for variable in position_vars] or list(pattern or [])
                for widget in position_frame.winfo_children():
                    widget.destroy()
                position_vars.clear()
                for index in range(count):
                    variable = tk.StringVar(value=previous[index] if index < len(previous) else "num")
                    position_vars.append(variable)
                    row, column = divmod(index, 4)
                    tk.Label(position_frame, text=str(index + 1)).grid(row=row, column=column * 2, padx=(2, 4), pady=3)
                    ttk.Combobox(position_frame, textvariable=variable, values=("num", "text"),
                                 state="readonly", width=7).grid(row=row, column=column * 2 + 1, padx=(0, 8), pady=3)
            def toggle():
                entry.configure(state="disabled" if check_var.get() else "normal")
                if check_var.get():
                    refer_frame.pack(fill="x",pady=6,before=grid_canvas if grid_canvas.winfo_manager() else None)
                    grid_canvas.pack(side="left", fill="x", expand=True, pady=8)
                    scrollbar.pack(side="right", fill="y", pady=8)
                else:
                    refer_frame.pack_forget()
                    grid_canvas.pack_forget()
                    scrollbar.pack_forget()
            tk.Checkbutton(controls, text="check", variable=check_var, command=toggle).pack(side="left")
            tk.Label(controls, text="digits").pack(side="left", padx=(12, 4))
            digits = tk.Spinbox(controls, from_=1, to=512, textvariable=digits_var, width=6, command=rebuild)
            digits.pack(side="left")
            digits_var.set(str(len(pattern) if pattern else 2))
            digits.bind("<KeyRelease>", rebuild)
            digits.bind("<FocusOut>", rebuild)
            rebuild()
            toggle()
        result = [None]
        def accept(event=None):
            value = entry.get("1.0", "end-1c")
            enabled = self.kind in ("OCR", "label") and check_var.get()
            if enabled:
                try:
                    count = int(digits_var.get())
                except ValueError:
                    count = 0
                if count != len(position_vars) or not 1 <= count <= 512:
                    messagebox.showerror("Invalid digits", "Choose 1 to 512 positions.", parent=dialog)
                    return "break"
                if not refer_var.get():
                    messagebox.showerror("refer", "Enter the actual characters printed in this training image. Test format is checked separately.",parent=dialog)
                    return "break"
            if not enabled and not value:
                return "break"
            result[0] = {"text": value}
            if self.kind in ("OCR", "label"):
                result[0].update(check=enabled, pattern=[variable.get() for variable in position_vars] if enabled else [],
                                 refer=refer_var.get() if enabled else "")
            dialog.destroy()
            return "break"
        def newline(event):
            entry.insert("insert", "\n")
            return "break"
        entry.bind("<Return>", accept)
        entry.bind("<Shift-Return>", newline)
        dialog.bind("<Escape>", lambda event: dialog.destroy())
        tk.Button(dialog, text="OK", command=accept).pack(pady=(0, 12))
        dialog.grab_set()
        entry.focus_set()
        dialog.wait_window()
        self.window.grab_set()
        self.canvas.focus_set()
        return result[0]

    def wheel(self, event):
        new_zoom = min(16, max(0.25, self.zoom * (1.15 if event.delta > 0 else 1 / 1.15)))
        ratio = new_zoom / self.zoom
        self.offset = [event.x - (event.x - self.offset[0]) * ratio,
                       event.y - (event.y - self.offset[1]) * ratio]
        self.zoom = new_zoom
        self.draw()

    def paste(self, event=None):
        if self.clipboard or self.clipboard_rois:
            self.remember_roi()
            self.regions.extend(copy.deepcopy(self.clipboard))
            self.rois.extend(copy.deepcopy(self.clipboard_rois))
            self.mode = "draw"
            self.draw()
        return "break"

    def copy_at_pointer(self):
        event = SimpleNamespace(x=self.canvas.winfo_pointerx() - self.canvas.winfo_rootx(),
                                y=self.canvas.winfo_pointery() - self.canvas.winfo_rooty())
        x, y = self.point(event)
        for region in reversed(self.regions):
            x1, y1, x2, y2 = region["box"]
            from inspection.conn_geometry import center_inside, area_geometry
            if region["role"] != "auto_exclude" and center_inside((x*self.image.width, y*self.image.height),
                    area_geometry(region, (self.image.height, self.image.width))):
                self.clipboard = [copy.deepcopy(region)]
                self.clipboard_rois = []
                return
        if self.kind in ("OCR", "label"):
            import cv2
            import numpy as np
            for roi in reversed(self.rois):
                if cv2.pointPolygonTest(np.asarray(roi["points"],np.float32),(x,y),False) >= 0:
                    self.clipboard = []
                    self.clipboard_rois = [copy.deepcopy(roi)]
                    return

    def move(self, step):
        if self.save() is False:
            return
        self.index = min(len(self.paths) - 1, max(0, self.index + step))
        self.load()

    def jump_to_frame(self, event=None):
        try:
            number = int(self.frame_number.get())
            if not 1 <= number <= len(self.paths):
                raise ValueError
        except ValueError:
            messagebox.showwarning("Frame number", f"Enter 1 to {len(self.paths)}.", parent=self.window)
            return "break"
        self.playing = False
        if self.save() is False:
            return "break"
        self.index = number - 1
        self.load()
        self.canvas.focus_set()
        return "break"

    def next_labeled_frame(self):
        self.playing = False
        if self.save() is False:
            return
        for index in range(self.index+1,len(self.paths)):
            data = read_json(self.directory/f"{self.paths[index].stem}.json",{})
            if data.get("rois") or any(region.get("role") not in ("exclude","auto_exclude")
                                       for region in data.get("regions",[])):
                self.index = index
                self.load()
                self.canvas.focus_set()
                return
        self.info.set("No later labeled frame")

    def cancel_action(self):
        self.editor.cancel(self.rotation_regions())
        if self.translation.baseline is not None:
            self.regions = self._translation_before["regions"]
            self.rois = self._translation_before["rois"]
        elif self.rotation_selection.baseline is not None:
            self.regions = self._rotation_before["regions"]
            self.rois = self._rotation_before["rois"]
        elif getattr(self,"roi_vertex",None) is not None and self.history:
            previous = self.history.pop()
            self.regions,self.rois = previous["regions"],previous["rois"]
        self.playing = False
        self.mode = "edit"
        self.start = self.pin_copy = self.group_copy = self.pin_axis = self.roi_vertex = None
        self.roi_pending = []
        self.copy_selected = []
        self.rotation_selection.clear()
        self.translation = TranslationInteraction()
        self.canvas.delete("drag")
        self.draw()

    def key(self, event):
        if isinstance(getattr(event, "widget", None), (tk.Entry, ttk.Entry)) and event.keysym.lower() != "escape":
            return
        key = event.keysym.lower()
        if event.state & 4:
            return
        if key == "escape":
            self.cancel_action()
        elif key in ("left", "right", "up", "down"):
            dx, dy = {"left": (80, 0), "right": (-80, 0), "up": (0, 80), "down": (0, -80)}[key]
            self.offset[0] += dx
            self.offset[1] += dy
            self.draw()
        elif key in ("n", "p"):
            self.move(1 if key == "n" else -1)
        elif key == "e":
            self.mode = "exclude"
            self.playing = False
            self.start = self.pin_copy = self.group_copy = None
            self.translation = TranslationInteraction()
            self.draw()
        elif self.kind == "conn" and key == "m":
            self.mode = "boundary"
            self.start = self.pin_copy = self.group_copy = None
            self.copy_selected = []
            self.playing = False
            self.draw()
        elif key in ("bracketleft", "bracketright", "braceleft", "braceright"):
            from inspection.rotated_regions import region_polygon, point_in_region
            x = self.canvas.winfo_pointerx() - self.canvas.winfo_rootx()
            y = self.canvas.winfo_pointery() - self.canvas.winfo_rooty()
            point = self.point(SimpleNamespace(x=x, y=y))
            point = (point[0] * self.image.width, point[1] * self.image.height)
            shape = (self.image.height, self.image.width)
            for region in reversed(self.regions):
                if region["role"] == "auto_exclude" or not point_in_region(point, region_polygon(region, shape)):
                    continue
                delta = (-1 if key in ("bracketleft", "braceleft") else 1) * (1 if key in ("braceleft", "braceright") else 5)
                candidate = dict(region, angle=(region.get("angle", 0) + delta) % 360,clip_to_frame=True)
                polygon = region_polygon(candidate, shape)
                if (polygon >= 0).all() and (polygon[:, 0] < self.image.width).all() and (polygon[:, 1] < self.image.height).all():
                    self.remember()
                    region.update(candidate)
                    self.draw()
                break
        elif key == "u" and self.history:
            previous = self.history.pop()
            if isinstance(previous, dict):
                self.rois, self.regions = previous["rois"], previous["regions"]
            else:
                self.regions = previous
            self.rotation_selection.clear()
            self.copy_selected = []
            self.dirty = True
            self.translation = TranslationInteraction()
            self.draw()
        elif key in ("d", "c"):
            self.mode = "delete" if key == "d" else "copy"
            if key == "c":
                self.copy_at_pointer()
            self.draw()
        elif key == "space":
            self.playing = not self.playing
        elif self.kind == "label" and key in "1234567890" and len(key) == 1:
            self.number_var.set(int(key))
            self.select_number()
        elif self.kind == "conn" and key in ("1", "2", "3"):
            self.role.set({"1": "pin_point", "2": "pin", "3": "slot"}[key])
            self.mode = "draw"

    def tick(self):
        if not self.window.winfo_exists():
            return
        if getattr(self, "_tick_after", None):
            self.window.after_cancel(self._tick_after)
        if self.playing:
            if self.index == len(self.paths) - 1:
                self.playing = False
                self.save()
                self.index = 0
                self.load()
            else:
                self.move(1)
        self._tick_after = self.window.after(100, self.tick)

    def close(self):
        self.playing = False
        if self.save() is False:
            return
        if getattr(self, "_tick_after", None):
            self.window.after_cancel(self._tick_after)
        self.window.destroy()
