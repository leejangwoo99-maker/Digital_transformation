"""Native controls and original-image viewport for the appearance labeler."""
import tkinter as tk
from tkinter import messagebox, ttk

import cv2
import numpy as np
from PIL import Image,ImageTk

from edit_interaction import EditInteraction

SHORTCUT_MANUAL = (
    "0-9: Select class  |  A: Anchor  |  Ctrl+V: Paste  |  U: Undo  |  Esc: Cancel / Edit\n"
    "N / P: Next / Previous frame  |  Space: Play / Pause  |  Wheel: Zoom  |  Arrows: Pan\n"
    "[ / ]: Rotate 5 degrees (Shift: 1 degree)  |  Alt+F4: Save and close"
)


def render_viewport(image,size,scale,offset):
    return image.transform(size,Image.Transform.AFFINE,
        (1/scale,0,-offset[0]/scale,0,1/scale,-offset[1]/scale),
        resample=Image.Resampling.BICUBIC)


class AppearanceLabelerWindow:
    def __init__(self,controller,parent=None):
        self.controller = controller
        self.owned_root = None
        if parent is None:
            parent = tk._default_root
        if parent is None:
            self.owned_root = parent = tk.Tk()
            parent.withdraw()
        self.window = tk.Toplevel(parent)
        self.window.title("Open Labeler")
        self.window.geometry("1100x800")
        self.window.protocol("WM_DELETE_WINDOW",self.close)
        self.window.bind("<Alt-F4>",lambda event:(self.close(),"break")[1])
        self.closed = False
        self.timer = None
        self.source_key = None
        self.viewport_size = None
        self.frame_number = tk.StringVar(value="1")
        self.info = tk.StringVar()
        self.class_number = tk.IntVar(value=-1)
        toolbar = tk.Frame(self.window)
        toolbar.pack(fill="x")
        self.frame_entry = tk.Entry(toolbar,textvariable=self.frame_number,width=7)
        self.frame_entry.pack(side="left",padx=(8,0))
        self.frame_entry.bind("<Return>",self.jump)
        tk.Label(toolbar,text=f"/{len(controller.image_paths)}").pack(side="left")
        tk.Button(toolbar,text=">>",command=self.next_labeled).pack(side="left",padx=4)
        self.proto_frames = tk.StringVar()
        tk.Label(toolbar,text="Source frames").pack(side="left",padx=(8,2))
        tk.Entry(toolbar,textvariable=self.proto_frames,width=16).pack(side="left")
        tk.Button(toolbar,text="proto-label",command=self.proto_label).pack(side="left",padx=4)
        tk.Label(toolbar,textvariable=self.info,anchor="w").pack(side="left",padx=8)
        choices = tk.Frame(self.window)
        choices.pack(fill="x")
        for index,(number,name) in enumerate(sorted(controller.classes.items())):
            if number == controller.auto_exclude_class_id:
                continue
            tk.Radiobutton(choices,text=f"{number} {name}",variable=self.class_number,value=number,
                command=self.select_class).grid(row=index//8,column=index%8,sticky="w")
        tools = tk.Frame(self.window)
        tools.pack(fill="x")
        for text,mode,enabled in (("Anchor","anchor",any(x is not None for x in controller.anchor_class_ids)),
                                  ("ROI","roi",controller.roi_class_id is not None),
                                  ("Exclude","exclude",controller.exclude_class_id is not None)):
            tk.Button(tools,text=text,command=lambda mode=mode:self.tool(mode),
                      state="normal" if enabled else "disabled").pack(side="left")
        shortcuts = [(number,key) for number,key in (
            (controller.roi_class_id,"R"),(controller.exclude_class_id,"E"),
            (controller.except_class_id,"T"),(controller.essential_class_id,"F")) if number is not None]
        shortcut_labels = [f"{number}: {key}" for number,key in shortcuts] + ["C: Copy","D: Delete"]
        tk.Label(tools,text="   |   ".join(shortcut_labels)).pack(side="left",padx=(12,8))
        if controller.essential_class_id is not None:
            from inspection.essential_shapes import load_essential_shapes
            try:
                schedule = load_essential_shapes(controller.image_paths,controller.label_dir,controller.classes)
            except (OSError,ValueError):
                schedule = {}
            self.essential_group_numbers = {1} | {r['group'] for record in schedule.values()
                                                  for r in record['regions'] if r.get('group') is not None}
            self.essential_group = tk.StringVar(value=str(controller.essential_group))
            tk.Label(tools,text="Essential group").pack(side="left",padx=(8,3))
            self.essential_group_field = ttk.Combobox(tools,width=6,textvariable=self.essential_group,
                                                      values=sorted(self.essential_group_numbers))
            self.essential_group_field.pack(side="left")
            self.essential_group_field.bind("<<ComboboxSelected>>",self.select_essential_group)
            self.essential_group_field.bind("<Return>",self.select_essential_group)
            tk.Button(tools,text="Apply group",command=self.select_essential_group).pack(side="left",padx=4)
        self.manual = tk.Label(self.window,text=SHORTCUT_MANUAL,bg="black",fg="white",
                               justify="left",anchor="w",font=("Segoe UI",10))
        self.manual.pack(fill="x")
        self.manual.bind("<Configure>",lambda event:self.manual.configure(wraplength=max(1,event.width-16)))
        self.canvas = tk.Canvas(self.window,bg="black",highlightthickness=0)
        self.canvas.pack(fill="both",expand=True)
        self.canvas.bind("<Configure>",self.draw)
        for event,kind in (("<Button-1>",cv2.EVENT_LBUTTONDOWN),
                           ("<B1-Motion>",cv2.EVENT_MOUSEMOVE),
                           ("<Motion>",cv2.EVENT_MOUSEMOVE),
                           ("<ButtonRelease-1>",cv2.EVENT_LBUTTONUP)):
            self.canvas.bind(event,lambda event,kind=kind:self.mouse(event,kind))
        self.canvas.bind("<MouseWheel>",self.wheel)
        self.window.bind("<KeyPress>",self.key)
        controller.header_height = 0
        controller.label_dir.mkdir(parents=True,exist_ok=True)
        controller._load_current_image()
        self.window.update_idletasks()
        self.draw()
        self.canvas.focus_set()
        self.timer = self.window.after(100,self.tick)

    def run(self):
        self.window.grab_set()
        self.window.wait_window()
        if self.owned_root is not None:
            self.owned_root.destroy()

    def proto_label(self):
        from inspection.proto_labels import parse_frames,generate
        import queue
        import threading
        try:
            numbers = parse_frames(self.proto_frames.get(),len(self.controller.image_paths))
            self.controller.playing = False
            self.controller._save_label()
        except (ValueError,OSError) as exc:
            messagebox.showerror("proto-label",str(exc),parent=self.window)
            return
        if not messagebox.askyesno("proto-label","Supplement missing labels from manual source frames. Defects use YOLO confidence >= 70%; Essential groups are added only if every shape matches. Existing labels are preserved.",parent=self.window):
            return
        busy = tk.Toplevel(self.window)
        busy.title("proto-label")
        status = tk.StringVar(value="Preparing training...")
        tk.Label(busy,textvariable=status,padx=20,pady=16).pack()
        bar = ttk.Progressbar(busy,mode="indeterminate",length=320)
        bar.pack(padx=20,pady=(0,16))
        bar.start()
        busy.protocol("WM_DELETE_WINDOW",lambda:None)
        busy.grab_set()
        messages = queue.Queue()
        def worker():
            try:
                result = generate(self.controller,numbers,lambda text:messages.put(("status",text)))
                messages.put(("done",result))
            except Exception as exc:
                messages.put(("error",str(exc)))
        def poll():
            while not messages.empty():
                kind,value = messages.get()
                if kind == "status":
                    status.set(value)
                    continue
                bar.stop()
                busy.destroy()
                self.window.grab_set()
                self.controller._load_current_image()
                self.draw()
                if kind == "error":
                    messagebox.showerror("proto-label",value,parent=self.window)
                else:
                    path,report = value
                    messagebox.showinfo("proto-label",f"Added: {len(report['generated'])}\nPreserved: {len(report['preserved'])}\nSkipped: {len(report['skipped'])}\nReport: {path}\nReview generated labels before final training.",parent=self.window)
                return
            self.window.after(100,poll)
        threading.Thread(target=worker,daemon=True).start()
        poll()

    def select_class(self):
        c = self.controller
        c._cancel_current_action()
        c.current_class_id = self.class_number.get()
        c._set_mode("label")
        self.canvas.focus_set()
        self.draw()

    def select_essential_group(self,event=None):
        c = self.controller
        try:
            group = int(self.essential_group.get())
            if not 1 <= group <= 999999:
                raise ValueError("Enter an Essential group number from 1 to 999999.")
            c._cancel_current_action()
            c.select_essential_group(group)
        except (OSError,ValueError) as exc:
            messagebox.showerror("Essential group",str(exc),parent=self.window)
            return "break"
        self.essential_group_numbers.add(group)
        self.essential_group_field.configure(values=sorted(self.essential_group_numbers))
        self.essential_group.set(str(group))
        self.class_number.set(c.essential_class_id)
        self.select_class()
        return "break"

    def tool(self,mode):
        c = self.controller
        if (mode == "roi" and c.roi_class_id is None) or (mode == "exclude" and c.exclude_class_id is None):
            return
        c._cancel_current_action()
        c._set_mode(mode)
        if mode == "roi":
            c.current_class_id = c.roi_class_id
        elif mode == "exclude":
            c.current_class_id = c.exclude_class_id
        self.class_number.set(-1)
        self.canvas.focus_set()
        self.draw()

    def jump(self,event=None):
        if not self.controller.jump_to_frame(self.frame_number.get()):
            messagebox.showwarning("Frame number",f"Enter 1 to {len(self.controller.image_paths)}.",parent=self.window)
            return "break"
        self.draw()
        self.canvas.focus_set()
        return "break"

    def next_labeled(self):
        c = self.controller
        c.playing = False
        c._save_label()
        excluded = {c.auto_exclude_class_id,c.exclude_class_id}
        for index in range(c.index+1,len(c.image_paths)):
            path = c.label_dir/f"{c.image_paths[index].stem}.txt"
            if path.exists() and any(row.split() and int(float(row.split()[0])) not in excluded
                                     for row in path.read_text(encoding="utf-8").splitlines()):
                c.index = index
                c._load_current_image()
                self.draw()
                return
        self.info.set("No later labeled frame")

    def mouse(self,event,kind):
        self.canvas.focus_set()
        flags = cv2.EVENT_FLAG_SHIFTKEY if event.state & 1 else 0
        self.controller._on_mouse(kind,event.x,event.y,flags,None)
        self.draw()

    def wheel(self,event):
        self.controller._zoom_at(event.x,event.y,1.15 if event.delta > 0 else 1/1.15)
        self.draw()

    def key(self,event):
        name = event.keysym.lower()
        if isinstance(event.widget,(tk.Entry,ttk.Combobox)) and name != "escape":
            return
        c = self.controller
        if name == "escape":
            c._cancel_current_action()
            self.class_number.set(-1)
        elif event.state & 4:
            if name == "v":
                c._paste_clipboard_boxes()
                c._save_label()
            else:
                return
        elif name in ("n","p"):
            c._move(1 if name == "n" else -1)
        elif name in ("left","right","up","down"):
            c._pan_by(*{"left":(80,0),"right":(-80,0),"up":(0,80),"down":(0,-80)}[name])
        elif name == "space":
            c.playing = not c.playing
        elif name in ("a","r","e","d","c"):
            self.tool({"a":"anchor","r":"roi","e":"exclude","d":"delete","c":"copy"}[name])
            return
        elif name in ("t","f"):
            number = c.except_class_id if name == "t" else c.essential_class_id
            if number is not None:
                self.class_number.set(number)
                self.select_class()
            return
        elif name == "u":
            if c.edit_undo and c.edit_undo[-1][0] == c.boxes:
                _,c.boxes,c.rotations = c.edit_undo.pop()
            elif c.translation_undo and c.translation_undo[-1][0] == c.boxes:
                _,c.boxes,c.rotations = c.translation_undo.pop()
            elif c.rotation_undo and c.rotation_undo[-1][0] == c.boxes:
                _,c.rotations = c.rotation_undo.pop()
            elif c.boxes:
                c.boxes.pop()
            c.editor = EditInteraction()
            c._save_label()
        elif name in ("bracketleft","bracketright","braceleft","braceright"):
            c._rotate_hovered((-1 if name in ("bracketleft","braceleft") else 1)*
                             (1 if event.state & 1 else 5))
        elif len(name) == 1 and name.isdigit() and int(name) in c.classes and int(name) != c.auto_exclude_class_id:
            self.class_number.set(int(name))
            self.select_class()
            return
        self.draw()

    def draw(self,event=None):
        c = self.controller
        if c.image is None or self.closed:
            return
        width,height = max(1,self.canvas.winfo_width()),max(1,self.canvas.winfo_height())
        if self.viewport_size != (width,height):
            self.viewport_size = width,height
            c.max_display_width,c.max_display_height = width,height
            c.base_scale = min(width/c.image.shape[1],height/c.image.shape[0])
            c._recenter_image()
        key = c.image_paths[c.index]
        if self.source_key != key:
            self.source_key = key
            self.source = Image.fromarray(cv2.cvtColor(c.image,cv2.COLOR_BGR2RGB))
        self.photo = ImageTk.PhotoImage(render_viewport(self.source,(width,height),c.scale,(c.offset_x,c.offset_y)))
        self.canvas.delete("all")
        self.canvas.create_image(0,0,anchor="nw",image=self.photo)
        from inspection.rotated_regions import region_polygon
        offset = np.array([c.offset_x,c.offset_y])
        for box,region in zip(c.boxes,c._rotation_regions()):
            if box[0] == c.auto_exclude_class_id:
                continue
            polygon = region_polygon(region,c.image.shape)*c.scale+offset
            b,g,r = c._box_color(box[0])
            color = f"#{r:02x}{g:02x}{b:02x}"
            self.canvas.create_polygon(*polygon.ravel(),fill="",outline=color,width=2)
            x,y = polygon.min(axis=0)
            label = f"{box[0]}:{c.classes.get(box[0],'')}"
            if box[0] == c.essential_class_id and box in c.essential_groups:
                label += f" G{c.essential_groups[box]}"
            self.canvas.create_text(x,y-3,text=label,fill=color,
                                    anchor="sw",font=("Segoe UI",11))
        if c.drag_start is not None and c.drag_end is not None:
            a,b = [np.asarray(point)*c.scale+offset for point in (c.drag_start,c.drag_end)]
            self.canvas.create_rectangle(*a,*b,outline="#ffff00",width=2)
        if c.edit_mode:
            polygons,handles = c.editor.geometry(c._rotation_regions(),c.image.shape,c.scale)
            for polygon in polygons:
                self.canvas.create_polygon(*(polygon*c.scale+offset).ravel(),fill="",outline="#ffff00",width=3)
            if handles:
                self.canvas.create_line(*(handles["move"]*c.scale+offset),*(handles["rotate"]*c.scale+offset),fill="#ffff00")
            for name,point in handles.items():
                x,y = point*c.scale+offset
                if name in ("move","rotate"):
                    self.canvas.create_oval(x-6,y-6,x+6,y+6,fill="#00ffff" if name == "move" else "#ffff00",outline="black")
                else:
                    self.canvas.create_rectangle(x-4,y-4,x+4,y+4,fill="white",outline="black")
            if c.editor.action == "select":
                a,b = [point*c.scale+offset for point in (c.editor.start,c.editor.end)]
                self.canvas.create_rectangle(*a,*b,outline="#ffff00",width=2)
        mode = ("edit" if c.edit_mode else "copy" if c.copy_mode else "delete" if c.delete_mode else
                "anchor" if c.anchor_mode else "ROI" if c.roi_mode else "exclude" if c.exclude_mode else "draw")
        self.info.set(f"{c.index+1}/{len(c.image_paths)}  {key.name}  {c.zoom:.2f}x  {mode}")
        if self.window.focus_get() is not self.frame_entry:
            self.frame_number.set(str(c.index+1))

    def tick(self):
        if self.closed:
            return
        c = self.controller
        if c.playing:
            if c.index == len(c.image_paths)-1:
                c._save_label()
                c.index = 0
                c._load_current_image()
                c.playing = False
            else:
                c._move(1)
            self.draw()
        self.timer = self.window.after(100,self.tick)

    def close(self):
        if self.closed:
            return
        try:
            self.controller._save_label()
        except Exception as exc:
            messagebox.showerror("Save failed",str(exc),parent=self.window)
            return
        self.closed = True
        self.controller.playing = False
        if self.timer is not None:
            self.window.after_cancel(self.timer)
        self.window.destroy()
