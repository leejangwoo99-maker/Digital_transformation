"""Latest-only delivery keeps inspection workers independent of Tk rendering."""
import threading
import tkinter as tk
from tkinter import ttk

from PIL import Image, ImageTk


class LatestResult:
    def __init__(self):
        self.lock = threading.Lock()
        self.value = None

    def submit(self, image, number, total, statuses, details=()):
        with self.lock:
            self.value = image, number, total, tuple(statuses), tuple(details)

    def take(self):
        with self.lock:
            value, self.value = self.value, None
            return value


class LiveResultView(tk.Frame):
    def __init__(self, parent):
        super().__init__(parent)
        self.caption = tk.StringVar(value="Inspection result: waiting")
        caption = tk.Label(self,textvariable=self.caption,anchor="w",justify="left",wraplength=700)
        caption.pack(fill="x")
        caption.bind("<Configure>",lambda event:caption.configure(wraplength=max(80,event.width-12)))
        tools = tk.Frame(self)
        tools.pack(fill="x")
        self.pipeline_time = tk.StringVar(value='pipeline time(sec) : 0.0000000')
        tk.Label(tools,textvariable=self.pipeline_time).pack(side='right',padx=8)
        self.pipeline_date = tk.StringVar(value='pipeline date : -')
        tk.Label(self,textvariable=self.pipeline_date,anchor='w').pack(fill='x')
        for label,command in (("-",lambda:self.change_zoom(1/1.25)),("+",lambda:self.change_zoom(1.25)),("Fit",self.fit)):
            ttk.Button(tools,text=label,width=5,command=command).pack(side="left")
        self.zoom_label = tk.StringVar(value="100%")
        tk.Label(tools,textvariable=self.zoom_label).pack(side="left",padx=8)
        self.panes = tk.PanedWindow(self,orient="horizontal",sashwidth=6)
        self.panes.pack(fill="both",expand=True)
        self.canvas = tk.Canvas(self.panes,bg="black",highlightthickness=0)
        detail_frame = tk.Frame(self.panes)
        self.details = tk.Text(detail_frame,width=32,wrap="word",font=("Malgun Gothic",10),state="disabled")
        scroll = ttk.Scrollbar(detail_frame,orient="vertical",command=self.details.yview)
        self.details.configure(yscrollcommand=scroll.set)
        scroll.pack(side="right",fill="y")
        self.details.pack(fill="both",expand=True)
        self.panes.add(self.canvas,minsize=180,stretch="always")
        self.panes.add(detail_frame,minsize=160,width=280,stretch="never")
        self.image = None
        self.zoom = 1.
        self.pan = [0.,0.]
        self.canvas.bind("<Configure>",lambda event:self.render())
        self.canvas.bind("<MouseWheel>",lambda event:self.change_zoom(1.25 if event.delta > 0 else 1/1.25))
        self.canvas.bind("<ButtonPress-1>",self.begin_pan)
        self.canvas.bind("<B1-Motion>",self.drag_pan)
        self.canvas.bind("<Double-Button-1>",lambda event:self.fit())

    def show(self, image, number, total, statuses, details=()):
        self.image = image
        self.caption.set(f"Inspected frame {number}/{total}\n"+' | '.join(statuses))
        from inspection.annotation_details import failure_detail_lines
        self.details.configure(state="normal")
        self.details.delete("1.0","end")
        self.details.insert("1.0","\n".join(list(statuses)+failure_detail_lines(details)))
        self.details.configure(state="disabled")
        self.render()

    def fit(self):
        self.zoom,self.pan = 1.,[0.,0.]
        self.render()

    def reset(self):
        self.image = None
        self.canvas.delete("all")
        self.caption.set("Inspection result: waiting")
        self.details.configure(state="normal")
        self.details.delete("1.0","end")
        self.details.configure(state="disabled")
        self.zoom,self.pan = 1.,[0.,0.]
        self.zoom_label.set("100%")

    def change_zoom(self, factor):
        self.zoom = min(12.,max(1.,self.zoom*factor))
        if self.zoom == 1.:
            self.pan = [0.,0.]
        self.render()

    def begin_pan(self, event):
        self.drag_start = (event.x,event.y,*self.pan)

    def drag_pan(self, event):
        if hasattr(self,"drag_start"):
            x,y,px,py = self.drag_start
            self.pan = [px+event.x-x,py+event.y-y]
            self.render()

    def render(self):
        if self.image is None:
            return
        width,height = max(1,self.canvas.winfo_width()),max(1,self.canvas.winfo_height())
        source_width,source_height = self.image.size
        scale = min(width/source_width,height/source_height)*self.zoom
        rendered_width,rendered_height = source_width*scale,source_height*scale
        self.pan = [max(-max(0,(rendered_width-width)/2),min(max(0,(rendered_width-width)/2),self.pan[0])),
                    max(-max(0,(rendered_height-height)/2),min(max(0,(rendered_height-height)/2),self.pan[1]))]
        left,top = (width-rendered_width)/2+self.pan[0],(height-rendered_height)/2+self.pan[1]
        # Render only the visible source crop, never allocate an oversized zoomed image.
        x1,y1 = max(0.,-left/scale),max(0.,-top/scale)
        x2,y2 = min(source_width,(width-left)/scale),min(source_height,(height-top)/scale)
        image = self.image.resize((max(1,round((x2-x1)*scale)),max(1,round((y2-y1)*scale))),
                                  Image.Resampling.BILINEAR,box=(x1,y1,x2,y2))
        self.photo = ImageTk.PhotoImage(image)
        self.canvas.delete("all")
        self.canvas.create_image(max(0,left),max(0,top),image=self.photo,anchor="nw")
        self.zoom_label.set(f"{self.zoom*100:.0f}%")
