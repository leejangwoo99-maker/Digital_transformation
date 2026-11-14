"""Display-only draggable camera alignment guides."""
import tkinter as tk

from PIL import ImageTk


class AlignmentPreview(tk.Canvas):
    def __init__(self, parent):
        super().__init__(parent, bg="black", highlightthickness=0)
        self.source = None
        self.guide_x = self.guide_y = .5
        self.drag_axis = None
        self.image_bounds = None
        self.inspection_guides = []
        self.guides_visible = True
        self.bind("<Configure>", self.render)
        self.bind("<ButtonPress-1>", self.begin_drag)
        self.bind("<B1-Motion>", self.drag)
        self.bind("<ButtonRelease-1>", self.end_drag)
        self.bind("<Motion>", self.hover)

    def show(self, image):
        self.source = image.copy()
        self.render()

    def set_inspection_guides(self, layers):
        self.inspection_guides = layers
        self.render()

    def show_inspection_guides(self, visible):
        self.guides_visible = bool(visible)
        self.render()

    def render(self, event=None):
        if self.source is None:
            return
        image = self.source.copy()
        width, height = max(1,self.winfo_width()), max(1,self.winfo_height())
        image.thumbnail((width,height))
        left, top = (width-image.width)/2, (height-image.height)/2
        right, bottom = left+image.width, top+image.height
        self.image_bounds = left,top,right,bottom
        self.photo = ImageTk.PhotoImage(image)
        self.delete("all")
        self.create_image(left,top,image=self.photo,anchor="nw")
        if self.guides_visible and self.inspection_guides:
            from camera.inspection_guides import projected_guides
            for guide in projected_guides(self.inspection_guides,self.source.size):
                points = guide['points']*[image.width/self.source.width,image.height/self.source.height]+[left,top]
                self.create_polygon(*points.ravel(),outline=guide['color'],fill='',width=2,tags='inspection-guide')
        x,y = left+self.guide_x*image.width, top+self.guide_y*image.height
        self.create_line(x,top,x,bottom,fill="#ff2222",width=2,tags="vertical")
        self.create_line(left,y,right,y,fill="#ff2222",width=2,tags="horizontal")

    def hit(self, event):
        if self.image_bounds is None:
            return None
        left,top,right,bottom = self.image_bounds
        if not (left <= event.x <= right and top <= event.y <= bottom):
            return None
        dx = abs(event.x-(left+self.guide_x*(right-left)))
        dy = abs(event.y-(top+self.guide_y*(bottom-top)))
        if min(dx,dy) > 8:
            return None
        if max(dx,dy) <= 8:
            return "both"
        return "x" if dx < dy else "y"

    def hover(self, event):
        axis = self.hit(event)
        self.configure(cursor={"x":"sb_h_double_arrow","y":"sb_v_double_arrow",
                               "both":"fleur"}.get(axis,""))

    def begin_drag(self, event):
        self.drag_axis = self.hit(event)

    def drag(self, event):
        if self.drag_axis is None or self.image_bounds is None:
            return
        left,top,right,bottom = self.image_bounds
        if self.drag_axis in ("x","both"):
            self.guide_x = min(1.,max(0.,(event.x-left)/(right-left)))
        if self.drag_axis in ("y","both"):
            self.guide_y = min(1.,max(0.,(event.y-top)/(bottom-top)))
        self.render()

    def end_drag(self, event):
        self.drag_axis = None
