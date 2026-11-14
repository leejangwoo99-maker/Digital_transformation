"""Appearance-only per-frame confidence records and a native review window."""
import json
import math
import re
from pathlib import Path

import cv2


def draw_review_boxes(image, detections, threshold):
    import numpy as np
    annotated = image.copy()
    for item, label in detections:
        value = float(item["confidence"])*100
        color = (0,0,255) if value >= threshold else (0,170,0)
        if item.get("polygon") is not None:
            cv2.polylines(annotated,[np.round(item["polygon"]).astype(np.int32)],True,color,2)
        else:
            x1,y1,x2,y2 = map(round,item["xyxy"])
            cv2.rectangle(annotated,(x1,y1),(x2,y2),color,2)
        x,y = map(round,item["xyxy"][:2])
        state = 'NG' if value >= threshold else 'OK'
        caption = f"{label} {value:.2f}% {state}"
        width = cv2.getTextSize(caption,cv2.FONT_HERSHEY_SIMPLEX,.6,2)[0][0]
        cv2.putText(annotated,caption,(max(0,min(x,image.shape[1]-width-2)),max(18,y-5)),
                    cv2.FONT_HERSHEY_SIMPLEX,.6,color,2,cv2.LINE_AA)
    return annotated


class ConfidenceRecorder:
    def __init__(self, result_directory, group, threshold, source, fps, run=None, *, review_thresholds=None):
        if review_thresholds:
            review_thresholds = sorted({float(value) for value in review_thresholds})
            if any(not 1 <= value <= 100 for value in review_thresholds):
                raise ValueError('Review thresholds must be within the recorded range (1 to 100%).')
        parent = Path(result_directory).resolve()
        self.run = run
        self.directory = Path(run[0]) if run else parent / "confidence_point_frame"
        if self.directory.is_symlink() or self.directory.resolve().parent != parent:
            raise ValueError("Unsafe confidence record directory")
        self.directory.mkdir(parents=True,exist_ok=True)
        # Only replace files owned by this recorder, never other inspection outputs.
        for path in (() if run else self.directory.glob("frame_no_*_*.png")):
            if re.fullmatch(r"frame_no_\d+_\d+\.\d{2}\.png",path.name) and path.is_file() and not path.is_symlink():
                path.unlink()
        self.data = dict(version=1,group=group,threshold_percent=float(threshold),
                         recording_floor_percent=1.,source=str(source),fps=float(fps),status="running",frames=[],
                         raw_frame_images=True)
        if review_thresholds:
            self.data['review_thresholds'] = review_thresholds
        if run:
            self.data.update(run_number=run[1],parent_name=parent.name)
        self.flush()

    def finish(self,status):
        self.flush(status)
        if self.run:
            from inspection.confidence_runs import finish_run
            self.directory = finish_run(self.directory,self.run[1])
            self.run = None
            if status == 'completed':
                from inspection.cpk_review import save_label_points
                save_label_points(self.directory.parent)

    def flush(self, status=None):
        if status:
            self.data["status"] = status
        path = self.directory / "index.json"
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps(self.data,ensure_ascii=False,indent=2),encoding="utf-8")
        temporary.replace(path)

    def add(self, number, image, detections):
        score = max((float(item["confidence"]) for item in detections),default=0.)
        percent = score*100
        failed = bool(detections) and percent >= self.data["threshold_percent"]
        name = f"frame_no_{number}_{percent:.2f}.png"
        if not cv2.imwrite(str(self.directory/name),image,[cv2.IMWRITE_PNG_COMPRESSION,1]):
            raise RuntimeError(f"Could not save confidence frame: {name}")
        self.data["frames"].append(dict(frame_number=number,score_percent=round(percent,2),
            max_confidence=score,result="NG" if failed else "OK",image=name,detections=detections))


def load_review(folder):
    directory = Path(folder)
    if not (directory/'index.json').is_file() and not (directory/'confidence_point_frame/index.json').is_file():
        from inspection.confidence_runs import load_runs
        runs = load_runs(directory)
        data = dict(runs[-1][1])
        data['frames'] = []
        for run_folder,run_data in runs:
            _,validated = load_review(run_folder)
            for item in validated['frames']:
                relative = run_folder.resolve().relative_to(directory.resolve())
                data['frames'].append(dict(item,image=str(relative/item['image']),run_number=run_data['run_number'],
                                           raw_frame_image=validated.get('raw_frame_images',False)))
        data['runs'] = runs
        return directory.resolve(),data
    if not (directory/'index.json').is_file():
        directory /= "confidence_point_frame"
    directory = directory.resolve()
    path = directory/"index.json"
    if not path.exists():
        raise ValueError("No confidence records in this folder. Run a new appearance inspection first.")
    data = json.loads(path.read_text(encoding="utf-8"))
    threshold = float(data["threshold_percent"])
    if not math.isfinite(threshold) or not 0 < threshold <= 100:
        raise ValueError("Invalid threshold in confidence records")
    frames = sorted(data["frames"],key=lambda item:item["frame_number"])
    if not frames:
        raise ValueError("No recorded appearance inspection frames")
    if len({item['frame_number'] for item in frames}) != len(frames):
        raise ValueError('Duplicate frame numbers in confidence records')
    for item in frames:
        if isinstance(item['frame_number'],bool) or not isinstance(item['frame_number'],int) or item['frame_number'] < 1:
            raise ValueError('Invalid confidence frame number')
        image = (directory/item["image"]).resolve()
        if image.parent != directory or not image.is_file():
            raise ValueError(f"Missing or invalid frame image: {item['image']}")
        if not math.isfinite(float(item["score_percent"])) or not 0 <= item["score_percent"] <= 100:
            raise ValueError("Invalid confidence score")
        raw = float(item["max_confidence"])
        if not math.isfinite(raw) or not 0 <= raw <= 1 or item["result"] not in ("OK","NG"):
            raise ValueError("Invalid confidence result")
    data["frames"] = frames
    return directory,data


def load_review_sets(folder):
    directory = Path(folder).resolve()
    if (directory/'index.json').is_file() or (directory/'confidence_point_frame/index.json').is_file():
        directory,data = load_review(directory)
        thresholds = review_thresholds(data)
        return directory,{threshold:review_at_threshold(data,threshold) for threshold in thresholds}
    from inspection.confidence_runs import load_runs
    grouped = {}
    for run_folder,run_data in load_runs(directory,allow_mixed_thresholds=True):
        _,validated = load_review(run_folder)
        run = run_data['run_number']
        for threshold in review_thresholds(validated):
            records = grouped.setdefault(threshold,{})
            physical = threshold == float(validated['threshold_percent'])
            if run not in records or physical:
                records[run] = (run_folder,dict(validated,run_number=run),physical)
    sets = {}
    for threshold,records in grouped.items():
        data = None
        for run,(run_folder,validated,_) in sorted(records.items()):
            view = review_at_threshold(validated,threshold)
            if data is None:
                data = dict(view,frames=[],runs=[])
            data['runs'].append((run_folder,view))
            relative = run_folder.resolve().relative_to(directory)
            data['frames'].extend(dict(item,image=str(relative/item['image']),run_number=run,
                                      raw_frame_image=validated.get('raw_frame_images',False))
                                  for item in view['frames'])
        sets[threshold] = data
    return directory,dict(sorted(sets.items()))


def review_thresholds(data):
    values = {float(data['threshold_percent'])}
    floor = float(data.get('recording_floor_percent',1.))
    if data.get('raw_frame_images'):
        values.update(value for item in data.get('review_thresholds',[])
                      if floor <= (value := float(item)) <= 100)
    return sorted(values)


def review_at_threshold(data,threshold):
    if threshold == float(data['threshold_percent']):
        return data
    return dict(data,threshold_percent=threshold,frames=[
        dict(item,result='NG' if item['detections'] and item['max_confidence']*100 >= threshold else 'OK')
        for item in data['frames']])


def changed_box_scores(current, other):
    """Compare same-frame, same-class nearest boxes in paired runs."""
    from inspection.cpk_review import match_detections
    from collections import defaultdict

    def by_run(data):
        runs = defaultdict(dict)
        for item in data['frames']:
            runs[item.get('run_number',data.get('run_number',1))][item['frame_number']] = item
        return runs

    left,right = by_run(current),by_run(other)
    pairs = [(run,run) for run in sorted(left.keys() & right.keys())]
    if not pairs:
        pairs = list(zip(sorted(left),sorted(right)))
    changed = set()
    for run,other_run in pairs:
        for number in left[run].keys() & right[other_run].keys():
            if left[run][number]['detections'] is right[other_run][number]['detections']:
                continue
            tracks = []
            match_detections(tracks,left[run][number]['detections'],1)
            match_detections(tracks,right[other_run][number]['detections'],2)
            for track in tracks:
                scores = track['scores']
                if 1 in scores and 2 in scores and scores[1] != scores[2]:
                    changed.add((run,number,track['detections'][1]))
    return changed


class ThresholdReviewWindow:
    def __init__(self,parent,folder):
        import tkinter as tk
        from tkinter import ttk
        self.directory,self.threshold_sets = load_review_sets(folder)
        self.thresholds = list(self.threshold_sets)
        self.threshold = self.thresholds[0]
        self.changed_scores = {}
        self.reports = {}
        self.set_review_data(self.threshold)
        self.window = tk.Toplevel(parent)
        self.window.title("Review threshold")
        self.window.geometry("1300x850")
        self.window.minsize(850,550)
        self.selected_box = None
        self.active_track = None
        self.box_filter = 'All boxes'
        self.image_zoom = 1.
        self.image_offset = (0.,0.)
        self.drag_position = None
        self.plot_zoom = 1.
        self.plot_origin = (0.,0.)
        self.plot_drag = None
        self.threshold_hits = []
        self.cpk_parent = Path(folder).resolve()
        self.selected = 0
        self.photo = self.source_image = None
        self.points = []
        header = ttk.Frame(self.window,padding=8)
        header.pack(fill="x")
        self.header_text = tk.StringVar(value=self.review_title())
        ttk.Label(header,textvariable=self.header_text).pack(side="left")
        self.summary = tk.StringVar()
        ttk.Label(header,textvariable=self.summary).pack(side="right")
        ttk.Button(header,text='cpk download',command=self.download_cpk).pack(side='right',padx=4)
        ttk.Button(header,text='cpk view',command=self.view_cpk).pack(side='right',padx=4)
        self.visible_run = tk.StringVar(value='All runs')
        self.run_picker = run_picker = ttk.Combobox(header,textvariable=self.visible_run,state='readonly',width=12,
            values=['All runs','Per frame']+[f'{number}st' for number in self.cpk_report['runs']])
        run_picker.pack(side='left',padx=8)
        run_picker.bind('<<ComboboxSelected>>',lambda event:self.change_run())
        panes = ttk.Panedwindow(self.window,orient="horizontal")
        panes.pack(fill="both",expand=True)
        left = ttk.Frame(panes)
        panes.add(left,weight=1)
        plot_controls = ttk.Frame(left,padding=4)
        plot_controls.pack(fill='x')
        for label,command in (('Fit',self.reset_plot_view),('-',lambda:self.zoom_plot(.8)),('+',lambda:self.zoom_plot(1.25))):
            ttk.Button(plot_controls,text=label,width=4,command=command).pack(side='left')
        if len(self.thresholds)>1:
            self.threshold_choice = tk.StringVar(value=f'{self.threshold:g}%')
            picker = ttk.Combobox(plot_controls,textvariable=self.threshold_choice,
                                  values=[f'{value:g}%' for value in self.thresholds],state='readonly',width=8)
            picker.pack(side='left',padx=(8,0))
            picker.bind('<<ComboboxSelected>>',lambda event:self.change_threshold(float(self.threshold_choice.get()[:-1])))
            ttk.Label(plot_controls,text='Full Yellow circle : score changed').pack(side='left',padx=8)
        self.plot = tk.Canvas(left,bg="white",highlightthickness=0,width=580)
        self.plot.pack(fill="both",expand=True)
        right = ttk.Frame(panes)
        panes.add(right,weight=1)
        controls = ttk.Frame(right,padding=4)
        controls.pack(fill="x")
        ttk.Button(controls,text="<",width=4,command=lambda:self.select(self.selected-1)).pack(side="left")
        ttk.Button(controls,text=">",width=4,command=lambda:self.select(self.selected+1)).pack(side="left")
        self.caption = tk.StringVar()
        ttk.Label(controls,textvariable=self.caption).pack(side="left",padx=8)
        jump = ttk.Frame(right,padding=(4,0,4,4))
        jump.pack(fill='x')
        self.jump_run = tk.StringVar(value=str(self.cpk_report['runs'][0]))
        self.jump_frame = tk.StringVar()
        ttk.Label(jump,text='Run').pack(side='left')
        self.run_entry = run_entry = ttk.Combobox(jump,textvariable=self.jump_run,values=[str(n) for n in self.cpk_report['runs']],
                                 state='readonly',width=6)
        run_entry.pack(side='left',padx=(4,12))
        ttk.Label(jump,text='Frame').pack(side='left')
        self.frame_entry = frame_entry = ttk.Spinbox(jump,textvariable=self.jump_frame,from_=1,
                                  to=max(item['frame_number'] for item in self.frames),width=7)
        frame_entry.pack(side='left',padx=4)
        run_entry.bind('<Return>',self.jump_to_frame)
        frame_entry.bind('<Return>',self.jump_to_frame)
        ttk.Button(jump,text='Go',command=self.jump_to_frame,width=5).pack(side='left',padx=4)
        self.box_choice = tk.StringVar(value='All boxes')
        self.box_picker = ttk.Combobox(jump,textvariable=self.box_choice,state='readonly',width=23)
        self.box_picker.pack(side='left',padx=4)
        self.box_picker.bind('<<ComboboxSelected>>',self.change_box)
        for label,factor in (('+',1.25),('-',.8)):
            ttk.Button(controls,text=label,width=3,command=lambda factor=factor:self.zoom_image(factor)).pack(side='right')
        ttk.Button(controls,text='Fit',width=4,command=self.reset_image_view).pack(side='right')
        self.image_canvas = tk.Canvas(right,bg="#202020",highlightthickness=0,height=500)
        self.image_canvas.pack(fill="both",expand=True)
        self.details = tk.Text(right,height=10,wrap="word",font=("Segoe UI",10))
        self.details.pack(fill="x")
        self.plot.bind("<Configure>",lambda event:self.draw_plot())
        self.plot.bind("<MouseWheel>",self.wheel_plot)
        self.plot.bind("<ButtonPress-1>",self.start_plot_drag)
        self.plot.bind("<B1-Motion>",self.drag_plot)
        self.plot.bind("<ButtonRelease-1>",self.finish_plot_drag)
        self.image_canvas.bind("<Configure>",lambda event:self.draw_image())
        self.image_canvas.bind("<MouseWheel>",self.wheel_image)
        self.image_canvas.bind("<ButtonPress-1>",self.start_image_drag)
        self.image_canvas.bind("<B1-Motion>",self.drag_image)
        self.image_canvas.bind("<ButtonRelease-1>",lambda event:setattr(self,'drag_position',None))
        self.window.bind("<Left>",lambda event:self.navigate(event,-1))
        self.window.bind("<Right>",lambda event:self.navigate(event,1))
        self.select(0)

    def set_review_data(self,threshold):
        from inspection.cpk_review import build_cpk
        data = self.threshold_sets[threshold]
        if threshold not in self.reports:
            self.reports[threshold] = build_cpk(data.get('runs',[(self.directory,data)]))
        if threshold not in self.changed_scores:
            self.changed_scores[threshold] = set().union(
                *(changed_box_scores(data,other) for key,other in self.threshold_sets.items() if key != threshold))
        self.threshold,self.data,self.frames = threshold,data,data['frames']
        self.cpk_report = self.reports[threshold]
        self.box_tracks,self.track_members = {},{}
        for row in self.cpk_report['rows']:
            track = (row['frame_number'],row['label_number'])
            for run,box in row['detections'].items():
                self.box_tracks[run,track[0],box] = track
                self.track_members.setdefault(track,{})[run] = box

    def review_title(self):
        return f"{self.data['group']} | Threshold {self.threshold:g}% | {len(self.frames)} frames"

    def change_threshold(self,threshold):
        if threshold == self.threshold:
            return
        previous = self.frames[self.selected]
        number,run = previous['frame_number'],self.frame_run(previous)
        self.set_review_data(threshold)
        self.threshold_choice.set(f'{threshold:g}%')
        self.header_text.set(self.review_title())
        runs = self.cpk_report['runs']
        self.run_picker.configure(values=['All runs','Per frame']+[f'{n}st' for n in runs])
        self.run_entry.configure(values=[str(n) for n in runs])
        self.frame_entry.configure(to=max(item['frame_number'] for item in self.frames))
        if self.visible_run.get() not in ('All runs','Per frame') and run not in runs:
            self.visible_run.set('All runs')
        candidates = [i for i,item in enumerate(self.frames)
                      if self.visible_run.get() in ('All runs','Per frame')
                      or self.visible_run.get() == f'{self.frame_run(item)}st']
        index = min(candidates,key=lambda i:(
            self.frames[i]['frame_number'] != number,self.frame_run(self.frames[i]) != run,i))
        self.selected = index
        self.select(index)

    def view_cpk(self):
        from inspection.cpk_review import show_cpk
        return show_cpk(self.window,self.cpk_report)

    def change_run(self):
        wanted = self.visible_run.get()
        number = self.frames[self.selected]['frame_number']
        candidates = [index for index,item in enumerate(self.frames)
                      if wanted in ('All runs','Per frame') or wanted == f"{self.frame_run(item)}st"]
        index = self.selected if self.selected in candidates else next((index for index in candidates if self.frames[index]['frame_number'] == number),
                     candidates[0] if candidates else None)
        if index is not None:
            self.select(index)

    def frame_run(self, item):
        return item.get('run_number',self.data.get('run_number',self.cpk_report['runs'][0]))

    def jump_to_frame(self, event=None):
        from tkinter import messagebox
        try:
            run,number = int(self.jump_run.get()),int(self.jump_frame.get())
            if run < 1 or number < 1:
                raise ValueError
        except ValueError:
            messagebox.showwarning('Go to frame','Enter positive run and frame numbers.',parent=self.window)
            return 'break'
        for index,item in enumerate(self.frames):
            if self.frame_run(item) == run and item['frame_number'] == number:
                if self.visible_run.get() != 'Per frame':
                    self.visible_run.set(f'{run}st')
                self.select(index)
                return 'break'
        messagebox.showwarning('Go to frame',f'No recorded frame {number} in run {run}.',parent=self.window)
        return 'break'

    def navigate(self,event,step):
        if event.widget.winfo_class() in ('Entry','TEntry','Spinbox','TSpinbox','TCombobox','Text'):
            return
        self.select(self.selected+step)
        return 'break'

    def download_cpk(self):
        from tkinter import filedialog,messagebox
        from inspection.cpk_review import export_cpk
        suffix = f'_threshold_{self.threshold:g}pct' if len(self.thresholds)>1 else ''
        path = filedialog.asksaveasfilename(parent=self.window,initialdir=str(self.cpk_parent),
            initialfile=f'{self.cpk_parent.name}{suffix}_cpk.xlsx',defaultextension='.xlsx',filetypes=[('Excel workbook','*.xlsx')])
        if path:
            try:
                export_cpk(self.cpk_report,path)
            except Exception as exc:
                messagebox.showerror('Cpk download',str(exc),parent=self.window)

    def draw_plot(self):
        canvas = self.plot
        canvas.delete("all")
        width,height = max(200,canvas.winfo_width()),max(200,canvas.winfo_height())
        left,right,top,bottom = 60,width-22,30,height-55
        self.plot_bounds = left,right,top,bottom
        ox,oy = self.plot_origin
        zoom = self.plot_zoom
        threshold = self.data["threshold_percent"]
        def y(value):
            return top+(bottom-top)*zoom*(1-value/100-oy)
        mode = self.visible_run.get()
        selected_item = self.frames[self.selected]
        track = self.active_track if mode != 'Per frame' else None
        axis = ([selected_item['frame_number']] if mode == 'Per frame' else
                self.cpk_report['runs'] if track is not None
                else [item['frame_number'] for item in self.frames])
        xmin,xmax = min(axis),max(axis)
        def x(number):
            fraction = .5 if xmin == xmax else (number-xmin)/(xmax-xmin)
            return left+(right-left)*zoom*(fraction-ox)
        step = max(1,round(20/zoom))
        for value in range(0,101,step):
            if top <= y(value) <= bottom:
                canvas.create_line(left,y(value),right,y(value),fill="#e5e5e5")
                canvas.create_text(left-8,y(value),text=str(value),anchor="e",font=("Segoe UI",10))
        canvas.create_line(left,top,left,bottom,right,bottom,fill="black",width=2)
        canvas.create_text(left,12,text="Confidence (%)",anchor="w",font=("Segoe UI",11))
        axis_label = (f"{self.frame_run(selected_item)}st | Frame {selected_item['frame_number']}" if mode == 'Per frame'
                      else "Frame No." if track is None else
                      f"Run No. | Frame {track[0]} | {self.box_label(selected_item,self.selected_box)}")
        canvas.create_text((left+right)/2,height-15,text=axis_label,font=("Segoe UI",11))
        for number in sorted({round(xmin+(xmax-xmin)*(ox+i/(4*zoom))) for i in range(5)}):
            if left <= x(number) <= right:
                canvas.create_text(x(number),bottom+15,text=str(number),font=("Segoe UI",10))
        self.threshold_hits = []
        for value in self.thresholds:
            line_y = y(value)
            if not top <= line_y <= bottom:
                continue
            active = value == threshold
            if active and len(self.thresholds)>1:
                canvas.create_line(left,line_y,right,line_y,fill="#eab308",width=6)
            color = '#dd2222' if active else '#737373'
            canvas.create_line(left,line_y,right,line_y,fill=color,dash=(7,4),width=2)
            text = canvas.create_text(right,line_y-10,text=f"Threshold {value:g}%",anchor="e",fill=color)
            self.threshold_hits.append((value,line_y,canvas.bbox(text)))
        self.points = []
        self.point_indices = []
        highlight = None
        for index,item in enumerate(self.frames):
            if mode == 'Per frame' and index != self.selected:
                continue
            if mode not in ('All runs','Per frame') and mode != f"{self.frame_run(item)}st":
                continue
            run = self.frame_run(item)
            if track is not None:
                if item['frame_number'] != track[0]:
                    continue
                box = self.track_members.get(track,{}).get(run)
                if box is None and track[1] is None and index == self.selected:
                    box = self.selected_box
                if box is None:
                    if left <= x(run) <= right and top <= y(0) <= bottom:
                        canvas.create_text(x(run),y(0),text='x',fill='#666666')
                    continue
                entries = [(box,item['detections'][box]['confidence'])]
            else:
                entries = [(box,d['confidence']) for box,d in enumerate(item['detections'])] or [(None,0)]
            for box,score in entries:
                if not self.score_visible(score):
                    continue
                px,py = x(run if track is not None else item['frame_number']),y(score*100)
                if not (left <= px <= right and top <= py <= bottom):
                    continue
                radius = 3
                color = "#dc2626" if score*100 >= threshold else "#16a34a"
                if (run,item['frame_number'],box) in self.changed_scores[self.threshold]:
                    color = "#eab308"
                canvas.create_oval(px-radius,py-radius,px+radius,py+radius,fill=color,
                                   outline="")
                if box is not None and index == self.selected and box == self.selected_box:
                    highlight = px,py
                self.points.append((px,py))
                self.point_indices.append((index,box))
        if highlight is not None:
            px,py = highlight
            canvas.create_oval(px-7,py-7,px+7,py+7,outline="#eab308",width=3)
        self.summary.set(self.cpk_report['verdict'])

    def click(self,event):
        left,right,_,_ = self.plot_bounds
        if len(self.thresholds)>1 and left <= event.x <= right:
            for value,line_y,box in sorted(self.threshold_hits,key=lambda item:abs(event.y-item[1])):
                if abs(event.y-line_y)<=4 or (box and box[0]<=event.x<=box[2] and box[1]<=event.y<=box[3]):
                    self.change_threshold(value)
                    return
        if not self.points:
            return
        index = min(range(len(self.points)),key=lambda i:(self.points[i][0]-event.x)**2+(self.points[i][1]-event.y)**2)
        if (self.points[index][0]-event.x)**2+(self.points[index][1]-event.y)**2 <= 144:
            self.select(*self.point_indices[index],compare_box=self.active_track is not None)

    def reset_plot_view(self):
        self.plot_zoom,self.plot_origin = 1.,(0.,0.)
        self.draw_plot()

    def _set_plot_origin(self,x,y):
        maximum = 1.-1./self.plot_zoom
        self.plot_origin = max(0.,min(maximum,x)),max(0.,min(maximum,y))

    def zoom_plot(self,factor,anchor=None):
        left,right,top,bottom = self.plot_bounds
        fx,fy = (.5,.5) if anchor is None else (
            max(0.,min(1.,(anchor[0]-left)/(right-left))),
            max(0.,min(1.,(anchor[1]-top)/(bottom-top))))
        old = self.plot_zoom
        self.plot_zoom = max(1.,min(8.,old*factor))
        delta = 1./old-1./self.plot_zoom
        x,y = self.plot_origin
        self._set_plot_origin(x+fx*delta,y+fy*delta)
        self.draw_plot()

    def wheel_plot(self,event):
        if event.delta:
            self.zoom_plot(1.25 if event.delta < 0 else .8,(event.x,event.y))
        return 'break'

    def start_plot_drag(self,event):
        self.plot_drag = (event.x,event.y,self.plot_origin)
        self.plot_moved = False

    def drag_plot(self,event):
        if self.plot_drag is None:
            return
        x,y,(ox,oy) = self.plot_drag
        if self.plot_moved or abs(event.x-x)+abs(event.y-y)>4:
            self.plot_moved = True
            if self.plot_zoom == 1:
                return
            left,right,top,bottom = self.plot_bounds
            self._set_plot_origin(ox-(event.x-x)/((right-left)*self.plot_zoom),
                                  oy-(event.y-y)/((bottom-top)*self.plot_zoom))
            self.draw_plot()

    def finish_plot_drag(self,event):
        if self.plot_drag is not None and not self.plot_moved:
            self.click(event)
        self.plot_drag = None

    def score_visible(self,score):
        if self.box_filter == 'All boxes':
            return True
        failed = score*100 >= self.data['threshold_percent']
        return failed == (self.box_filter == 'Fail')

    def change_box(self,event=None):
        choice = self.box_picker.current()
        if 0 <= choice < 3:
            self.box_filter = ('All boxes','Pass','Fail')[choice]
            self.select(self.selected)
        elif choice >= 3:
            self.select(self.selected,self.box_options[choice-3],compare_box=True)

    def box_label(self,item,box):
        detection = item['detections'][box]
        track = self.box_tracks.get((self.frame_run(item),item['frame_number'],box))
        return f"{detection['class_name']} #{track[1]}" if track else f"{detection['class_name']} (box {box+1}, unmatched)"

    def select(self,index,box=None,compare_box=False):
        from PIL import Image
        if not 0 <= index < len(self.frames):
            return
        direction = 1 if index >= self.selected else -1
        while self.visible_run.get() not in ('All runs','Per frame') and self.visible_run.get() != f"{self.frame_run(self.frames[index])}st":
            index += direction
            if not 0 <= index < len(self.frames):
                return
        self.selected = index
        item = self.frames[index]
        self.selected_box = box
        self.active_track = (None if box is None or not compare_box or self.visible_run.get() == 'Per frame' else
            self.box_tracks.get((self.frame_run(item),item['frame_number'],box),(item['frame_number'],None)))
        if self.active_track is not None and self.visible_run.get() != 'All runs':
            self.visible_run.set('All runs')
        self.box_options = [n for n,d in enumerate(item['detections']) if self.score_visible(d['confidence'])]
        self.box_picker.configure(values=['All boxes','Pass','Fail']+[self.box_label(item,n) for n in self.box_options])
        self.box_picker.current(('All boxes','Pass','Fail').index(self.box_filter) if box is None
                                else 3+self.box_options.index(box))
        self.jump_run.set(str(self.frame_run(item)))
        self.jump_frame.set(str(item['frame_number']))
        raw = item.get('raw_frame_image',self.data.get('raw_frame_images',False))
        shown = [(n,item['detections'][n]) for n in self.box_options] if box is None else [(box,item['detections'][box])]
        with Image.open(self.directory/item["image"]) as image:
            self.source_image = image.convert("RGB")
        if raw or box is not None or self.box_filter != 'All boxes':
            import numpy as np
            self.source_image = Image.fromarray(draw_review_boxes(
                np.asarray(self.source_image)[:,:,::-1],
                [(d,self.box_label(item,n)) for n,d in shown],self.data['threshold_percent'])[:,:,::-1])
        state = "FAIL" if item["result"] == "NG" else "PASS"
        run = f"{self.frame_run(item)}st | "
        self.caption.set(f"{run}Frame {item['frame_number']} | {item['score_percent']:.2f}% | {state}")
        lines = [f"Source: {self.data['source']}",f"Threshold: {self.data['threshold_percent']:g}%",
                 f"Frame maximum: {item['score_percent']:.2f}%",f"Frame result: {state}"]
        if box is not None:
            score = float(item['detections'][box]['confidence'])*100
            box_state = 'NG' if score >= self.data['threshold_percent'] else 'OK'
            self.caption.set(f"{run}Frame {item['frame_number']} | {self.box_label(item,box)} | {score:.2f}% | {box_state}")
        if not item["detections"]:
            lines.append("0.00% (no detections at or above 1%)")
        if not raw and (box is not None or self.box_filter != 'All boxes'):
            lines.append("Legacy image: other boxes are baked into this PNG. New tests save clean frames.")
        for number,detection in shown:
            coords = ", ".join(f"{value:.1f}" for value in detection["xyxy"])
            state = 'NG' if detection['confidence']*100 >= self.data['threshold_percent'] else 'OK'
            changed = (self.frame_run(item),item['frame_number'],number) in self.changed_scores[self.threshold]
            note = ' | score changed across thresholds' if changed else ''
            lines.append(f"{self.box_label(item,number)}: {detection['confidence']*100:.2f}% {state} | box [{coords}]{note}")
        run_index = self.cpk_report['runs'].index(self.frame_run(item))
        for row in self.cpk_report['rows']:
            if box is None and self.box_filter == 'All boxes' and row['frame_number'] == item['frame_number']:
                lines.append(f"{row['label']}: {row['scores'][run_index]:.2f}%")
        self.details.configure(state="normal")
        self.details.delete("1.0","end")
        self.details.insert("1.0","\n".join(lines))
        self.details.configure(state="disabled")
        self.draw_plot()
        self.draw_image()

    def reset_image_view(self):
        self.image_zoom = 1.
        self.image_offset = (0.,0.)
        self.draw_image()

    def zoom_image(self,factor,anchor=None):
        previous = self.image_zoom
        self.image_zoom = max(1.,min(8.,previous*factor))
        if anchor is not None:
            cx = self.image_canvas.winfo_width()/2
            cy = self.image_canvas.winfo_height()/2
            dx,dy = self.image_offset
            x,y = anchor
            ratio = self.image_zoom/previous
            self.image_offset = (x-cx+(dx+cx-x)*ratio,y-cy+(dy+cy-y)*ratio)
        self.draw_image()

    def wheel_image(self,event):
        if event.delta:
            self.zoom_image(1.25 if event.delta < 0 else .8,(event.x,event.y))
        return 'break'

    def start_image_drag(self,event):
        self.drag_position = event.x,event.y

    def drag_image(self,event):
        if self.drag_position is not None and self.image_zoom > 1:
            x,y = self.drag_position
            dx,dy = self.image_offset
            self.drag_position = event.x,event.y
            self.image_offset = (dx+event.x-x,dy+event.y-y)
            self.draw_image()

    def draw_image(self):
        from PIL import Image,ImageTk
        if self.source_image is None:
            return
        width,height = max(1,self.image_canvas.winfo_width()),max(1,self.image_canvas.winfo_height())
        source_width,source_height = self.source_image.size
        scale = min(width/source_width,height/source_height)*self.image_zoom
        max_x = max(0.,(source_width*scale-width)/2)
        max_y = max(0.,(source_height*scale-height)/2)
        dx,dy = self.image_offset
        dx,dy = max(-max_x,min(max_x,dx)),max(-max_y,min(max_y,dy))
        self.image_offset = dx,dy
        left = (source_width-width/scale)/2-dx/scale
        top = (source_height-height/scale)/2-dy/scale
        preview = self.source_image.transform((width,height),Image.Transform.EXTENT,
            (left,top,left+width/scale,top+height/scale),
            resample=Image.Resampling.BICUBIC,fillcolor='#202020')
        self.photo = ImageTk.PhotoImage(preview)
        self.image_canvas.delete("all")
        self.image_canvas.create_image(width/2,height/2,image=self.photo)
