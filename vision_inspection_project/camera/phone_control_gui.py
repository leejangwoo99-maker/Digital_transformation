import math
import asyncio
import shutil
import tempfile
import threading
import time
import tkinter as tk
from datetime import datetime
from pathlib import Path
from tkinter import filedialog, messagebox, simpledialog, ttk

import cv2
from PIL import Image, ImageTk

from camera.phone_usb import PhoneUSB, PhoneCapture, adb
from camera.alignment_preview import AlignmentPreview
from camera.live_result_view import LatestResult, LiveResultView
from camera.record_conditions import (load_conditions, save_conditions, validate_conditions,
                                      recording_actions, condition_files, named_condition_path)
from main import ORIGINAL_VIDEO_DIR, DEFAULT_TEST_VIDEO_DIR, TEST_LOG_DIR, inspect_video, unique_path


class PhoneControlWindow:
    def __init__(self, app, live_mode=False):
        self.app, self.live_mode = app, live_mode
        self.client = PhoneUSB()
        self.capture = None
        self.stop_pending = threading.Event()
        self.connected = self.busy = self.connecting = self.closed = False
        self.condition_path = None
        self.inspection_count = 0
        self.pipeline_started_at = None
        self.pipeline_seconds = 0.
        self.media_jobs = set()
        self.repeat_remaining = 0
        self.repeat_total = 0
        self.repeat_after = None
        self.preview_zoom_lock = threading.Lock()
        self.preview_zoom_generation = 0
        self.preview_zoom_limits = (.1,100.)
        self.conditions = load_conditions(default_fps=app.target_fps_var.get())
        self.window = tk.Toplevel(app.root)
        self.window.title("USB Live Test" if live_mode else "Recording by Phone")
        self.window.geometry("1400x900")
        self.window.protocol("WM_DELETE_WINDOW", self.close)
        self.zoom = tk.DoubleVar(value=self.conditions["initial"]["zoom"])
        self.focus = tk.DoubleVar(value=self.conditions["initial"]["focus"])
        self.auto_focus = tk.BooleanVar(value=self.conditions["initial"]["auto_focus"])
        self.fps = tk.DoubleVar(value=self.conditions["fps"])
        self.total_time = tk.DoubleVar(value=self.conditions["total_time"])
        self.interval_rows = []
        self.bot_rows = []
        self.bot_choices = {}
        self.bot_enabled = tk.BooleanVar(value=False)
        self.bot_address = tk.StringVar()
        self.bot_status = tk.StringVar(value='SwitchBot: disabled')
        selector = tk.Frame(self.window)
        if not live_mode:
            selector.pack(fill="x", padx=10, pady=8)
        tk.Label(selector, text="record_condition").pack(side="left")
        self.condition_name = tk.StringVar()
        self.condition_combo = ttk.Combobox(selector, textvariable=self.condition_name, state="readonly", width=48)
        self.condition_combo.pack(side="left", padx=8)
        self.condition_combo.bind("<<ComboboxSelected>>", self.select_condition)
        if live_mode:
            self.condition_summary = tk.StringVar()
            tk.Label(self.window, textvariable=self.condition_summary, anchor="w").pack(fill="x", padx=10)
        else:
            self.build_record_settings()
        commands = tk.Frame(self.window)
        commands.pack(fill="x", padx=10, pady=8)
        start_column = tk.Frame(commands)
        start_column.pack(side="left", padx=6)
        self.start_button = tk.Button(start_column, text="start" if live_mode else "start recording", state="disabled", command=self.start)
        self.start_button.pack()
        self.elapsed_seconds = tk.StringVar(value="0.0 sec")
        tk.Label(start_column, textvariable=self.elapsed_seconds).pack()
        tk.Button(commands, text="stop", command=self.stop).pack(side="left", padx=6)
        self.record_indicator = tk.Canvas(commands, width=20, height=20, highlightthickness=0)
        self.record_indicator.pack(side="left", padx=(12, 2))
        self.record_dot = self.record_indicator.create_oval(3, 3, 17, 17, fill="gray", outline="")
        self.record_status = tk.StringVar(value="Connecting")
        tk.Label(commands, textvariable=self.record_status).pack(side="left")
        self.inspection_count_var = tk.StringVar(value='Inspection count : 0')
        if live_mode:
            tk.Label(commands,textvariable=self.inspection_count_var).pack(side='left',padx=12)
        self.state = tk.StringVar(value="Installing phone app and connecting USB...")
        tk.Label(self.window, textvariable=self.state, wraplength=950, justify="left").pack(fill="x", padx=10)
        tk.Label(self.window,textvariable=self.bot_status,anchor='w',fg='#a03020',wraplength=950).pack(fill='x',padx=10)
        if live_mode:
            self.window.geometry("1500x900")
            self.result_mailbox = LatestResult()
            self.views = tk.PanedWindow(self.window,orient="horizontal",sashwidth=6)
            self.views.pack(fill="both",expand=True,padx=10,pady=10)
            camera_view = tk.Frame(self.views)
            camera_header = tk.Frame(camera_view)
            camera_header.pack(fill='x')
            tk.Label(camera_header,text="Live camera",anchor="w").pack(side='left')
            tk.Label(camera_header,text='Label',fg='#8a6500').pack(side='right',padx=4)
            tk.Label(camera_header,text='OCR',fg='#007c91').pack(side='right',padx=4)
            guide_controls = tk.Frame(camera_view)
            guide_controls.pack(fill='x')
            tk.Label(guide_controls,text='Guide frame').pack(side='left')
            self.guide_frame = tk.StringVar(value='Off')
            self.guide_combo = ttk.Combobox(guide_controls,textvariable=self.guide_frame,state='readonly',width=16)
            self.guide_combo.pack(side='left',fill='x',expand=True,padx=4)
            self.guide_combo.bind('<<ComboboxSelected>>',self.select_guide_frame)
            self.guide_status = tk.StringVar()
            guide_message = tk.Label(camera_view,textvariable=self.guide_status,anchor='w',justify='left',wraplength=230)
            guide_message.pack(fill='x')
            guide_message.bind('<Configure>',lambda event:guide_message.configure(wraplength=max(80,event.width-8)))
            self.live_zoom_controls = tk.Frame(camera_view)
            self.live_zoom_controls.pack(fill='x')
            self.live_zoom_controls.columnconfigure(2,weight=1)
            tk.Label(self.live_zoom_controls,text='Zoom').grid(row=0,column=0,padx=(0,4))
            self.live_zoom_value = ttk.Spinbox(self.live_zoom_controls,from_=.1,to=100,increment=.1,
                textvariable=self.zoom,width=5,command=self.apply_live_zoom)
            self.live_zoom_value.grid(row=0,column=1)
            self.live_zoom_value.bind('<Return>',self.apply_live_zoom)
            self.live_zoom_value.bind('<FocusOut>',self.apply_live_zoom)
            self.live_zoom_slider = tk.Scale(self.live_zoom_controls,from_=.1,to=100,resolution=.1,
                variable=self.zoom,orient='horizontal',showvalue=False,length=100)
            self.live_zoom_slider.grid(row=0,column=2,sticky='ew')
            self.live_zoom_slider.bind('<ButtonRelease-1>',self.apply_live_zoom)
            self.live_zoom_slider.bind('<KeyRelease>',self.apply_live_zoom)
            self.preview_label = AlignmentPreview(camera_view)
            self.preview_label.pack(fill="both",expand=True)
            self.result_view = LiveResultView(self.views)
            self.views.add(camera_view,minsize=240,width=600)
            self.views.add(self.result_view,minsize=300)
            self.window.after(50,self.refresh_result)
        else:
            self.preview_label = AlignmentPreview(self.window)
            self.preview_label.pack(fill="both", expand=True, padx=10, pady=10)
        if live_mode:
            from camera.live_settings import resolve_live_settings
            self.live_settings, data = resolve_live_settings(app)
            self.condition_path = Path(self.live_settings["record_condition"])
            self.apply_conditions(data)
            self.load_guide_frames()
        else:
            self.refresh_conditions()
        self.window.after(200, self.refresh_preview)
        self.window.after(100, self.refresh_elapsed)
        self.window.after(100, self.setup_camera)

    def build_record_settings(self):
        settings = tk.Frame(self.window)
        settings.pack(fill="x", padx=10)
        tk.Label(settings, text="zoom").pack(side="left")
        self.zoom_slider = tk.Scale(settings, variable=self.zoom, from_=.1, to=100, resolution=.1, orient="horizontal", length=220)
        self.zoom_slider.pack(side="left")
        self.zoom_slider.bind("<ButtonRelease-1>", lambda event: self.apply())
        tk.Checkbutton(settings, text="auto focus", variable=self.auto_focus, command=self.apply).pack(side="left", padx=10)
        tk.Label(settings, text="manual focus (diopters)").pack(side="left")
        self.focus_slider = tk.Scale(settings, variable=self.focus, from_=0, to=100, resolution=.01, orient="horizontal", length=200)
        self.focus_slider.pack(side="left")
        self.focus_slider.bind("<ButtonRelease-1>", self.apply_manual_focus)
        tk.Button(settings, text="apply", command=self.apply).pack(side="left", padx=8)
        tk.Label(self.window, text="- setting -").pack(anchor="w", padx=10, pady=(8, 0))
        row = tk.Frame(self.window)
        row.pack(fill="x", padx=10, pady=8)
        tk.Label(row, text="record FPS").pack(side="left", anchor="n")
        tk.Spinbox(row, textvariable=self.fps, from_=.5, to=30, increment=.5, width=7).pack(side="left", anchor="n", padx=6)
        timing = tk.Frame(row)
        timing.pack(side="left", anchor="n", padx=(12, 8))
        tk.Label(timing, text="Total time:").pack(side="left")
        tk.Entry(timing, textvariable=self.total_time, width=8).pack(side="left", padx=6)
        tk.Label(timing, text="sec").pack(side="left")
        bot_column = tk.Frame(row)
        bot_column.pack(side='left',anchor='n',padx=8)
        bot_select = tk.Frame(bot_column)
        bot_select.pack(fill='x')
        tk.Checkbutton(bot_select,text='SwitchBot',variable=self.bot_enabled).pack(side='left')
        self.bot_combo = ttk.Combobox(bot_select,textvariable=self.bot_address,width=24,state='readonly')
        self.bot_combo.pack(side='left',padx=4)
        tk.Button(bot_select,text='scan BLE',command=self.scan_switchbot).pack(side='left')
        self.bot_intervals_frame = tk.Frame(bot_column)
        self.bot_intervals_frame.pack(fill='x')
        tk.Button(bot_column,text='+ stop / go',command=self.add_bot_interval).pack(anchor='w')
        zoom_column = tk.Frame(row)
        zoom_column.pack(side="left", anchor="n", padx=8)
        self.intervals_frame = tk.Frame(zoom_column)
        self.intervals_frame.pack(fill="x")
        tk.Button(zoom_column, text="+ zoom", command=self.add_interval).pack(anchor="w", pady=(3, 0))
        tk.Button(row, text="save", command=self.save_settings).pack(side="left", anchor="n", padx=6)

    def post(self, callback):
        if not self.closed:
            try:
                self.window.after(0, lambda: callback() if not self.closed else None)
            except (RuntimeError, tk.TclError):
                pass

    def scan_switchbot(self):
        if self.busy:
            return
        self.bot_status.set('SwitchBot: scanning BLE (20 sec)...')
        from camera.switchbot import scan_bots
        def found(devices):
            self.bot_choices = {f"{item['name']} [{item['address']}]":item['address']
                                for item in devices if item['mode'] == 'Press'}
            self.bot_combo.configure(values=list(self.bot_choices))
            self.bot_status.set(f'SwitchBot: {len(self.bot_choices)} Press-mode device(s) found; select a device.'
                if self.bot_choices else 'SwitchBot: no Press-mode Bot found. Check Bluetooth, battery and Bot app mode.')
        self.background(lambda:asyncio.run(scan_bots()),found)

    def add_bot_interval(self, interval=None):
        interval = interval or dict(stop=1.,go=2.)
        row = tk.Frame(self.bot_intervals_frame)
        row.pack(fill='x',pady=2)
        variables = [tk.DoubleVar(value=interval[key]) for key in ('stop','go')]
        for key,variable in zip(('STOP','GO'),variables):
            tk.Label(row,text=key).pack(side='left')
            tk.Entry(row,textvariable=variable,width=6).pack(side='left',padx=3)
            tk.Label(row,text='sec').pack(side='left')
        item = row,variables
        self.bot_rows.append(item)
        tk.Button(row,text='-',command=lambda:self.remove_bot_interval(item)).pack(side='left',padx=4)

    def remove_bot_interval(self,item):
        self.bot_rows.remove(item)
        item[0].destroy()

    def background(self, operation, success=None):
        def work():
            try:
                result = operation()
                if success:
                    self.post(lambda: success(result))
            except Exception as exc:
                error = str(exc)
                self.post(lambda: messagebox.showerror("USB Camera", error, parent=self.window))
        threading.Thread(target=work, daemon=True).start()

    def setup_camera(self):
        if self.connecting or self.connected or self.closed:
            return
        self.connecting = True
        def work():
            try:
                apk = Path(__file__).resolve().parents[1] / "android_phone_camera/VisionUsbCamera.apk"
                if not apk.exists():
                    raise RuntimeError("Camera APK missing. Run android_phone_camera/build.ps1.")
                adb("install", "-r", str(apk))
                status = self.client.connect()
                self.post(lambda: self.camera_ready(status))
            except Exception as exc:
                error = str(exc)
                self.post(lambda: self.connection_failed(error))
            finally:
                self.connecting = False
        threading.Thread(target=work, daemon=True).start()

    def connection_failed(self, error):
        self.state.set(f"Connection failed: {error}")
        self.record_status.set("Disconnected")

    def camera_ready(self, status):
        self.connected = True
        if self.live_mode:
            low,high = float(status.get('zoom_min',.1)),float(status.get('zoom_max',100.))
            self.preview_zoom_limits = low,high
            self.live_zoom_slider.configure(from_=low,to=high)
            self.live_zoom_value.configure(from_=low,to=high)
            self.zoom.set(min(high,max(low,self.zoom.get())))
            self.apply_live_zoom()
        if not self.live_mode:
            current = self.values()
            self.zoom_slider.configure(from_=status["zoom_min"], to=status["zoom_max"])
            self.focus_slider.configure(to=status["focus_max"])
            self.zoom.set(current["zoom"])
            self.focus.set(current["focus"])
        self.start_button.configure(state="normal")
        self.record_status.set("Idle")
        self.state.set(f"Connected: {status['height']}x{status['width']}")

    def refresh_conditions(self, selected=None):
        self.profile_paths = {path.stem: path for path in condition_files()}
        self.condition_combo.configure(values=sorted(self.profile_paths))
        name = selected.stem if selected is not None else self.condition_name.get()
        if name not in self.profile_paths:
            name = next(reversed(self.profile_paths), "")
        self.condition_name.set(name)
        if name:
            self.select_condition()
        else:
            self.apply_conditions(self.conditions)

    def select_condition(self, event=None):
        if self.busy:
            self.condition_name.set(self.condition_path.stem if self.condition_path else "")
            return
        path = self.profile_paths.get(self.condition_name.get())
        if path is None:
            return
        try:
            data = load_conditions(path)
            self.apply_conditions(data)
            self.condition_path = path
        except Exception as exc:
            messagebox.showerror("Recording settings", str(exc), parent=self.window)

    def apply_conditions(self, data):
        self.conditions = data
        self.fps.set(data["fps"])
        self.total_time.set(data["total_time"])
        self.zoom.set(data["initial"]["zoom"])
        self.focus.set(data["initial"]["focus"])
        self.auto_focus.set(data["initial"]["auto_focus"])
        bot = data.get('switchbot',{})
        self.bot_enabled.set(bot.get('enabled',False))
        self.bot_address.set(bot.get('address',''))
        self.bot_status.set('SwitchBot: Press mode; initial STOP confirmation required' if bot.get('enabled') else 'SwitchBot: disabled')
        if self.live_mode:
            self.condition_summary.set(f"{self.live_settings['special_group']} | {self.condition_path.name} | {data['fps']:g} FPS / {data['total_time']:g} sec")
        else:
            self.bot_combo.configure(values=list(self.bot_choices)+([bot['address']] if bot.get('address') else []))
            for row,_ in self.bot_rows:
                row.destroy()
            self.bot_rows.clear()
            for interval in bot.get('intervals',[]):
                self.add_bot_interval(interval)
            for row, _ in self.interval_rows:
                row.destroy()
            self.interval_rows.clear()
            for interval in data["zoom_intervals"]:
                self.add_interval(interval)

    def add_interval(self, interval=None):
        interval = interval or {"zoom": 2, "start": 0, "end": self.total_time.get()}
        row = tk.Frame(self.intervals_frame)
        row.pack(fill="x", pady=2)
        variables = [tk.DoubleVar(value=interval[key]) for key in ("zoom", "start", "end")]
        tk.Label(row, text="zoom:").pack(side="left")
        for index, variable in enumerate(variables):
            tk.Entry(row, textvariable=variable, width=7).pack(side="left", padx=5)
            tk.Label(row, text=("/", "~", "sec")[index]).pack(side="left")
        item = (row, variables)
        self.interval_rows.append(item)
        tk.Button(row, text="-", command=lambda: self.remove_interval(item)).pack(side="left", padx=8)

    def remove_interval(self, item):
        self.interval_rows.remove(item)
        item[0].destroy()

    def read_conditions(self):
        if self.live_mode:
            from camera.live_settings import resolve_live_settings
            self.live_settings, data = resolve_live_settings(self.app)
            self.condition_path = Path(self.live_settings["record_condition"])
            self.apply_conditions(data)
            return data
        return validate_conditions({"fps": self.fps.get(), "total_time": self.total_time.get(),
                                    "switchbot":dict(enabled=self.bot_enabled.get(),
                                        address=self.bot_choices.get(self.bot_address.get(),self.bot_address.get()),
                                        intervals=[dict(zip(('stop','go'),[v.get() for v in variables])) for _,variables in self.bot_rows]),
                                    "initial": self.values(), "zoom_intervals": [
                                        dict(zip(("zoom", "start", "end"), [value.get() for value in variables]))
                                        for _, variables in self.interval_rows]})

    def save_settings(self):
        try:
            data = self.read_conditions()
            initial = self.condition_path.stem.removesuffix("_record_condition") if self.condition_path else ""
            name = simpledialog.askstring("Save recording condition", "Condition name", initialvalue=initial, parent=self.window)
            if name is None:
                return
            path = named_condition_path(name)
            if path.exists() and not messagebox.askyesno("Overwrite condition?", f"Replace {path.name}?", parent=self.window):
                return
            save_conditions(data, path)
            self.refresh_conditions(path)
            self.state.set(f"Saved: {path}")
        except Exception as exc:
            messagebox.showerror("Recording settings", str(exc), parent=self.window)

    def set_recording(self, recording, message=None):
        self.record_indicator.itemconfigure(self.record_dot, fill="red" if recording else "gray")
        self.record_status.set("REC" if recording else "Idle")
        self.start_button.configure(state="disabled" if recording else "normal")
        if message is not None:
            self.state.set(message)
        self.update_guide_visibility()

    def load_guide_frames(self):
        from camera.inspection_guides import common_frames
        try:
            self.guide_frames = common_frames(self.live_settings)
            self.guide_choices = {f'Frame {number}':number for number in self.guide_frames}
            self.guide_combo.configure(values=['Off',*self.guide_choices])
            self.guide_frame.set(next(iter(self.guide_choices),'Off'))
            self.guide_status.set('' if self.guide_choices else 'No shared OCR/Label frame')
            self.select_guide_frame()
        except Exception as exc:
            self.guide_choices = {}
            self.guide_frame.set('Off')
            self.guide_combo.configure(values=['Off'])
            self.preview_label.set_inspection_guides([])
            self.guide_status.set(f'Guide unavailable: {exc}')

    def select_guide_frame(self, event=None):
        if self.busy:
            return
        from camera.inspection_guides import guide_layers
        number = self.guide_choices.get(self.guide_frame.get())
        try:
            layers = guide_layers(self.guide_frames[number]) if number is not None else []
            self.preview_label.set_inspection_guides(layers)
            if number is not None:
                self.guide_status.set('')
        except Exception as exc:
            self.preview_label.set_inspection_guides([])
            self.guide_status.set(f'Guide unavailable: {exc}')
        self.update_guide_visibility()

    def update_guide_visibility(self):
        if not getattr(self,'live_mode',False) or not hasattr(self,'guide_combo'):
            return
        visible = not self.busy and not self.repeat_remaining and self.repeat_after is None
        self.preview_label.show_inspection_guides(visible)
        self.guide_combo.configure(state='readonly' if visible else 'disabled')
        if hasattr(self,'live_zoom_controls'):
            if visible:
                self.live_zoom_controls.pack(fill='x',before=self.preview_label)
            else:
                self.live_zoom_controls.pack_forget()

    def apply_live_zoom(self, event=None):
        if not self.live_mode or self.busy or not self.connected:
            return
        try:
            values = self.values()
            low,high = self.preview_zoom_limits
            if not math.isfinite(values['zoom']) or not low <= values['zoom'] <= high:
                raise ValueError(f'Zoom must be between {low:g} and {high:g}.')
        except (ValueError,tk.TclError) as exc:
            messagebox.showerror('Camera zoom',str(exc),parent=self.window)
            return
        self.preview_zoom_generation += 1
        generation = self.preview_zoom_generation
        def configure():
            with self.preview_zoom_lock:
                if self.closed or self.busy or generation != self.preview_zoom_generation:
                    return
                self.client.configure(values)
        self.background(configure)

    def refresh_elapsed(self):
        if self.closed:
            return
        capture = self.capture
        if self.live_mode:
            if self.busy and capture is not None and capture.started_at is not None:
                if self.pipeline_started_at is None:
                    from datetime import timedelta
                    date = (datetime.now()-timedelta(seconds=time.monotonic()-capture.started_at)).strftime('%Y-%m-%d_%H:%M:%S')
                    self.result_view.pipeline_date.set(f'pipeline start date : {date}')
                self.pipeline_started_at = capture.started_at
            elapsed = (time.monotonic()-self.pipeline_started_at if self.busy and self.pipeline_started_at is not None
                       else 0. if self.busy else self.pipeline_seconds)
            self.result_view.pipeline_time.set(f'pipeline time(sec) : {max(0.,elapsed):.7f}')
        if capture is not None and capture.started_at is not None:
            if getattr(capture,'switchbot',None) is not None:
                bot = capture.switchbot.report()
                warnings = [event['message'] for event in bot['events'] if event['action'] == 'WARNING']
                self.bot_status.set('SwitchBot WARNING: '+warnings[-1] if warnings else f"SwitchBot: {bot['state']} (assumed; physical motion not verified)")
            elapsed = capture.duration if capture.duration is not None else time.monotonic() - capture.started_at
            if capture.finished.is_set() and capture.duration is None:
                elapsed = capture.count / capture.fps
            self.elapsed_seconds.set(f"{max(0, elapsed):.1f} sec")
            if capture.stop_requested.is_set() or capture.finished.is_set():
                self.record_indicator.itemconfigure(self.record_dot, fill="gray")
                if self.busy:
                    self.record_status.set("Processing buffered frames" if self.live_mode else "Saving")
        self.window.after(100, self.refresh_elapsed)

    def values(self):
        return {"zoom": float(self.zoom.get()), "focus": float(self.focus.get()), "auto_focus": self.auto_focus.get()}

    def apply_manual_focus(self, event=None):
        self.auto_focus.set(False)
        self.apply()

    def apply(self):
        if self.connected:
            values = self.values()
            self.background(lambda: self.capture.change(values) if self.capture and not self.capture.finished.is_set()
                            else self.client.configure(values))

    def start(self, repeat=False):
        if self.busy or self.app.live_test_running or not self.connected:
            return
        if self.repeat_after is not None and not repeat:
            return
        clicked_at = time.monotonic()
        settings = None
        try:
            conditions = self.read_conditions()
            if self.live_mode:
                settings = dict(self.live_settings)
                if not math.isclose(settings.get('group_fps',settings["test_fps"]), conditions["fps"], abs_tol=1e-6):
                    raise ValueError("Record condition FPS must match the selected frame group's FPS.")
                from inspection.pipeline_audit import audit_pipeline
                audit = audit_pipeline(settings)
                if audit["errors"]:
                    raise ValueError("\n".join(audit["errors"]))
                from inspection.frame_conditions import sampled_frame_count
                settings["live_expected_frames"] = sampled_frame_count(
                    math.ceil(conditions["fps"] * conditions["total_time"]),conditions['fps'],settings['test_fps'])
                settings["record_condition"] = str(self.condition_path)
                settings.pop('resource_status_callback',None)
            actions = recording_actions(conditions)
        except Exception as exc:
            self.repeat_remaining = 0
            self.update_guide_visibility()
            self.start_button.configure(state='normal')
            messagebox.showerror("Recording settings", str(exc), parent=self.window)
            return
        if self.live_mode and not repeat:
            count = simpledialog.askinteger('Live test','Number of inspections:',initialvalue=1,minvalue=1,parent=self.window)
            if count is None:
                return
            self.repeat_total = self.repeat_remaining = count
        if self.live_mode:
            self.repeat_remaining = max(0,self.repeat_remaining-1)
        self.busy = self.app.live_test_running = True
        self.update_guide_visibility()
        self.stop_pending.clear()
        self.capture = None
        self.elapsed_seconds.set("0.0 sec")
        if self.live_mode:
            self.inspection_count += 1
            run_number = self.inspection_count
            self.inspection_count_var.set(f'Inspection count : {run_number} | {self.repeat_total-self.repeat_remaining}/{self.repeat_total}')
            self.pipeline_started_at = None
            self.pipeline_seconds = 0.
            self.result_view.pipeline_time.set('pipeline time(sec) : 0.0000000')
            self.result_view.pipeline_date.set('pipeline date : -')
            self.active_run_settings = settings
            self.result_mailbox.take()
            self.result_view.reset()
        self.state.set("Loading inspection models..." if self.live_mode else "Starting timed recording...")
        self.start_button.configure(state="disabled")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        if self.live_mode:
            settings.update(live_async_media=True,live_clock_from_capture=True,serial_naming=True,
                            live_log_timestamp=timestamp,live_inspection_count=run_number)
            self.media_jobs.add(run_number)
            def media_done(error):
                import json
                path = settings.get('_live_result_log')
                if path:
                    try:
                        audit = Path(path).with_suffix('.pipeline.json')
                        data = json.loads(audit.read_text(encoding='utf-8'))
                        data['media_save_status'] = 'failed' if error else 'completed'
                        data['media_save_error'] = error
                        data['media_completed_seconds'] = time.monotonic()-settings.get('_live_started_at',clicked_at)
                        data['performance'] = settings['_live_performance'].report() | {
                            k:v for k,v in data['performance'].items() if k not in ('stages','total_wall_seconds','slowest_stage','note')}
                        temporary_log = audit.with_suffix('.tmp')
                        temporary_log.write_text(json.dumps(data,ensure_ascii=False,indent=2),encoding='utf-8')
                        temporary_log.replace(audit)
                    except Exception as exc:
                        error = error or f'Could not finalize media audit: {exc}'
                if error and path:
                    with Path(path).with_suffix('.media_error.txt').open('w',encoding='utf-8') as stream:
                        stream.write(str(error))
                def update():
                    self.media_jobs.discard(run_number)
                    if error:
                        messagebox.showerror('Live media save',f'Inspection {run_number}: {error}',parent=self.window)
                self.post(update)
            settings['media_done_callback'] = media_done
        result_path = unique_path(DEFAULT_TEST_VIDEO_DIR / f"{timestamp}_live_test.mp4")
        def create_capture(path):
            if self.stop_pending.is_set():
                raise RuntimeError("Stopped before camera capture started.")
            if conditions.get('switchbot',{}).get('enabled'):
                self.post(lambda:self.bot_status.set('SwitchBot: connecting BLE before recording (may take up to 48 sec)...'))
            with self.preview_zoom_lock:
                capture = PhoneCapture(self.client, conditions["fps"], path, actions, True, save_recorded_actions=False,
                    switchbot_warning=lambda message:self.post(lambda:self.bot_status.set('SwitchBot WARNING: '+message)),
                    external_stop=self.stop_pending)
            self.capture = capture
            if self.live_mode:
                settings['cycle_complete_callback'] = lambda elapsed,date:self.post(lambda:self.result_view.pipeline_date.set(f'pipeline date : {date}'))
            if self.stop_pending.is_set():
                capture.stop()
            self.post(lambda: self.set_recording(True, "Live testing..." if self.live_mode else "Recording..."))
            return capture
        def work():
            temporary = None
            error, saved = None, None
            try:
                if self.live_mode:
                    saved = inspect_video("http://127.0.0.1/usb-phone", settings, result_path,
                                          show_window=False, file_test_log=True, log_dir=TEST_LOG_DIR,
                                          capture_factory=lambda: create_capture(None),result_callback=self.receive_result)
                else:
                    temporary = tempfile.TemporaryDirectory(prefix="vision_phone_")
                    path = Path(temporary.name) / "recording.mp4"
                    capture = create_capture(path)
                    while capture.read()[0]:
                        pass
            except Exception as exc:
                error = str(exc)
            finally:
                if self.capture and not self.live_mode:
                    try:
                        self.capture.release()
                    except Exception as exc:
                        error = error or str(exc)
                if self.live_mode:
                    actual_start = getattr(self.capture,'started_at',None)
                    self.pipeline_seconds = settings.get('_live_result_ready_seconds',time.monotonic()-actual_start if actual_start is not None else 0.)
                    if error:
                        self.media_jobs.discard(run_number)
                self.post(lambda: self.finish(temporary, saved, error, timestamp))
        threading.Thread(target=work, daemon=True).start()

    def finish(self, temporary, saved, error, timestamp):
        try:
            if temporary is not None and error is None:
                ORIGINAL_VIDEO_DIR.mkdir(parents=True, exist_ok=True)
                destination = filedialog.asksaveasfilename(parent=self.window, title="Save recording",
                                                          initialdir=str(ORIGINAL_VIDEO_DIR), initialfile=f"{timestamp}.mp4",
                                                          defaultextension=".mp4", filetypes=[("MP4 video", "*.mp4")])
                if destination:
                    shutil.copy2(Path(temporary.name) / "recording.mp4", destination)
                    saved = destination
        except Exception as exc:
            error = str(exc)
        finally:
            if temporary is not None:
                temporary.cleanup()
            self.app.live_test_running = self.busy = False
            message = f"Failed: {error}" if error else f"Saved: {saved}" if saved else "Recording discarded."
            self.set_recording(False, message)
            if self.live_mode and error is None:
                self.state.set(f'Ready for next inspection. TXT saved; media may still be saving: {saved}')
                self.result_view.pipeline_time.set(f'pipeline time(sec) : {self.pipeline_seconds:.7f}')
            if self.live_mode:
                if error or self.stop_pending.is_set():
                    self.repeat_remaining = 0
                    self.update_guide_visibility()
                if self.repeat_remaining:
                    self.start_button.configure(state='disabled')
                    self.repeat_after = self.window.after(100,self.start_next)

    def start_next(self):
        self.repeat_after = None
        if not self.closed and self.repeat_remaining and not self.stop_pending.is_set():
            self.start(repeat=True)

    def stop(self):
        self.repeat_remaining = 0
        if self.repeat_after is not None:
            self.window.after_cancel(self.repeat_after)
            self.repeat_after = None
            if not self.busy:
                self.start_button.configure(state='normal')
                self.update_guide_visibility()
        self.stop_pending.set()
        if self.capture:
            self.background(self.capture.stop)

    def refresh_preview(self):
        if self.closed:
            return
        if self.connected and not getattr(self, "preview_busy", False):
            self.preview_busy = True
            def fetch():
                try:
                    frame = self.client.preview()
                    if frame is not None:
                        image = Image.fromarray(cv2.cvtColor(frame, cv2.COLOR_BGR2RGB))
                        self.post(lambda: self.show_preview(image))
                except Exception:
                    pass
                finally:
                    self.preview_busy = False
            threading.Thread(target=fetch, daemon=True).start()
        self.window.after(200, self.refresh_preview)

    def show_preview(self, image):
        self.preview_label.show(image)

    def receive_result(self, frame, number, total, statuses, details=()):
        image = Image.fromarray(cv2.cvtColor(frame,cv2.COLOR_BGR2RGB))
        self.result_mailbox.submit(image,number,total,statuses,details)

    def refresh_result(self):
        if self.closed:
            return
        result = self.result_mailbox.take()
        if result is not None:
            self.result_view.show(*result)
        self.window.after(50,self.refresh_result)

    def close(self):
        if self.busy or self.connecting:
            messagebox.showwarning("USB Camera", "Wait for connection/recording to finish before closing.", parent=self.window)
            return
        if self.repeat_after is not None:
            self.window.after_cancel(self.repeat_after)
            self.repeat_after = None
        self.repeat_remaining = 0
        self.connected = False
        if self.client.port:
            self.background(self.client.disconnect)
        self.closed = True
        self.window.destroy()
