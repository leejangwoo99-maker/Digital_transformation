from __future__ import annotations

import json
import math
import os
import queue
import secrets
import shutil
import subprocess
import threading
import tempfile
import time
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen
from urllib.error import HTTPError

import cv2
import numpy as np


ACTION_FILE = Path(r"C:\vision_inspection\operation\phone_camera_actions.json")


def adb_path():
    candidates = [shutil.which("adb")]
    for name in ("ANDROID_SDK_ROOT", "ANDROID_HOME"):
        if os.environ.get(name):
            candidates.append(str(Path(os.environ[name]) / "platform-tools/adb.exe"))
    local = Path.home() / "AppData/Local"
    roots = [local]
    if os.environ.get("LOCALAPPDATA"):
        roots.insert(0, Path(os.environ["LOCALAPPDATA"]))
    roots.extend((local / "Packages").glob("OpenAI.Codex_*/LocalCache/Local"))
    candidates.extend(str(root / "Android/Sdk/platform-tools/adb.exe") for root in roots)
    for candidate in candidates:
        if candidate and Path(candidate).is_file():
            return candidate
    raise RuntimeError("ADB was not found. Set ANDROID_SDK_ROOT to the installed Android SDK folder, or run android_phone_camera/setup-sdk.ps1.")


def adb(*args, serial=None):
    command = [adb_path()] + (["-s", serial] if serial else []) + list(args)
    options = {"creationflags": subprocess.CREATE_NO_WINDOW} if os.name == "nt" else {}
    result = subprocess.run(command, capture_output=True, text=True, timeout=60, **options)
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip())
    return result.stdout.strip()


def load_actions(path=ACTION_FILE):
    if not path.exists():
        return {"enabled": False, "initial": {"zoom": 1.0, "focus": 0.0, "auto_focus": True}, "events": [], "duration": None}
    data = json.loads(path.read_text(encoding="utf-8"))
    duration = data.get("duration")
    if duration is not None and (not math.isfinite(duration) or duration <= 0):
        raise ValueError("Invalid saved camera duration")
    for event in data.get("events", []):
        if not math.isfinite(event["seconds"]) or event["seconds"] < 0:
            raise ValueError("Invalid saved camera action time")
    return data


def save_actions(data, path=ACTION_FILE):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(data, indent=2), encoding="utf-8")
    temporary.replace(path)


class PhoneUSB:
    def __init__(self):
        self.serial = None
        self.port = None
        self.token = secrets.token_hex(24)

    def connect(self):
        rows = [line.split() for line in adb("devices").splitlines()[1:] if line.strip()]
        devices = [row[0] for row in rows if len(row) >= 2 and row[1] == "device" and ":" not in row[0] and not row[0].startswith("emulator-")]
        if len(devices) != 1:
            raise RuntimeError("Connect exactly one USB phone and allow USB debugging on its screen.\n" + adb("devices"))
        self.serial = devices[0]
        adb("shell", "am", "start", "-n", "com.visioninspection.phone/.MainActivity", serial=self.serial)
        self.port = int(adb("forward", "tcp:0", "tcp:8765", serial=self.serial))
        last_error = None
        for _ in range(30):
            try:
                status = self.request("pair", new_token=self.token, allow_error=True)
                if status.get("ready"):
                    return status
                last_error = status.get("error") or "Grant camera permission and keep Vision USB Camera open on the phone."
            except Exception as exc:
                last_error = str(exc)
            time.sleep(.3)
        raise RuntimeError(str(last_error))

    def request(self, endpoint, binary=False, allow_error=False, **values):
        if self.port is None:
            raise RuntimeError("Connect phone first")
        url = f"http://127.0.0.1:{self.port}/{endpoint}?" + urlencode(dict(token=self.token, **values))
        try:
            response = urlopen(url, timeout=5)
        except HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            try:
                message = json.loads(body).get("error", body)
            except ValueError:
                message = body or exc.reason
            raise RuntimeError(f"Camera {endpoint}: {message} (HTTP {exc.code})") from exc
        with response:
            body = response.read()
            if binary:
                return body, response.headers
            result = json.loads(body)
            if result.get("error") and not allow_error:
                raise RuntimeError(result["error"])
            return result

    def configure(self, values):
        return self.request("configure", zoom=float(values["zoom"]), focus=float(values["focus"]),
                            auto_focus=str(bool(values["auto_focus"])).lower())

    def preview(self):
        body, _ = self.request("preview", binary=True)
        frame = cv2.imdecode(np.frombuffer(body, np.uint8), cv2.IMREAD_COLOR) if body else None
        return cv2.rotate(frame, cv2.ROTATE_90_CLOCKWISE) if frame is not None else None

    def disconnect(self):
        if self.serial and self.port:
            adb("forward", "--remove", f"tcp:{self.port}", serial=self.serial)
        self.port = None


class PhoneCapture:
    def __init__(self, client, fps, original_path, actions, enabled, replay=True, save_recorded_actions=True,switchbot_warning=None,external_stop=None):
        self.client, self.fps = client, float(fps)
        from inspection.performance import Performance
        self.performance = Performance()
        self.original_path = original_path
        self.actions = actions
        self.enabled = enabled
        self.save_recorded_actions = save_recorded_actions
        self.replay = bool(enabled and replay and actions.get("duration"))
        self.initial = dict(actions["initial"])
        self.events = []
        self.frames = queue.Queue()
        self.frame_buffer = tempfile.TemporaryDirectory(prefix="vision_live_frames_")
        self.finished = threading.Event()
        self.stop_requested = threading.Event()
        self.stop_lock = threading.Lock()
        self.switchbot = None
        self.external_stop = external_stop
        self.replay_cancelled = threading.Event()
        self.clock_ready = threading.Event()
        self.error = None
        self.started_at = None
        self.duration = None
        self.count = 0
        self.status = client.request("status", allow_error=True)
        if self.fps <= 0 or self.fps > self.status["fps_max"]:
            raise ValueError(f"Phone capture FPS must be <= {self.status['fps_max']}")
        for values in [self.initial] + [event["values"] for event in actions.get("events", [])]:
            if not self.status.get("zoom_min", 0) - .0001 <= values["zoom"] <= self.status.get("zoom_max", float("inf")) + .0001:
                raise ValueError("A scheduled zoom exceeds this phone's supported zoom range.")
            if not values["auto_focus"] and (self.status.get("focus_max", float("inf")) <= 0 or values["focus"] > self.status.get("focus_max", float("inf"))):
                raise ValueError("Manual focus exceeds this phone's supported focus range.")
        self.client.configure(self.initial)
        if actions.get('switchbot',{}).get('enabled'):
            from camera.switchbot import ToggleTimeline
            self.switchbot = ToggleTimeline(actions['switchbot'],actions['duration'],switchbot_warning)
            self.switchbot.prepare()
        try:
            if self.external_stop is not None and self.external_stop.is_set():
                raise RuntimeError('Stopped before camera capture started.')
            self.client.request("start", fps=self.fps)
        except Exception:
            if self.switchbot:
                self.switchbot.close()
            raise
        self.worker = threading.Thread(target=self._produce, daemon=True)
        self.timer = threading.Thread(target=self._replay, daemon=True)
        self.worker.start()
        self.timer.start()

    def isOpened(self):
        return True

    def get(self, prop):
        return {cv2.CAP_PROP_FPS: self.fps, cv2.CAP_PROP_FRAME_WIDTH: self.status["height"],
                cv2.CAP_PROP_FRAME_HEIGHT: self.status["width"], cv2.CAP_PROP_FRAME_COUNT: 0}.get(prop, 0)

    def _produce(self):
        writer = None
        waiting_since = time.monotonic()
        try:
            while True:
                body, headers = self.performance.call("usb_receive_wait",self.client.request,"frame", binary=True)
                if not body:
                    state = self.client.request("status")
                    if not state["recording"]:
                        break
                    if time.monotonic() - waiting_since > 10:
                        raise RuntimeError("Camera produced no frame for 10 seconds; inspection stopped.")
                    continue
                waiting_since = time.monotonic()
                number = int(headers["X-Frame-Number"])
                if number != self.count + 1:
                    raise RuntimeError("USB camera frames are missing; frame-number inspection stopped.")
                frame = self.performance.call("capture_jpeg_decode",cv2.imdecode,np.frombuffer(body, np.uint8), cv2.IMREAD_COLOR)
                if frame is None:
                    raise RuntimeError("USB camera JPEG decode failed")
                frame = cv2.rotate(frame, cv2.ROTATE_90_CLOCKWISE)
                if self.started_at is None:
                    if self.external_stop is not None and self.external_stop.is_set():
                        self.stop()
                        break
                    if self.original_path is not None:
                        self.original_path.parent.mkdir(parents=True, exist_ok=True)
                        writer = cv2.VideoWriter(str(self.original_path), cv2.VideoWriter_fourcc(*"mp4v"), self.fps,
                                                 (frame.shape[1], frame.shape[0]))
                        if not writer.isOpened():
                            raise RuntimeError("Original camera recording writer failed")
                    self.started_at = time.monotonic()
                    if self.switchbot:
                        self.switchbot.start(self.started_at)
                    self.clock_ready.set()
                if writer is not None:
                    writer.write(frame)
                self.count = number
                # Keep original JPEGs on disk so slow inference cannot stop capture
                # or accumulate decoded full-resolution images in memory.
                buffered = Path(self.frame_buffer.name) / f"{number:09d}.jpg"
                self.performance.call("capture_buffer_write",buffered.write_bytes,body)
                self.frames.put(buffered)
                if self.replay and self.count >= math.ceil(self.actions["duration"] * self.fps):
                    self.stop()
                    break
        except Exception as exc:
            self.error = exc
            self.stop()
        finally:
            if self.switchbot:
                self.switchbot.finish()
            if writer:
                writer.release()
            try:
                if self.save_recorded_actions and self.enabled and not self.replay and self.started_at and self.count and self.error is None:
                    save_actions({"enabled": True, "initial": self.initial, "events": self.events,
                                  "duration": self.duration or self.count / self.fps,
                                  "final": self.events[-1]["values"] if self.events else self.initial})
            except Exception as exc:
                self.error = self.error or exc
            finally:
                self.finished.set()

    def _replay(self):
        while not self.clock_ready.wait(.1):
            if self.finished.is_set():
                return
        if not self.replay:
            return
        try:
            for event in sorted(self.actions["events"], key=lambda item: item["seconds"]):
                delay = self.started_at + event["seconds"] - time.monotonic()
                if self.replay_cancelled.wait(max(0, delay)):
                    return
                self.client.configure(event["values"])
            delay = self.started_at + self.actions["duration"] - time.monotonic()
            if not self.replay_cancelled.wait(max(0, delay)):
                self.stop()
        except Exception as exc:
            self.error = exc
            self.stop()

    def change(self, values):
        if self.replay:
            raise RuntimeError("Timed recording is running. Stop recording before changing camera controls.")
        self.client.configure(values)
        if self.started_at:
            self.events.append({"seconds": time.monotonic() - self.started_at, "values": dict(values)})

    def cancel_replay(self):
        self.replay_cancelled.set()
        self.replay = False

    def read(self):
        while True:
            if self.error:
                raise RuntimeError(str(self.error))
            try:
                buffered = self.performance.call("inspection_queue_wait",self.frames.get,timeout=.2)
                try:
                    frame = self.performance.call("inspection_buffer_decode",cv2.imdecode,np.frombuffer(buffered.read_bytes(), np.uint8), cv2.IMREAD_COLOR)
                    if frame is None:
                        raise RuntimeError("Buffered USB camera JPEG decode failed")
                    return True, cv2.rotate(frame, cv2.ROTATE_90_CLOCKWISE)
                finally:
                    buffered.unlink(missing_ok=True)
            except queue.Empty:
                if self.finished.is_set():
                    return False, None

    def stop(self):
        with self.stop_lock:
            first_stop = not self.stop_requested.is_set()
            self.stop_requested.set()
            self.replay_cancelled.set()
        if self.switchbot:
            self.switchbot.finish()
        if first_stop:
            if self.started_at:
                self.duration = time.monotonic() - self.started_at
            try:
                self.client.request("stop")
            except Exception as exc:
                self.error = self.error or exc

    def release(self):
        self.stop()
        self.worker.join(timeout=8)
        self.timer.join(timeout=3)
        if self.switchbot:
            self.switchbot.close()
        if self.worker.is_alive():
            raise RuntimeError("USB camera shutdown timed out")
        self.frame_buffer.cleanup()
