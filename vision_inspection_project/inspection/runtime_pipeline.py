"""Bounded ordered I/O and appearance prefetch; failures are never discarded."""
from concurrent.futures import ThreadPoolExecutor
from queue import Queue, Empty, Full
from threading import Event, Thread
from time import perf_counter


def lower_save_priority():
    """A saver must keep draining its bounded queue even while inspection is active."""
    import os
    if os.name == 'nt':
        import ctypes
        kernel = ctypes.WinDLL('kernel32',use_last_error=True)
        kernel.GetCurrentThread.restype = ctypes.c_void_p
        kernel.SetThreadPriority.argtypes = (ctypes.c_void_p,ctypes.c_int)
        kernel.SetThreadPriority(kernel.GetCurrentThread(),-1)


def write_image(path,image):
    import cv2
    if not cv2.imwrite(str(path),image):
        raise RuntimeError(f'Could not save inspection photo: {path}')


class ReleaseOnce:
    def __init__(self,capture):
        self.capture = capture
        self.released = False

    def __getattr__(self,name):
        return getattr(self.capture,name)

    def release(self):
        if not self.released:
            self.released = True
            self.capture.release()


class OrderedSaver:
    def __init__(self, performance, capacity=3):
        self.performance = performance
        self.pool = ThreadPoolExecutor(max_workers=1,thread_name_prefix='inspection-save',
                                       initializer=lower_save_priority)
        self.pending = []
        self.capacity = capacity
        self.peak = 0

    def submit(self, stage, function, *args):
        if len(self.pending) >= self.capacity:
            started = perf_counter()
            self.pending.pop(0).result()
            self.performance.add_since('save_queue_backpressure',started)
        self.pending.append(self.pool.submit(self.performance.call,stage,function,*args))
        self.peak = max(self.peak,len(self.pending))

    def close(self):
        try:
            for task in self.pending:
                task.result()
        finally:
            self.pool.shutdown(wait=True,cancel_futures=True)
            self.pending.clear()


class LiveMediaSaver(OrderedSaver):
    """Keep media finalization outside the MES-result critical path."""
    def __init__(self,performance,capacity=3):
        super().__init__(performance,capacity)
        self.sealed = False

    def close(self):
        # Normal pipeline cleanup must not join the media lane before TXT publication.
        pass

    def abort(self):
        OrderedSaver.close(self)

    def finish_background(self,callback=None):
        if self.sealed:
            return
        self.sealed = True
        def finish():
            error = None
            try:
                OrderedSaver.close(self)
            except Exception as exc:
                error = str(exc)
            if callback:
                callback(error)
        Thread(target=finish,name='live-media-finalize',daemon=False).start()


class AppearanceStream:
    """Only predictions are queued, not 4K images. Consumption is frame-ordered."""
    def __init__(self, source, fps, detector, performance, capacity=64,gate=None):
        self.queue = Queue(maxsize=capacity)
        self.stop = Event()
        self.capacity = capacity
        self.gate = gate
        self.peak = 0
        self.performance = performance
        self.thread = Thread(target=self._run,args=(source,fps,detector,performance),daemon=True)
        self.thread.start()

    def _put(self,value):
        while not self.stop.is_set():
            try:
                self.queue.put(value,timeout=.1)
                self.peak = max(self.peak,self.queue.qsize())
                return
            except Full:
                pass

    def _run(self,source,fps,detector,performance):
        import cv2
        capture = cv2.VideoCapture(str(source))
        try:
            if not capture.isOpened():
                raise RuntimeError('Appearance prefetch cannot open video')
            source_fps = capture.get(cv2.CAP_PROP_FPS)
            index,number = 0,0
            while not self.stop.is_set():
                ok,frame = capture.read()
                if not ok:
                    break
                source_index = index
                index += 1
                if source_index/source_fps+1e-9 < number/fps:
                    continue
                number += 1
                if self.gate is not None:
                    self.gate.before_submit()
                performance.mark('first_appearance_inference_started')
                detections,_ = performance.call('appearance_inference',detector.predict,frame)
                self._put((number,detections,None))
            self._put((None,None,EOFError('Appearance prefetch ended early')))
        except Exception as exc:
            self._put((None,None,exc))
        finally:
            capture.release()

    def take(self,number):
        started = perf_counter()
        try:
            while True:
                try:
                    actual,detections,error = self.queue.get(timeout=.1)
                    break
                except Empty:
                    if not self.thread.is_alive():
                        raise RuntimeError('Appearance worker ended without a result')
        finally:
            self.performance.add('appearance_prefetch_wait',perf_counter()-started)
        if error is not None:
            raise error
        if actual != number:
            raise RuntimeError(f'Appearance frame mismatch: expected {number}, received {actual}')
        return detections,None

    def close(self):
        self.stop.set()
        self.thread.join()
