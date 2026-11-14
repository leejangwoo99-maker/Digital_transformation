"""Bounded independent inspectors; join a frame before committing its results."""
from concurrent.futures import ThreadPoolExecutor


class ParallelChecks:
    def __init__(self, workers, performance):
        self.pool = ThreadPoolExecutor(max_workers=workers,thread_name_prefix='special-check')
        self.performance = performance
        self.parallel_frames = 0

    def run(self, frame, number, total, checks, registration):
        children = []
        restore = []
        for inspector in checks.values():
            if getattr(inspector,'labeled_frames_only',False):
                records = [schedule[number] for schedule in getattr(inspector,'annotation_frames',{}).values() if number in schedule]
                if hasattr(inspector,'conn_reference'):
                    record = inspector.conn_reference(number)
                    if record is not None:
                        records.append(record)
            else:
                records = list(getattr(inspector,'references',{}).values())
            if hasattr(registration,'fork') and any(r.get('role') == 'registration' for record in records for r in record.get('regions',[])):
                child = self.performance.call('shared_reference_frame_preparation',registration.fork,frame)
                restore.append((inspector,getattr(inspector,'shape_registration',None)))
                inspector.shape_registration = child
                children.append(child)
        tasks = {key:self.pool.submit(self.performance.call,'parallel_check.'+key,
                    inspector.check_frame,frame,number,total) for key,inspector in checks.items()}
        try:
            results = {key:task.result() for key,task in tasks.items()}
            self.parallel_frames += int(len(tasks)>1)
            return results
        finally:
            # A failing lane must not leave another lane mutating a reused inspector.
            try:
                for task in tasks.values():
                    try:
                        task.result()
                    except Exception:
                        pass
                if children:
                    registration.merge_forks(children)
            finally:
                for inspector,previous in restore:
                    inspector.shape_registration = previous

    def close(self):
        self.pool.shutdown(wait=True,cancel_futures=True)
