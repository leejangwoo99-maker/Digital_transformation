"""Keep test sampling separate from training annotation ordinals."""
import math


class TestSampling:
    def __init__(self, group_fps, test_fps):
        self.group_fps = float(group_fps)
        self.test_fps = float(test_fps)
        if not (math.isfinite(self.group_fps) and math.isfinite(self.test_fps)
                and 0 < self.test_fps <= self.group_fps):
            raise ValueError("Test fps must be positive and no greater than group FPS.")

    def annotation_number(self, number):
        return math.floor((number-1)*self.group_fps/self.test_fps + .5)+1

    def first_sample_at_or_after(self, annotation):
        number = max(1,math.floor((annotation-1.5)*self.test_fps/self.group_fps)+1)
        while self.annotation_number(number) < annotation:
            number += 1
        return number

    def interval(self, bounds):
        start, end = bounds
        first = math.ceil((start-1)*self.test_fps/self.group_fps-1e-9)+1
        last = math.floor((end-1)*self.test_fps/self.group_fps+1e-9)+1
        if first > last:
            raise ValueError(f"Inspection range {start}~{end} has no samples at {self.test_fps:g} FPS. Increase Test fps.")
        return first, last

    def tasks(self, tasks):
        return {key:self.interval(bounds) for key,bounds in tasks.items()}

    def start_groups(self, bounds):
        """Group original start ordinals by their first eligible test sample."""
        start, end = bounds
        first, last = self.interval(bounds)
        for sample in range(first, last+1):
            lower = max(start, self.annotation_number(sample-1)+1) if sample > 1 else start
            upper = min(end, self.annotation_number(sample))
            if lower <= upper:
                yield range(lower, upper+1)

    def schedule(self, schedule):
        if not schedule:
            return {}
        last = math.ceil((max(schedule)-1)*self.test_fps/self.group_fps-1e-9)+1
        return {number:schedule[self.annotation_number(number)] for number in range(1,last+1)
                if self.annotation_number(number) in schedule}
