"""Estimate file-test batch completion from observed wall-clock work."""
from dataclasses import dataclass


@dataclass
class BatchProgress:
    total: int
    started: float
    completed: int = 0
    frame_fraction: float = 0.

    def update_frames(self, count, total):
        if total:
            self.frame_fraction = max(self.frame_fraction,min(.95,max(0.,count/total)))

    def finish_run(self):
        self.completed = min(self.total,self.completed+1)
        self.frame_fraction = 0.

    def estimate(self, now):
        done = self.completed+self.frame_fraction
        percent = min(99.9,100*done/self.total)
        if self.completed == self.total:
            return 100.,0.
        if done < .1:
            return percent,None
        elapsed = max(0.,now-self.started)
        return percent,elapsed*(self.total-done)/done
