# Live cycle boundary

Live cycles count from one in the open Live window; closing it resets the count.
Start asks how many cycles to run. Each successful completion (including NG)
automatically starts the next cycle until that count is reached. Stop or an
execution error cancels the remaining cycles; cancelling the count dialog starts
nothing. The pipeline timer remains zero during preparation/BLE startup and
starts at the camera capture's actual `started_at`, continuing through final inspection. A completed verdict
TXT is flushed to disk and published atomically, then the UI permits the next
cycle. The counter is not a count of individual inspected video frames.

The displayed `pipeline time(sec)` uses seven decimal places. TXT includes the
elapsed time sampled immediately before its final timing line and atomic publish;
JSON `pipeline_time_seconds` includes the final flush/publication overhead.
Seven decimal places are formatting, not a precision guarantee. GUI dispatch
after the TXT/JSON write adds a small scheduling interval before Start is enabled.

Live outputs save automatically with no naming prompt. Video/photos retain their
timestamp-based names. Logs use:
`<group>_live_test_threshold_<percent>_result_<p|f>_[<fps>fps]_<yyyymmdd_hhmmss>.txt`
and the matching `.pipeline.json`. The existing testlog directory is unchanged;
no new MES folder or network upload is configured by this change.

Media writes use a bounded ordered queue. Release, rename and video encoding are
queued behind frame writes, not awaited before publishing the verdict TXT.
Queue backpressure still protects memory and may extend cycle time on slow
storage. Inspection and camera operations never overlap each other across runs;
only previous-run media saving may overlap the next cycle.

The JSON initially reports `media_save_status: background`. On completion it is
atomically updated with completion/error information and final performance data.
`pipeline_time_seconds` remains the earlier TXT boundary; total wall time may
increase after media completion. A late media error is shown separately and
written to a `.media_error.txt` sidecar; it does not rewrite the inspection verdict.
Closing the window does not cancel already queued media saves. Process termination
or power failure can still interrupt persistence.
