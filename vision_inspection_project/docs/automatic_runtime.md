# Automatic test runtime

The main window exposes Threshold and Test fps only. Detection IoU is fixed at
45 percent, including when older group settings contain a different value.
Threshold still controls NG decisions; the optional file-review recording floor
remains 1 percent. Live tests do not save scatter-plot frames.

Selecting a trained frame group starts background model preparation. The first
test also prepares models when necessary. Preparation loads and warms inference
backends, reference descriptors and Mark variants before capture where possible.
Wait for the ready status before starting Live capture.

With sufficient free resources, representative real model calls measure bounded
CPU-thread and parallel/serial combinations, followed by Mark worker/thread
trials. Mark candidates must preserve the reference verdicts. The selected
profile is stored as `<group>_runtime_profile.json` in the inspection info folder.
Hardware/model/annotation changes invalidate it. This is measured selection,
not an exhaustive guarantee of the fastest possible combination. Under high
CPU or memory load, calibration is deferred and conservative settings are used.
First-time preparation/calibration can take substantially longer than later runs.
Calibration runs only during frame-group selection, never opportunistically at
Test start. If selection-time calibration was deferred, Test uses the existing
profile or conservative defaults instead. Required model loading still occurs
if no background preparation has completed.

Appearance/special-check parallelism is now enabled whenever the current
CPU/RAM allocation permits it, even if a saved calibration profile selected
serial execution. Calibration still selects thread counts and Mark workers.
This user-selected policy is not a promise that parallel execution is faster:
CPU/memory contention can increase individual stage times. Low-resource runs
retain the serial fallback.

File tests can prefetch appearance predictions while OCR/Label photos are being
inspected. Only ordered predictions, not full-resolution frames, are buffered.
Live appearance inference can overlap special checks. A single ordered save
worker separates writes from inspection, with a bounded queue and backpressure.
Save errors propagate; inspection frames are not silently skipped. Existing
image/video encoding and inspection verdict rules are unchanged.

CPU 80 percent and memory 90 percent are soft admission limits. Sustained overload
slows new submissions; it cannot impose an OS-level utilization cap. Live does
not display an additional resource-delay message. Resource statistics remain in
the pipeline log.

`performance.automatic_runtime` records selected settings, preparation time,
profile/model reuse and save queue peak. Stage timers overlap now: do not sum
stage durations or subtract asynchronous save durations from total wall time to
estimate inspection latency. Validate Live throughput on the actual camera;
saved-video timing alone does not establish real-time performance.

## Repeated-product preparation and media priority

Prepared special-check assets (annotations, schedules, text/Mark models, Conn
model and reference transforms) are retained for the current configuration.
Every product gets a fresh inspector with new verdict, Serial, QR and tracking
state. OCR readers and reference-only polygon data can be reused; recognized
product text and OK results cannot. Model, annotation, referenced-image, ordered
frame-list, inspection-condition and alignment-setting changes invalidate reuse.
`performance.automatic_runtime.fixed_data_reused` records this decision. A warmed
group no longer rebuilds preparation samples on every uncalibrated Test start.

`performance.ocr_compile_cache_totals` includes preparation and inspection calls
within the run; `performance.ocr_compile_cache` splits inspection-only calls by
OCR/Label. Fields are `cache_hits`, `cache_misses`, `compiles`, `recompiles`,
`disk_cache_hits` and `hit_rate` (0..1, null when unused). A compile means an
OpenVINO compile-model invocation on a memory-cache miss. A recompile is a repeat
of a previously compiled identical model/mtime/width key in this process;
it may still load a disk cache, so it is not necessarily a fresh native compile.
The process-wide memory cache is now adaptive (see below). Identical capture
conditions do not guarantee hits if model widths vary or entries are evicted.
Concurrent cache misses are serialized to avoid duplicate compilation.

On Windows the ordered media worker uses below-normal thread priority and FFmpeg
uses below-normal process priority. Encoding and decoding each use one FFmpeg
thread. CRF/preset, image formats and verdict rules are unchanged. This favors
inspection without suspending the saver: pausing a bounded saver until inspection
finishes could deadlock a producer waiting for queue space. Storage errors still
propagate; slower media completion or queue backpressure remains possible.

## Adaptive OCR cache

The compiled-model cache monitors system memory at lookup (at most once per
second on hits, and again after every compilation). It targets system memory
below 80%, with an estimated cache budget at most 20% of physical RAM and a
maximum of 128 entries. Soft shrinking requires memory at or above 80% for
three seconds, then removes approximately 10% of retained entries (rounded up)
at intervals of at least two seconds, only as far as the estimated target needs.
Recovery below 80% resets the pressure timer; growth resumes below 78%.
An estimate change alone does not evict retained models while RAM is below 80%.
At 90% memory usage it immediately releases retained entries and
executes without retaining a new compiled model until memory recovers. These
are soft retention limits, not guarantees about allocations made during an
individual inference or about when the native allocator returns RAM to Windows.
LRU entries are removed first. No in-flight inference is killed or skipped.

Entry costs start at 64 MiB. The first compilation's RSS delta is excluded
because plugin/shared initialization is not a recurring per-model allocation.
The estimate uses a rolling median of the latest 16 positive subsequent deltas,
bounded to 16..96 MiB, rather than retaining the largest-ever process RSS spike.
This is a capacity-planning heuristic, not measured exclusive OpenVINO memory:
other lanes and allocator reuse affect RSS. Actual system-memory feedback still
gradually shrinks or immediately releases the cache at the 80/90 percent
thresholds respectively, even when
the heuristic underestimates a large model. The log includes the raw RSS peak,
sample count and estimate strategy for diagnosing this distinction.
`ocr_compile_cache_totals` adds per-run `evictions`, `pressure_evictions` and
`capacity_changes`; its `memory_policy` includes current memory percentage,
estimated bytes, limits, cumulative eviction counts and peak retained entries.

## Flexible text and codes

OCR/Label have a Flexible selection mode next to PASS/FAIL. Clicking an existing
Text/QR box toggles its `flexible` flag without changing PASS/FAIL or Serial.
Unmarked fields keep exact fixed-text inspection. Existing `check` format rules
remain supported for backward compatibility. Product verdicts/recognized strings
are never reused across runs, for either fixed or Flexible fields.

Flexible text is read from the current product. If a pattern is configured, it
must match. Paired Label QR data must also exactly concatenate the actual OCR
values in box order, including any fixed fields. Without a paired code, Flexible
text requires a configured pattern: nonempty OCR alone is not a pass. OCR now
offers the same check/digits/num/text format editor as Label. A missing rule is
reported as `flexible_condition_missing`. QR flags do not relax paired text rules.
Flexible text does not use a previous fixed-print image exemplar to veto changed
characters; recognition, format and actual code comparison still apply. Fixed
fields retain image checks and exact OCR, so marking other fields Flexible does
not suppress missing/incorrect print detection.

The Flexible flag alone is runtime metadata and does not require retraining;
changing existing text/pattern/check/training labels still follows existing
training-signature validation and may require Train.

## Startup visibility

`automatic_runtime.preparation_stages_seconds` separates appearance loading,
fixed-asset loading, fresh-product state, sample preparation, appearance/OCR
warmup and Mark warmup. Calibration (when explicitly enabled at selection) and
other overhead are not all included in these sub-stage sums.
`lock_wait_before_pipeline_seconds` records serialized-runtime waiting before
the pipeline enters, for example when group preparation is still running.

`performance.milestones_seconds` records elapsed time from pipeline entry to
`models_ready`, `first_special_inspection_started`,
`first_appearance_inference_started` (file prefetch),
`file_photo_inspections_ready` and `first_result_frame_ready` when applicable.
Legacy seconds-based file photo selection includes real OCR/Label inspection
before the normal video preview loop, so an unmoving preview does not imply no
inference is running. Frame-range mode now uses the inline path described below.
These milestone times are not a replacement for the Start-to-TXT CT and do not
include time spent waiting for the serialized lock before pipeline entry.

## Selection readiness and inline file inspection

Selecting a frame group starts the existing model/asset warmup and CPU-profile
preparation. File Test and opening Live Test are disabled until this preparation
finishes. Generation tokens prevent an older group's completion callback from
reenabling testing while the latest group is still preparing. Unchanged runs
reuse the models and only reset product state; changed files/settings still
invalidate the relevant runtime fingerprint. Preparation errors are shown as
deferred and do not mark a model successfully warmed.

For frame-range file tests, OCR/Label now execute in the ordinary ordered frame
loop, like Live, rather than scanning the video in a prepass. Appearance can run
in the bounded per-frame inference lane while that frame's special checks run.
Preview frames before the inspection range are available immediately, and every
inspection box is drawn on its inspected frame. During a long frame inspection
the preview can still pause; this is not a promise of real-time playback speed.
Ranges, source annotation-number mapping, item-level OK latching and the
requirement that all ranges pass remain unchanged. File scatter frames remain
enabled; Live scatter frames remain disabled. Legacy seconds-based photo
selection retains its existing preselection behavior.

The runtime log records `inline_file_photo_inspection` and per-kind
`inline_photo_inspection.*` stage times; `inline_photo_inspections_ready` marks
completion of all ranges. Inline file and Live tests use per-frame appearance
parallelism, without lookahead. Alignment and CPU/RAM admission guards remain.

OCR/Label inspections use the configured ranges and sampled annotations.
Legacy per-item start files are ignored. Passed items still latch within each range.

Completed inspections profile OpenVINO OCR model usage by model identity,
input width and thread count. Profiles are separate by group, Test FPS and OCR
worker/thread configuration. Before reading inspection frames, the next run
in the same process prepares missing models from the matching profile in
frequency order, limited by the current memory budget. Less frequent widths
outside that budget remain on-demand; input shapes and verdicts are unchanged.
Existing memory pressure limits still apply. Preparation time and compiles are
reported separately from inspection-time cache counters. File-test CT still
includes preparation after Start. OCR text and verdicts are never cached across
products.

Automatic OCR and Label use square worker/thread settings. The CPU candidate
is one third of logical processors, rounded down (minimum one and capped by
the CPU budget). For example, 12 logical CPUs allow 4x4 when at least 2.25 GiB
of headroom remains below the 90% memory limit. The existing 80% and 85%
memory limits still cap it at 2x2 and 1x1, respectively. Pipeline logs record
the CPU and memory candidates separately from the selected combination.

`performance.verdict_timeline` in the pipeline JSON reports both pipeline
entry to TXT publication and OCR/Label result readiness to TXT publication.
Conn finalization is timed separately. Stage durations are clipped to each
window, including media tasks still running at publication. Lane totals can
overlap; exclusive lane time, disjoint overlap combinations, and unmeasured
time sum to the wall-clock window. Decode/input waits and appearance-result
waits are not inference time.

After inline OCR/Label ranges finish, the appearance worker can read and
predict one sampled frame ahead while the current frame is rendered and
presented. The frame loop consumes results in order. Logs record the bounded
tail lookahead separately from legacy whole-video prefetch.

Appearance YOLO uses OpenVINO CPU when `model.appearance_backend` is
`openvino`. The original checkpoint is unchanged. Dynamic FP32 exports are
reused by checkpoint version and input size. The pipeline reports the active
appearance backend independently of OCR. Set the value to `pytorch` for a
comparison with the original checkpoint.

The file Test options require choosing Normal, scatter data, or a speed sweep.
Normal retains photos, video, and verdict logs without scatter data. Scatter
runs only the requested repetitions at the current inspection threshold and
stores the entered review thresholds alongside a single set of raw frames
and scores per run. Review derives threshold-specific verdicts on demand. The speed sweep
uses the current CPU/memory allocation as its worker/thread ceilings and
tests all pairs within two steps below each ceiling (minimum 2x2), all PyTorch
first, then OpenVINO. Each
video/condition gets a full warmup excluded from ranking. Media drains
between runs, outside the measured Start-to-TXT CT. Benchmark media is
separate from normal results, while the usual pipeline JSON identifies the
benchmark, condition and warmup. Confirming the lowest mean CT saves that
condition for the frame group and current PC, for file and Live tests.
The batch progress and ETA include warmups when running a speed sweep.

