# Bounded parallel checks and file-run layout

For an automatic runtime with at least eight budgeted logical threads, OCR,
Label and the remaining inspector (Conn) use at most three independent lanes.
Each OCR/Label reader retains batching by model, input width and numeric mode;
job groups use an automatically selected N-worker pool with N native OpenVINO
threads per job and independent InferRequests. With sufficient RAM, 12 logical
CPUs select 2x2 and 24 select 3x3. Smaller CPU or RAM budgets scale back.
Torch appearance/Conn threads are capped at two in this mode.
Within each pool, groups with higher input width times crop count are submitted
first. Batch contents and result merge order are unchanged.
Torch OCR fallback loading/inference is serialized to protect shared readers.
These are bounded scheduling settings, not a guarantee of a faster CT.

Only inspectors and models are shared where safe. Each lane has its own QR
reader, polygon aligner, reference-match state, recognition statistics and
item-level verdict state. Results are joined in deterministic input order on
every frame before publishing its result or moving on. Worker failures join the
other lanes and propagate; no frames or inspection items are skipped.

For active reference areas, the current-frame SIFT/flow feature preparation is
shared once. Matching each reference is performed using isolated forks, then
tracking state/statistics are merged after all tasks finish. Flow fallback can
still perform an additional extraction when necessary. Reference areas that
are not active for the current labeled frame do not trigger this preparation.
Legacy seconds-based photo selection and alignment-enabled paths retain their
existing conservative path.

Pipeline fields: `automatic_runtime.special_workers`, `ocr_job_workers`,
`ocr_inference_threads`, `special_parallel_frames`, `ocr_job_execution`,
`special_frame_parallel_wall`, `parallel_check.*` and
`shared_reference_frame_preparation`. Wall timers overlap individual lanes;
do not add them to their nested timings. Runtime fingerprint version 5
invalidates earlier CPU profiles on selection.

## File result paths

File tests now publish under `test/<selected-video-stem>/`:

```text
proto_1/
  1st_<Serial>_<group>_threshold_0.10_[5fps]/
    1st_<Serial>_<group>_threshold_0.10_result_f_[5fps]_<timestamp>.mp4
    1st_<Serial>_<group>_threshold_0.10_[5fps]_point_frame_<timestamp>_[5fps]/
  2st_<Serial>_<group>_threshold_0.10_[5fps]/
```

The next number is one greater than the highest numbered result directory in
that selected-file container, even across separate batches or changed Serial /
threshold / FPS settings. Folder reservation uses atomic mkdir and retries
collisions. A pre-existing 5st means the next five runs use 6st through 10st.
The result files, point-frame directory and index run_number use the same number;
the parent prefix is never repeated as `1st_1st_`. Live timestamp-based grouping
and central TXT/pipeline log publication are unchanged. Existing results are
not moved or renamed.

Review threshold accepts a single numbered result folder or the selected-file
container with multiple numbered results. Both current single-prefix point
folders and older double-prefix names are readable. Nested image paths remain
relative to the chosen review root. Each run's original parent/index identity
is still validated. Source video, group, threshold and Test FPS must agree;
mixed FPS still raises exactly `different Test fps`. Duplicate/incomplete runs
and unrelated directories remain errors. Data from different settings is never
silently combined for Cpk.

Review's image panel now has Run / Frame inputs and Go (or Enter) for exact
navigation. Existing frame/run selectors and plot clicks synchronize these
inputs. Changing the run preserves the frame number where available. An absent
run/frame shows a warning and leaves the current selection unchanged. Directly
opening a later point-frame folder uses its actual run number, not an assumed 1.

## Settled OpenVINO configuration

The diagnostic comparison is no longer exposed or enabled in the main window.
N repetitions now means N tests per selected video, not 2N. The standard
plan scales OCR jobs and OpenVINO threads together, with Torch threads capped
at two and Mark at 1x1. Smaller budgets retain the
conservative fallback. Explicit comparison profiles remain supported internally
for reproducible diagnostics, with warmup outside the corresponding cycle clocks.
This is a bounded comparison, not a guarantee of low CPU utilization: native
thread budgets can compete and soft resource admission remains enabled.

`performance.openvino_comparison` records profile and within-profile repetition.
`ocr_runtime_diagnostics` records dispatch group counts, worker settings, bounded
per-batch width/model/size/backend/thread/timing/cache traces and observed native
OpenVINO request concurrency. Busy wall time is the union of request intervals;
summed inference elapsed time overlaps and is not CT. Request creation and
native execution are logged separately. These diagnostics exclude selection
warmup and do not record product text or cache recognition results.

`ocr_compile_cache_totals.events` records changes since the run's initial cache
snapshot: capacity/retained entries before and after, eviction count, system RAM,
estimated entry size, memory budget and trigger reason. LRU replacement without
a capacity change is distinguished from pressure eviction. Traces are bounded
to 512 rows with explicit truncation indicators; counters remain complete.
`memory_policy` records the delayed gradual retention policy: three seconds of
sustained 80% RAM pressure before shrinking, two seconds between soft steps,
approximately 10% of retained entries per step (rounded up), and immediate
release at 90%. The actual policy name is also recorded for explicit comparisons.
