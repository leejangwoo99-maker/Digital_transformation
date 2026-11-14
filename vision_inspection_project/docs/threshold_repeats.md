# Repeated Appearance Tests

The main Test button accepts multiple videos and a repetition count. Videos run
sequentially: all repetitions of A, then all repetitions of B. A failure or a
user-stopped inspection stops the batch; no partial inspection is counted as a
completed repeat. The frame group and test settings are captured at batch start.

## Storage

File tests use `<video>_<group>_threshold_0.50` (50 percent threshold). FPS already
present in the group name is not repeated. Existing legacy results are untouched.
Each new appearance run reserves an ordinal and stores confidence frames in a
hidden pending directory. After inspection, encoding and logs complete, it is
renamed to `1st_<parent>_point_frame_yyyy-mm-dd_hh-mm-ss`. Milliseconds are appended
if needed. Later runs use `2st`, `3st`, etc., even across application restarts.
Videos and special-inspection photos retain the ordinal prefix in the parent;
TXT and pipeline JSON logs retain it in the testlog directory. No previous run is
overwritten. Pending directories are not included in Cpk.

`index.json` in each run retains every inspected frame, maximum score and all
eligible appearance detections from the 1 percent recording floor. Actual NG
judging still uses the configured threshold. `label_points.json` in the parent
records the matched label numbers and percent scores across runs.

## Label Matching

Matching is independent for each frame number and defect class. The earliest
appearance of a label fixes its reference center. Its identity is not the detector
output order. A detection may match only its nearest existing same-class label,
within `max(12 pixels, 0.75 * smaller box diagonal)`. If two detections claim one
label, the closer one wins. The other is not assigned to its second-nearest label.
An unmatched label receives 0 for that run. A new distant label receives a new
number, with 0 in earlier runs. Frames with no detections in any run retain a
zero-score no-detection row. Training annotations are not modified.

All completed runs must have the same source, group, FPS, threshold and inspected
frame-number set. Unexpected child directory names or inconsistent metadata stop
review instead of mixing data silently.

## Cpk

Scores use percent units: 70.00, not 0.70. For each frame/label row, the median
over all repeats defines LSL = median - 0.05 and USL = median + 0.05, without
clamping at 0 or 100. The mean and sample standard deviation (ddof=1) define
`Cpk = min(USL - mean, mean - LSL) / (3 * standard deviation)`.
Formula reference: https://www.itl.nist.gov/div898/handbook/ppc/section4/ppc46.htm

One run cannot produce Cpk. Zero standard deviation is reported separately as
unchanged/pass or unchanged/fail. Otherwise grades are >=1.67 excellent,
>=1.33 good, >=1.00 caution, and <1.00 fail. The worst row determines the summary.
This custom median-centered score reproducibility metric is not evidence of
product quality or process capability under a fixed engineering specification.

Review keeps its scatter plot, image and detection details. A run selector helps
inspect overlapping points. Only the Cpk verdict is shown in the main summary;
Cpk View opens a scrollable table. Cpk Download writes a real XLSX workbook and
defaults to `<selected parent>/<parent>_cpk.xlsx` in the save dialog.
