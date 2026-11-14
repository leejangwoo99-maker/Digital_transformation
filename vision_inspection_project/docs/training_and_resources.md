# Selective Training and Group Combine

Train opens an ALL / open_labeler / conn / label / OCR selector. Only selected
model outputs are regenerated. OCR includes Text image/character models and Mark
templates. ALL retains the existing full training flow. open_labeler updates only
the appearance model and its dataset; conn only updates the connector model.

In label revision, select two different underlying frame groups and choose
Combine. Special entries ending in _OCR, _conn or _label resolve to their parent
frame group. The full frames are appended in the displayed source order, and
ordinary labels, rotations and all three special annotation kinds are copied.
Special annotation image paths, group names and 1-based frame numbers are updated.
The two source groups remain untouched. FPS must match.

The popup independently selects the donor for inspection ranges,
record_condition and test_condition. Inspection ranges are copied verbatim from
the selected donor: no union, added interval or frame offset is applied. This is
distinct from renumbering training annotations. Missing donor settings are
reported for configuration before testing. The new name receives _<fps>fps if
needed. It cannot alias a source or share its recording-condition filename.

An existing destination requires confirmation. A staged file-only commit replaces
its frames, labels and selected settings, removes obsolete destination files and
invalidates only that destination's model files. Failed commits restore previous
files. The combined group must be trained before inspection.

# Worker and Spread

Worker is the requested Mark comparison process count; Spread is the internal
OpenCV/BLAS thread count per process. Defaults are 2 and 1. Both persist in the
group's test_condition file and apply to file and Live tests. Inputs must be
positive integers. Windows process pools have a ceiling of 61 workers; spread
cannot exceed the PC's logical CPU count. These platform ceilings are not
recommendations to allocate that many processes.

The Resources check reports logical cores, available RAM and an estimated worker
memory budget, with warnings against an 80% CPU thread budget and 90% RAM target.
Per-worker estimates include a Python runtime, model and bounded 256 MiB pose
cache; estimates are not measurements or guarantees. Actual initial pool size is
also bounded by available work and estimated CPU/memory budgets. Requested and
effective counts are recorded separately.

Mark variants are prewarmed per requested key when they fit the bounded cache,
and reused thereafter. FAIL-first comparison and all original decision rules
remain unchanged. Only as many jobs as the active pool size are in flight.

During inspection, CPU and RAM are sampled every 250 ms. CPU above 80% or RAM
above 90% for at least one second causes bounded delays before admitting new
frame/Mark work. This is soft throttling, not an OS-enforced utilization cap.
In-flight work is never killed and frames are never dropped by this policy.
Load pressure clears the delay when it recovers. The UI reports Resource delay;
performance logs retain thresholds, delay counts and total wait time.

The phone preview remains separate from inspection admission. Live capture may
still buffer frames and finish inspection after capture; throttling is not a
claim of hard real-time operation.
