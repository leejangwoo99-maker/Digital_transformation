# Type-scoped group absorption

Label revision Combine now opens a source picker, then a destination picker.
Only another group of the same selected type is eligible: open_labeler, OCR,
label or conn. A final confirmation states the direction and removal.
The selected source type is removed only after staged copies and committed
files pass SHA-256 verification. Failed commits or source removals roll back.

Destination images and labels keep their current names and positions. New
images append after the highest existing filename number; special annotations
point to their new images and use their new ordered frame positions. Only the
selected annotation type is copied. Destination inspection, record_condition
and test_condition files are untouched. No training happens automatically.
The next Train dialog defaults to the absorbed type; the existing explicit ALL
option remains available.

Special-type absorption leaves appearance labels and shared originals intact.
Appearance absorption moves TXT/rotation data and removes unshared source images.
Images still referenced by another special group are retained. An ownership
marker hides the consumed appearance entry in Label revision; adding new
appearance labels makes it visible again. Other types and their model files
are not removed or retrained. Existing source conditions and historical outputs
are retained. A per-destination/type absorption audit records each operation.
