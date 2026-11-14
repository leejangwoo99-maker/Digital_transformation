# Test FPS and Item Success

Test fps is saved per frame group in its test condition file. Its default is
the group's extraction FPS. It must be positive and cannot exceed group FPS
or the selected video's source FPS. Training and source videos are unchanged.

Inspection ranges remain stored in original group frame numbers. Runtime maps
their inclusive timestamps onto the test sampling grid. For 23 -> 10 FPS,
24-47 becomes 11-21. Each test sample uses the nearest original group ordinal
at its timestamp, never the new ordinal's training annotations. An unlabeled
mapped ordinal is skipped, not replaced with another labeled frame. A range
with no sample or no usable labels is an error.

OCR/Label ranges latch successful items independently. Successful Text/Mark
items are not evaluated again. Label Text readings and valid QR payloads are
reused while remaining components retry; the existing exact QR/Text comparison
must still pass. Every item in each configured range must succeed. State is
reset between ranges and test runs. Conn retains its existing decision rules.

Historical successful items appear in details with their test frame number,
but their old boxes are not drawn onto a newer frame. The photo inspection
JSON records original annotation frame numbers and item success frames.

Live capture still uses record_condition FPS and duration; only inspection
sampling changes. Video/image compression and save formats are unchanged.
