# Serial naming and file-test cycles

In Label, select Serial beside PASS/FAIL and click existing QR/Data Matrix boxes. The order
of clicks defines concatenation order. Clicking a marked box again removes its
designation. Serial order is saved in the annotation JSON and displayed on boxes.
It does not replace the transcript, pattern, PASS/FAIL or QR comparison settings.
For multiple labeled frames, designate corresponding Serial parts consistently.

Naming uses actual decoded QR/Data Matrix payloads, not OCR or saved training
transcripts. Every designated part must have a reading;
otherwise the name is `no_reading`. Invalid Windows filename characters are
replaced with underscores. The normal inspection verdict is not overridden by
the filename fallback.

File-test result parents replace the source-video name with the Serial. Existing
same-name folders are not reused: new folders use 1st_, 2st_, etc. prefixes. Run
numbers and timestamps preserve repeated results. Live parents and files start
with Serial, retain the recording timestamp, and use `live` rather than
`phone_stream`. Original input paths remain in pipeline audit metadata.

Each run initially writes into a private `.inspection_*` directory. Once Serial
is known, final paths are reserved and recorded in the verdict TXT/JSON; pending
media is moved to those paths in order after writer release and encoding. A media
error leaves recovery artifacts rather than deleting the private directory.
Confidence history is finalized inside each separate run folder, preserving
review records. A folder containing only one run has insufficient repeats for Cpk.
Its source identity is the Serial; original_source retains
the input video identity.

File Test measures from each file's test start (including preparation) through
completed TXT publication. Live retains its actual-camera-start boundary.
The main window and Live result view show pipeline time and date. TXT and JSON
include start date, completed TXT date and elapsed seconds. File repeat order
remains A N times, then B N times. After TXT/JSON publication the next inspection
can run while previous media is finalized. Bounded queue backpressure may still
extend cycle time on slow storage. No MES network destination is configured here.
