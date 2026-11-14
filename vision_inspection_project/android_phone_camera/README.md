# Vision USB Camera

1. Enable Developer options and USB debugging on the Android phone.
2. Connect a USB data cable and approve this computer on the phone.
3. Run main.py and open recording by phone or live test. App installation and USB connection run automatically.
4. Grant camera permission to Vision USB Camera and keep it open in the foreground.
5. Wait for Connected. Choose settings and start recording or testing.

The setting section contains record FPS, Total time (10 seconds by default),
zoom intervals with add/remove controls, Start, Stop and Save. Save writes
C:\vision_inspection\test\info\<name>_record_condition.txt, after confirming any overwrite.
The record_condition selector loads named profiles; without a selection the most
recently saved profile is loaded. The old record_condition.txt is still readable.
A zoom interval applies its zoom at the start and restores the base zoom at the
end. Intervals may touch but must not overlap. Start always uses the current
settings and stops automatically after Total time. A red REC indicator is shown
while recording. The previous action-save checkbox and replay file are not used
by this window anymore. Preview, saved recording and live inference all use a
90-degree clockwise rotation (1080x1920 for a 1920x1080 phone stream).
Elapsed seconds appear below Start and remain visible after stopping.
Late camera callbacks no longer abort recording on their own; overdue sampling
is rebased without creating duplicate images. Transmitted sequence gaps, queue
overflow and prolonged absence of frames still stop inspection. Consequently,
sustained camera stalls can produce fewer real frames than duration times FPS.
After this update, reinstall the APK to replace the old phone-side delay checks.
Autofocus reproduces the autofocus command, not a guaranteed identical optical result.

Live test opens a separate window with record_condition and select frame controls
and Start/Stop. Choose the saved profile and a trained group; their FPS must match.
Live inspection begins after model loading; it does not first record a source file.
Results are saved in C:\vision_inspection\test, with matching .txt and
.pipeline.json logs in C:\vision_inspection\testlog.
Ordinary recording opens a Save As dialog at completion, defaulting to
C:\vision_inspection\original. Cancel discards the temporary recording.
Camera controls are
applied when releasing a slider; manual focus turns autofocus off.
Results include _live_test in their name. The current phone transport supports up to 1920x1080 and 30 FPS; if inference
cannot keep up, lower the selected FPS. Missing frames stop inspection instead of
silently changing frame numbering. A USB data cable alone does not expose Camera2
controls: the supplied phone app and authorized ADB connection are required.

Hover a label box and use [ or ] to rotate by 5 degrees; Shift uses 1-degree steps.
Alternatively press O, enclose one or more labels, and drag the single yellow
rotation handle. All selected labels rotate around their own centers without
moving their positions. Press O again to return to drawing. R remains ROI.
Rotated appearance and pin-point annotations train oriented bounding-box models.
Retrain after changing rotations. OCR and QR crops are deskewed before recognition.

Conn labeling starts with pin point: drag a rectangle around the visible pin;
its center is the inspected point. Pin area is an independent circular tolerance
region: click anywhere for the circle center and drag to its edge. It need not
be centered on a pin and may be drawn before pin points. The annotation view
automatically shows PASS/FAIL from pin-center containment. Runtime inspection
uses detected pin centers, not the annotation centers; one center per area is
required, and missing, additional or outside centers fail. Slot creation is
removed; legacy slot regions are discarded when reopening Conn annotations.
When starting a pin area within 12 screen pixels of a pin point center, the
circle snaps to that center and asks for its diameter in original image pixels.
Dragging the center of an existing pin point or area creates an axis-locked
copy; horizontal movement keeps Y unchanged and vertical movement keeps X
unchanged. Hold C and drag to fully enclose pin points and pin areas together;
then drag any selected center and release to create an axis-locked group copy.
The original labels remain unchanged. No separate Copy/Paste buttons are shown.
U undoes copied regions. Legacy Ctrl+V remains available.

Numeric maximum horizontal/vertical gap controls and checks have been removed.
Legacy conditions.json files are preserved but are no longer used for judgment.

Press M and drag to create a fixed red rectangular boundary on the current frame.
Drawing another replaces the old boundary; U undoes it and D can delete it.
The boundary is saved with that frame and shown in test/result video. On frames
with M boundaries, zero or one detected pin center outside ALL red boxes passes
the boundary condition, while two or more outside fail.
Centers exactly on the edge count as inside. Individual pin-area/missing-pin
checks still apply separately, so passing the boundary alone is not overall PASS.
C selection also includes partially overlapping or edge-touching M boundaries.
Pin points and pin areas still require full enclosure. Boundaries can be copied with
pin points/areas or alone using the highlighted center, with the same axis-locked
offset as the rest of the selection. All copied boundaries are saved and tested.

Test/result overlays show purple pin areas, cyan detected pin-point boxes and
centers (numbered by the matched area), and red maximum boundaries. Whole-frame
extra-pin detections are only candidates: they must be detected again in local
views at the connector training scale before being counted. This avoids counting
large printed marks as out-of-boundary pins simply because the overview used a
different image scale. Missing or double-detected pins still fail independently
of the maximum boundary rule.
Before counting physical pins, overlapping hypotheses with IoU >= 0.25 and
center offsets <= 0.6 of the smaller box dimensions are deduplicated, keeping
the higher-confidence hypothesis. Distinct/nonoverlapping pins are not merged
just because they share an area. Pipeline JSON reports the suppressed count;
validate this detector postprocessing against real multiple-pin defects too.

Result export now keeps only the requested result MP4, encoded as H.264 without
resizing or changing FPS. Separate _playback files and playback log entries are
no longer generated. Encoding uses a temporary directory that is removed after
success/failure; on encoder failure the original result is retained. Existing
_playback files from previous runs are left untouched.

Build tools: run setup-sdk.ps1 -AcceptLicenses, then build.ps1. The APK is signed
with a local development key, not a production distribution key. Actual camera
capabilities and sustained capture performance must be verified on the phone.
