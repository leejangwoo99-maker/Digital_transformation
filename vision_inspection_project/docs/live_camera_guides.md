# Live camera positioning guides

Live Test has a Guide frame selector on the camera side. Choices are original
frame numbers shared by saved OCR and Label annotations in the selected group,
not resampled Test FPS numbers. The first shared frame is selected by default;
Off hides the overlays. Missing shared annotations or unreadable reference
images are shown as guide status, without blocking the inspection pipeline.

OCR Text/Mark outlines are cyan; Label Text/QR outlines are yellow. Box labels
are omitted to avoid overlapping text on densely annotated products. Each
reference image is fitted uniformly into the camera image rectangle using its
original width and height, then transformed into the camera canvas's actual
letterboxed image bounds. Rotation/clipping of saved regions is preserved.
Different aspect ratios are centered, never stretched. Camera zoom/rotation are
not changed automatically; this is a physical positioning aid, not registration.

Guides are read-only Tk canvas items. They never modify the preview source,
camera capture, annotation coordinates, inference, result images or saved video.
They disappear once Start is accepted, remain hidden throughout automatic
repeats, and return when the window becomes idle after completion or failure.
The selected guide frame is retained within that Live window. Recording by
Phone and the separate inspection-result view are unchanged.

## Camera zoom for positioning

Idle Live camera has a Zoom slider and numeric input. These send real phone
camera zoom commands, not a digital enlargement of the panel or of the guides.
The range comes from the connected phone's zoom_min/zoom_max. Commands are
submitted on release/Enter/focus-out, not on every slider motion. Superseded or
queued commands are discarded once inspection starts; a lock ensures an
in-flight positioning command finishes before capture restores its own settings.

Start hides the zoom controls and guide boxes. Positioning zoom never changes
record_condition on disk or its in-memory initial/interval values. Capture still
restores and replays the saved initial zoom and timed zoom conditions. At idle,
zoom controls and the selected guide return. The next positioning adjustment
again affects only the preview camera, not stored inspection conditions.
