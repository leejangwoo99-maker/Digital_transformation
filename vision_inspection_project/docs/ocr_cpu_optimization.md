# OCR CPU and Reference Tracking

## Reference correction

The first usable frame extracts SIFT features. Later frames track the feature
positions using pyramidal Lucas-Kanade optical flow. Forward/backward error,
tracking coverage, descriptor matches and RANSAC similarity support must pass
before accepting a pose. A failed track/match falls back to full SIFT, never
blindly reuses an old pose. Translation, rotation and scale remain supported.

All special inspectors in one run share the registration engine. Reference
descriptors are cached by image path, modification time, area and angle, with
a bounded 32-reference process cache. Reference shape changes invalidate them.
Set `reference_tracking: false` in internal settings for a full-search baseline.

## OpenVINO OCR

Existing CTC weights convert directly to OpenVINO IR, using FP32 weights and
FP32 CPU inference. No INT8 quantization, image compression, alphabet, crop
preprocessing, numeric masking or decoder changes are made. Conversion checks
PyTorch/OpenVINO logits and argmax for several batch sizes and input widths.

At runtime the batch dimension is dynamic; crop width is specialized and
compiled execution plans are cached (16 in memory plus CPU disk cache).
CPU inference uses four threads and one stream. The first use can include
compilation overhead; compare cold startup and warm inference separately.

Validated artifacts load automatically. Missing/stale artifacts or missing
OpenVINO use PyTorch; failures are reported in `ocr_backend_errors`.
When a verified IR includes its character table, only the CTC converter is
loaded: a duplicate PyTorch recognizer is not constructed for that reader.
Set `ocr_backend: pytorch` in internal settings to force the baseline.
Updated trained checkpoints invalidate old artifacts. Future OCR/Label
training attempts FP32 conversion after saving each checkpoint; conversion
failure retains the working PyTorch checkpoint.

To prepare an existing group without retraining:

```powershell
python tools/prepare_openvino_ocr.py weights/Proto_23fps_OCR_text_image.ocr.json weights/Proto_23fps_label_text_image.ocr.json
```

This also prepares the existing numeric fallback reader. Original `.pt` and
`.pth` files are not overwritten. Group IR files live in `weights/openvino_ocr`.
The pretrained numeric IR lives beside its EasyOCR model cache.

## Measurement

Pipeline TXT/JSON now include `reference_tracking`, `ocr_backends`,
`ocr_execution_seconds` (preprocessing, reader loading, compile lookup and
actual inference), and `ocr_backend_errors`. Stage timings still include registration inside each
inspection kind; do not add nested timings twice.

`tools/benchmark_ocr_reference.py` compares actual video crops and registration
poses without touching the phone or SwitchBot. Its timings are component
benchmarks, not a prediction of end-to-end Live latency. Current benchmark
sampling is explicitly 23 -> 10 FPS and test frames 5-13.

API references:
- https://docs.openvino.ai/2026/api/ie_python_api/_autosummary/openvino.convert_model.html
- https://docs.openvino.ai/2026/openvino-workflow/running-inference/optimize-inference/precision-control.html
