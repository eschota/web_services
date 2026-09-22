# Native Comfy video artifact selection

## Production failure and correction

ComfyUI 0.37's native `LoadVideo` includes a preview of the uploaded driving
video in its history outputs. That preview is a file with `type: input`.
`SaveVideo` reports the actual generated video as `type: output` in the same
history object. The old resolver removed temporary files but retained input
files; with two MP4 entries, insertion order selected the driving clip.

The first two saved-Avatar jobs therefore completed their real GPU renders,
but AutoRig initially published the 480×270 driving previews under the result
URLs. This was an artifact transport failure, not a failed identity generation.

Source commit `314ac452`, deployed in immutable release
`ai-defaults-20260922-r`, changes artifact selection to:

1. Return explicitly generated `output` files when available.
2. Retain legacy `temp` preview fallback only when no generated output exists.
3. Never return `input` or unknown file categories as generated artifacts.

Both normal queue completion and the managed artifact spool call this shared
resolver. Three targeted tests reproduce the native LoadVideo/SaveVideo
history, an input-only history, and output-versus-temp precedence. All three
passed locally and in the production release before activation.

## Recovery without duplicate rendering

The correct SaveVideo artifacts remained on worker-4090. The two affected
public files were atomically replaced with those exact bytes, and their
persisted task SHA-256 receipts were repaired while the Renderfin process was
stopped. The original incorrect files and original DB payloads were saved in
the release's `.qa` folder. The worker GPU process was not restarted and its
accepted render continued.

| Task | Correct artifact SHA-256 | Delivery |
|---|---|---|
| `9ab7a533-51f1-4491-918b-6f0c5df1b697` | `474a981865a909fb5c69af38b6a565f0dee2c26a54de490ec6fca0a7801e348a` | 960×540, 97 frames, 24 fps |
| `2d96f091-ae43-41a3-a95d-a4a0a0e620a5` | `4cda16e205237f0bedda0bbe850528e74595077d4f37c83ccc041b27c98bb12b` | 960×540, 97 frames, 24 fps |

The corrected first file was downloaded again through the public HTTPS path
and its SHA-256 matched the worker artifact. The real owner browser graph
`30cb8f68083f` was refreshed after the short media-cache expiry; both video
elements reported 960×540, 4.041667 seconds, active playback and looping.
The enlarged Maya video was visibly the generated Maya, with her bob haircut
and mustard jacket, rather than the source actress in a white hood.

All-frame quality reviews belong in the story review report. The incorrect
transport copies must not be counted as failed model samples.
