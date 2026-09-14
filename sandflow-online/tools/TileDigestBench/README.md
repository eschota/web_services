# Tile digest allocation fixture

This is a bounded .NET CPU/allocation microbenchmark, **not** a WebGPU/FPS benchmark or game-dev sealed run. The preserved baseline uses the original per-tile `SHA256.Create().ComputeHash()` helper, temporary tile bytes and BinaryWriter/MemoryStream encoding. Do not replace its implementation with a newer convenience hash API when comparing cost.

Two deterministic four-field fixtures include scalar/vector/max-stride-16 floats, exact uints, quantization boundaries/extrema, partial right/bottom/corner tiles, and a representative 400x256 grid. Baseline and candidate encoded digests must be identical. Five repetitions alternate execution order. Parallel equality checks report actual managed thread IDs rather than treating scheduled jobs as independent threads.

Run with .NET SDK and project-local caches:

```powershell
$env:DOTNET_CLI_HOME='R:\autorig\sandflow-online\.work\dotnet-home'
$env:NUGET_PACKAGES='R:\autorig\sandflow-online\.work\nuget'
$env:TEMP='R:\autorig\sandflow-online\.work\temp'
$env:TMP=$env:TEMP
dotnet run --project tools/TileDigestBench -c Release -- --output tools/TileDigestBench/.work/result.json --fixtures tools/TileDigestBench/.work/fixtures
```

Output paths must stay inside the SandFlow server project. Exported `.sfs` and `.baseline.digest` files allow the Game's `TileDigestCompatibilitySmoke.Run` to check the same golden bytes in Unity. Raw fixtures/build/cache files are non-shipping and stay ignored.

The candidate retains one roughly 37 KiB scratch/digest workspace plus a SHA instance **per participating thread**, allocated on first use. Steady-state allocated bytes exclude this bounded warmup cost. The original digest format and quantization are unchanged. Raw-state codec SHA and encoded-upload SHA are unrelated contracts and were not modified.

2026-09-14 corrected run: partial-edge steady-state allocation 1,181,963 → 2,683 bytes/digest; representative 400x256 allocation 19,006,266 → 5,685 bytes/digest. Timing variance and all raw repeats are preserved in the linked Game QA evidence. Do not extrapolate allocation counts or CPU timing into an unmeasured browser frame-rate improvement.
