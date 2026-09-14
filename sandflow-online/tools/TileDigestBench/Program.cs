using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using SandFlow.Protocol;

const int Warmups = 3;
const int Repeats = 5;
var edge = Fixture(97, 65, "partial-edge-and-quantization");
var representative = Fixture(400, 256, "representative-400x256");
var edgeReport = RunCase(edge, 24);
var representativeReport = RunCase(representative, 3);
var threadIds = ConcurrentEquality(representative);
var report = new ComparativeReport("tile-digest-exact-baseline-vs-thread-local-candidate", Repeats,
    true, threadIds, edgeReport, representativeReport);
var json = JsonSerializer.Serialize(report, new JsonSerializerOptions { WriteIndented = true });
Console.WriteLine(json);
var projectRoot = FindProjectRoot();
for (var argument = 0; argument < args.Length; argument += 2)
{
    if (argument + 1 >= args.Length) throw new ArgumentException("Every option requires a path.");
    var path = ProjectLocal(projectRoot, args[argument + 1]);
    if (args[argument] == "--output")
    { Directory.CreateDirectory(Path.GetDirectoryName(path)!); File.WriteAllText(path, json + Environment.NewLine); }
    else if (args[argument] == "--fixtures") ExportFixtures(path, edge, representative);
    else throw new ArgumentException("Unknown option: " + args[argument]);
}

static CaseReport RunCase(WorldSnapshot snapshot, int iterations)
{
    var baselineBytes = TileStateProtocol.EncodeDigest(Baseline.CreateDigest(snapshot));
    var candidateBytes = TileStateProtocol.EncodeDigest(TileStateProtocol.CreateDigest(snapshot));
    if (!baselineBytes.AsSpan().SequenceEqual(candidateBytes)) throw new InvalidOperationException("Digest mismatch.");
    for (var i = 0; i < Warmups; i++) { Baseline.CreateDigest(snapshot); TileStateProtocol.CreateDigest(snapshot); }
    var baseline = new Measurement[Repeats]; var candidate = new Measurement[Repeats];
    for (var repeat = 0; repeat < Repeats; repeat++)
    {
        if ((repeat & 1) == 0)
        { baseline[repeat] = Measure(() => Baseline.CreateDigest(snapshot), iterations); candidate[repeat] = Measure(() => TileStateProtocol.CreateDigest(snapshot), iterations); }
        else
        { candidate[repeat] = Measure(() => TileStateProtocol.CreateDigest(snapshot), iterations); baseline[repeat] = Measure(() => Baseline.CreateDigest(snapshot), iterations); }
    }
    return new CaseReport(snapshot.MetadataJson, snapshot.ResX, snapshot.ResZ, snapshot.Fields.Count,
        TileStateProtocol.TileCount(snapshot), iterations, Convert.ToHexString(SHA256.HashData(baselineBytes)),
        true, baseline, candidate);
}

static Measurement Measure(Action action, int iterations)
{
    GC.Collect(); GC.WaitForPendingFinalizers(); GC.Collect();
    var allocated = GC.GetAllocatedBytesForCurrentThread(); var watch = Stopwatch.StartNew();
    for (var i = 0; i < iterations; i++) action();
    watch.Stop();
    return new Measurement(watch.Elapsed.TotalMilliseconds / iterations,
        (GC.GetAllocatedBytesForCurrentThread() - allocated) / iterations);
}

static int[] ConcurrentEquality(WorldSnapshot snapshot)
{
    var expected = TileStateProtocol.EncodeDigest(Baseline.CreateDigest(snapshot));
    var failures = new System.Collections.Concurrent.ConcurrentQueue<string>();
    var threads = new System.Collections.Concurrent.ConcurrentDictionary<int, byte>();
    Parallel.For(0, Math.Max(8, Environment.ProcessorCount * 2), worker =>
    {
        threads.TryAdd(Environment.CurrentManagedThreadId, 0);
        for (var repeat = 0; repeat < 4; repeat++)
        {
            var actual = TileStateProtocol.EncodeDigest(TileStateProtocol.CreateDigest(snapshot));
            if (!actual.AsSpan().SequenceEqual(expected)) failures.Enqueue(worker + ":" + repeat);
        }
    });
    if (!failures.IsEmpty) throw new InvalidOperationException("Concurrent digest mismatch: " + string.Join(",", failures));
    return threads.Keys.OrderBy(x => x).ToArray();
}

static string FindProjectRoot()
{
    var directory = new DirectoryInfo(AppContext.BaseDirectory);
    while (directory != null)
    {
        if (File.Exists(Path.Combine(directory.FullName, "contracts", "SandFlow.Protocol", "SandFlow.Protocol.csproj")))
            return directory.FullName;
        directory = directory.Parent;
    }
    throw new DirectoryNotFoundException("SandFlow Online project root was not found.");
}

static string ProjectLocal(string root, string requested)
{
    var path = Path.GetFullPath(requested);
    var prefix = root.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar) + Path.DirectorySeparatorChar;
    if (!path.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
        throw new InvalidOperationException("Output must remain inside the SandFlow Online project.");
    return path;
}

static void ExportFixtures(string directory, params WorldSnapshot[] fixtures)
{
    Directory.CreateDirectory(directory);
    var entries = new List<FixtureEntry>();
    foreach (var fixture in fixtures)
    {
        var name = fixture.ResX == 97 ? "partial-edge-97x65" : "representative-400x256";
        var snapshotName = name + ".sfs"; var digestName = name + ".baseline.digest";
        var snapshotBytes = SnapshotCodec.Encode(fixture);
        var digestBytes = TileStateProtocol.EncodeDigest(Baseline.CreateDigest(fixture));
        File.WriteAllBytes(Path.Combine(directory, snapshotName), snapshotBytes);
        File.WriteAllBytes(Path.Combine(directory, digestName), digestBytes);
        entries.Add(new FixtureEntry(name, snapshotName, snapshotBytes.Length,
            Convert.ToHexString(SHA256.HashData(snapshotBytes)), digestName, digestBytes.Length,
            Convert.ToHexString(SHA256.HashData(digestBytes)), fixture.ResX, fixture.ResZ,
            fixture.Fields.Count, TileStateProtocol.TileCount(fixture)));
    }
    var manifest = new FixtureManifest(1, "Exact pre-candidate baseline digest bytes for Unity/IL2CPP equivalence; not performance evidence.", entries.ToArray());
    File.WriteAllText(Path.Combine(directory, "manifest.json"),
        JsonSerializer.Serialize(manifest, new JsonSerializerOptions { WriteIndented = true }) + Environment.NewLine);
}

static WorldSnapshot Fixture(int width, int height, string name)
{
    var snapshot = new WorldSnapshot
    {
        WorldId = "0123456789abcdef0123456789abcdef", Epoch = 7, Tick = 480, Sequence = 91,
        SimTime = 2.0, FixedDt = 1f / 240f, ResX = width, ResZ = height, CellSize = .045f,
        MetadataJson = "{\"scope\":\"" + name + "\"}"
    };
    var floats = new byte[width * height * 4];
    var vectors = new byte[width * height * 8];
    var vectors4 = new byte[width * height * 16];
    var uints = new byte[width * height * 4];
    float[] extremes = { 0f, .000049f, .00005f, -.00005f, 127.9999f, 128f, 128.0001f,
        -127.9999f, -128f, -128.0001f, float.MaxValue, float.MinValue, 1e-30f, -1e-30f };
    for (var i = 0; i < width * height; i++)
    {
        Write(floats, i * 4, extremes[i % extremes.Length]);
        Write(vectors, i * 8, extremes[(i * 3) % extremes.Length]);
        Write(vectors, i * 8 + 4, extremes[(i * 5 + 1) % extremes.Length]);
        for (var component = 0; component < 4; component++)
            Write(vectors4, i * 16 + component * 4, extremes[(i * 7 + component * 3) % extremes.Length]);
        Buffer.BlockCopy(BitConverter.GetBytes(unchecked((uint)(i * 2654435761u))), 0, uints, i * 4, 4);
    }
    snapshot.Fields.Add(new SnapshotField { Name = "FloatScalar", Stride = 4, Unsigned = false, Data = floats });
    snapshot.Fields.Add(new SnapshotField { Name = "FloatVector2", Stride = 8, Unsigned = false, Data = vectors });
    snapshot.Fields.Add(new SnapshotField { Name = "FloatVector4MaxStride", Stride = 16, Unsigned = false, Data = vectors4 });
    snapshot.Fields.Add(new SnapshotField { Name = "Unsigned", Stride = 4, Unsigned = true, Data = uints });
    snapshot.Sections.Add(new SnapshotSection { Name = "fixture", Data = Encoding.ASCII.GetBytes(name) });
    SnapshotCodec.Validate(snapshot);
    return snapshot;
}

static void Write(byte[] target, int offset, float value) => Buffer.BlockCopy(BitConverter.GetBytes(value), 0, target, offset, 4);

internal sealed record Measurement(double MeanMilliseconds, long AllocatedBytesPerDigest);
internal sealed record CaseReport(string Name, int ResX, int ResZ, int Fields, int TilesPerField,
    int IterationsPerRepeat, string EncodedDigestSha256, bool DigestBytesEquivalent,
    Measurement[] Baseline, Measurement[] Candidate);
internal sealed record ComparativeReport(string Kind, int InterleavedRepeats,
    bool ConcurrentDigestEquality, int[] ConcurrentManagedThreadIds,
    CaseReport PartialEdge, CaseReport Representative);
internal sealed record FixtureEntry(string Name, string SnapshotPath, int SnapshotBytes, string SnapshotSha256,
    string BaselineDigestPath, int BaselineDigestBytes, string BaselineDigestSha256,
    int ResX, int ResZ, int Fields, int TilesPerField);
internal sealed record FixtureManifest(int SchemaVersion, string Scope, FixtureEntry[] Fixtures);

internal static class Baseline
{
    private static readonly UTF8Encoding Utf8 = new(false, true);
    public static TileDigest CreateDigest(WorldSnapshot snapshot)
    {
        SnapshotCodec.Validate(snapshot);
        var digest = new TileDigest { Cursor = Cursor(snapshot), ModuleHash = ModuleHash(snapshot) };
        var count = TileStateProtocol.TileCount(snapshot);
        foreach (var field in snapshot.Fields)
        {
            var item = new TileDigestField { Name = field.Name, Stride = field.Stride, Unsigned = field.Unsigned, Hashes = new ulong[count] };
            for (var tile = 0; tile < count; tile++) item.Hashes[tile] = HashTile(snapshot, field, tile);
            digest.Fields.Add(item);
        }
        return digest;
    }
    private static ulong HashTile(WorldSnapshot snapshot, SnapshotField field, int tile)
    {
        var raw = ReadTile(snapshot, field, tile);
        using var stream = new MemoryStream(raw.Length * 3);
        using var writer = new BinaryWriter(stream, Utf8, true);
        for (var i = 0; i < raw.Length; i += 4)
        {
            var bits = BitConverter.ToUInt32(raw, i);
            if (field.Unsigned) writer.Write(bits);
            else
            {
                var value = BitConverter.ToSingle(raw, i);
                if (Math.Abs(value) < 128) { writer.Write((byte)0); writer.Write((long)Math.Round(value / TileStateProtocol.AbsolutePrecision, MidpointRounding.ToEven)); }
                else { writer.Write((byte)1); writer.Write(bits & ~7u); }
            }
        }
        writer.Flush();
        return BitConverter.ToUInt64(Hash(stream.ToArray()), 0);
    }
    private static byte[] ReadTile(WorldSnapshot snapshot, SnapshotField field, int tile)
    {
        Bounds(snapshot, tile, out var x, out var z, out var width, out var height);
        var bytes = new byte[width * height * field.Stride];
        for (var row = 0; row < height; row++)
            Buffer.BlockCopy(field.Data, ((z + row) * snapshot.ResX + x) * field.Stride, bytes, row * width * field.Stride, width * field.Stride);
        return bytes;
    }
    private static void Bounds(WorldSnapshot snapshot, int index, out int x, out int z, out int width, out int height)
    {
        var columns = (snapshot.ResX + TileStateProtocol.TileSize - 1) / TileStateProtocol.TileSize;
        x = index % columns * TileStateProtocol.TileSize; z = index / columns * TileStateProtocol.TileSize;
        width = Math.Min(TileStateProtocol.TileSize, snapshot.ResX - x); height = Math.Min(TileStateProtocol.TileSize, snapshot.ResZ - z);
    }
    private static byte[] ModuleHash(WorldSnapshot snapshot)
    {
        using var stream = new MemoryStream(); using var writer = new BinaryWriter(stream, Utf8, true);
        WriteText(writer, snapshot.MetadataJson);
        foreach (var section in snapshot.Sections.OrderBy(x => x.Name, StringComparer.Ordinal))
        { WriteText(writer, section.Name); writer.Write(section.Data.Length); writer.Write(section.Data); }
        writer.Flush(); return Hash(stream.ToArray());
    }
    private static byte[] Hash(byte[] bytes) { using var sha = SHA256.Create(); return sha.ComputeHash(bytes); }
    private static void WriteText(BinaryWriter writer, string text)
    { var bytes = Utf8.GetBytes(text); writer.Write(bytes.Length); writer.Write(bytes); }
    private static WorldSnapshot Cursor(WorldSnapshot s) => new()
    { WorldId=s.WorldId,Epoch=s.Epoch,Tick=s.Tick,Sequence=s.Sequence,SimTime=s.SimTime,FixedDt=s.FixedDt,
      ResX=s.ResX,ResZ=s.ResZ,CellSize=s.CellSize,OriginX=s.OriginX,OriginY=s.OriginY,OriginZ=s.OriginZ };
}
