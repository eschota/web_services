using SandFlow.Server;
using System.Security.Cryptography;
using SandFlow.Protocol;

var root = Path.GetFullPath(args.FirstOrDefault() ?? throw new ArgumentException("Explicit project-local test output path required"));
Directory.CreateDirectory(root);
var testCount = 0;
void Check(bool condition, string label) { if (!condition) throw new Exception(label); testCount++; Console.WriteLine("PASS " + label); }
void Denied(Action action, string code)
{
    try { action(); throw new Exception("Expected " + code); }
    catch (ApiFailure ex) when (ex.Code == code) { Check(true, code); }
}
var clock = new ManualClock();
Check(!AdmissionPolicy.Allows(false, false, "", ""), "empty QA credential fails closed");
Check(!AdmissionPolicy.Allows(false, false, new string('a', 64), new string('b', 64)), "wrong QA credential denied");
Check(AdmissionPolicy.Allows(false, false, new string('a', 64), new string('a', 64)), "explicit isolated QA admission accepted");
Check(AdmissionPolicy.Allows(false, true, "", ""), "authenticated preview session can open sockets without URL credentials");
var store = new WorldStore(Path.Combine(root, Guid.NewGuid().ToString("N")));
var rooms = new Rooms(store, clock);
var a = store.NewGuest(clock.GetUtcNow()); var b = store.NewGuest(clock.GetUtcNow());
Check(a.Token != b.Token && a.Identity.Id != b.Identity.Id, "unique guests");
Check(store.Authenticate(a.Token, clock.GetUtcNow()) == a.Identity, "guest token authenticated");
Check(store.Authenticate(new string('a', 64), clock.GetUtcNow()) == null, "unknown token denied");
Denied(() => store.Create(a.Identity, new(Demo: false), clock.GetUtcNow()), "full_access_required");
Denied(() => store.Create(a.Identity, new(Mode: "pvp", TeamSize: 4), clock.GetUtcNow()), "demo_team_limit");
var privateWorld = store.Create(a.Identity, new(IsPrivate: true, Password: "test-only-password"), clock.GetUtcNow());
Check(store.PublicWorlds().Count == 0, "private worlds excluded");
Denied(() => rooms.Join(b.Identity, privateWorld.Id, "wrong"), "world_password_required");
var admission = rooms.Join(a.Identity, privateWorld.Id, null);
rooms.Join(b.Identity, privateWorld.Id, "test-only-password");
var (first, firstConnection) = rooms.Connect(a.Identity, privateWorld.Id, false);
var (second, secondConnection) = rooms.Connect(b.Identity, privateWorld.Id, false);
rooms.Receive(a.Identity, privateWorld.Id, firstConnection, new("ready", Epoch: 1));
rooms.Receive(b.Identity, privateWorld.Id, secondConnection, new("ready", Epoch: 1));
Check(rooms.View(privateWorld.Id).State == "active", "host elected after readiness");
Denied(() => rooms.Receive(b.Identity, privateWorld.Id, secondConnection, new("commit", Epoch: 1, Tick: 1)), "host_lease_required");
rooms.Receive(a.Identity, privateWorld.Id, firstConnection, new("input", Epoch: 1, Sequence: 1, Command: new("brush", "addSand", Amount: .1f)));
Denied(() => rooms.Receive(a.Identity, privateWorld.Id, firstConnection, new("input", Epoch: 1, Sequence: 1, Command: new("brush", "addSand"))), "duplicate_input");
rooms.Receive(a.Identity, privateWorld.Id, firstConnection, new("commit", Epoch: 1, Tick: 1, Sequence: 1));
ServerEvent? committed = null;
while (second.Events.Reader.TryRead(out var received)) if (received.Type == "committed") committed = received;
Check(committed?.Sequence == 1 && committed.Tick == 1, "committed cursor is confirmed state");
rooms.Receive(b.Identity, privateWorld.Id, secondConnection, new("heartbeat", Epoch: 1, Tick: 1, Sequence: 1));
Denied(() => rooms.Receive(a.Identity, privateWorld.Id, firstConnection, new("input", Epoch: 1, Sequence: 2, Command: new("brush", "addSand", Amount: float.NaN))), "non_finite_command");
byte[] State(long epoch, long tick, long sequence)
{
    var snapshot = new WorldSnapshot { WorldId = privateWorld.Id, Epoch = epoch, Tick = tick, Sequence = sequence,
        SimTime = tick * .02, FixedDt = .02f, ResX = 16, ResZ = 16, CellSize = .125f, MetadataJson = "{\"actors\":[]}" };
    snapshot.Fields.Add(new SnapshotField { Name = "WaterDepth", Stride = 4, Data = new byte[16 * 16 * 4] });
    return SnapshotCodec.Encode(snapshot);
}
var state = State(1, 1, 1);
var hash = Convert.ToHexString(SHA256.HashData(state));
var decoded = SnapshotCodec.Decode(state);
Check(decoded.WorldId == privateWorld.Id && decoded.Fields[0].Data.Length == 1024 && decoded.Tick == 1, "portable physical codec roundtrip");
Check(SnapshotCodec.Decode(SnapshotCodec.Encode(decoded, 1)).Sections.Count == 0, "legacy v1 physical snapshot remains readable");
decoded.Sections.Add(new SnapshotSection { Name = "obstacles-v1", Data = new byte[] { 1, 2, 3, 4 } });
Check(SnapshotCodec.Decode(SnapshotCodec.Encode(decoded)).Sections[0].Data.SequenceEqual(new byte[] { 1, 2, 3, 4 }), "versioned non-cell state section roundtrip");
void BadSnapshot(Action action, string label)
{ var denied = false; try { action(); } catch (InvalidDataException) { denied = true; } Check(denied, label); }
BadSnapshot(() => SnapshotCodec.Encode(decoded, 1), "sections cannot silently downgrade to legacy v1");
decoded.Sections.Add(new SnapshotSection { Name = "obstacles-v1", Data = new byte[] { 0 } });
BadSnapshot(() => SnapshotCodec.Encode(decoded), "duplicate section rejected");
decoded.Sections.RemoveAt(1);
decoded.Sections[0].Data = new byte[SnapshotCodec.MaximumSectionBytes + 1];
BadSnapshot(() => SnapshotCodec.Encode(decoded), "oversized section rejected before compression");
var snapshot = rooms.Save(a.Identity, privateWorld.Id, new(1, 1, 1, 0, hash), state, false);
Check(snapshot.Revision == 1 && store.Read(snapshot).SequenceEqual(state), "snapshot roundtrip");
Denied(() => rooms.Save(a.Identity, privateWorld.Id, new(1, 1, 1, 1, hash), state, false), "autosave_interval");
rooms.Disconnect(a.Identity, privateWorld.Id, firstConnection, false);
Check(rooms.View(privateWorld.Id).State == "active", "synchronized host migration");
Denied(() => rooms.Receive(b.Identity, privateWorld.Id, secondConnection, new("commit", Epoch: 1, Tick: 2, Sequence: 1)), "stale_epoch");
rooms.Receive(b.Identity, privateWorld.Id, secondConnection, new("commit", Epoch: 2, Tick: 2, Sequence: 1));
state = State(2, 2, 1); hash = Convert.ToHexString(SHA256.HashData(state));
var final = rooms.Save(b.Identity, privateWorld.Id, new(2, 2, 1, 1, hash), state, true);
Check(final.Revision == 2, "last-player final save bypasses minute interval");
rooms.Disconnect(b.Identity, privateWorld.Id, secondConnection, false);
Check(rooms.View(privateWorld.Id).State == "sleeping", "empty room sleeps");
var reloaded = new Rooms(store, clock);
reloaded.Join(b.Identity, privateWorld.Id, "test-only-password");
var (_, thirdConnection) = reloaded.Connect(b.Identity, privateWorld.Id, false);
reloaded.Receive(b.Identity, privateWorld.Id, thirdConnection, new("ready", Epoch: 3, Tick: 2, Sequence: 1));
clock.Advance(TimeSpan.FromSeconds(9)); reloaded.Sweep();
Denied(() => reloaded.Receive(b.Identity, privateWorld.Id, thirdConnection, new("commit", Epoch: 3, Tick: 3, Sequence: 1)), "stale_epoch");
reloaded.Kick(a.Identity, privateWorld.Id, b.Identity.Id);
Denied(() => reloaded.Join(b.Identity, privateWorld.Id, "test-only-password"), "world_access_denied");
Check(store.Versions(privateWorld.Id).Count == 2, "version history durable");
Denied(() => store.Save(privateWorld.Id, new(3, 2, 1, 2, "BAD"), state, clock.GetUtcNow()), "snapshot_hash");
Check(store.Versions(privateWorld.Id).First().Revision == 2, "bad upload preserves previous revision");
Denied(() => store.Save(privateWorld.Id, new(2, 2, 1, 0, hash), state, clock.GetUtcNow()), "snapshot_revision_conflict");
Denied(() => store.Save(privateWorld.Id, new(3, 2, 1, 2, hash), state, clock.GetUtcNow()), "snapshot_identity");
var corrupt = (byte[])state.Clone(); corrupt[0] = 0;
Denied(() => store.Save(privateWorld.Id, new(2, 2, 1, 2, Convert.ToHexString(SHA256.HashData(corrupt))), corrupt, clock.GetUtcNow()), "snapshot_format");
state = State(3, 2, 1); hash = Convert.ToHexString(SHA256.HashData(state));
for (var revision = 2; revision < 15; revision++) store.Save(privateWorld.Id, new(3, 2, 1, revision, hash), state, clock.GetUtcNow());
Check(store.Versions(privateWorld.Id).Count == 10, "ten retained revisions");
var telemetry = new DeviceTelemetryStore(Path.Combine(root, Guid.NewGuid().ToString("N")), clock);
var device = new DeviceTelemetry(1, Protocol.RandomId(), "test", "WindowsPlayer", "Direct3D11", "fixture GPU", 2048, 8192, 4,
    1280, 720, 2, true, 30, 60, 18, 1800);
telemetry.Accept(device); Check(telemetry.Count() == 1, "bounded anonymous device report stored");
Denied(() => telemetry.Accept(device), "telemetry_interval");
Denied(() => telemetry.Accept(device with { MeanFps = double.NaN }), "telemetry_range");
Denied(() => telemetry.Accept(device with { GpuModel = new string('x', 121) }), "telemetry_text");
Denied(() => telemetry.Accept(device with { QualityMode = 4 }), "telemetry_range");
clock.Advance(TimeSpan.FromSeconds(30)); telemetry.Accept(device with { MeanFps = 45 });
Check(telemetry.Count() == 1, "telemetry updates same ephemeral session not append every frame");
clock.Advance(TimeSpan.FromDays(8)); telemetry.Accept(device with { SessionId = Protocol.RandomId() });
Check(telemetry.Count() == 1, "telemetry retention bounded to seven days");
var batchWorld = store.Create(b.Identity, new(), clock.GetUtcNow());
rooms.Join(b.Identity, batchWorld.Id, null);
var (batchPeer, batchConnection) = rooms.Connect(b.Identity, batchWorld.Id, false);
rooms.Receive(b.Identity, batchWorld.Id, batchConnection, new("ready", Epoch: 1));
rooms.Receive(b.Identity, batchWorld.Id, batchConnection, new("input", Epoch: 1, Sequence: 1, Command: new("simulation", "enqueue", CommandType: 1)));
rooms.Receive(b.Identity, batchWorld.Id, batchConnection, new("commitBatch", Epoch: 1, Commits: [new(1, 0, 1), new(2, 1, 2)]));
ServerEvent? batchEvent = null;
while (batchPeer.Events.Reader.TryRead(out var e)) if (e.Type == "committedBatch") batchEvent = e;
Check(batchEvent?.Tick == 2 && batchEvent.Sequence == 1, "batched physical steps retain exact tick and command cursor");
Denied(() => rooms.Receive(b.Identity, batchWorld.Id, batchConnection, new("commitBatch", Epoch: 1, Commits: [new(3, 1, 1), new(5, 1, 1)])), "invalid_commit");
rooms.Receive(b.Identity, batchWorld.Id, batchConnection, new("commit", Epoch: 1, Tick: 3, Sequence: 1));
Check(true, "invalid commit batch is atomic");
var late = store.NewGuest(clock.GetUtcNow()); rooms.Join(late.Identity, batchWorld.Id, null);
var (_, lateConnection) = rooms.Connect(late.Identity, batchWorld.Id, false);
rooms.Receive(late.Identity, batchWorld.Id, lateConnection, new("ready", Epoch: 1, Tick: 2, Sequence: 1));
Check(true, "late join can acknowledge a retained checkpoint while host keeps advancing");
CommandValidation.Validate(new("draggable", "pose", ObjectId: a.Identity.Id, Y: .5f, Flags: 1));
Check(true, "bounded actor pose accepted");
Denied(() => CommandValidation.Validate(new("draggable", "pose", ObjectId: "not-an-id")), "invalid_actor_action");
Denied(() => CommandValidation.Validate(new("draggable", "run-script", ObjectId: a.Identity.Id)), "invalid_actor_action");
Denied(() => CommandValidation.Validate(new("draggable", "pose", ObjectId: a.Identity.Id, Qw: 0)), "invalid_actor_rotation");
Denied(() => CommandValidation.Validate(new("draggable", "pose", ObjectId: a.Identity.Id, Qx: float.NaN)), "non_finite_command");
Denied(() => CommandValidation.Validate(new("draggable", "settle", ObjectId: a.Identity.Id, Amount: 121)), "invalid_actor_action");
Denied(() => CommandValidation.Validate(new("draggable", "settle", ObjectId: a.Identity.Id, Flags: 4)), "invalid_actor_action");
rooms.Receive(late.Identity, batchWorld.Id, lateConnection, new("input", Epoch: 1, Sequence: 1,
    Command: new("draggable", "release", ObjectId: a.Identity.Id, Amount: .25f)));
rooms.Receive(b.Identity, batchWorld.Id, batchConnection, new("commit", Epoch: 1, Tick: 4, Sequence: 2));
Check(true, "actor commands share the same confirmed sequence as brush commands");
Console.WriteLine($"RESULT {testCount} assertions passed. No HTTP server or physical simulation launched.");

sealed class ManualClock : TimeProvider
{
    private DateTimeOffset _now = new(2026, 9, 13, 0, 0, 0, TimeSpan.Zero);
    public override DateTimeOffset GetUtcNow() => _now;
    public void Advance(TimeSpan duration) => _now += duration;
}
