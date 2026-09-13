using SandFlow.Server;
using System.Security.Cryptography;

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
var state = Enumerable.Range(0, 128).Select(x => (byte)x).ToArray();
var hash = Convert.ToHexString(SHA256.HashData(state));
var snapshot = rooms.Save(a.Identity, privateWorld.Id, new(1, 1, 1, 0, hash), state, false);
Check(snapshot.Revision == 1 && store.Read(snapshot).SequenceEqual(state), "snapshot roundtrip");
Denied(() => rooms.Save(a.Identity, privateWorld.Id, new(1, 1, 1, 1, hash), state, false), "autosave_interval");
rooms.Disconnect(a.Identity, privateWorld.Id, firstConnection, false);
Check(rooms.View(privateWorld.Id).State == "active", "synchronized host migration");
Denied(() => rooms.Receive(b.Identity, privateWorld.Id, secondConnection, new("commit", Epoch: 1, Tick: 2, Sequence: 1)), "stale_epoch");
rooms.Receive(b.Identity, privateWorld.Id, secondConnection, new("commit", Epoch: 2, Tick: 2, Sequence: 1));
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
Denied(() => store.Save(privateWorld.Id, new(3, 2, 1, 0, hash), state, clock.GetUtcNow()), "snapshot_revision_conflict");
for (var revision = 2; revision < 15; revision++) store.Save(privateWorld.Id, new(3, 2, 1, revision, hash), state, clock.GetUtcNow());
Check(store.Versions(privateWorld.Id).Count == 10, "ten retained revisions");
Console.WriteLine($"RESULT {testCount} assertions passed. No HTTP server or physical simulation launched.");

sealed class ManualClock : TimeProvider
{
    private DateTimeOffset _now = new(2026, 9, 13, 0, 0, 0, TimeSpan.Zero);
    public override DateTimeOffset GetUtcNow() => _now;
    public void Advance(TimeSpan duration) => _now += duration;
}
