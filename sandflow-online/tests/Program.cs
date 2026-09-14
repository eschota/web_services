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
rooms.Receive(a.Identity, privateWorld.Id, firstConnection, new("ready", TuningDefaultsHash:CanonicalTuningDefaults.Fingerprint, Epoch: 1));
rooms.Receive(b.Identity, privateWorld.Id, secondConnection, new("ready", TuningDefaultsHash:CanonicalTuningDefaults.Fingerprint, Epoch: 1));
Check(rooms.View(privateWorld.Id).State == "active", "host elected after readiness");
Denied(() => rooms.Receive(b.Identity, privateWorld.Id, secondConnection, new("commit", Epoch: 1, Tick: 1)), "host_lease_required");
rooms.Receive(a.Identity, privateWorld.Id, firstConnection, new("input", Epoch: 1, Sequence: 1, Command: new("simulation", "enqueue", Amount: .1f)));
Denied(() => rooms.Receive(a.Identity, privateWorld.Id, firstConnection, new("input", Epoch: 1, Sequence: 1, Command: new("simulation", "enqueue"))), "duplicate_input");
rooms.Receive(a.Identity, privateWorld.Id, firstConnection, new("commit", Epoch: 1, Tick: 1, Sequence: 1));
ServerEvent? committed = null;
while (second.Events.Reader.TryRead(out var received)) if (received.Type == "committed") committed = received;
Check(committed?.Sequence == 1 && committed.Tick == 1, "committed cursor is confirmed state");
rooms.Receive(b.Identity, privateWorld.Id, secondConnection, new("heartbeat", Epoch: 1, Tick: 1, Sequence: 1));
Denied(() => rooms.Receive(a.Identity, privateWorld.Id, firstConnection, new("input", Epoch: 1, Sequence: 2, Command: new("simulation", "enqueue", Amount: float.NaN))), "non_finite_command");
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
rooms.Receive(b.Identity,privateWorld.Id,secondConnection,new("resync",Epoch:1));
Check(!second.Ready,"full resync removes follower from authority eligibility");
rooms.Receive(b.Identity,privateWorld.Id,secondConnection,new("ready", TuningDefaultsHash:CanonicalTuningDefaults.Fingerprint,Epoch:1,Tick:1,Sequence:1));
Check(second.Ready,"current saved checkpoint can restore follower readiness");
Denied(() => rooms.Save(a.Identity, privateWorld.Id, new(1, 1, 1, 1, hash), state, false), "autosave_interval");
rooms.Disconnect(a.Identity, privateWorld.Id, firstConnection, false);
Check(rooms.View(privateWorld.Id).State == "active", "synchronized host migration");
Denied(() => rooms.Receive(b.Identity, privateWorld.Id, secondConnection, new("commit", Epoch: 1, Tick: 2, Sequence: 1)), "stale_epoch");
rooms.Receive(b.Identity, privateWorld.Id, secondConnection, new("commit", Epoch: 2, Tick: 2, Sequence: 1));
state = State(2, 2, 1); hash = Convert.ToHexString(SHA256.HashData(state));
Denied(()=>rooms.Save(b.Identity,privateWorld.Id,new(2,2,1,1,hash),state,true),"departure_required");
var finalOperation=Protocol.RandomId();
rooms.Receive(b.Identity,privateWorld.Id,secondConnection,new("depart_begin",Epoch:2,Tick:2,Sequence:1,OperationId:finalOperation));
var final = rooms.CompleteDeparture(b.Identity,privateWorld.Id,finalOperation,new(2,2,1,1,hash),state);
Check(final.Snapshot.Revision == 2, "fenced last-player departure saves before leaving and bypasses minute interval");
rooms.Disconnect(b.Identity, privateWorld.Id, secondConnection, false);
Check(rooms.View(privateWorld.Id).State == "sleeping", "empty room sleeps");
var reloaded = new Rooms(store, clock);
reloaded.Join(b.Identity, privateWorld.Id, "test-only-password");
var (_, thirdConnection) = reloaded.Connect(b.Identity, privateWorld.Id, false);
reloaded.Receive(b.Identity, privateWorld.Id, thirdConnection, new("ready", TuningDefaultsHash:CanonicalTuningDefaults.Fingerprint, Epoch: 3, Tick: 2, Sequence: 1));
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
rooms.Receive(b.Identity, batchWorld.Id, batchConnection, new("ready", TuningDefaultsHash:CanonicalTuningDefaults.Fingerprint, Epoch: 1));
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
rooms.Receive(late.Identity, batchWorld.Id, lateConnection, new("ready", TuningDefaultsHash:CanonicalTuningDefaults.Fingerprint, Epoch: 1, Tick: 2, Sequence: 1));
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
var tileBase = new WorldSnapshot { WorldId = privateWorld.Id, Epoch = 4, Tick = 480, Sequence = 3,
    ResX = 35, ResZ = 34, CellSize = .125f, FixedDt = 1f/240f, SimTime = 2 };
var tileCells = tileBase.ResX * tileBase.ResZ;
var initialWater = new byte[tileCells * 4];
for (var i=0;i<tileCells;i++) Buffer.BlockCopy(BitConverter.GetBytes(1.25f),0,initialWater,i*4,4);
tileBase.Fields.Add(new SnapshotField { Name="WaterDepth", Data=initialWater });
tileBase.Fields.Add(new SnapshotField { Name="WaterMomentum", Stride=8, Data=new byte[tileCells*8] });
tileBase.Fields.Add(new SnapshotField { Name="ObstacleMask", Unsigned=true, Data=new byte[tileCells*4] });
var tileAuthority=SnapshotCodec.Decode(SnapshotCodec.Encode(tileBase));
Buffer.BlockCopy(BitConverter.GetBytes(1.3f),0,tileAuthority.Fields[0].Data,(tileCells-1)*4,4);
Buffer.BlockCopy(BitConverter.GetBytes(0xffffffffu),0,tileAuthority.Fields[2].Data,(tileCells-1)*4,4);
tileAuthority.MetadataJson="{\"state\":2}";
tileAuthority.Sections.Add(new SnapshotSection { Name="actor-state",Data=new byte[]{10,20} });
var tileDigest=TileStateProtocol.DecodeDigest(TileStateProtocol.EncodeDigest(TileStateProtocol.CreateDigest(tileBase)));
Check(tileDigest.Fields.All(x=>x.Hashes.Length==4), "32x32 digests include partial edge tiles");
var tileRepair=TileStateProtocol.CreateRepair(tileAuthority,tileDigest);
Check(tileRepair.Tiles.Count==2&&tileRepair.Tiles.All(x=>x.Index==3&&x.Data.Length==24), "repair contains only two changed partial field tiles");
var repairBytes=TileStateProtocol.EncodeRepair(tileRepair);
var tilePatched=TileStateProtocol.ApplyRepair(tileBase,TileStateProtocol.DecodeRepair(repairBytes));
Check(tilePatched.Fields.Zip(tileAuthority.Fields).All(x=>x.First.Data.SequenceEqual(x.Second.Data)), "historical tile application restores all changed float and uint fields");
Check(tilePatched.MetadataJson==tileAuthority.MetadataJson&&tilePatched.Sections[0].Data.SequenceEqual(new byte[]{10,20}), "repair preserves module and metadata updates");
Check(BitConverter.ToSingle(tileBase.Fields[0].Data,(tileCells-1)*4)==1.25f&&tileBase.Sections.Count==0, "retained checkpoint is immutable after repair");
var unchangedRepair=TileStateProtocol.CreateRepair(tileBase,tileDigest);
Check(unchangedRepair.Tiles.Count==0&&unchangedRepair.MetadataJson==null, "unchanged checkpoint needs no field or module payload");
var near=SnapshotCodec.Decode(SnapshotCodec.Encode(tileBase));
Buffer.BlockCopy(BitConverter.GetBytes(1.250001f),0,near.Fields[0].Data,0,4);
Check(TileStateProtocol.CreateRepair(near,tileDigest).Tiles.Count==0, "subprecision float difference can share a digest bucket");
var wrongTick=SnapshotCodec.Decode(SnapshotCodec.Encode(tileBase));wrongTick.Tick++;
BadSnapshot(()=>TileStateProtocol.ApplyRepair(wrongTick,tileRepair), "historical repair refuses current or wrong step");
var wrongBase=SnapshotCodec.Decode(SnapshotCodec.Encode(tileBase));wrongBase.Fields[0].Data[3]=0;
BadSnapshot(()=>TileStateProtocol.ApplyRepair(wrongBase,tileRepair), "repair is bound to the retained baseline digest");
var badTile=TileStateProtocol.DecodeRepair(repairBytes);badTile.Tiles.Add(badTile.Tiles[0]);
BadSnapshot(()=>TileStateProtocol.ApplyRepair(tileBase,badTile), "duplicate tile application rejected");
badTile=TileStateProtocol.DecodeRepair(repairBytes);badTile.Tiles[0].Data=new byte[4];
BadSnapshot(()=>TileStateProtocol.ApplyRepair(tileBase,badTile), "malformed edge tile length rejected");
badTile=TileStateProtocol.DecodeRepair(repairBytes);Buffer.BlockCopy(BitConverter.GetBytes(float.NaN),0,badTile.Tiles[0].Data,0,4);
BadSnapshot(()=>TileStateProtocol.ApplyRepair(tileBase,badTile), "nonfinite repair field rejected before GPU application");
var badRepairBytes=(byte[])repairBytes.Clone();badRepairBytes[20]^=1;
BadSnapshot(()=>TileStateProtocol.DecodeRepair(badRepairBytes), "corrupted repair checksum rejected");
var ring=new CheckpointRing(2,1024*1024);
ring.Add(tileBase);ring.TryGet(tileBase.WorldId,tileBase.Epoch,tileBase.Tick,out var retained);
retained.Fields[0].Data[0]^=1;
ring.TryGet(tileBase.WorldId,tileBase.Epoch,tileBase.Tick,out retained);
Check(retained.Fields[0].Data.SequenceEqual(tileBase.Fields[0].Data), "checkpoint ring copies cannot mutate retained baseline");
var next=SnapshotCodec.Decode(SnapshotCodec.Encode(tileBase));next.Tick=960;ring.Add(next);
next=SnapshotCodec.Decode(SnapshotCodec.Encode(tileBase));next.Tick=1440;ring.Add(next);
Check(ring.Count==2&&ring.OldestTick==960&&ring.RetainedBytes<=1024*1024, "checkpoint ring evicts by count and accounts memory");
Check(!ring.Add(tileBase)&&ring.OldestTick==960, "late old GPU callback cannot evict useful retained states");
Check(!ring.TryGet(tileBase.WorldId,5,960,out _), "checkpoint ring rejects stale generation reads");
next.Epoch=5;BadSnapshot(()=>ring.Add(next), "checkpoint ring rejects mixed authority generations");
ring.Clear();Check(ring.Count==0&&ring.RetainedBytes==0&&ring.Add(next), "authority transition explicitly clears checkpoint history");
var routeWorld=store.Create(a.Identity,new(),clock.GetUtcNow());
var c=store.NewGuest(clock.GetUtcNow());
foreach(var identity in new[]{a.Identity,b.Identity,c.Identity})rooms.Join(identity,routeWorld.Id,null);
var (routeA,routeAc)=rooms.Connect(a.Identity,routeWorld.Id,false);
var (routeB,routeBc)=rooms.Connect(b.Identity,routeWorld.Id,false);
var (routeC,routeCc)=rooms.Connect(c.Identity,routeWorld.Id,false);
rooms.Receive(a.Identity,routeWorld.Id,routeAc,new("ready", TuningDefaultsHash:CanonicalTuningDefaults.Fingerprint,Epoch:1));
rooms.Receive(b.Identity,routeWorld.Id,routeBc,new("ready", TuningDefaultsHash:CanonicalTuningDefaults.Fingerprint,Epoch:1));
rooms.Receive(c.Identity,routeWorld.Id,routeCc,new("ready", TuningDefaultsHash:CanonicalTuningDefaults.Fingerprint,Epoch:1));
var (_,routeAb)=rooms.Connect(a.Identity,routeWorld.Id,true);
var (_,routeBb)=rooms.Connect(b.Identity,routeWorld.Id,true);
rooms.Connect(c.Identity,routeWorld.Id,true);
rooms.Receive(a.Identity,routeWorld.Id,routeAc,new("commitBatch",Epoch:1,Commits:[new(1,0,1,true)]));
byte[] RoutedFrame(byte kind,string target,long tick=1)
{
    var frame=new byte[49];frame[0]=(byte)'S';frame[1]=(byte)'F';frame[2]=(byte)'O';frame[3]=Protocol.Version;frame[4]=kind;
    BitConverter.GetBytes(1L).CopyTo(frame,8);BitConverter.GetBytes(tick).CopyTo(frame,16);
    BitConverter.GetBytes(1u).CopyTo(frame,24);BitConverter.GetBytes((ushort)1).CopyTo(frame,30);
    Guid.ParseExact(target,"N").ToByteArray().CopyTo(frame,32);frame[48]=10;return frame;
}
var digestFrame=RoutedFrame(2,b.Identity.Id);
rooms.RelayBulk(b.Identity,routeWorld.Id,routeBb,digestFrame);
Check(routeA.Bulk.Reader.TryRead(out var routedDigest)&&routedDigest.SequenceEqual(digestFrame)&&!routeC.Bulk.Reader.TryRead(out _), "follower digest routes only to elected host");
var repairFrame=RoutedFrame(3,b.Identity.Id);
rooms.RelayBulk(a.Identity,routeWorld.Id,routeAb,repairFrame);
Check(routeB.Bulk.Reader.TryRead(out var routedRepair)&&routedRepair.SequenceEqual(repairFrame)&&!routeC.Bulk.Reader.TryRead(out _), "host repair routes only to addressed room participant");
Denied(()=>rooms.RelayBulk(b.Identity,routeWorld.Id,routeBb,RoutedFrame(2,c.Identity.Id)),"digest_sender");
Denied(()=>rooms.RelayBulk(b.Identity,routeWorld.Id,routeBb,RoutedFrame(3,a.Identity.Id)),"host_lease_required");
Denied(()=>rooms.RelayBulk(a.Identity,routeWorld.Id,routeAb,RoutedFrame(3,Protocol.RandomId())),"repair_recipient");
Denied(()=>rooms.RelayBulk(b.Identity,routeWorld.Id,routeBb,RoutedFrame(2,b.Identity.Id,2)),"unscheduled_checkpoint");
var invalidChunks=RoutedFrame(2,b.Identity.Id);BitConverter.GetBytes((ushort)12).CopyTo(invalidChunks,30);
Denied(()=>rooms.RelayBulk(b.Identity,routeWorld.Id,routeBb,invalidChunks),"bulk_chunks");
routeB.BulkWindow=clock.GetUtcNow();routeB.BulkBytes=1024*1024-48;
Denied(()=>rooms.RelayBulk(b.Identity,routeWorld.Id,routeBb,digestFrame),"bulk_rate");
Denied(()=>rooms.Receive(a.Identity,routeWorld.Id,routeAc,new("commitBatch",Epoch:1,Commits:[new(2,0,1,true)])),"checkpoint_rate");
rooms.Receive(b.Identity,routeWorld.Id,routeBc,new("repairing",Epoch:1));
rooms.Receive(c.Identity,routeWorld.Id,routeCc,new("heartbeat",Epoch:1,Tick:1));
rooms.Disconnect(a.Identity,routeWorld.Id,routeAc,false);
ServerEvent? newHost=null;while(routeC.Events.Reader.TryRead(out var routeEvent))if(routeEvent.Type=="host")newHost=routeEvent;
Check(newHost?.Epoch==2&&System.Text.Json.JsonSerializer.Serialize(newHost.Data).Contains(c.Identity.Id), "rewinding participant is excluded from host election");
var savedWorld=store.Create(b.Identity,new(),clock.GetUtcNow());
rooms.Join(b.Identity,savedWorld.Id,null);rooms.Join(c.Identity,savedWorld.Id,null);
var (_,savedHost)=rooms.Connect(b.Identity,savedWorld.Id,false);
var (savedFollower,savedClient)=rooms.Connect(c.Identity,savedWorld.Id,false);
rooms.Receive(b.Identity,savedWorld.Id,savedHost,new("ready", TuningDefaultsHash:CanonicalTuningDefaults.Fingerprint,Epoch:1));
rooms.Receive(b.Identity,savedWorld.Id,savedHost,new("commit",Epoch:1,Tick:1));
var savedState=new WorldSnapshot { WorldId=savedWorld.Id,Epoch=1,Tick=1,FixedDt=.02f,SimTime=.02,ResX=16,ResZ=16,CellSize=.125f };
savedState.Fields.Add(new SnapshotField { Name="WaterDepth",Data=new byte[1024] });
var savedBytes=SnapshotCodec.Encode(savedState);
rooms.Save(b.Identity,savedWorld.Id,new(1,1,0,0,Convert.ToHexString(SHA256.HashData(savedBytes))),savedBytes,false);
rooms.Receive(b.Identity,savedWorld.Id,savedHost,new("commit",Epoch:1,Tick:2));
rooms.Receive(c.Identity,savedWorld.Id,savedClient,new("ready", TuningDefaultsHash:CanonicalTuningDefaults.Fingerprint,Epoch:1,Tick:1));
Check(savedFollower.Ready&&savedFollower.AcknowledgedTick==1,"late ready accepts saved baseline after host advanced and its commit was trimmed");
var qaClock=new ManualClock();var qaAccess=new QaBrowserAccess(qaClock);
var challenge=qaAccess.Begin(null);
Check(Protocol.ValidId(challenge.Code)&&challenge.CookieValue.Length==97,"QA browser receives public code and separate nonce proof");
Check(qaAccess.Begin(challenge.CookieValue).Code==challenge.Code,"QA page reload reuses its bounded challenge");
var grants=0;
SessionGrant GrantQa(){grants++;return store.NewGuest(qaClock.GetUtcNow(),true,TimeSpan.FromHours(2));}
Denied(()=>qaAccess.Claim(challenge.CookieValue,GrantQa),"qa_approval_pending");
Denied(()=>qaAccess.Approve(challenge.Code,routeWorld.Id,false),"qa_authority_required");
qaAccess.Approve(challenge.Code,routeWorld.Id,true);
Denied(()=>qaAccess.Claim(challenge.Code+"."+new string('0',64),GrantQa),"qa_browser_proof_required");
var qaClaim=qaAccess.Claim(challenge.CookieValue,GrantQa);
Check(qaClaim.Session.Identity.PreviewApproved&&!qaClaim.Session.Identity.FullAccess&&qaClaim.WorldId==routeWorld.Id,"QA claim grants demo preview only for the approved browser");
Check(qaAccess.Claim(challenge.CookieValue,GrantQa).Session.Token==qaClaim.Session.Token&&grants==1,"QA claim retry does not issue a second identity");
Denied(()=>qaAccess.Approve(challenge.Code,privateWorld.Id,true),"qa_challenge_already_bound");
Check(store.Authenticate(qaClaim.Session.Token,qaClock.GetUtcNow().AddHours(2))==null,"browser QA session expires after two hours on the server");
qaClock.Advance(TimeSpan.FromMinutes(6));
Denied(()=>qaAccess.Claim(challenge.CookieValue,GrantQa),"qa_browser_proof_required");
Denied(()=>qaAccess.Approve(challenge.Code,routeWorld.Id,true),"qa_challenge_expired");
var inputBudget=new InputCommandBudget(qaClock.GetUtcNow());
Check(Enumerable.Range(0,120).All(_=>inputBudget.Take(qaClock.GetUtcNow()))&&!inputBudget.Take(qaClock.GetUtcNow()),"input budget allows bounded burst then rejects excess");
qaClock.Advance(TimeSpan.FromMilliseconds(10));
Check(inputBudget.Take(qaClock.GetUtcNow())&&!inputBudget.Take(qaClock.GetUtcNow()),"input tokens replenish continuously without a fixed-window edge");
Check(!inputBudget.Take(qaClock.GetUtcNow().AddSeconds(-1)),"clock reversal cannot replenish input budget");
var streamOwner=store.NewGuest(clock.GetUtcNow()).Identity;
var streamWorld=store.Create(streamOwner,new(),clock.GetUtcNow());
rooms.Join(streamOwner,streamWorld.Id,null);
var (streamPeer,streamConnection)=rooms.Connect(streamOwner,streamWorld.Id,false);
rooms.Receive(streamOwner,streamWorld.Id,streamConnection,new("ready", TuningDefaultsHash:CanonicalTuningDefaults.Fingerprint,Epoch:1));
for(var sequence=1;sequence<=6000;sequence++)
{
    clock.Advance(TimeSpan.FromSeconds(1d/60));
    rooms.Receive(streamOwner,streamWorld.Id,streamConnection,new("input",Epoch:1,Sequence:sequence,Command:new("simulation","enqueue",CommandType:1)));
    if(sequence%2==0)rooms.Receive(streamOwner,streamWorld.Id,streamConnection,new("commitBatch",Epoch:1,Commits:[new(sequence,sequence,1)]));
    else rooms.Receive(streamOwner,streamWorld.Id,streamConnection,new("commit",Epoch:1,Tick:sequence,Sequence:sequence));
    var observedOrder=false;
    while(streamPeer.Events.Reader.TryRead(out var streamed))
    {
        if(streamed.Type=="ordered")observedOrder=true;
        if(streamed.Type is "committed" or "committedBatch" && !observedOrder)throw new Exception("Commit delivered before input.");
    }
}
Check(streamPeer.Ready&&store.Versions(streamWorld.Id).Count==0,"6000 continuous inputs stay bounded without a cloud save and preserve FIFO order");
rooms.Receive(streamOwner,streamWorld.Id,streamConnection,new("input",Epoch:1,Sequence:6001,Command:new("simulation","enqueue",CommandType:1)));
rooms.Join(b.Identity,streamWorld.Id,null);
var (streamLate,_)=rooms.Connect(b.Identity,streamWorld.Id,false);
streamLate.Events.Reader.TryRead(out var streamWelcome);
var welcomeData=System.Text.Json.JsonSerializer.SerializeToElement(streamWelcome!.Data);
var pendingCommands=welcomeData.GetProperty("commands");
Check(welcomeData.GetProperty("hostId").GetString()==streamOwner.Id&&pendingCommands.GetArrayLength()==1&&pendingCommands[0].GetProperty("Sequence").GetInt64()==6001,"live welcome retains only uncommitted input and identifies snapshot host");
var departureRoot=Path.Combine(root,Guid.NewGuid().ToString("N"));
var departureStore=new WorldStore(departureRoot);var departureClock=new ManualClock();var departureRooms=new Rooms(departureStore,departureClock);
var departing=departureStore.NewGuest(departureClock.GetUtcNow()).Identity;var staying=departureStore.NewGuest(departureClock.GetUtcNow()).Identity;
var arriving=departureStore.NewGuest(departureClock.GetUtcNow()).Identity;
var departureWorld=departureStore.Create(departing,new(),departureClock.GetUtcNow());
departureRooms.Join(departing,departureWorld.Id,null);departureRooms.Join(staying,departureWorld.Id,null);
var (leaver,leaverConnection)=departureRooms.Connect(departing,departureWorld.Id,false);
var (stayer,stayerConnection)=departureRooms.Connect(staying,departureWorld.Id,false);
departureRooms.Receive(departing,departureWorld.Id,leaverConnection,new("ready", TuningDefaultsHash:CanonicalTuningDefaults.Fingerprint,Epoch:1));
departureRooms.Receive(staying,departureWorld.Id,stayerConnection,new("ready", TuningDefaultsHash:CanonicalTuningDefaults.Fingerprint,Epoch:1));
byte[] DepartureBytes(long tick,long sequence)
{
    var checkpoint=SnapshotCodec.Decode(State(1,tick,sequence));checkpoint.WorldId=departureWorld.Id;return SnapshotCodec.Encode(checkpoint);
}
SnapshotUpload DepartureUpload(byte[] payload,long tick,long sequence,long expected)=>new(1,tick,sequence,expected,Convert.ToHexString(SHA256.HashData(payload)));
departureRooms.Receive(departing,departureWorld.Id,leaverConnection,new("input",Epoch:1,Sequence:1,Command:new("simulation","enqueue",CommandType:1)));
departureRooms.Receive(departing,departureWorld.Id,leaverConnection,new("commit",Epoch:1,Tick:1,Sequence:1));
var beforeDeparture=DepartureBytes(1,1);departureRooms.Save(departing,departureWorld.Id,DepartureUpload(beforeDeparture,1,1,0),beforeDeparture,false);
departureRooms.Receive(staying,departureWorld.Id,stayerConnection,new("input",Epoch:1,Sequence:1,Command:new("simulation","enqueue",CommandType:1)));
var departureOperation=Protocol.RandomId();
departureRooms.Receive(departing,departureWorld.Id,leaverConnection,new("depart_begin",Epoch:1,Tick:1,Sequence:1,OperationId:departureOperation));
departureRooms.Receive(departing,departureWorld.Id,leaverConnection,new("depart_begin",Epoch:1,Tick:1,Sequence:1,OperationId:departureOperation));
Check(departureRooms.View(departureWorld.Id).State=="handoff"&&!stayer.Ready,"departure freezes admission and removes unsettled follower from authority election");
Check(departureRooms.VoiceMember(staying,departureWorld.Id)==stayer,"admitted voice access is independent of temporary physics readiness");
Denied(()=>departureRooms.Join(arriving,departureWorld.Id,null),"world_handoff");
while(stayer.Events.Reader.TryRead(out _)){}
departureRooms.Receive(staying,departureWorld.Id,stayerConnection,new("input",Epoch:1,Sequence:2,Command:new("simulation","enqueue",CommandType:1)));
Check(stayer.Events.Reader.TryRead(out var rejectedInput)&&rejectedInput.Type=="input_rejected"&&stayer.ConnectionId==stayerConnection,"in-flight input gets explicit handoff rejection without dropping the peer");
Denied(()=>departureRooms.Receive(departing,departureWorld.Id,leaverConnection,new("commit",Epoch:1,Tick:3,Sequence:2)),"departure_fence");
departureRooms.Receive(departing,departureWorld.Id,leaverConnection,new("commit",Epoch:1,Tick:2,Sequence:2));
var departurePayload=DepartureBytes(2,2);var departureUpload=DepartureUpload(departurePayload,2,2,1);
Denied(()=>departureRooms.Save(departing,departureWorld.Id,departureUpload,departurePayload,false),"departure_in_progress");
var damagedDeparture=(byte[])departurePayload.Clone();damagedDeparture[^1]^=1;
Denied(()=>departureRooms.CompleteDeparture(departing,departureWorld.Id,departureOperation,departureUpload,damagedDeparture),"snapshot_hash");
Check(departureStore.Versions(departureWorld.Id).First().Revision==1&&departureRooms.Member(departing,departureWorld.Id)==leaver,"bad departure upload preserves old version and current authority");
var departureReceipt=departureRooms.CompleteDeparture(departing,departureWorld.Id,departureOperation,departureUpload,departurePayload);
Check(departureReceipt.Snapshot.Revision==2&&departureStore.Versions(departureWorld.Id).Count==2,"departure saves final pending input before host removal");
var departureEvents=new List<ServerEvent>();while(stayer.Events.Reader.TryRead(out var departureEvent))departureEvents.Add(departureEvent);
Check(departureEvents.FindIndex(x=>x.Type=="saved"&&x.Data is SnapshotRecord {Revision:2})>=0&&
    departureEvents.FindIndex(x=>x.Type=="saved")<departureEvents.FindIndex(x=>x.Type=="paused"),"new durable revision is announced before authority changes");
Denied(()=>departureRooms.Member(departing,departureWorld.Id),"not_admitted");
Check(departureRooms.CompleteDeparture(departing,departureWorld.Id,departureOperation,departureUpload,departurePayload)==departureReceipt&&departureStore.Versions(departureWorld.Id).Count==2,"identical completion retry is idempotent after disconnect");
Denied(()=>departureRooms.CompleteDeparture(departing,departureWorld.Id,departureOperation,departureUpload,damagedDeparture),"departure_retry_mismatch");
Denied(()=>departureRooms.DepartureStatus(staying,departureWorld.Id,departureOperation),"departure_owner_required");
var restartedStore=new WorldStore(departureRoot);var restartedRooms=new Rooms(restartedStore,departureClock);
Check(restartedRooms.CompleteDeparture(departing,departureWorld.Id,departureOperation,departureUpload,departurePayload)==departureReceipt,"completion receipt survives service restart and lost HTTP response");
departureClock.Advance(TimeSpan.FromSeconds(3));departureRooms.Sweep();
departureRooms.Receive(staying,departureWorld.Id,stayerConnection,new("ready", TuningDefaultsHash:CanonicalTuningDefaults.Fingerprint,Epoch:2,Tick:2,Sequence:2));
Check(departureRooms.View(departureWorld.Id).State=="active"&&departureRooms.Member(staying,departureWorld.Id).Ready,"remaining peer resumes from latest departure snapshot without lost interval");
var staleEpochBytes=SnapshotCodec.Decode(departurePayload);staleEpochBytes.Epoch=2;var staleBytes=SnapshotCodec.Encode(staleEpochBytes);
Denied(()=>departureRooms.Save(staying,departureWorld.Id,new(2,2,2,1,Convert.ToHexString(SHA256.HashData(staleBytes))),staleBytes,false),"snapshot_revision_conflict");
var revisionNotice=false;while(stayer.Events.Reader.TryRead(out var latestEvent))if(latestEvent.Type=="saved"&&latestEvent.Data is SnapshotRecord {Revision:2})revisionNotice=true;
Check(revisionNotice&&departureStore.Versions(departureWorld.Id).Count==2,"stale writer learns current confirmed revision without overwriting a snapshot");
Denied(()=>departureRooms.LeaveParticipant(staying,departureWorld.Id),"host_departure_required");
var nextDeparture=Protocol.RandomId();
departureClock.Advance(TimeSpan.FromSeconds(7));
departureRooms.Receive(staying,departureWorld.Id,stayerConnection,new("depart_begin",Epoch:2,Tick:2,Sequence:2,OperationId:nextDeparture));
departureRooms.Receive(staying,departureWorld.Id,stayerConnection,new("depart_abort",Epoch:2,OperationId:nextDeparture));
Check(departureRooms.View(departureWorld.Id).State=="active","cancelled departure resumes room without creating another save");
departureClock.Advance(TimeSpan.FromSeconds(2));departureRooms.Receive(staying,departureWorld.Id,stayerConnection,new("heartbeat",Epoch:2,Tick:2,Sequence:2));
departureClock.Advance(TimeSpan.FromSeconds(7));departureRooms.Receive(staying,departureWorld.Id,stayerConnection,new("heartbeat",Epoch:2,Tick:2,Sequence:2));
departureClock.Advance(TimeSpan.FromSeconds(1));
departureRooms.Receive(staying,departureWorld.Id,stayerConnection,new("depart_begin",Epoch:2,Tick:2,Sequence:2,OperationId:Protocol.RandomId()));
for(var interval=0;interval<9;interval++){departureClock.Advance(TimeSpan.FromSeconds(7));departureRooms.Receive(staying,departureWorld.Id,stayerConnection,new("heartbeat",Epoch:2,Tick:2,Sequence:2));}
departureRooms.Sweep();Check(departureRooms.View(departureWorld.Id).State=="active","departure timeout clears freeze while valid host lease continues");
async Task BadUploadBody(int announced,int actual,string code)
{
    var context=new Microsoft.AspNetCore.Http.DefaultHttpContext();context.Request.ContentLength=announced;context.Request.Body=new MemoryStream(new byte[actual]);
    foreach(var header in new[]{"X-SF-Epoch","X-SF-Tick","X-SF-Sequence","X-SF-Revision"})context.Request.Headers[header]="1";
    try{await SnapshotHttp.Read(context);throw new Exception("Expected "+code);}catch(ApiFailure error)when(error.Code==code){Check(true,code);}
}
await BadUploadBody(20,16,"snapshot_truncated");await BadUploadBody(16,20,"snapshot_size");
Microsoft.AspNetCore.Http.DefaultHttpContext UploadContext()
{
    var context=new Microsoft.AspNetCore.Http.DefaultHttpContext();context.Request.ContentLength=16;context.Request.Body=new MemoryStream(new byte[16]);
    foreach(var header in new[]{"X-SF-Epoch","X-SF-Tick","X-SF-Sequence","X-SF-Revision"})context.Request.Headers[header]="1";
    return context;
}
using(var uploadSlotA=await SnapshotHttp.Read(UploadContext()))
using(var uploadSlotB=await SnapshotHttp.Read(UploadContext()))
{
    try{using var deniedSlot=await SnapshotHttp.Read(UploadContext());throw new Exception("Expected snapshot_capacity");}
    catch(ApiFailure error)when(error.Code=="snapshot_capacity"){Check(true,"snapshot capacity stays held through storage processing");}
    uploadSlotA.Dispose();uploadSlotA.Dispose();
    using var releasedSlot=await SnapshotHttp.Read(UploadContext());
    Check(releasedSlot.Payload.Length==16,"upload lease releases exactly once and admits a later bounded body");
}
var backlogClock=new ManualClock();var backlogStore=new WorldStore(Path.Combine(root,Guid.NewGuid().ToString("N")));var backlogRooms=new Rooms(backlogStore,backlogClock);
var backlogOwner=backlogStore.NewGuest(backlogClock.GetUtcNow()).Identity;var backlogWorld=backlogStore.Create(backlogOwner,new(),backlogClock.GetUtcNow());
backlogRooms.Join(backlogOwner,backlogWorld.Id,null);var (backlogPeer,backlogConnection)=backlogRooms.Connect(backlogOwner,backlogWorld.Id,false);
backlogRooms.Receive(backlogOwner,backlogWorld.Id,backlogConnection,new("ready", TuningDefaultsHash:CanonicalTuningDefaults.Fingerprint,Epoch:1));
for(var input=1;input<=600;input++)
{
    backlogClock.Advance(TimeSpan.FromMilliseconds(10));
    backlogRooms.Receive(backlogOwner,backlogWorld.Id,backlogConnection,new("input",Epoch:1,Sequence:input,Command:new("simulation","enqueue",CommandType:1)));
    while(backlogPeer.Events.Reader.TryRead(out _)){}
}
var backlogOperation=Protocol.RandomId();backlogRooms.Receive(backlogOwner,backlogWorld.Id,backlogConnection,new("depart_begin",Epoch:1,OperationId:backlogOperation));
backlogRooms.Receive(backlogOwner,backlogWorld.Id,backlogConnection,new("commitBatch",Epoch:1,Commits:[new(1,256,1),new(2,512,1),new(3,600,1)]));
var backlogSnapshot=SnapshotCodec.Decode(State(1,3,600));backlogSnapshot.WorldId=backlogWorld.Id;var backlogPayload=SnapshotCodec.Encode(backlogSnapshot);
var backlogReceipt=backlogRooms.CompleteDeparture(backlogOwner,backlogWorld.Id,backlogOperation,new(1,3,600,0,Convert.ToHexString(SHA256.HashData(backlogPayload))),backlogPayload);
Check(backlogReceipt.Snapshot.Tick==3&&backlogReceipt.Snapshot.Sequence==600&&backlogRooms.View(backlogWorld.Id).State=="sleeping","departure drains more than 256 pending inputs across a bounded final step range");
var tuning = TuningSettings.All.Where(value => value.Scope == SettingScope.Shared)
    .ToDictionary(value => value.Key, value => (value.Min + value.Max) * .5f, StringComparer.Ordinal);
Check(TuningSettings.All.Count == 100 && TuningSettings.SharedCount == 99, "reviewed tuning registry has 99 shared dials and one local quality dial");
var tuningBytes = TuningSettings.Encode(tuning);
var canonical=TuningSettings.Encode(CanonicalTuningDefaults.Create());
Check(CanonicalTuningDefaults.Fingerprint==Convert.ToHexString(SHA256.HashData(canonical)).ToLowerInvariant(),"canonical tuning fingerprint matches complete bounded values");
var modifiedDefaults=CanonicalTuningDefaults.Create();modifiedDefaults["ErosionStrength"]=.3f;
Check(CanonicalTuningDefaults.Create()["ErosionStrength"]!=.3f,"canonical defaults do not depend on caller mutations or local resources");
Denied(()=>CommandValidation.Validate(new("ufo","unimplemented")),"invalid_command");
Denied(()=>CommandValidation.Validate(new("simulation","run-script")),"invalid_command");
CommandValidation.Validate(new("setting","set",Settings:[new("ErosionStrength",.4f),new("SunIntensity",1.2f)]));
Check(true,"bounded shared tuning batch accepted");
CommandValidation.Validate(new("simulation","enqueue",Settings:[]));
Check(true,"Unity empty default tuning array does not reject simulation commands");
Denied(()=>CommandValidation.Validate(new("setting","set",Settings:[new("PixelDensity",1)])),"invalid_tuning_value");
Denied(()=>CommandValidation.Validate(new("setting","set",Settings:[new("ErosionStrength",float.NaN)])),"invalid_tuning_value");
Denied(()=>CommandValidation.Validate(new("setting","set",Settings:[new("ErosionStrength",.1f),new("ErosionStrength",.2f)])),"invalid_tuning_batch");
Denied(()=>CommandValidation.Validate(new("setting","set",Settings:[])),"invalid_tuning_batch");
Denied(()=>CommandValidation.Validate(new("setting","reset",Settings:[new("ErosionStrength",.1f)])),"invalid_tuning_batch");
Denied(()=>CommandValidation.Validate(new("simulation","enqueue",Settings:[new("ErosionStrength",.1f)])),"unexpected_tuning_batch");
var tuningClock=new ManualClock();var tuningStore=new WorldStore(Path.Combine(root,Guid.NewGuid().ToString("N")));var tuningRooms=new Rooms(tuningStore,tuningClock);
var tuningOwner=tuningStore.NewGuest(tuningClock.GetUtcNow()).Identity;var tuningWorld=tuningStore.Create(tuningOwner,new(),tuningClock.GetUtcNow());
tuningRooms.Join(tuningOwner,tuningWorld.Id,null);var(tuningPeer,tuningConnection)=tuningRooms.Connect(tuningOwner,tuningWorld.Id,false);
Denied(()=>tuningRooms.Receive(tuningOwner,tuningWorld.Id,tuningConnection,new("ready", TuningDefaultsHash:CanonicalTuningDefaults.Fingerprint,Version:1,Epoch:1)),"protocol_version");
Denied(()=>tuningRooms.Receive(tuningOwner,tuningWorld.Id,tuningConnection,new("ready",Epoch:1)),"tuning_defaults_version");
Denied(()=>tuningRooms.Receive(tuningOwner,tuningWorld.Id,tuningConnection,new("ready",Epoch:1,TuningDefaultsHash:new string('0',64))),"tuning_defaults_version");
tuningRooms.Receive(tuningOwner,tuningWorld.Id,tuningConnection,new("ready", TuningDefaultsHash:CanonicalTuningDefaults.Fingerprint,Epoch:1));
tuningRooms.Receive(tuningOwner,tuningWorld.Id,tuningConnection,new("input",Epoch:1,Sequence:7,Command:new("setting","set",Settings:[new("ErosionStrength",.4f)])));
OrderedCommand? tuningOrdered=null;while(tuningPeer.Events.Reader.TryRead(out var tuningEvent))if(tuningEvent.Type=="ordered")tuningOrdered=tuningEvent.Data as OrderedCommand;
Check(tuningOrdered?.ClientSequence==7&&tuningOrdered.Sequence==1&&tuningOrdered.Command.Settings?[0].Value==.4f,"setting shares global order and confirms the sender input cursor");
tuningRooms.Receive(tuningOwner,tuningWorld.Id,tuningConnection,new("commit",Epoch:1,Tick:1,Sequence:1));
Check(tuningRooms.View(tuningWorld.Id).State=="active","setting batch commits through normal step ordering");
var tuningCopy = TuningSettings.Decode(tuningBytes);
var tunedPhysical=SnapshotCodec.Decode(State(1,1,1));tunedPhysical.WorldId=tuningWorld.Id;
tunedPhysical.MetadataJson="{\"scope\":\""+TuningSettings.WorldScope+"\"}";
tunedPhysical.Sections.Add(new SnapshotSection{Name=TuningSettings.SectionName,Data=tuningBytes});
SnapshotRecord SaveTuned(long revision)
{
    var bytes=SnapshotCodec.Encode(tunedPhysical);
    return tuningStore.Save(tuningWorld.Id,new(1,1,1,revision,Convert.ToHexString(SHA256.HashData(bytes))),bytes,tuningClock.GetUtcNow());
}
Check(SaveTuned(0).Revision==1,"validated tuning is stored with physical state");
tunedPhysical.Sections[0].Data=[1,2,3];Denied(()=>SaveTuned(1),"snapshot_tuning");
tunedPhysical.Sections.Clear();Denied(()=>SaveTuned(1),"snapshot_tuning");
Check(tuningStore.Versions(tuningWorld.Id).First().Revision==1,"corrupt or missing tuning cannot replace prior valid save");
Check(tuningCopy.Count == 99 && tuning.All(pair => tuningCopy[pair.Key] == pair.Value), "all shared tuning values roundtrip losslessly");
var reversedTuning = tuning.Reverse().ToDictionary(pair => pair.Key, pair => pair.Value, StringComparer.Ordinal);
Check(TuningSettings.Encode(reversedTuning).SequenceEqual(tuningBytes), "tuning encoding independent of insertion order");
BadSnapshot(() => TuningSettings.ValidateShared("PixelDensity", 1), "local rendering preference cannot enter shared tuning");
BadSnapshot(() => TuningSettings.ValidateShared("unknown-key", 1), "unknown tuning key denied");
BadSnapshot(() => TuningSettings.ValidateShared("ErosionStrength", float.NaN), "NaN tuning denied");
BadSnapshot(() => TuningSettings.ValidateShared("ErosionStrength", float.PositiveInfinity), "infinite tuning denied");
BadSnapshot(() => TuningSettings.ValidateShared("ErosionStrength", 3), "out-of-range tuning denied");
var incompleteTuning = new Dictionary<string, float>(tuning); incompleteTuning.Remove("ErosionStrength");
BadSnapshot(() => TuningSettings.Encode(incompleteTuning), "incomplete tuning checkpoint denied");
BadSnapshot(() => TuningSettings.Decode(tuningBytes[..^1]), "truncated tuning checkpoint denied");
BadSnapshot(() => TuningSettings.Decode([..tuningBytes,0]), "trailing tuning data denied");
var maliciousTuning = (byte[])tuningBytes.Clone(); maliciousTuning[12] = 255;
BadSnapshot(() => TuningSettings.Decode(maliciousTuning), "forged tuning string length rejected before allocation");
var badTuningVersion = (byte[])tuningBytes.Clone(); badTuningVersion[4] = 99;
BadSnapshot(() => TuningSettings.Decode(badTuningVersion), "unknown tuning snapshot version denied");
Console.WriteLine($"RESULT {testCount} assertions passed. No HTTP server or physical simulation launched.");

sealed class ManualClock : TimeProvider
{
    private DateTimeOffset _now = new(2026, 9, 13, 0, 0, 0, TimeSpan.Zero);
    public override DateTimeOffset GetUtcNow() => _now;
    public void Advance(TimeSpan duration) => _now += duration;
}
