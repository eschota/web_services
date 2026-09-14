using System.Threading.Channels;

namespace SandFlow.Server;

public sealed class RoomPeer(Identity identity, int team, DateTimeOffset admitted)
{
    public Identity Identity { get; } = identity;
    public int Team { get; } = team;
    public string? ConnectionId { get; set; }
    public DateTimeOffset Admitted { get; } = admitted;
    public bool Ready { get; set; }
    public long AcknowledgedSequence { get; set; }
    public long AcknowledgedTick { get; set; }
    public Channel<ServerEvent> Events { get; } = Channel.CreateBounded<ServerEvent>(new BoundedChannelOptions(512)
    { FullMode = BoundedChannelFullMode.Wait, SingleReader = true, SingleWriter = false });
    public Channel<byte[]> Bulk { get; } = Channel.CreateBounded<byte[]>(new BoundedChannelOptions(8)
    { FullMode = BoundedChannelFullMode.Wait, SingleReader = true, SingleWriter = false });
    public string? BulkConnectionId { get; set; }
    public long LastClientSequence { get; set; }
    public InputCommandBudget InputBudget { get; } = new(admitted);
    public DateTimeOffset BulkWindow { get; set; }
    public int BulkBytes { get; set; }
}

public sealed class RoomState(WorldRecord world, SnapshotRecord? snapshot)
{
    public WorldRecord World { get; } = world;
    public Dictionary<string, RoomPeer> Peers { get; } = [];
    public List<OrderedCommand> Tail { get; } = [];
    public SortedDictionary<long, CommitRecord> Commits { get; } = [];
    public SortedSet<long> Checkpoints { get; } = [];
    public long Epoch { get; set; } = (snapshot?.Epoch ?? 0) + 1;
    public long Tick { get; set; } = snapshot?.Tick ?? 0;
    public long Sequence { get; set; } = snapshot?.Sequence ?? 0;
    public long ConfirmedSequence { get; set; } = snapshot?.Sequence ?? 0;
    public long Revision { get; set; } = snapshot?.Revision ?? 0;
    public string? HostId { get; set; }
    public DateTimeOffset LeaseUntil { get; set; }
    public DateTimeOffset? DirtySince { get; set; }
    public DateTimeOffset LastSnapshot { get; set; } = snapshot == null ? DateTimeOffset.MinValue : DateTimeOffset.FromUnixTimeSeconds(snapshot.CreatedUnix);
    public DateTimeOffset LastSaveRequest { get; set; }
    public DateTimeOffset LastStructuralChange { get; set; }
    public DateTimeOffset? RecoveryUntil { get; set; }
    public DepartureState? Departure { get; set; }
    public DateTimeOffset LastDepartureAttempt { get; set; }
}

public sealed record CommitRecord(long Tick, long Sequence, int Substeps, bool Checkpoint = false);

/// <summary>Server ordering/lease authority only. This class contains no physical simulation.</summary>
public sealed partial class Rooms(WorldStore store, TimeProvider time)
{
    private readonly Dictionary<string, RoomState> _rooms = [];
    private readonly object _gate = new();
    public object Gate => _gate;
    private RoomState Get(string id)
    {
        if (!_rooms.TryGetValue(id, out var room))
            _rooms.Add(id, room = new(store.Find(id), store.Versions(id).FirstOrDefault()));
        return room;
    }
    public WorldView View(string id) { lock (_gate) return ViewOf(Get(id)); }
    private static WorldView ViewOf(RoomState room) => new(room.World.Id, room.World.Mode, room.World.Map,
        room.World.IsPrivate, room.World.Demo, room.World.Capacity, room.Peers.Count,
        room.Peers.Count == 0 ? "sleeping" : room.Departure != null ? "handoff" : room.HostId == null ? "synchronizing" : "active", room.Revision);
    public Admission Join(Identity identity, string worldId, string? password)
    {
        lock (_gate)
        {
            SweepLocked();
            var room = Get(worldId);
            if (!room.World.Demo && !identity.FullAccess) throw new ApiFailure(403, "full_access_required");
            if (store.IsBlocked(worldId, identity.Id)) throw new ApiFailure(403, "world_access_denied");
            if (room.Peers.TryGetValue(identity.Id, out var existing))
                return new(ViewOf(room), identity.Id, existing.Team, Protocol.Version);
            if(room.Departure!=null)throw new ApiFailure(409,"world_handoff");
            if (room.World.IsPrivate && room.World.OwnerId != identity.Id && !Passwords.Verify(password ?? "", room.World.PasswordHash))
                throw new ApiFailure(403, "world_password_required");
            if (room.Peers.Count >= room.World.Capacity) throw new ApiFailure(409, "world_full");
            if (_rooms.Values.Sum(x => x.Peers.Count) >= Protocol.MaximumMembers) throw new ApiFailure(503, "server_capacity");
            var team = room.World.Mode == "pvp" && room.Peers.Values.Count(x => x.Team == 0) > room.Peers.Values.Count(x => x.Team == 1) ? 1 : 0;
            room.Peers.Add(identity.Id, new(identity, team, time.GetUtcNow()));
            return new(ViewOf(room), identity.Id, team, Protocol.Version);
        }
    }
    public Admission AutoJoin(Identity identity)
    {
        lock (_gate)
        {
            foreach (var world in store.PublicWorlds().Where(x => x.Demo && x.Mode == "coop"))
            {
                var room = Get(world.Id);
                if (room.Departure==null&&room.Peers.Count < world.Capacity && !store.IsBlocked(world.Id, identity.Id))
                    return Join(identity, world.Id, null);
            }
            var created = store.Create(identity, new(), time.GetUtcNow());
            return Join(identity, created.Id, null);
        }
    }
    public RoomPeer Member(Identity identity, string id)
    {
        lock (_gate)
        {
            var room = Get(id);
            if (!room.Peers.TryGetValue(identity.Id, out var peer) || store.IsBlocked(id, identity.Id))
                throw new ApiFailure(403, "not_admitted");
            return peer;
        }
    }
    public (RoomPeer Peer, string Connection) Connect(Identity identity, string id, bool bulk)
    {
        lock (_gate)
        {
            var peer = Member(identity, id); var connection = Protocol.RandomId();
            if (bulk)
            {
                if (peer.ConnectionId == null) throw new ApiFailure(409, "control_required");
                if (peer.BulkConnectionId != null) throw new ApiFailure(409, "already_connected");
                peer.BulkConnectionId = connection;
                var room = Get(id);
                if (room.HostId != null && room.HostId != identity.Id)
                    Send(room, room.Peers[room.HostId], "snapshot_requested", new { participantId = identity.Id });
            }
            else
            {
                if (peer.ConnectionId != null) throw new ApiFailure(409, "already_connected");
                peer.ConnectionId = connection;
                var room = Get(id); var snapshot = store.Versions(id).FirstOrDefault();
                Send(room, peer, "welcome", new { version = Protocol.Version, tileSize = Protocol.TileSize, participantId = identity.Id,
                    tuningDefaultsHash=SandFlow.Protocol.CanonicalTuningDefaults.Fingerprint,
                    team = peer.Team, hostId = room.HostId, snapshot, commands = room.Tail.ToArray(), commits = room.Commits.Values.ToArray(), sequence = room.ConfirmedSequence, departureProtocol = 1 });
            }
            return (peer, connection);
        }
    }
    public void Receive(Identity identity, string id, string connection, ControlMessage message)
    {
        lock (_gate)
        {
            var room = Get(id); var peer = Member(identity, id);
            if (peer.ConnectionId != connection) throw new ApiFailure(409, "stale_connection");
            if (message.Version != Protocol.Version) throw new ApiFailure(409, "protocol_version");
            if(message.Type=="ready"&&message.TuningDefaultsHash!=SandFlow.Protocol.CanonicalTuningDefaults.Fingerprint)
                throw new ApiFailure(409,"tuning_defaults_version");
            if (message.Epoch != room.Epoch) throw new ApiFailure(409, "stale_epoch");
            var now = time.GetUtcNow();
            switch (message.Type)
            {
                case "ready":
                    var current = message.Sequence == room.ConfirmedSequence && message.Tick == room.Tick;
                    var retained = room.Commits.TryGetValue(message.Tick, out var readyAt) && readyAt.Sequence == message.Sequence;
                    var saved = !current && !retained ? store.Versions(id).FirstOrDefault() : null;
                    var atSaved = saved != null && saved.Tick == message.Tick && saved.Sequence == message.Sequence;
                    if (!current && !retained && !atSaved)
                        throw new ApiFailure(409, "baseline_mismatch");
                    peer.Ready = true; peer.AcknowledgedSequence = message.Sequence; peer.AcknowledgedTick = message.Tick;
                    Elect(room); Broadcast(room, "presence", ViewOf(room)); break;
                case "heartbeat":
                    if (room.HostId == identity.Id) RequireHost(room, peer, message.Epoch);
                    if (room.HostId == identity.Id) room.LeaseUntil = now.AddSeconds(8);
                    if (message.Sequence > room.ConfirmedSequence || message.Sequence < 0 || message.Tick > room.Tick || message.Tick < 0)
                        throw new ApiFailure(400, "invalid_ack");
                    peer.AcknowledgedSequence = message.Sequence; peer.AcknowledgedTick = message.Tick;
                    if (room.RecoveryUntil != null) Elect(room);
                    break;
                case "input":
                    if (!peer.InputBudget.Take(now)) throw new ApiFailure(429, "input_rate");
                    if(room.Departure!=null){Send(room,peer,"input_rejected",new { reason="world_handoff",clientSequence=message.Sequence });break;}
                    if (!peer.Ready || room.HostId == null) throw new ApiFailure(409, "world_synchronizing");
                    if (message.Sequence <= peer.LastClientSequence) throw new ApiFailure(409, "duplicate_input");
                    if (room.Tail.Count >= 4096) throw new ApiFailure(429, "checkpoint_required");
                    if (message.Command == null) throw new ApiFailure(400, "missing_command");
                    CommandValidation.Validate(message.Command);
                    if (message.Command.Kind is "map" or "reset")
                    {
                        if (now - room.LastStructuralChange < TimeSpan.FromSeconds(30)) throw new ApiFailure(429, "structural_rate");
                        // The real client must supply a pre-reset checkpoint; do not let incomplete adapters destroy state.
                        throw new ApiFailure(409, "structural_checkpoint_required");
                    }
                    var command = new OrderedCommand(++room.Sequence, identity.Id, message.Command, message.Sequence);
                    peer.LastClientSequence = message.Sequence; room.Tail.Add(command); room.DirtySince ??= now;
                    Broadcast(room, "ordered", command, command.Sequence); break;
                case "commit":
                    RequireHost(room, peer, message.Epoch);
                    ValidateDepartureCommit(room,message.Tick,message.Sequence);
                    if (message.Tick != room.Tick + 1 || message.Sequence < room.ConfirmedSequence || message.Sequence > room.Sequence ||
                        message.Sequence - room.ConfirmedSequence > 256 || message.Substeps is < 1 or > 16)
                        throw new ApiFailure(400, "invalid_commit");
                    room.Tick = message.Tick; room.ConfirmedSequence = message.Sequence;
                    TrimConfirmedInputs(room);
                    room.Commits[message.Tick] = new(message.Tick, message.Sequence, message.Substeps);
                    while (room.Commits.Count > 4096) room.Commits.Remove(room.Commits.First().Key);
                    room.LeaseUntil = now.AddSeconds(8); peer.AcknowledgedSequence = message.Sequence; peer.AcknowledgedTick = message.Tick; room.DirtySince ??= now;
                    Broadcast(room, "committed", new { substeps = message.Substeps }); break;
                case "commitBatch":
                    RequireHost(room, peer, message.Epoch);
                    if (message.Commits == null || message.Commits.Length is < 1 or > 32) throw new ApiFailure(400, "commit_batch_size");
                    var cursorTick = room.Tick; var cursorSequence = room.ConfirmedSequence;
                    var checkpointTick = room.Checkpoints.Count == 0 ? -240 : room.Checkpoints.Max;
                    foreach (var frame in message.Commits)
                    {
                        ValidateDepartureCommit(room,frame.Tick,frame.Sequence);
                        if (frame.Tick != ++cursorTick || frame.Sequence < cursorSequence || frame.Sequence > room.Sequence ||
                            frame.Sequence - cursorSequence > 256 || frame.Substeps is < 1 or > 16) throw new ApiFailure(400, "invalid_commit");
                        cursorSequence = frame.Sequence;
                        if (frame.Checkpoint)
                        { if (frame.Tick - checkpointTick < 240) throw new ApiFailure(429, "checkpoint_rate"); checkpointTick = frame.Tick; }
                    }
                    foreach (var frame in message.Commits)
                    { room.Commits[frame.Tick] = frame; if (frame.Checkpoint) room.Checkpoints.Add(frame.Tick); }
                    while (room.Checkpoints.Count > 8) room.Checkpoints.Remove(room.Checkpoints.Min);
                    while (room.Commits.Count > 4096) room.Commits.Remove(room.Commits.First().Key);
                    room.Tick = cursorTick; room.ConfirmedSequence = cursorSequence; room.LeaseUntil = now.AddSeconds(8);
                    TrimConfirmedInputs(room);
                    peer.AcknowledgedSequence = cursorSequence; peer.AcknowledgedTick = cursorTick; room.DirtySince ??= now;
                    Broadcast(room, "committedBatch", new { commits = message.Commits }); break;
                case "resync":
                    if (room.HostId != identity.Id) peer.Ready = false;
                    if (room.HostId != null) Send(room, room.Peers[room.HostId], "snapshot_requested", new { participantId = identity.Id });
                    else Send(room, peer, "restore_required", new { snapshot = store.Versions(id).FirstOrDefault() });
                    break;
                case "repairing":
                    if (room.HostId == identity.Id) throw new ApiFailure(409, "host_cannot_rewind");
                    peer.Ready = false; break;
                case "peer_resync":
                    RequireHost(room, peer, message.Epoch);
                    if (message.ParticipantId == null || !room.Peers.TryGetValue(message.ParticipantId, out var resyncPeer))
                        throw new ApiFailure(400, "repair_recipient");
                    Send(room, resyncPeer, "checkpoint_required"); break;
                case "depart_begin":
                    try { BeginDeparture(room,peer,message); }
                    catch(ApiFailure ex) when(ex.Status is 409 or 429)
                    {Send(room,peer,"depart_failed",new {operationId=message.OperationId,reason=ex.Code});}
                    break;
                case "depart_abort":
                    RequireHost(room,peer,message.Epoch);
                    if(room.Departure?.OperationId==message.OperationId)AbortDeparture(room,"cancelled");
                    break;
                default: throw new ApiFailure(400, "unknown_message");
            }
        }
    }
    private static void TrimConfirmedInputs(RoomState room)
    {
        // Existing peers already received these commands through their ordered FIFO.
        // A live newcomer MUST start from a fresh host snapshot; the cloud save alone
        // cannot reconstruct this interval. Keep every not-yet-committed command.
        room.Tail.RemoveAll(command => command.Sequence <= room.ConfirmedSequence);
    }
    private void RequireHost(RoomState room, RoomPeer peer, long epoch)
    {
        if (room.HostId != peer.Identity.Id || epoch != room.Epoch || room.LeaseUntil <= time.GetUtcNow())
            throw new ApiFailure(409, "host_lease_required");
    }
    public void RelayBulk(Identity identity, string id, string connection, byte[] data)
    {
        lock (_gate)
        {
            var room = Get(id); var peer = Member(identity, id);
            if (peer.BulkConnectionId != connection) throw new ApiFailure(409, "stale_connection");
            if (data.Length is < 32 or > Protocol.MaxBulkBytes || data[0] != 'S' || data[1] != 'F' || data[2] != 'O' || data[3] != Protocol.Version)
                throw new ApiFailure(400, "bulk_header");
            var epoch = System.Buffers.Binary.BinaryPrimitives.ReadInt64LittleEndian(data.AsSpan(8));
            var tick = System.Buffers.Binary.BinaryPrimitives.ReadInt64LittleEndian(data.AsSpan(16));
            var kind = data[4];
            if (epoch != room.Epoch) throw new ApiFailure(409, "stale_epoch");
            if (kind is < 1 or > 3 || data.Length > 48 * 1024 + (kind == 1 ? 32 : 48)) throw new ApiFailure(400, "bulk_kind");
            var transferId = System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(data.AsSpan(24));
            var part = System.Buffers.Binary.BinaryPrimitives.ReadUInt16LittleEndian(data.AsSpan(28));
            var parts = System.Buffers.Binary.BinaryPrimitives.ReadUInt16LittleEndian(data.AsSpan(30));
            if (transferId == 0 || parts == 0 || parts > (kind == 2 ? 11 : 1366) || part >= parts || data[5] != 0 || data[6] != 0 || data[7] != 0)
                throw new ApiFailure(400, "bulk_chunks");
            var now = time.GetUtcNow();
            if (now - peer.BulkWindow >= TimeSpan.FromSeconds(1)) { peer.BulkWindow = now; peer.BulkBytes = 0; }
            peer.BulkBytes += data.Length;
            if (peer.BulkBytes > (room.HostId == identity.Id ? 16 : 1) * 1024 * 1024) throw new ApiFailure(429, "bulk_rate");
            if (kind != 1)
            {
                if (data.Length < 49 || !room.Checkpoints.Contains(tick)) throw new ApiFailure(400, "unscheduled_checkpoint");
                var target = new Guid(data.AsSpan(32, 16)).ToString("N");
                RoomPeer recipient;
                if (kind == 2)
                {
                    if (target != identity.Id || room.HostId == null || room.HostId == identity.Id)
                        throw new ApiFailure(403, "digest_sender");
                    recipient = room.Peers[room.HostId];
                }
                else
                {
                    RequireHost(room, peer, epoch);
                    if (target == identity.Id || !room.Peers.TryGetValue(target, out recipient!)) throw new ApiFailure(403, "repair_recipient");
                }
                if (recipient.BulkConnectionId == null || !recipient.Bulk.Writer.TryWrite(data))
                    throw new ApiFailure(429, "state_consumer_capacity");
                return;
            }
            RequireHost(room, peer, epoch);
            if (tick < 0 || tick > room.Tick) throw new ApiFailure(400, "bulk_tick");
            foreach (var other in room.Peers.Values.Where(x => x.Identity.Id != identity.Id && x.BulkConnectionId != null))
                if (!other.Bulk.Writer.TryWrite(data)) { other.Ready = false; Send(room, other, "checkpoint_required"); }
        }
    }
    public SnapshotRecord Save(Identity identity, string id, SnapshotUpload request, byte[] payload, bool finalSave)
    {
        lock (_gate)
        {
            var room = Get(id); var peer = Member(identity, id); RequireHost(room, peer, request.Epoch);
            if(request.ExpectedRevision!=room.Revision)
            {
                var latest=store.Versions(id).FirstOrDefault();
                if(latest!=null)Send(room,peer,"saved",latest); // Latest durable revision, not a new save.
                throw new ApiFailure(409,"snapshot_revision_conflict");
            }
            var atCurrent = request.Tick == room.Tick && request.Sequence == room.ConfirmedSequence;
            var atRetained = room.Commits.TryGetValue(request.Tick, out var commit) && commit.Sequence == request.Sequence;
            if (!atCurrent && !atRetained) throw new ApiFailure(409, "snapshot_tick");
            if (finalSave) throw new ApiFailure(409, "departure_required");
            if(room.Departure!=null)throw new ApiFailure(409,"departure_in_progress");
            if (!finalSave && time.GetUtcNow() - room.LastSnapshot < TimeSpan.FromSeconds(60)) throw new ApiFailure(429, "autosave_interval");
            var result = store.Save(id, request, payload, time.GetUtcNow());
            room.Revision = result.Revision; room.LastSnapshot = time.GetUtcNow();
            room.DirtySince = result.Tick == room.Tick && result.Sequence == room.ConfirmedSequence ? null : time.GetUtcNow();
            room.Tail.RemoveAll(x => x.Sequence <= result.Sequence);
            foreach (var tick in room.Commits.Keys.Where(x => x <= result.Tick).ToArray()) room.Commits.Remove(tick);
            Broadcast(room, "saved", result); return result;
        }
    }
    public SnapshotRecord Latest(Identity identity, string id)
    { lock (_gate) { Member(identity, id); return store.Versions(id).FirstOrDefault() ?? throw new ApiFailure(404, "no_snapshot"); } }
    public void Disconnect(Identity identity, string id, string connection, bool bulk)
    {
        lock (_gate)
        {
            var room = Get(id);
            if (!room.Peers.TryGetValue(identity.Id, out var peer)) return;
            if (bulk) { if (peer.BulkConnectionId == connection) peer.BulkConnectionId = null; return; }
            if (peer.ConnectionId != connection) return;
            peer.Events.Writer.TryComplete(); peer.Bulk.Writer.TryComplete(); room.Peers.Remove(identity.Id);
            if (room.HostId == identity.Id) LoseHost(room);
            Broadcast(room, "presence", ViewOf(room));
        }
    }
    public void Kick(Identity owner, string worldId, string player)
    {
        lock (_gate)
        {
            var room = Get(worldId);
            if (room.World.OwnerId != owner.Id || player == owner.Id || !Protocol.ValidId(player)) throw new ApiFailure(403, "owner_required");
            store.Block(worldId, player);
            if (room.Peers.TryGetValue(player, out var peer))
            {
                peer.Events.Writer.TryComplete(); peer.Bulk.Writer.TryComplete(); room.Peers.Remove(player);
                if (room.HostId == player) LoseHost(room);
            }
        }
    }
    public void Sweep() { lock (_gate) SweepLocked(); }
    private void SweepLocked()
    {
        var now = time.GetUtcNow();
        foreach (var room in _rooms.Values)
        {
            foreach (var peer in room.Peers.Values.Where(x => x.ConnectionId == null && now - x.Admitted > TimeSpan.FromSeconds(30)).ToArray())
                room.Peers.Remove(peer.Identity.Id);
            if (room.HostId != null && room.LeaseUntil <= now) LoseHost(room);
            if (room.RecoveryUntil != null && now >= room.RecoveryUntil) RecoverDurable(room);
            if(room.Departure!=null&&now>=room.Departure.Deadline)AbortDeparture(room,"timeout");
            if (room.Departure==null&&room.DirtySince != null && room.HostId != null && now - room.LastSnapshot >= TimeSpan.FromSeconds(60) &&
                now - room.LastSaveRequest >= TimeSpan.FromSeconds(5))
            { room.LastSaveRequest = now; Send(room, room.Peers[room.HostId], "save_due"); }
        }
    }
    private void LoseHost(RoomState room)
    {
        var old = room.HostId; room.HostId = null; room.Epoch++;
        room.Departure=null;
        room.Checkpoints.Clear();
        if (old != null && room.Peers.TryGetValue(old, out var oldPeer)) oldPeer.Ready = false;
        Broadcast(room, "paused", new { reason = "host_lost" });
        room.RecoveryUntil = time.GetUtcNow().AddSeconds(2);
        Elect(room);
        if (room.HostId == null && room.Peers.Count == 0) RecoverDurable(room);
    }
    private void RecoverDurable(RoomState room)
    {
        if (room.HostId == null) {
            var durable = store.Versions(room.World.Id).FirstOrDefault();
            var lostTick = room.Tick;
            room.Tick = durable?.Tick ?? 0; room.Sequence = room.ConfirmedSequence = durable?.Sequence ?? 0;
            room.Tail.Clear(); room.Commits.Clear(); room.RecoveryUntil = null;
            foreach (var peer in room.Peers.Values) peer.Ready = false;
            Broadcast(room, "restore_required", new { snapshot = durable, lostThroughTick = lostTick });
        }
    }
    private void Elect(RoomState room)
    {
        if (room.HostId != null) return;
        var candidate = room.Peers.Values.Where(x => x.ConnectionId != null && x.Ready && x.AcknowledgedSequence == room.ConfirmedSequence && x.AcknowledgedTick == room.Tick)
            .OrderBy(x => x.Admitted).ThenBy(x => x.Identity.Id).FirstOrDefault();
        if (candidate == null) return;
        room.HostId = candidate.Identity.Id; room.LeaseUntil = time.GetUtcNow().AddSeconds(8); room.RecoveryUntil = null;
        Broadcast(room, "host", new { participantId = room.HostId, leaseUntil = room.LeaseUntil, revision=room.Revision, revisionKnown=true });
    }
    private static void Send(RoomState room, RoomPeer peer, string type, object? data = null, long? sequence = null)
    {
        if (!peer.Events.Writer.TryWrite(new(type, room.World.Id, room.Epoch, room.Tick, sequence ?? room.ConfirmedSequence, data)))
        { peer.Ready = false; peer.Events.Writer.TryComplete(new IOException("Slow consumer")); }
    }
    private static void Broadcast(RoomState room, string type, object? data = null, long? sequence = null)
    { foreach (var peer in room.Peers.Values.Where(x => x.ConnectionId != null)) Send(room, peer, type, data, sequence); }
}
