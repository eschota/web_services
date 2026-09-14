using System.Security.Cryptography;

namespace SandFlow.Server;

public sealed partial class Rooms
{
    public void RequireSnapshotWriter(Identity identity,string world)
    {lock(_gate){var room=Get(world);RequireHost(room,Member(identity,world),room.Epoch);}}
    public RoomPeer VoiceMember(Identity identity,string world)
    {
        lock(_gate)
        {
            var peer=Member(identity,world);
            // Rewinding/handoff changes physics readiness, not admitted voice access.
            if(peer.ConnectionId==null)throw new ApiFailure(409,"active_membership_required");
            return peer;
        }
    }
    private void BeginDeparture(RoomState room,RoomPeer peer,ControlMessage message)
    {
        RequireHost(room,peer,message.Epoch);
        if(message.OperationId==null||!Protocol.ValidId(message.OperationId))throw new ApiFailure(400,"departure_identity");
        room.LeaseUntil=time.GetUtcNow().AddSeconds(8);
        if(room.Departure!=null)
        {
            if(room.Departure.OperationId!=message.OperationId)throw new ApiFailure(409,"departure_in_progress");
            SendDeparturePrepared(room,peer);return;
        }
        if(message.Tick!=room.Tick||message.Sequence!=room.ConfirmedSequence)throw new ApiFailure(409,"departure_cursor");
        if(time.GetUtcNow()-room.LastDepartureAttempt<TimeSpan.FromSeconds(10))throw new ApiFailure(429,"departure_rate");
        if(store.FindDeparture(room.World.Id,message.OperationId,time.GetUtcNow())!=null)throw new ApiFailure(409,"departure_already_completed");
        room.LastDepartureAttempt=time.GetUtcNow();
        var finalSteps=(room.Sequence-room.ConfirmedSequence+255)/256;
        room.Departure=new(message.OperationId,peer.Identity.Id,room.Epoch,room.Tick+finalSteps,room.Sequence,time.GetUtcNow().AddSeconds(60));
        foreach(var other in room.Peers.Values.Where(x=>x.Identity.Id!=peer.Identity.Id))other.Ready=false;
        Broadcast(room,"depart_pending",new {operationId=message.OperationId,participantId=peer.Identity.Id});
        SendDeparturePrepared(room,peer);
    }
    private static void SendDeparturePrepared(RoomState room,RoomPeer peer)
    {
        var departure=room.Departure!;
        Send(room,peer,"depart_prepared",new {operationId=departure.OperationId,tick=departure.Tick,sequence=departure.Sequence,deadlineUnix=departure.Deadline.ToUnixTimeSeconds()});
    }
    private static void ValidateDepartureCommit(RoomState room,long tick,long sequence)
    {
        if(room.Departure is { } d&&(tick>d.Tick||sequence>d.Sequence||(tick==d.Tick&&sequence!=d.Sequence)))throw new ApiFailure(409,"departure_fence");
    }
    private static void AbortDeparture(RoomState room,string reason)
    {
        var operation=room.Departure?.OperationId;room.Departure=null;
        Broadcast(room,"depart_aborted",new {operationId=operation,reason});
    }
    public DepartureReceipt? DepartureStatus(Identity identity,string world,string operation)
    {
        var receipt=store.FindDeparture(world,operation,time.GetUtcNow());
        if(receipt!=null&&receipt.ParticipantId!=identity.Id)throw new ApiFailure(403,"departure_owner_required");
        return receipt;
    }
    public DepartureReceipt CompleteDeparture(Identity identity,string world,string operation,SnapshotUpload upload,byte[] payload)
    {
        lock(_gate)
        {
            var prior=DepartureStatus(identity,world,operation);
            if(prior!=null)
            {
                var saved=prior.Snapshot;
                if(saved.Epoch!=upload.Epoch||saved.Tick!=upload.Tick||saved.Sequence!=upload.Sequence||prior.ExpectedRevision!=upload.ExpectedRevision||
                    !saved.Sha256.Equals(upload.Sha256,StringComparison.OrdinalIgnoreCase)||!saved.Sha256.Equals(Convert.ToHexString(SHA256.HashData(payload)),StringComparison.OrdinalIgnoreCase))
                    throw new ApiFailure(409,"departure_retry_mismatch");
                return prior;
            }
            var room=Get(world);var peer=Member(identity,world);RequireHost(room,peer,upload.Epoch);
            var departure=room.Departure;
            if(departure==null||departure.OperationId!=operation||departure.ParticipantId!=identity.Id||time.GetUtcNow()>=departure.Deadline)
                throw new ApiFailure(409,"departure_required");
            if(upload.Tick!=departure.Tick||upload.Sequence!=departure.Sequence||room.Tick!=departure.Tick||room.ConfirmedSequence!=departure.Sequence)
                throw new ApiFailure(409,"departure_fence");
            var savedRecord=store.Save(world,upload,payload,time.GetUtcNow(),new(operation,identity.Id));
            room.Revision=savedRecord.Revision;room.LastSnapshot=time.GetUtcNow();room.DirtySince=null;
            var result=new DepartureReceipt(operation,identity.Id,upload.ExpectedRevision,savedRecord);
            // Save+receipt are durable BEFORE losing authority. An uncertain response
            // can be resolved after disconnect or process restart without another save.
            Disconnect(identity,world,peer.ConnectionId!,false);
            return result;
        }
    }
    public void LeaveParticipant(Identity identity,string world)
    {
        lock(_gate)
        {
            var room=Get(world);
            if(!room.Peers.TryGetValue(identity.Id,out var peer))return;
            if(room.HostId==identity.Id)throw new ApiFailure(409,"host_departure_required");
            if(room.HostId==null&&room.Peers.Count==1)throw new ApiFailure(409,"world_synchronizing");
            if(peer.ConnectionId!=null)Disconnect(identity,world,peer.ConnectionId,false);
            else room.Peers.Remove(identity.Id);
        }
    }
}
