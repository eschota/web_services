using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace SandFlow.Protocol
{
    /// <summary>Bounded retained checkpoints owned by one world/authority generation.</summary>
    public sealed class CheckpointRing
    {
        private sealed class Entry { public WorldSnapshot Snapshot; public long Bytes; }
        private readonly SortedDictionary<long,Entry> entries=new SortedDictionary<long,Entry>();
        private readonly int maximumEntries;
        private readonly long maximumBytes;
        private string worldId;
        private long epoch;
        public long RetainedBytes { get; private set; }
        public int Count => entries.Count;
        public long? OldestTick => entries.Count==0?(long?)null:entries.First().Key;
        public CheckpointRing(int maximumEntries=3,long maximumBytes=96L*1024*1024)
        {
            if(maximumEntries<1||maximumEntries>8||maximumBytes<4096||maximumBytes>512L*1024*1024)throw new ArgumentOutOfRangeException();
            this.maximumEntries=maximumEntries;this.maximumBytes=maximumBytes;
        }
        public bool Add(WorldSnapshot snapshot)
        {
            SnapshotCodec.Validate(snapshot);
            if(worldId!=null&&(worldId!=snapshot.WorldId||epoch!=snapshot.Epoch))throw new InvalidDataException("Checkpoint ring generation mismatch.");
            if(entries.ContainsKey(snapshot.Tick))throw new InvalidDataException("Checkpoint tick already retained.");
            var bytes=Estimate(snapshot);
            if(bytes>maximumBytes)throw new InvalidDataException("Checkpoint exceeds retained-state memory budget.");
            // Out-of-order completion older than a full window is unnecessary. Do not
            // evict useful newer states to admit a late callback that will be evicted itself.
            if(entries.Count>0&&snapshot.Tick<entries.First().Key&&(entries.Count>=maximumEntries||RetainedBytes+bytes>maximumBytes))return false;
            while(entries.Count>=maximumEntries||RetainedBytes+bytes>maximumBytes)RemoveOldest();
            worldId=snapshot.WorldId;epoch=snapshot.Epoch;
            entries.Add(snapshot.Tick,new Entry{Snapshot=Copy(snapshot),Bytes=bytes});RetainedBytes+=bytes;return true;
        }
        /// <summary>A caller gets an independent copy; a patch cannot mutate another pending baseline.</summary>
        public bool TryGet(string requestedWorld,long requestedEpoch,long tick,out WorldSnapshot snapshot)
        {
            snapshot=null;
            if(worldId!=requestedWorld||epoch!=requestedEpoch||!entries.TryGetValue(tick,out var entry))return false;
            snapshot=Copy(entry.Snapshot);return true;
        }
        public void Clear(){entries.Clear();RetainedBytes=0;worldId=null;epoch=0;}
        private void RemoveOldest(){var first=entries.First();RetainedBytes-=first.Value.Bytes;entries.Remove(first.Key);}
        private static long Estimate(WorldSnapshot s)=>4096L+Encoding.UTF8.GetByteCount(s.MetadataJson)+
            s.Fields.Sum(x=>x.Data.LongLength+512)+s.Sections.Sum(x=>x.Data.LongLength+512);
        private static WorldSnapshot Copy(WorldSnapshot s)
        {
            var copy=new WorldSnapshot{WorldId=s.WorldId,Epoch=s.Epoch,Tick=s.Tick,Sequence=s.Sequence,SimTime=s.SimTime,FixedDt=s.FixedDt,
                ResX=s.ResX,ResZ=s.ResZ,CellSize=s.CellSize,OriginX=s.OriginX,OriginY=s.OriginY,OriginZ=s.OriginZ,MetadataJson=s.MetadataJson};
            foreach(var field in s.Fields)copy.Fields.Add(new SnapshotField{Name=field.Name,Stride=field.Stride,Unsigned=field.Unsigned,Data=(byte[])field.Data.Clone()});
            foreach(var section in s.Sections)copy.Sections.Add(new SnapshotSection{Name=section.Name,Data=(byte[])section.Data.Clone()});
            return copy;
        }
    }
}
