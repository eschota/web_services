using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Security.Cryptography;
using System.Text;

namespace SandFlow.Protocol
{
    public sealed class TileDigestField
    { public string Name=""; public int Stride; public bool Unsigned; public ulong[] Hashes=Array.Empty<ulong>(); }
    public sealed class TileDigest
    { public WorldSnapshot Cursor=new WorldSnapshot(); public byte[] ModuleHash=Array.Empty<byte>(); public List<TileDigestField> Fields=new List<TileDigestField>(); }
    public sealed class TilePatch
    { public string Field=""; public int Index; public byte[] Data=Array.Empty<byte>(); }
    public sealed class TileRepair
    {
        public WorldSnapshot Cursor=new WorldSnapshot();
        public byte[] BaselineDigest=Array.Empty<byte>();
        public List<TilePatch> Tiles=new List<TilePatch>();
        public string MetadataJson;
        public List<SnapshotSection> Sections=new List<SnapshotSection>();
    }

    /// <summary>Historical 32x32 repairs. This codec never writes into a running/current GPU world.</summary>
    public static class TileStateProtocol
    {
        public const int TileSize=32;
        public const double AbsolutePrecision=1e-4;
        public const int MaximumDigestBytes=512*1024;
        private static readonly UTF8Encoding Utf8=new UTF8Encoding(false,true);
        private static readonly byte[] DigestMagic=Encoding.ASCII.GetBytes("SFDIG001");
        private static readonly byte[] RepairMagic=Encoding.ASCII.GetBytes("SFREPR01");
        [ThreadStatic] private static TileHashWorkspace tileHashWorkspace;
        private sealed class TileHashWorkspace
        {
            // 32*32 cells * maximum four 32-bit components * nine encoded bytes per float.
            public readonly byte[] Encoded=new byte[TileSize*TileSize*4*9];
            public readonly byte[] Digest=new byte[32];
            public readonly SHA256 Sha=SHA256.Create();
        }
        public static TileDigest CreateDigest(WorldSnapshot snapshot)
        {
            SnapshotCodec.Validate(snapshot);
            var digest=new TileDigest{Cursor=Cursor(snapshot),ModuleHash=ModuleHash(snapshot)};
            var count=TileCount(snapshot);
            foreach(var field in snapshot.Fields)
            {
                var item=new TileDigestField{Name=field.Name,Stride=field.Stride,Unsigned=field.Unsigned,Hashes=new ulong[count]};
                for(var tile=0;tile<count;tile++)item.Hashes[tile]=HashTile(snapshot,field,tile);
                digest.Fields.Add(item);
            }
            return digest;
        }
        public static TileRepair CreateRepair(WorldSnapshot authority,TileDigest baseline)
        {
            SnapshotCodec.Validate(authority);ValidateDigest(baseline);RequireSameCursor(authority,baseline.Cursor);
            if(authority.Fields.Count!=baseline.Fields.Count)throw new InvalidDataException("Repair field layout mismatch.");
            var repair=new TileRepair{Cursor=Cursor(authority),BaselineDigest=Hash(EncodeDigest(baseline))};
            foreach(var field in authority.Fields)
            {
                var summary=baseline.Fields.SingleOrDefault(x=>x.Name==field.Name);
                if(summary==null||summary.Stride!=field.Stride||summary.Unsigned!=field.Unsigned)throw new InvalidDataException("Repair field layout mismatch.");
                for(var tile=0;tile<summary.Hashes.Length;tile++)
                    if(HashTile(authority,field,tile)!=summary.Hashes[tile])repair.Tiles.Add(new TilePatch{Field=field.Name,Index=tile,Data=ReadTile(authority,field,tile)});
            }
            if(!ModuleHash(authority).SequenceEqual(baseline.ModuleHash))
            {repair.MetadataJson=authority.MetadataJson;foreach(var section in authority.Sections)repair.Sections.Add(Clone(section));}
            return repair;
        }
        /// <summary>Returns a new corrected historical checkpoint; the retained baseline stays immutable.</summary>
        public static WorldSnapshot ApplyRepair(WorldSnapshot baseline,TileRepair repair)
        {
            SnapshotCodec.Validate(baseline);RequireSameCursor(baseline,repair.Cursor);
            if(repair.BaselineDigest==null||!Hash(EncodeDigest(CreateDigest(baseline))).SequenceEqual(repair.BaselineDigest))
                throw new InvalidDataException("Repair baseline digest mismatch.");
            var result=Cursor(baseline);result.MetadataJson=baseline.MetadataJson;
            foreach(var field in baseline.Fields)result.Fields.Add(new SnapshotField{Name=field.Name,Stride=field.Stride,Unsigned=field.Unsigned,Data=(byte[])field.Data.Clone()});
            foreach(var section in baseline.Sections)result.Sections.Add(Clone(section));
            var seen=new HashSet<string>(StringComparer.Ordinal);
            if(repair.Tiles==null||repair.Tiles.Count>64*TileCount(baseline))throw new InvalidDataException("Repair tile count.");
            foreach(var patch in repair.Tiles)
            {
                if(patch==null||!seen.Add(patch.Field+":"+patch.Index))throw new InvalidDataException("Duplicate repair tile.");
                var field=result.Fields.SingleOrDefault(x=>x.Name==patch.Field)??throw new InvalidDataException("Unknown repair field.");
                TileBounds(result,patch.Index,out var x,out var z,out var width,out var height);
                if(patch.Data==null||patch.Data.Length!=width*height*field.Stride)throw new InvalidDataException("Repair tile shape.");
                for(var row=0;row<height;row++)Buffer.BlockCopy(patch.Data,row*width*field.Stride,field.Data,((z+row)*result.ResX+x)*field.Stride,width*field.Stride);
            }
            if(repair.MetadataJson!=null)
            {
                result.MetadataJson=repair.MetadataJson;result.Sections.Clear();
                if(repair.Sections==null)throw new InvalidDataException("Repair sections.");
                foreach(var section in repair.Sections)result.Sections.Add(Clone(section));
            }
            else if(repair.Sections==null||repair.Sections.Count!=0)throw new InvalidDataException("Repair module marker.");
            SnapshotCodec.Validate(result);return result;
        }
        public static byte[] EncodeDigest(TileDigest digest)
        {
            ValidateDigest(digest);
            using(var stream=new MemoryStream())using(var writer=new BinaryWriter(stream,Utf8,true))
            {
                writer.Write(DigestMagic);WriteCursor(writer,digest.Cursor);writer.Write(digest.ModuleHash);writer.Write(digest.Fields.Count);
                foreach(var field in digest.Fields){WriteText(writer,field.Name,64);writer.Write(field.Stride);writer.Write(field.Unsigned);writer.Write(field.Hashes.Length);foreach(var hash in field.Hashes)writer.Write(hash);}
                writer.Flush();if(stream.Length>MaximumDigestBytes)throw new InvalidDataException("Digest size.");return stream.ToArray();
            }
        }
        public static TileDigest DecodeDigest(byte[] bytes)
        {
            if(bytes==null||bytes.Length<8||bytes.Length>MaximumDigestBytes)throw new InvalidDataException("Digest size.");
            using(var stream=new MemoryStream(bytes,false))using(var reader=new BinaryReader(stream,Utf8,true))
            {
                RequireMagic(reader,DigestMagic);var digest=new TileDigest{Cursor=ReadCursor(reader),ModuleHash=Exact(reader,32)};
                var count=reader.ReadInt32();if(count<1||count>64)throw new InvalidDataException("Digest fields.");
                for(var i=0;i<count;i++)
                {
                    var field=new TileDigestField{Name=ReadText(reader,64),Stride=reader.ReadInt32(),Unsigned=ReadBool(reader)};
                    var tiles=reader.ReadInt32();if(tiles!=TileCount(digest.Cursor)||tiles*8L>stream.Length-stream.Position)throw new InvalidDataException("Digest tile count.");
                    field.Hashes=new ulong[tiles];for(var j=0;j<tiles;j++)field.Hashes[j]=reader.ReadUInt64();digest.Fields.Add(field);
                }
                if(stream.Position!=stream.Length)throw new InvalidDataException("Digest trailing bytes.");ValidateDigest(digest);return digest;
            }
        }
        public static byte[] EncodeRepair(TileRepair repair)
        {
            SnapshotCodec.ValidateHeader(repair.Cursor);
            if(repair.BaselineDigest==null||repair.BaselineDigest.Length!=32||repair.Tiles==null||repair.Tiles.Count>64*TileCount(repair.Cursor))throw new InvalidDataException("Repair header.");
            byte[] raw;
            using(var stream=new MemoryStream())using(var writer=new BinaryWriter(stream,Utf8,true))
            {
                WriteCursor(writer,repair.Cursor);writer.Write(repair.BaselineDigest);writer.Write(repair.Tiles.Count);
                foreach(var tile in repair.Tiles)
                {
                    if(tile==null||tile.Data==null||tile.Data.Length<4||tile.Data.Length>TileSize*TileSize*16)throw new InvalidDataException("Repair tile size.");
                    TileBounds(repair.Cursor,tile.Index,out _,out _,out _,out _);
                    WriteText(writer,tile.Field,64);writer.Write(tile.Index);writer.Write(tile.Data.Length);writer.Write(tile.Data);
                    if(stream.Length>SnapshotCodec.MaximumDecodedBytes)throw new InvalidDataException("Repair size.");
                }
                writer.Write(repair.MetadataJson!=null);
                if(repair.MetadataJson!=null)
                {
                    WriteText(writer,repair.MetadataJson,SnapshotCodec.MaximumMetadataBytes);
                    if(repair.Sections==null||repair.Sections.Count>32)throw new InvalidDataException("Repair sections.");
                    writer.Write(repair.Sections.Count);
                    foreach(var section in repair.Sections)
                    {
                        if(section==null||section.Data==null||section.Data.Length<1||section.Data.Length>SnapshotCodec.MaximumSectionBytes)throw new InvalidDataException("Repair section size.");
                        WriteText(writer,section.Name,64);writer.Write(section.Data.Length);writer.Write(section.Data);
                        if(stream.Length>SnapshotCodec.MaximumDecodedBytes)throw new InvalidDataException("Repair size.");
                    }
                }
                else if(repair.Sections==null||repair.Sections.Count!=0)throw new InvalidDataException("Repair module marker.");
                writer.Flush();raw=stream.ToArray();
            }
            return PackRepair(raw);
        }
        public static TileRepair DecodeRepair(byte[] bytes)
        {
            var raw=UnpackRepair(bytes);
            using(var stream=new MemoryStream(raw,false))using(var reader=new BinaryReader(stream,Utf8,true))
            {
                var repair=new TileRepair{Cursor=ReadCursor(reader),BaselineDigest=Exact(reader,32)};
                var count=reader.ReadInt32();if(count<0||count>64*TileCount(repair.Cursor))throw new InvalidDataException("Repair tile count.");
                var seen=new HashSet<string>();
                for(var i=0;i<count;i++)
                {
                    var patch=new TilePatch{Field=ReadText(reader,64),Index=reader.ReadInt32()};TileBounds(repair.Cursor,patch.Index,out _,out _,out _,out _);
                    if(!seen.Add(patch.Field+":"+patch.Index))throw new InvalidDataException("Duplicate repair tile.");
                    var length=reader.ReadInt32();if(length<4||length>TileSize*TileSize*16||length>stream.Length-stream.Position)throw new InvalidDataException("Repair tile size.");
                    patch.Data=Exact(reader,length);repair.Tiles.Add(patch);
                }
                if(ReadBool(reader))
                {
                    repair.MetadataJson=ReadText(reader,SnapshotCodec.MaximumMetadataBytes);
                    count=reader.ReadInt32();if(count<0||count>32)throw new InvalidDataException("Repair sections.");
                    var names=new HashSet<string>();
                    for(var i=0;i<count;i++)
                    {
                        var name=ReadText(reader,64);var length=reader.ReadInt32();
                        if(!names.Add(name)||length<1||length>SnapshotCodec.MaximumSectionBytes||length>stream.Length-stream.Position)throw new InvalidDataException("Repair section size.");
                        repair.Sections.Add(new SnapshotSection{Name=name,Data=Exact(reader,length)});
                    }
                }
                if(stream.Position!=stream.Length)throw new InvalidDataException("Repair trailing bytes.");return repair;
            }
        }
        public static int TileCount(WorldSnapshot snapshot)=>((snapshot.ResX+TileSize-1)/TileSize)*((snapshot.ResZ+TileSize-1)/TileSize);
        private static void TileBounds(WorldSnapshot snapshot,int index,out int x,out int z,out int width,out int height)
        {
            if(index<0||index>=TileCount(snapshot))throw new InvalidDataException("Repair tile index.");
            var columns=(snapshot.ResX+TileSize-1)/TileSize;x=(index%columns)*TileSize;z=(index/columns)*TileSize;
            width=Math.Min(TileSize,snapshot.ResX-x);height=Math.Min(TileSize,snapshot.ResZ-z);
        }
        private static byte[] ReadTile(WorldSnapshot snapshot,SnapshotField field,int tile)
        {
            TileBounds(snapshot,tile,out var x,out var z,out var width,out var height);var bytes=new byte[width*height*field.Stride];
            for(var row=0;row<height;row++)Buffer.BlockCopy(field.Data,((z+row)*snapshot.ResX+x)*field.Stride,bytes,row*width*field.Stride,width*field.Stride);
            return bytes;
        }
        private static ulong HashTile(WorldSnapshot snapshot,SnapshotField field,int tile)
        {
            TileBounds(snapshot,tile,out var x,out var z,out var width,out var height);
            var workspace=tileHashWorkspace??(tileHashWorkspace=new TileHashWorkspace());
            var encoded=workspace.Encoded;var encodedOffset=0;
            for(var row=0;row<height;row++)
            {
                var sourceOffset=((z+row)*snapshot.ResX+x)*field.Stride;
                var sourceEnd=sourceOffset+width*field.Stride;
                for(var i=sourceOffset;i<sourceEnd;i+=4)
                {
                    var bits=BinaryPrimitives.ReadUInt32LittleEndian(field.Data.AsSpan(i,4));
                    if(field.Unsigned)
                    {
                        BinaryPrimitives.WriteUInt32LittleEndian(encoded.AsSpan(encodedOffset,4),bits);
                        encodedOffset+=4;
                    }
                    else
                    {
                        var value=BitConverter.Int32BitsToSingle(unchecked((int)bits));
                        // Conservative quantization: same bucket differs by <=1e-4
                        // below 128, or <1e-6 relative above it. Boundary false positives
                        // request extra data, never loosen the reconstruction tolerance.
                        if(Math.Abs(value)<128)
                        {
                            encoded[encodedOffset++]=0;
                            BinaryPrimitives.WriteInt64LittleEndian(encoded.AsSpan(encodedOffset,8),
                                (long)Math.Round(value/AbsolutePrecision,MidpointRounding.ToEven));
                            encodedOffset+=8;
                        }
                        else
                        {
                            encoded[encodedOffset++]=1;
                            BinaryPrimitives.WriteUInt32LittleEndian(encoded.AsSpan(encodedOffset,4),bits&~7u);
                            encodedOffset+=4;
                        }
                    }
                }
            }
            if(!workspace.Sha.TryComputeHash(encoded.AsSpan(0,encodedOffset),workspace.Digest,out var written)||written!=32)
                throw new CryptographicException("Tile SHA-256 failed.");
            return BinaryPrimitives.ReadUInt64LittleEndian(workspace.Digest.AsSpan(0,8));
        }
        private static byte[] ModuleHash(WorldSnapshot snapshot)
        {
            using(var stream=new MemoryStream())using(var writer=new BinaryWriter(stream,Utf8,true))
            {WriteText(writer,snapshot.MetadataJson,SnapshotCodec.MaximumMetadataBytes);foreach(var section in snapshot.Sections.OrderBy(x=>x.Name,StringComparer.Ordinal)){WriteText(writer,section.Name,64);writer.Write(section.Data.Length);writer.Write(section.Data);}writer.Flush();return Hash(stream.ToArray());}
        }
        private static void ValidateDigest(TileDigest digest)
        {
            if(digest==null)throw new InvalidDataException("Digest missing.");SnapshotCodec.ValidateHeader(digest.Cursor);
            if(digest.ModuleHash==null||digest.ModuleHash.Length!=32||digest.Fields==null||digest.Fields.Count<1||digest.Fields.Count>64)throw new InvalidDataException("Digest shape.");
            var names=new HashSet<string>();foreach(var field in digest.Fields)
                if(field==null||string.IsNullOrWhiteSpace(field.Name)||Utf8.GetByteCount(field.Name)>64||!names.Add(field.Name)||field.Stride<4||field.Stride>16||field.Stride%4!=0||field.Hashes==null||field.Hashes.Length!=TileCount(digest.Cursor))throw new InvalidDataException("Digest field shape.");
        }
        private static void RequireSameCursor(WorldSnapshot a,WorldSnapshot b)
        {
            SnapshotCodec.ValidateHeader(a);SnapshotCodec.ValidateHeader(b);
            if(a.WorldId!=b.WorldId||a.Epoch!=b.Epoch||a.Tick!=b.Tick||a.Sequence!=b.Sequence||a.SimTime!=b.SimTime||a.FixedDt!=b.FixedDt||a.ResX!=b.ResX||a.ResZ!=b.ResZ||a.CellSize!=b.CellSize||a.OriginX!=b.OriginX||a.OriginY!=b.OriginY||a.OriginZ!=b.OriginZ)throw new InvalidDataException("Repair historical cursor mismatch.");
        }
        private static WorldSnapshot Cursor(WorldSnapshot s)=>new WorldSnapshot{WorldId=s.WorldId,Epoch=s.Epoch,Tick=s.Tick,Sequence=s.Sequence,SimTime=s.SimTime,FixedDt=s.FixedDt,ResX=s.ResX,ResZ=s.ResZ,CellSize=s.CellSize,OriginX=s.OriginX,OriginY=s.OriginY,OriginZ=s.OriginZ};
        private static SnapshotSection Clone(SnapshotSection s)=>new SnapshotSection{Name=s.Name,Data=(byte[])s.Data.Clone()};
        private static void WriteCursor(BinaryWriter writer,WorldSnapshot s)
        {WriteText(writer,s.WorldId,32);writer.Write(s.Epoch);writer.Write(s.Tick);writer.Write(s.Sequence);writer.Write(s.SimTime);writer.Write(s.FixedDt);writer.Write(s.ResX);writer.Write(s.ResZ);writer.Write(s.CellSize);writer.Write(s.OriginX);writer.Write(s.OriginY);writer.Write(s.OriginZ);}
        private static WorldSnapshot ReadCursor(BinaryReader reader)
        {var s=new WorldSnapshot{WorldId=ReadText(reader,32),Epoch=reader.ReadInt64(),Tick=reader.ReadInt64(),Sequence=reader.ReadInt64(),SimTime=reader.ReadDouble(),FixedDt=reader.ReadSingle(),ResX=reader.ReadInt32(),ResZ=reader.ReadInt32(),CellSize=reader.ReadSingle(),OriginX=reader.ReadSingle(),OriginY=reader.ReadSingle(),OriginZ=reader.ReadSingle()};SnapshotCodec.ValidateHeader(s);return s;}
        private static byte[] PackRepair(byte[] raw)
        {
            if(raw.Length>SnapshotCodec.MaximumDecodedBytes)throw new InvalidDataException("Repair decoded size.");
            using(var payload=new MemoryStream())
            {
                using(var deflate=new DeflateStream(payload,CompressionLevel.Fastest,true))deflate.Write(raw,0,raw.Length);
                using(var result=new MemoryStream())using(var writer=new BinaryWriter(result,Utf8,true))
                {writer.Write(RepairMagic);writer.Write(1);writer.Write(raw.Length);writer.Write((int)payload.Length);writer.Write(Hash(raw));writer.Write(payload.ToArray());writer.Flush();return result.ToArray();}
            }
        }
        private static byte[] UnpackRepair(byte[] bytes)
        {
            if(bytes==null||bytes.Length<52||bytes.Length>SnapshotCodec.MaximumEncodedBytes)throw new InvalidDataException("Repair encoded size.");
            using(var input=new MemoryStream(bytes,false))using(var reader=new BinaryReader(input,Utf8,true))
            {
                RequireMagic(reader,RepairMagic);if(reader.ReadInt32()!=1)throw new InvalidDataException("Repair version.");
                var rawLength=reader.ReadInt32();var length=reader.ReadInt32();
                if(rawLength<1||rawLength>SnapshotCodec.MaximumDecodedBytes||length!=bytes.Length-52)throw new InvalidDataException("Repair declared size.");
                var checksum=Exact(reader,32);var raw=new byte[rawLength];
                using(var deflate=new DeflateStream(input,CompressionMode.Decompress,true))
                {var offset=0;while(offset<raw.Length){var n=deflate.Read(raw,offset,raw.Length-offset);if(n==0)throw new InvalidDataException("Truncated repair.");offset+=n;}if(deflate.ReadByte()!=-1)throw new InvalidDataException("Repair expansion limit.");}
                if(!Hash(raw).SequenceEqual(checksum))throw new InvalidDataException("Repair checksum.");return raw;
            }
        }
        private static byte[] Hash(byte[] bytes){using(var sha=SHA256.Create())return sha.ComputeHash(bytes);}
        private static byte[] Exact(BinaryReader reader,int count){var bytes=reader.ReadBytes(count);if(bytes.Length!=count)throw new InvalidDataException("Truncated tile payload.");return bytes;}
        private static bool ReadBool(BinaryReader reader){var value=reader.ReadByte();if(value>1)throw new InvalidDataException("Tile boolean.");return value!=0;}
        private static void RequireMagic(BinaryReader reader,byte[] magic){if(!Exact(reader,magic.Length).SequenceEqual(magic))throw new InvalidDataException("Tile payload magic.");}
        private static void WriteText(BinaryWriter writer,string text,int max){if(text==null)throw new InvalidDataException("Tile text.");var bytes=Utf8.GetBytes(text);if(bytes.Length>max)throw new InvalidDataException("Tile text size.");writer.Write(bytes.Length);writer.Write(bytes);}
        private static string ReadText(BinaryReader reader,int max){var count=reader.ReadInt32();if(count<0||count>max)throw new InvalidDataException("Tile text size.");return Utf8.GetString(Exact(reader,count));}
    }
}
