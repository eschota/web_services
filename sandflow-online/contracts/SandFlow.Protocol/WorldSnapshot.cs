using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Security.Cryptography;
using System.Text;

namespace SandFlow.Protocol
{
    [Serializable]
    public sealed class WorldSnapshot
    {
        public string WorldId = "";
        public long Epoch;
        public long Tick;
        public long Sequence;
        public double SimTime;
        public float FixedDt;
        public int ResX;
        public int ResZ;
        public float CellSize;
        public float OriginX;
        public float OriginY;
        public float OriginZ;
        public string MetadataJson = "{}";
        public List<SnapshotField> Fields = new List<SnapshotField>();
        public List<SnapshotSection> Sections = new List<SnapshotSection>();
    }
    [Serializable]
    public sealed class SnapshotField
    {
        public string Name = "";
        public int Stride = 4;
        public bool Unsigned;
        public byte[] Data = Array.Empty<byte>();
    }
    [Serializable]
    public sealed class SnapshotSection
    {
        public string Name = "";
        public byte[] Data = Array.Empty<byte>();
    }

    /// <summary>Portable lossless physical-field container; no Unity or server dependencies.</summary>
    public static class SnapshotCodec
    {
        public const int Version = 2;
        public const int MaximumSectionBytes = 8 * 1024 * 1024;
        public const int MaximumDecodedBytes = 64 * 1024 * 1024;
        public const int MaximumEncodedBytes = MaximumDecodedBytes + 1024 * 1024;
        public const int MaximumCells = 640 * 512;
        public const int MaximumMetadataBytes = 1024 * 1024;
        private static readonly byte[] Magic = Encoding.ASCII.GetBytes("SFSNAP01");
        private static readonly UTF8Encoding Utf8 = new UTF8Encoding(false, true);

        public static byte[] Encode(WorldSnapshot snapshot, int formatVersion = Version)
        {
            Validate(snapshot);
            if (formatVersion < 1 || formatVersion > Version || (formatVersion == 1 && snapshot.Sections.Count != 0))
                throw new InvalidDataException("Snapshot version cannot represent sections.");
            byte[] raw;
            using (var output = new MemoryStream())
            using (var writer = new BinaryWriter(output, Utf8, true))
            {
                WriteText(writer, snapshot.WorldId, 32);
                writer.Write(snapshot.Epoch); writer.Write(snapshot.Tick); writer.Write(snapshot.Sequence);
                writer.Write(snapshot.SimTime); writer.Write(snapshot.FixedDt);
                writer.Write(snapshot.ResX); writer.Write(snapshot.ResZ); writer.Write(snapshot.CellSize);
                writer.Write(snapshot.OriginX); writer.Write(snapshot.OriginY); writer.Write(snapshot.OriginZ);
                WriteText(writer, snapshot.MetadataJson, MaximumMetadataBytes);
                writer.Write(snapshot.Fields.Count);
                foreach (var field in snapshot.Fields)
                {
                    WriteText(writer, field.Name, 64); writer.Write(field.Stride); writer.Write(field.Unsigned);
                    writer.Write(field.Data.Length); writer.Write(field.Data);
                }
                if (formatVersion >= 2)
                {
                    writer.Write(snapshot.Sections.Count);
                    foreach (var section in snapshot.Sections)
                    { WriteText(writer, section.Name, 64); writer.Write(section.Data.Length); writer.Write(section.Data); }
                }
                writer.Flush();
                if (output.Length > MaximumDecodedBytes) throw new InvalidDataException("Snapshot decoded size limit.");
                raw = output.ToArray();
            }
            byte[] compressed;
            using (var output = new MemoryStream())
            {
                using (var deflate = new DeflateStream(output, CompressionLevel.Fastest, true)) deflate.Write(raw, 0, raw.Length);
                compressed = output.ToArray();
            }
            using (var output = new MemoryStream())
            using (var writer = new BinaryWriter(output, Utf8, true))
            using (var sha = SHA256.Create())
            {
                writer.Write(Magic); writer.Write(formatVersion); writer.Write(raw.Length); writer.Write(compressed.Length);
                writer.Write(sha.ComputeHash(raw)); writer.Write(compressed); writer.Flush();
                if (output.Length > MaximumEncodedBytes) throw new InvalidDataException("Snapshot encoded size limit.");
                return output.ToArray();
            }
        }

        public static WorldSnapshot Decode(byte[] bytes)
        {
            if (bytes == null || bytes.Length < 52 || bytes.Length > MaximumEncodedBytes) throw new InvalidDataException("Snapshot encoded size.");
            using (var input = new MemoryStream(bytes, false))
            using (var reader = new BinaryReader(input, Utf8, true))
            {
                var magic = Exact(reader, 8);
                for (var i = 0; i < magic.Length; i++) if (magic[i] != Magic[i]) throw new InvalidDataException("Snapshot magic.");
                var formatVersion = reader.ReadInt32();
                if (formatVersion < 1 || formatVersion > Version) throw new InvalidDataException("Snapshot version.");
                var decodedLength = reader.ReadInt32(); var compressedLength = reader.ReadInt32();
                if (decodedLength < 1 || decodedLength > MaximumDecodedBytes || compressedLength < 1 || compressedLength != bytes.Length - 52)
                    throw new InvalidDataException("Snapshot declared size.");
                var digest = Exact(reader, 32); byte[] raw;
                using (var deflate = new DeflateStream(input, CompressionMode.Decompress, true))
                {
                    raw = new byte[decodedLength]; var count = 0;
                    while (count < raw.Length)
                    { var n = deflate.Read(raw, count, raw.Length - count); if (n == 0) throw new InvalidDataException("Truncated snapshot."); count += n; }
                    if (deflate.ReadByte() != -1) throw new InvalidDataException("Snapshot expands past declared size.");
                }
                using (var sha = SHA256.Create())
                {
                    var actual = sha.ComputeHash(raw); var difference = 0;
                    for (var i = 0; i < digest.Length; i++) difference |= actual[i] ^ digest[i];
                    if (difference != 0) throw new InvalidDataException("Snapshot checksum.");
                }
                return Parse(raw, formatVersion);
            }
        }
        private static WorldSnapshot Parse(byte[] bytes, int formatVersion)
        {
            using (var input = new MemoryStream(bytes, false))
            using (var reader = new BinaryReader(input, Utf8, true))
            {
                var snapshot = new WorldSnapshot
                {
                    WorldId = ReadText(reader, 32), Epoch = reader.ReadInt64(), Tick = reader.ReadInt64(), Sequence = reader.ReadInt64(),
                    SimTime = reader.ReadDouble(), FixedDt = reader.ReadSingle(), ResX = reader.ReadInt32(), ResZ = reader.ReadInt32(),
                    CellSize = reader.ReadSingle(), OriginX = reader.ReadSingle(), OriginY = reader.ReadSingle(), OriginZ = reader.ReadSingle(),
                    MetadataJson = ReadText(reader, MaximumMetadataBytes)
                };
                ValidateHeader(snapshot);
                var fields = reader.ReadInt32();
                if (fields < 1 || fields > 64) throw new InvalidDataException("Snapshot field count.");
                for (var i = 0; i < fields; i++)
                {
                    var field = new SnapshotField { Name = ReadText(reader, 64), Stride = reader.ReadInt32(), Unsigned = reader.ReadBoolean() };
                    var count = reader.ReadInt32();
                    if (field.Stride < 4 || field.Stride > 16 || field.Stride % 4 != 0 ||
                        count != (long)snapshot.ResX * snapshot.ResZ * field.Stride || count > input.Length - input.Position)
                        throw new InvalidDataException("Snapshot field shape.");
                    field.Data = Exact(reader, count); snapshot.Fields.Add(field);
                }
                if (formatVersion >= 2)
                {
                    var sections = reader.ReadInt32();
                    if (sections < 0 || sections > 32) throw new InvalidDataException("Snapshot section count.");
                    for (var i = 0; i < sections; i++)
                    {
                        var section = new SnapshotSection { Name = ReadText(reader, 64) };
                        var length = reader.ReadInt32();
                        if (length < 1 || length > MaximumSectionBytes || length > input.Length - input.Position)
                            throw new InvalidDataException("Snapshot section size.");
                        section.Data = Exact(reader, length); snapshot.Sections.Add(section);
                    }
                }
                if (input.Position != input.Length) throw new InvalidDataException("Snapshot trailing payload.");
                Validate(snapshot); return snapshot;
            }
        }
        public static void Validate(WorldSnapshot snapshot)
        {
            if (snapshot == null) throw new InvalidDataException("Snapshot is null.");
            ValidateHeader(snapshot);
            if (snapshot.Fields == null || snapshot.Fields.Count < 1 || snapshot.Fields.Count > 64) throw new InvalidDataException("Snapshot field count.");
            var names = new HashSet<string>(StringComparer.Ordinal); long total = Utf8.GetByteCount(snapshot.MetadataJson) + 256;
            foreach (var field in snapshot.Fields)
            {
                if (field == null || string.IsNullOrWhiteSpace(field.Name) || Utf8.GetByteCount(field.Name) > 64 || !names.Add(field.Name))
                    throw new InvalidDataException("Snapshot field name.");
                if (field.Stride < 4 || field.Stride > 16 || field.Stride % 4 != 0 || field.Data == null ||
                    field.Data.LongLength != (long)snapshot.ResX * snapshot.ResZ * field.Stride)
                    throw new InvalidDataException("Snapshot field shape.");
                total += field.Data.LongLength + 128;
                if (total > MaximumDecodedBytes) throw new InvalidDataException("Snapshot decoded size limit.");
                if (!field.Unsigned)
                    for (var offset = 0; offset < field.Data.Length; offset += 4)
                    {
                        // Exponent all-ones identifies NaN/Infinity without platform-endian conversion.
                        if ((field.Data[offset + 3] & 0x7f) == 0x7f && (field.Data[offset + 2] & 0x80) != 0)
                            throw new InvalidDataException("Non-finite snapshot field.");
                    }
            }
            if (snapshot.Sections == null || snapshot.Sections.Count > 32) throw new InvalidDataException("Snapshot section count.");
            foreach (var section in snapshot.Sections)
            {
                if (section == null || string.IsNullOrWhiteSpace(section.Name) || Utf8.GetByteCount(section.Name) > 64 || !names.Add(section.Name))
                    throw new InvalidDataException("Snapshot section name.");
                if (section.Data == null || section.Data.Length < 1 || section.Data.Length > MaximumSectionBytes)
                    throw new InvalidDataException("Snapshot section size.");
                total += section.Data.LongLength + 128;
                if (total > MaximumDecodedBytes) throw new InvalidDataException("Snapshot decoded size limit.");
            }
        }
        internal static void ValidateHeader(WorldSnapshot s)
        {
            if (s.WorldId == null || s.WorldId.Length != 32) throw new InvalidDataException("Snapshot world id.");
            foreach (var c in s.WorldId) if (!Uri.IsHexDigit(c)) throw new InvalidDataException("Snapshot world id.");
            if (s.Epoch < 1 || s.Tick < 0 || s.Sequence < 0 || double.IsNaN(s.SimTime) || double.IsInfinity(s.SimTime) || s.SimTime < 0 ||
                !Finite(s.FixedDt) || s.FixedDt <= 0 || s.FixedDt > 1 || !Finite(s.CellSize) || s.CellSize < .015f || s.CellSize > .25f ||
                s.ResX < 16 || s.ResZ < 16 || s.ResX > 4096 || s.ResZ > 4096 || (long)s.ResX * s.ResZ > MaximumCells ||
                s.ResX * s.CellSize > 120.001f || s.ResZ * s.CellSize > 120.001f || !Finite(s.OriginX) || !Finite(s.OriginY) || !Finite(s.OriginZ))
                throw new InvalidDataException("Snapshot header range.");
            if (s.MetadataJson == null || Utf8.GetByteCount(s.MetadataJson) > MaximumMetadataBytes) throw new InvalidDataException("Snapshot metadata size.");
        }
        private static bool Finite(float x) => !float.IsNaN(x) && !float.IsInfinity(x);
        private static byte[] Exact(BinaryReader reader, int count)
        { var result = reader.ReadBytes(count); if (result.Length != count) throw new InvalidDataException("Truncated snapshot."); return result; }
        private static void WriteText(BinaryWriter writer, string text, int maximum)
        { var bytes = Utf8.GetBytes(text); if (bytes.Length > maximum) throw new InvalidDataException("Snapshot text size."); writer.Write(bytes.Length); writer.Write(bytes); }
        private static string ReadText(BinaryReader reader, int maximum)
        { var count = reader.ReadInt32(); if (count < 0 || count > maximum) throw new InvalidDataException("Snapshot text size."); return Utf8.GetString(Exact(reader, count)); }
    }
}
