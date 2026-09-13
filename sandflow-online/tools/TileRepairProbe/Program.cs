using SandFlow.Protocol;
using System.Text.Json;

if(args.Length!=3)throw new ArgumentException("Usage: TileRepairProbe authority.sfs baseline.sfs project-output-folder");
var authority=SnapshotCodec.Decode(File.ReadAllBytes(args[0]));
var baseline=SnapshotCodec.Decode(File.ReadAllBytes(args[1]));
var digest=TileStateProtocol.EncodeDigest(TileStateProtocol.CreateDigest(baseline));
var repair=TileStateProtocol.CreateRepair(authority,TileStateProtocol.DecodeDigest(digest));
var wire=TileStateProtocol.EncodeRepair(repair);
var corrected=TileStateProtocol.ApplyRepair(baseline,TileStateProtocol.DecodeRepair(wire));
long outside=0;
foreach(var field in authority.Fields)
{
    var other=corrected.Fields.Single(x=>x.Name==field.Name);
    for(var i=0;i<field.Data.Length;i+=4)
    {
        if(field.Unsigned){if(BitConverter.ToUInt32(field.Data,i)!=BitConverter.ToUInt32(other.Data,i))outside++;}
        else
        {
            var a=BitConverter.ToSingle(field.Data,i);var b=BitConverter.ToSingle(other.Data,i);
            if(Math.Abs((double)a-b)>1e-4+Math.Max(Math.Abs(a),Math.Abs(b))*1e-6)outside++;
        }
    }
}
var modulesMatch=authority.MetadataJson==corrected.MetadataJson&&authority.Sections.Count==corrected.Sections.Count&&
    authority.Sections.All(x=>corrected.Sections.Any(y=>y.Name==x.Name&&y.Data.SequenceEqual(x.Data)));
var full=SnapshotCodec.Encode(authority);var folder=Path.GetFullPath(args[2]);Directory.CreateDirectory(folder);
File.WriteAllBytes(Path.Combine(folder,"corrected.sfs"),SnapshotCodec.Encode(corrected));
var report=new{schemaVersion=1,scope="Repair codec applied offline to two real captured states; no live GPU rewind/replay or network performance acceptance",tileSize=32,
    world=authority.WorldId,epoch=authority.Epoch,tick=authority.Tick,sequence=authority.Sequence,tilesSent=repair.Tiles.Count,
    totalFieldTiles=TileStateProtocol.TileCount(authority)*authority.Fields.Count,digestBytes=digest.Length,repairBytes=wire.Length,fullSnapshotBytes=full.Length,
    totalCorrectionBytes=digest.Length+wire.Length,ratioToFull=(double)(digest.Length+wire.Length)/full.Length,outsidePhysicalTolerance=outside,modulesMatch,
    passed=outside==0&&modulesMatch};
File.WriteAllText(Path.Combine(folder,"report.json"),JsonSerializer.Serialize(report,new JsonSerializerOptions{WriteIndented=true}));
Console.WriteLine(JsonSerializer.Serialize(report));
return report.passed?0:1;
