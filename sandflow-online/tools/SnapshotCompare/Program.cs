using SandFlow.Protocol;
using System.Text.Json;

if(args.Length!=3)throw new ArgumentException("Usage: SnapshotCompare left.sfs right.sfs output.json");
var left=SnapshotCodec.Decode(File.ReadAllBytes(args[0]));
var right=SnapshotCodec.Decode(File.ReadAllBytes(args[1]));
var comparable=left.WorldId==right.WorldId&&left.Epoch==right.Epoch&&left.Tick==right.Tick&&left.Sequence==right.Sequence&&
    left.ResX==right.ResX&&left.ResZ==right.ResZ&&left.CellSize==right.CellSize&&left.FixedDt==right.FixedDt&&left.SimTime==right.SimTime&&
    left.OriginX==right.OriginX&&left.OriginY==right.OriginY&&left.OriginZ==right.OriginZ;
var rows=new List<object>();var exact=true;var within=true;
foreach(var field in left.Fields)
{
    var other=right.Fields.SingleOrDefault(x=>x.Name==field.Name);
    if(other==null||other.Stride!=field.Stride||other.Unsigned!=field.Unsigned||other.Data.Length!=field.Data.Length)
    {comparable=false;rows.Add(new{field=field.Name,error="field layout mismatch"});continue;}
    var differing=0;var outside=0;double max=0,squares=0,sum=0;var count=field.Data.Length/4;
    for(var i=0;i<field.Data.Length;i+=4)
    {
        if(BitConverter.ToUInt32(field.Data,i)!=BitConverter.ToUInt32(other.Data,i))differing++;
        if(field.Unsigned){if(BitConverter.ToUInt32(field.Data,i)!=BitConverter.ToUInt32(other.Data,i))outside++;continue;}
        var a=BitConverter.ToSingle(field.Data,i);var b=BitConverter.ToSingle(other.Data,i);var delta=Math.Abs((double)a-b);
        max=Math.Max(max,delta);squares+=delta*delta;sum+=delta;
        if(delta>1e-4+Math.Max(Math.Abs(a),Math.Abs(b))*1e-6)outside++;
    }
    exact&=differing==0;within&=outside==0;
    rows.Add(new{field=field.Name,elements=count,differingBits=differing,outsideTolerance=outside,maxAbs=max,meanAbs=sum/Math.Max(1,count),rms=Math.Sqrt(squares/Math.Max(1,count))});
}
comparable&=left.Fields.Count==right.Fields.Count;
var sectionRows=new List<object>();var sectionsEqual=left.Sections.Count==right.Sections.Count;
foreach(var section in left.Sections)
{
    var other=right.Sections.SingleOrDefault(x=>x.Name==section.Name);
    var equal=other!=null&&section.Data.SequenceEqual(other.Data);sectionsEqual&=equal;
    var differences=new List<int>();var count=0;
    if(other!=null)for(var i=0;i<Math.Min(section.Data.Length,other.Data.Length);i++)
        if(section.Data[i]!=other.Data[i]){count++;if(differences.Count<16)differences.Add(i);}
    sectionRows.Add(new{section=section.Name,leftBytes=section.Data.Length,rightBytes=other?.Data.Length??0,equal,differingBytes=count,firstDifferenceOffsets=differences});
}
var capturedStateWithinTolerance=comparable&&within&&sectionsEqual&&left.MetadataJson==right.MetadataJson;
var report=new{schemaVersion=2,scope="Captured physical fields and optional module bytes; full schema completeness and visual equivalence are not established",comparable,capturedStateWithinTolerance,
    exactPhysicalFields=comparable&&exact,withinPhysicalTolerance=comparable&&within,absoluteTolerance=1e-4,relativeTolerance=1e-6,
    world=left.WorldId,leftEpoch=left.Epoch,rightEpoch=right.Epoch,leftTick=left.Tick,rightTick=right.Tick,leftSequence=left.Sequence,rightSequence=right.Sequence,
    metadataEqual=left.MetadataJson==right.MetadataJson,sectionsEqual,sections=sectionRows,fields=rows};
Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(args[2]))!);
File.WriteAllText(args[2],JsonSerializer.Serialize(report,new JsonSerializerOptions{WriteIndented=true}));
Console.WriteLine(JsonSerializer.Serialize(new{comparable,exactPhysicalFields=comparable&&exact,withinPhysicalTolerance=comparable&&within,sectionsEqual,capturedStateWithinTolerance,leftTick=left.Tick,rightTick=right.Tick}));
return !comparable?2:capturedStateWithinTolerance?0:1;
