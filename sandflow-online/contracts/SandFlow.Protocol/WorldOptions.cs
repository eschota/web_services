using System;
using System.Collections.Generic;
using System.IO;

namespace SandFlow.Protocol
{
    /// <summary>Ordered discrete switches. Spray also clears its simulated particle pool.</summary>
    public static class WorldOptions
    {
        public const string SectionName="world-options-v1";
        public const string WorldScope="physical-fields-obstacles-tuning-options-v4";
        private static readonly string[] Keys={"Foam","Spray","Caustics","CausticGlint","Fog","Specular","Ripples","Tonemap","PostMaster","TiltShift","Ssao"};
        public static readonly IReadOnlyList<string> All=Array.AsReadOnly(Keys);
        public static int Maximum(string key)=>key=="Tonemap"?2:1;
        public static int Default(string key)=>key=="Tonemap"?2:key=="TiltShift"||key=="Ssao"?0:1;
        public static void Validate(string key,float value)
        {
            if(key==null||Array.IndexOf(Keys,key)<0||float.IsNaN(value)||float.IsInfinity(value)||value!=(int)value||value<0||value>Maximum(key))
                throw new InvalidDataException("Invalid world option");
        }
        public static void ValidateDelta(IReadOnlyDictionary<string,int> values)
        {
            if(values==null||values.Count<1||values.Count>Keys.Length)throw new InvalidDataException("Invalid option batch");
            foreach(var pair in values)Validate(pair.Key,pair.Value);
        }
        public static Dictionary<string,int> Defaults()
        {var result=new Dictionary<string,int>(StringComparer.Ordinal);foreach(var key in Keys)result.Add(key,Default(key));return result;}
        public static byte[] Encode(IReadOnlyDictionary<string,int> values)
        {
            ValidateDelta(values);if(values.Count!=Keys.Length)throw new InvalidDataException("Incomplete world options");
            var bytes=new byte[6+Keys.Length];bytes[0]=83;bytes[1]=70;bytes[2]=79;bytes[3]=80;bytes[4]=1;bytes[5]=(byte)Keys.Length;
            for(int i=0;i<Keys.Length;i++)bytes[6+i]=(byte)values[Keys[i]];return bytes;
        }
        public static Dictionary<string,int> Decode(byte[] bytes)
        {
            if(bytes==null||bytes.Length!=6+Keys.Length||bytes[0]!=83||bytes[1]!=70||bytes[2]!=79||bytes[3]!=80||bytes[4]!=1||bytes[5]!=Keys.Length)
                throw new InvalidDataException("Invalid world-option checkpoint");
            var result=new Dictionary<string,int>(StringComparer.Ordinal);
            for(int i=0;i<Keys.Length;i++){Validate(Keys[i],bytes[6+i]);result.Add(Keys[i],bytes[6+i]);}return result;
        }
    }
}
