using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace SandFlow.Protocol
{
    public enum SettingScope { Shared, Local }
    public sealed class TuningSetting
    {
        public string Key { get; }
        public float Min { get; }
        public float Max { get; }
        public SettingScope Scope { get; }
        public TuningSetting(string key, float min, float max, SettingScope scope)
        { Key = key; Min = min; Max = max; Scope = scope; }
    }

    /// <summary>Reviewed wire registry. Unity coverage checks compare every dial/range against the prototype.</summary>
    public static class TuningSettings
    {
        public const int Version = 1;
        public const string SectionName = "tuning-v1";
        public const int MaximumBytes = 16 * 1024;
        private static readonly TuningSetting[] Definitions = {
            new TuningSetting("AbsorptionR", 0.02f, 0.60f, SettingScope.Shared),
            new TuningSetting("AbsorptionG", 0.005f, 0.30f, SettingScope.Shared),
            new TuningSetting("AbsorptionB", 0.002f, 0.20f, SettingScope.Shared),
            new TuningSetting("TintFloor", 0f, 0.6f, SettingScope.Shared),
            new TuningSetting("DepthTintRange", 0f, 12f, SettingScope.Shared),
            new TuningSetting("ScatterHue", 0f, 1f, SettingScope.Shared),
            new TuningSetting("ScatterSaturation", 0f, 1f, SettingScope.Shared),
            new TuningSetting("ScatterValue", 0f, 1f, SettingScope.Shared),
            new TuningSetting("ShallowHue", 0f, 1f, SettingScope.Shared),
            new TuningSetting("ShallowSaturation", 0f, 1f, SettingScope.Shared),
            new TuningSetting("ShallowValue", 0f, 1f, SettingScope.Shared),
            new TuningSetting("FresnelF0", 0.005f, 0.12f, SettingScope.Shared),
            new TuningSetting("RefractionStrength", 0f, 0.25f, SettingScope.Shared),
            new TuningSetting("NormalScale", 0.25f, 8f, SettingScope.Shared),
            new TuningSetting("DetailStrength", 0f, 10f, SettingScope.Shared),
            new TuningSetting("NormalLightStrength", 0f, 2f, SettingScope.Shared),
            new TuningSetting("NormalShadeStrength", 0f, 2f, SettingScope.Shared),
            new TuningSetting("DetailFlowRate", 0f, 2f, SettingScope.Shared),
            new TuningSetting("NormalDrift", 0f, 0.5f, SettingScope.Shared),
            new TuningSetting("WaveAmplitude", 0f, 0.15f, SettingScope.Shared),
            new TuningSetting("WaveLength", 1.2f, 8f, SettingScope.Shared),
            new TuningSetting("CrestVariance", 0f, 0.8f, SettingScope.Shared),
            new TuningSetting("CrestScale", 2f, 30f, SettingScope.Shared),
            new TuningSetting("GlintStrength", 0f, 4f, SettingScope.Shared),
            new TuningSetting("ShoreFadeDepth", 0.005f, 0.2f, SettingScope.Shared),
            new TuningSetting("ObstacleImpact", 0f, 6f, SettingScope.Shared),
            new TuningSetting("ObstacleChurn", 0f, 16f, SettingScope.Shared),
            new TuningSetting("BreakerIntensity", 0f, 15f, SettingScope.Shared),
            new TuningSetting("BreakerWidth", 0.3f, 3f, SettingScope.Shared),
            new TuningSetting("WhitewaterBrightness", 0f, 2f, SettingScope.Shared),
            new TuningSetting("SwashRichness", 0f, 2f, SettingScope.Shared),
            new TuningSetting("FoamThreshold", 0.05f, 0.9f, SettingScope.Shared),
            new TuningSetting("FoamDecayTau", 0.5f, 20f, SettingScope.Shared),
            new TuningSetting("FoamTexScale", 0.25f, 4f, SettingScope.Shared),
            new TuningSetting("FoamBump", 0f, 2f, SettingScope.Shared),
            new TuningSetting("FoamDrift", 0f, 0.5f, SettingScope.Shared),
            new TuningSetting("BeachFoamOpacity", 0f, 1f, SettingScope.Shared),
            new TuningSetting("BeachFoamSoftness", 0.06f, 0.6f, SettingScope.Shared),
            new TuningSetting("FloaterWake", 0f, 2f, SettingScope.Shared),
            new TuningSetting("FoamHue", 0f, 1f, SettingScope.Shared),
            new TuningSetting("FoamSaturation", 0f, 1f, SettingScope.Shared),
            new TuningSetting("FoamValue", 0.1f, 1.5f, SettingScope.Shared),
            new TuningSetting("SpraySpawnHeight", 0.005f, 0.20f, SettingScope.Shared),
            new TuningSetting("SpraySpawnSpeed", 0.05f, 3f, SettingScope.Shared),
            new TuningSetting("SprayDensity", 0f, 4f, SettingScope.Shared),
            new TuningSetting("SprayGravity", 2f, 20f, SettingScope.Shared),
            new TuningSetting("SpraySize", 0.002f, 0.06f, SettingScope.Shared),
            new TuningSetting("SpraySizeMax", 0.005f, 0.08f, SettingScope.Shared),
            new TuningSetting("SprayBrightness", 0f, 3f, SettingScope.Shared),
            new TuningSetting("SprayLifetime", 0.2f, 2.5f, SettingScope.Shared),
            new TuningSetting("SprayMistStrength", 0f, 2f, SettingScope.Shared),
            new TuningSetting("SprayMinFlow", 0f, 0.5f, SettingScope.Shared),
            new TuningSetting("SprayVerticalCull", 0f, 45f, SettingScope.Shared),
            new TuningSetting("CausticGain", 0f, 6f, SettingScope.Shared),
            new TuningSetting("CausticScale", 1f, 12f, SettingScope.Shared),
            new TuningSetting("DrySandHue", 0f, 1f, SettingScope.Shared),
            new TuningSetting("DrySandSaturation", 0f, 1f, SettingScope.Shared),
            new TuningSetting("DrySandValue", 0.2f, 1f, SettingScope.Shared),
            new TuningSetting("WetDarken", 0.35f, 0.95f, SettingScope.Shared),
            new TuningSetting("WetSmooth", 0f, 1f, SettingScope.Shared),
            new TuningSetting("SubmergedWetness", 0f, 1f, SettingScope.Shared),
            new TuningSetting("WetGloss", 0f, 2f, SettingScope.Shared),
            new TuningSetting("WetGlossPower", 2f, 96f, SettingScope.Shared),
            new TuningSetting("SandGrainStrength", 0f, 2f, SettingScope.Shared),
            new TuningSetting("GrainScale", 0.05f, 1f, SettingScope.Shared),
            new TuningSetting("SandTwoTone", 0f, 1f, SettingScope.Shared),
            new TuningSetting("SandPatchScale", 1f, 20f, SettingScope.Shared),
            new TuningSetting("SandSparkle", 0f, 2f, SettingScope.Shared),
            new TuningSetting("SandRippleTile", 0.5f, 8f, SettingScope.Shared),
            new TuningSetting("SandRippleStrength", 0f, 2f, SettingScope.Shared),
            new TuningSetting("SandRippleDirection", 0f, 360f, SettingScope.Shared),
            new TuningSetting("SandWarmAmbient", 0f, 1f, SettingScope.Shared),
            new TuningSetting("SlopeTint", 0f, 1f, SettingScope.Shared),
            new TuningSetting("SandStrata", 0f, 2f, SettingScope.Shared),
            new TuningSetting("SandWallShade", 0f, 1f, SettingScope.Shared),
            new TuningSetting("CastleHardness", 0f, 1f, SettingScope.Shared),
            new TuningSetting("CastleSoftenRate", 0.002f, 0.1f, SettingScope.Shared),
            new TuningSetting("CastleScale", 1f, 6f, SettingScope.Shared),
            new TuningSetting("ErosionStrength", 0f, 2f, SettingScope.Shared),
            new TuningSetting("SunElevation", 5f, 85f, SettingScope.Shared),
            new TuningSetting("SunAzimuth", 0f, 360f, SettingScope.Shared),
            new TuningSetting("SunHue", 0f, 1f, SettingScope.Shared),
            new TuningSetting("SunValue", 0.3f, 1f, SettingScope.Shared),
            new TuningSetting("SunIntensity", 0f, 2f, SettingScope.Shared),
            new TuningSetting("SunShadowSoftness", 0f, 1f, SettingScope.Shared),
            new TuningSetting("SunShadowStrength", 0f, 1f, SettingScope.Shared),
            new TuningSetting("PixelDensity", 0.5f, 2f, SettingScope.Local),
            new TuningSetting("PostBloom", 0f, 3f, SettingScope.Shared),
            new TuningSetting("PostVignette", 0f, 0.8f, SettingScope.Shared),
            new TuningSetting("PostSaturation", -60f, 60f, SettingScope.Shared),
            new TuningSetting("PostTiltShift", 0f, 1f, SettingScope.Shared),
            new TuningSetting("PostExposure", -3f, 3f, SettingScope.Shared),
            new TuningSetting("PostContrast", -50f, 50f, SettingScope.Shared),
            new TuningSetting("CurveLift", -0.3f, 0.3f, SettingScope.Shared),
            new TuningSetting("CurveGamma", -0.5f, 0.5f, SettingScope.Shared),
            new TuningSetting("CurveGain", -0.3f, 0.3f, SettingScope.Shared),
            new TuningSetting("SkyBrightness", 0.3f, 2.5f, SettingScope.Shared),
            new TuningSetting("SkyContrast", 0.5f, 3f, SettingScope.Shared),
            new TuningSetting("SkyHue", -0.5f, 0.5f, SettingScope.Shared),
            new TuningSetting("SkyHaze", 0f, 2f, SettingScope.Shared),
        };
        public static readonly IReadOnlyList<TuningSetting> All = Array.AsReadOnly(Definitions);
        private static readonly Dictionary<string, TuningSetting> ByKey = BuildIndex();
        public static int SharedCount { get; } = CountShared();
        private static Dictionary<string, TuningSetting> BuildIndex()
        {
            var result = new Dictionary<string, TuningSetting>(StringComparer.Ordinal);
            foreach (var entry in Definitions)
            {
                if (string.IsNullOrEmpty(entry.Key) || entry.Key.Length > 64 || !Finite(entry.Min) || !Finite(entry.Max) || entry.Min > entry.Max)
                    throw new InvalidOperationException("Invalid tuning registry.");
                result.Add(entry.Key, entry);
            }
            return result;
        }
        private static int CountShared() { int result = 0; foreach (var entry in Definitions) if (entry.Scope == SettingScope.Shared) result++; return result; }
        public static bool TryGet(string key, out TuningSetting setting)
        { if (key == null) { setting = null; return false; } return ByKey.TryGetValue(key, out setting); }
        public static bool Finite(float value) => !float.IsNaN(value) && !float.IsInfinity(value);
        public static void ValidateShared(string key, float value)
        {
            if (!TryGet(key, out var entry) || entry.Scope != SettingScope.Shared)
                throw new InvalidDataException("Unknown or local tuning key.");
            if (!Finite(value) || value < entry.Min || value > entry.Max)
                throw new InvalidDataException("Tuning value outside registry range.");
        }
        public static void ValidateDelta(IReadOnlyDictionary<string, float> values)
        {
            if (values == null || values.Count < 1 || values.Count > SharedCount)
                throw new InvalidDataException("Tuning delta count.");
            foreach (var pair in values) ValidateShared(pair.Key, pair.Value);
        }
        public static byte[] Encode(IReadOnlyDictionary<string, float> values)
        {
            ValidateDelta(values);
            if (values.Count != SharedCount) throw new InvalidDataException("Incomplete tuning checkpoint.");
            using (var stream = new MemoryStream())
            using (var writer = new BinaryWriter(stream, new UTF8Encoding(false, true), true))
            {
                writer.Write(0x53465455); writer.Write(Version); writer.Write(values.Count);
                // Registry order is fixed, independent of UI order or dictionary insertion history.
                foreach (var entry in Definitions)
                    if (entry.Scope == SettingScope.Shared) { writer.Write(entry.Key); writer.Write(values[entry.Key]); }
                writer.Flush();
                if (stream.Length > MaximumBytes) throw new InvalidDataException("Tuning checkpoint size.");
                return stream.ToArray();
            }
        }
        public static Dictionary<string, float> Decode(byte[] bytes)
        {
            if (bytes == null || bytes.Length < 12 || bytes.Length > MaximumBytes)
                throw new InvalidDataException("Tuning checkpoint size.");
            try
            {
                using (var stream = new MemoryStream(bytes, false))
                using (var reader = new BinaryReader(stream, new UTF8Encoding(false, true), true))
                {
                    if (reader.ReadInt32() != 0x53465455 || reader.ReadInt32() != Version || reader.ReadInt32() != SharedCount)
                        throw new InvalidDataException("Tuning checkpoint header.");
                    var result = new Dictionary<string, float>(StringComparer.Ordinal);
                    for (int i = 0; i < SharedCount; i++)
                    {
                        string key = ReadKey(reader); float value = reader.ReadSingle();
                        ValidateShared(key, value);
                        if (result.ContainsKey(key)) throw new InvalidDataException("Duplicate tuning key.");
                        result.Add(key, value);
                    }
                    if (stream.Position != stream.Length) throw new InvalidDataException("Trailing tuning data.");
                    return result;
                }
            }
            catch (EndOfStreamException error) { throw new InvalidDataException("Truncated tuning checkpoint.", error); }
            catch (DecoderFallbackException error) { throw new InvalidDataException("Invalid tuning text.", error); }
        }

        private static string ReadKey(BinaryReader reader)
        {
            // All registry keys are short ASCII identifiers. Reject a forged length before allocating.
            int length = reader.ReadByte();
            if (length < 1 || length > 64) throw new InvalidDataException("Tuning key length.");
            byte[] text = reader.ReadBytes(length);
            if (text.Length != length) throw new InvalidDataException("Truncated tuning key.");
            foreach (byte value in text)
                if (value < 32 || value > 126) throw new InvalidDataException("Tuning key encoding.");
            return Encoding.ASCII.GetString(text);
        }
    }
}
