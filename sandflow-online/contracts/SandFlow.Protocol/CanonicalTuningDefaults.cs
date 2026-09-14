using System;
using System.Collections.Generic;
using System.Security.Cryptography;

namespace SandFlow.Protocol
{
    /// <summary>Pinned defaults v1, independent of a client's local preset files or prior room.
    /// FactoryTuning values clamped to the reviewed table; four newer values use their shipped module defaults.</summary>
    public static class CanonicalTuningDefaults
    {
        public const int Version = 1;
        public const string ExpectedFingerprint = "2f3c8e11699e88e09c0a21c293f4a25ea8922dc79bd9f2d6881aee02da592e3d";
        private static readonly Dictionary<string,float> Values = new Dictionary<string,float>(StringComparer.Ordinal) {
            { "AbsorptionR", 0.136070162f },
            { "AbsorptionG", 0.0596795641f },
            { "AbsorptionB", 0.0300168470f },
            { "TintFloor", 0.114893310f },
            { "DepthTintRange", 0.782524645f },
            { "ScatterHue", 0.302679062f },
            { "ScatterSaturation", 0.473196745f },
            { "ScatterValue", 0.350586653f },
            { "ShallowHue", 0.515151501f },
            { "ShallowSaturation", 0.488888890f },
            { "ShallowValue", 0.262954146f },
            { "FresnelF0", 0.0476349778f },
            { "RefractionStrength", 0.0810657442f },
            { "NormalScale", 3.01082850f },
            { "DetailStrength", 2.84000254f },
            { "NormalLightStrength", 0.849694490f },
            { "NormalShadeStrength", 0.379906267f },
            { "DetailFlowRate", 0.699946940f },
            { "NormalDrift", 0.00370248873f },
            { "WaveAmplitude", 0.000303599052f },
            { "WaveLength", 2.15529108f },
            { "CrestVariance", 0.185544208f },
            { "CrestScale", 17.4838696f },
            { "GlintStrength", 1.35543847f },
            { "ShoreFadeDepth", 0.200000003f },
            { "ObstacleImpact", 6.00000000f },
            { "ObstacleChurn", 16.0000000f },
            { "BreakerIntensity", 1.97432244f },
            { "BreakerWidth", 0.808773935f },
            { "WhitewaterBrightness", 1.00000000f },
            { "SwashRichness", 1.53011143f },
            { "FoamThreshold", 0.899999976f },
            { "FoamDecayTau", 0.500000000f },
            { "FoamTexScale", 2.44313431f },
            { "FoamBump", 0.658558846f },
            { "FoamDrift", 0.0495831817f },
            { "BeachFoamOpacity", 0.00000000f },
            { "BeachFoamSoftness", 0.0599999987f },
            { "FloaterWake", 0.123014607f },
            { "FoamHue", 0.991163313f },
            { "FoamSaturation", 0.0791504383f },
            { "FoamValue", 1.50000000f },
            { "SpraySpawnHeight", 0.0199999996f },
            { "SpraySpawnSpeed", 0.250000000f },
            { "SprayDensity", 1.00000000f },
            { "SprayGravity", 9.81000042f },
            { "SpraySize", 0.0160000008f },
            { "SpraySizeMax", 0.0299999993f },
            { "SprayBrightness", 1.00000000f },
            { "SprayLifetime", 0.899999976f },
            { "SprayMistStrength", 0.800000012f },
            { "SprayMinFlow", 0.0599999987f },
            { "SprayVerticalCull", 15.0000000f },
            { "CausticGain", 4.57704735f },
            { "CausticScale", 5.00000000f },
            { "DrySandHue", 0.104123466f },
            { "DrySandSaturation", 0.375000000f },
            { "DrySandValue", 1.00000000f },
            { "WetDarken", 0.740000010f },
            { "WetSmooth", 0.850000024f },
            { "SubmergedWetness", 0.170089468f },
            { "WetGloss", 0.300000012f },
            { "WetGlossPower", 48.0000000f },
            { "SandGrainStrength", 1.00000000f },
            { "GrainScale", 0.280000001f },
            { "SandTwoTone", 1.00000000f },
            { "SandPatchScale", 7.00000000f },
            { "SandSparkle", 0.800000012f },
            { "SandRippleTile", 2.20000005f },
            { "SandRippleStrength", 0.899999976f },
            { "SandRippleDirection", 0.00000000f },
            { "SandWarmAmbient", 0.349999994f },
            { "SlopeTint", 0.210253343f },
            { "SandStrata", 1.00000000f },
            { "SandWallShade", 0.500000000f },
            { "CastleHardness", 1.00000000f },
            { "CastleSoftenRate", 0.0149999997f },
            { "CastleScale", 2.79999995f },
            { "ErosionStrength", 1.00000000f },
            { "SunElevation", 85.0000000f },
            { "SunAzimuth", 48.6711960f },
            { "SunHue", 0.107526876f },
            { "SunValue", 1.00000000f },
            { "SunIntensity", 2.00000000f },
            { "SunShadowSoftness", 0.250000000f },
            { "SunShadowStrength", 1.00000000f },
            { "PostBloom", 0.600000024f },
            { "PostVignette", 0.219999999f },
            { "PostSaturation", 0.00000000f },
            { "PostTiltShift", 0.349999994f },
            { "PostExposure", -0.100000083f },
            { "PostContrast", 4.00000191f },
            { "CurveLift", 0.00000000f },
            { "CurveGamma", 0.00000000f },
            { "CurveGain", 0.00000000f },
            { "SkyBrightness", 1.00000000f },
            { "SkyContrast", 1.14999998f },
            { "SkyHue", 0.00000000f },
            { "SkyHaze", 0.349999994f },
        };
        public static readonly string Fingerprint = Hash();
        public static Dictionary<string,float> Create() => new Dictionary<string,float>(Values,StringComparer.Ordinal);
        private static string Hash()
        {
            using(var sha=SHA256.Create())
            {
                var actual=BitConverter.ToString(sha.ComputeHash(TuningSettings.Encode(Values))).Replace("-","").ToLowerInvariant();
                if(actual!=ExpectedFingerprint)throw new InvalidOperationException("Canonical tuning defaults changed without a reviewed version/fingerprint update");
                return actual;
            }
        }
    }
}
