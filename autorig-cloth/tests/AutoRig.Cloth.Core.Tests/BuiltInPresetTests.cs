using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Nodes;
using AutoRig.Cloth.Core;
using Xunit;

namespace AutoRig.Cloth.Core.Tests
{
    /// <summary>
    /// The built-in presets, the defaults for unknown preset names and the kind fallback against
    /// <c>spec/builtin-presets.v1.json</c>, the one place their values live. The Regen producer's
    /// tests compare its copy against the same file.
    /// </summary>
    public class BuiltInPresetTests
    {
        private const double Tolerance = 1e-6;

        // Manifest keys in the order of the ClothParameters constructor (and of the spec's table).
        private static readonly string[] s_fields =
        {
            "gravity", "damping", "stiffness", "angle_limit_deg", "stretch", "connection_stiffness",
            "radius", "radius_tip", "inertia_move", "inertia_rotate", "drag", "wind",
        };

        private static readonly JsonSerializerOptions s_json = new JsonSerializerOptions { IncludeFields = true };

        private static readonly Lazy<JsonNode> s_canonical = new Lazy<JsonNode>(() =>
            JsonNode.Parse(File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "spec", "builtin-presets.v1.json"))));

        private static JsonNode Canonical => s_canonical.Value;

        private static float[] Values(ClothParameters p) => new[]
        {
            p.gravity, p.damping, p.stiffness, p.angleLimitDeg, p.stretch, p.connectionStiffness,
            p.radius, p.radiusTip, p.inertiaMove, p.inertiaRotate, p.drag, p.wind,
        };

        private static void AssertMatches(JsonNode expected, ClothParameters actual, string label)
        {
            float[] values = Values(actual);
            for (int i = 0; i < s_fields.Length; i++)
            {
                double want = expected[s_fields[i]].GetValue<double>();
                Assert.True(
                    Math.Abs(values[i] - want) <= Tolerance,
                    label + "." + s_fields[i] + " is " + values[i] + " in the runtime but " + want + " in builtin-presets.v1.json");
            }
        }

        [Fact]
        public void CanonicalFile_WritesEveryPresetField_WithTheManifestKeys()
        {
            Assert.Equal("autorig.cloth.builtin-presets", Canonical["format"].GetValue<string>());
            Assert.Equal(1, Canonical["version"].GetValue<int>());

            string[] keys = new[] { "name" }.Concat(s_fields).ToArray();
            string[] dtoFields = typeof(ManifestPreset).GetFields(BindingFlags.Public | BindingFlags.Instance).Select(f => f.Name).ToArray();
            Assert.Equal(keys.OrderBy(k => k, StringComparer.Ordinal), dtoFields.OrderBy(k => k, StringComparer.Ordinal));
            foreach (JsonNode preset in Canonical["presets"].AsArray().Append(Canonical["default"]))
            {
                Assert.Equal(keys, preset.AsObject().Select(p => p.Key).ToArray());
            }
        }

        [Fact]
        public void BuiltInPresets_MatchTheCanonicalFile()
        {
            JsonArray presets = Canonical["presets"].AsArray();
            Assert.Equal(presets.Select(p => p["name"].GetValue<string>()).ToArray(), BuiltInPresets.Names.ToArray());
            foreach (JsonNode preset in presets)
            {
                string name = preset["name"].GetValue<string>();
                Assert.True(BuiltInPresets.TryGet(name, out ClothParameters builtIn), name + " is not built in");
                AssertMatches(preset, builtIn, name);

                // A file that writes the canonical values under the built-in name reads back the same parameters.
                ManifestPreset written = JsonSerializer.Deserialize<ManifestPreset>(preset.ToJsonString(), s_json);
                var manifest = new ClothManifest { presets = new[] { written } };
                ClothParameters fromFile = ManifestRules.ResolvePreset(manifest, name, "hair", out PresetResolution resolution);
                Assert.Equal(PresetResolution.File, resolution);
                AssertMatches(preset, fromFile, name + " (read from a manifest)");
            }
        }

        [Fact]
        public void Default_MatchesTheCanonicalFile()
        {
            JsonNode expected = Canonical["default"];
            Assert.Equal(string.Empty, expected["name"].GetValue<string>());
            AssertMatches(expected, ClothParameters.Default, "default");

            // Used for a preset name that is neither in the file nor built in.
            Assert.False(BuiltInPresets.TryGet("no_such_preset", out ClothParameters unknown));
            Assert.Equal(ClothParameters.Default, unknown);
            Assert.Equal(ClothParameters.Default, ManifestRules.ResolvePreset(new ClothManifest(), "no_such_preset", "hair", out PresetResolution resolution));
            Assert.Equal(PresetResolution.Default, resolution);

            // radius_tip 0 = same as radius.
            Assert.Equal(0.0, expected["radius_tip"].GetValue<double>());
            Assert.Equal(ClothParameters.Default.radius, ClothParameters.Default.EffectiveRadiusTip);
        }

        [Fact]
        public void KindFallback_MatchesTheCanonicalFile()
        {
            JsonArray fallback = Canonical["kind_fallback"].AsArray();
            Assert.Equal(new[] { "hair", "cloth", "tail", "accessory" }, fallback.Select(e => e["kind"].GetValue<string>()).ToArray());
            foreach (JsonNode entry in fallback)
            {
                string kind = entry["kind"].GetValue<string>();
                string preset = entry["preset"].GetValue<string>();
                Assert.True(ManifestRules.IsKnownKind(kind), kind);
                Assert.Equal(preset, ManifestRules.DefaultPresetForKind(kind));

                // A group whose preset is empty gets that built-in preset.
                ClothParameters resolved = ManifestRules.ResolvePreset(new ClothManifest(), string.Empty, kind, out PresetResolution resolution);
                Assert.Equal(PresetResolution.BuiltIn, resolution);
                AssertMatches(Canonical["presets"].AsArray().Single(p => p["name"].GetValue<string>() == preset), resolved, kind + " fallback");
            }
        }
    }
}
