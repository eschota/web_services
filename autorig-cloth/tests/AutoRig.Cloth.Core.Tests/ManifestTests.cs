using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Nodes;
using AutoRig.Cloth.Core;
using Json.Schema;
using Xunit;

namespace AutoRig.Cloth.Core.Tests
{
    public class ManifestTests
    {
        // JsonUtility reads public fields by exact name; System.Text.Json does the same with IncludeFields.
        private static readonly JsonSerializerOptions s_json = new JsonSerializerOptions { IncludeFields = true, WriteIndented = true };
        private static readonly Lazy<JsonSchema> s_schema = new Lazy<JsonSchema>(() => JsonSchema.FromText(File.ReadAllText(SpecFile("cloth-manifest.v1.schema.json"))));

        private static string SpecFile(string relative) => Path.Combine(AppContext.BaseDirectory, "spec", relative);

        private static string ExampleText => File.ReadAllText(SpecFile(Path.Combine("examples", "hero.autorig-cloth.json")));

        private static EvaluationResults Validate(string json) =>
            s_schema.Value.Evaluate(JsonNode.Parse(json), new EvaluationOptions { OutputFormat = OutputFormat.List });

        private static string Errors(EvaluationResults results) =>
            string.Join("; ", (results.Details ?? new List<EvaluationResults>())
                .Where(d => d.Errors != null)
                .SelectMany(d => d.Errors.Select(e => d.InstanceLocation + ": " + e.Value)));

        [Fact]
        public void ExampleManifest_ValidatesAgainstTheSchema()
        {
            EvaluationResults results = Validate(ExampleText);
            Assert.True(results.IsValid, Errors(results));
        }

        [Fact]
        public void Schema_RejectsInvalidDocuments()
        {
            JsonNode node = JsonNode.Parse(ExampleText);
            node["colliders"][1]["to_bone"] = ""; // a capsule needs to_bone
            Assert.False(Validate(node.ToJsonString()).IsValid);

            node = JsonNode.Parse(ExampleText);
            node["format"] = "something.else";
            Assert.False(Validate(node.ToJsonString()).IsValid);

            node = JsonNode.Parse(ExampleText);
            node["groups"][0]["connection"] = "ring";
            Assert.False(Validate(node.ToJsonString()).IsValid);
        }

        [Fact]
        public void ExampleManifest_ReadsIntoTheDtos()
        {
            ClothManifest m = JsonSerializer.Deserialize<ClothManifest>(ExampleText, s_json);
            Assert.True(ManifestRules.ValidateHeader(m, out string error), error);
            var warnings = new List<string>();
            ManifestRules.CollectWarnings(m, warnings);
            Assert.Empty(warnings);

            Assert.Equal("Hips", m.calibration.bone_a);
            Assert.Equal(0.62f, m.calibration.distance);
            Assert.Equal(3, m.groups.Length);
            Assert.Equal(12, m.colliders.Length);

            ManifestGroup hair = m.groups.Single(g => g.name == "hair_back");
            ManifestGroup skirt = m.groups.Single(g => g.name == "skirt");
            ManifestGroup cape = m.groups.Single(g => g.name == "cape");
            Assert.True(ManifestRules.TryParseConnection(hair.connection, out ConnectionMode none) && none == ConnectionMode.None);
            Assert.True(ManifestRules.TryParseConnection(skirt.connection, out ConnectionMode loop) && loop == ConnectionMode.Loop);
            Assert.True(ManifestRules.TryParseConnection(cape.connection, out ConnectionMode open) && open == ConnectionMode.Open);
            Assert.Equal(8, skirt.chains.Length);
            Assert.All(skirt.chains, c => Assert.Equal(5, c.bones.Length));
            Assert.All(skirt.chains, c => Assert.EndsWith("_end", c.bones[4]));
            Assert.Equal(new[] { "body" }, skirt.collider_tags);

            ClothParameters skirtParams = ManifestRules.ResolvePreset(m, skirt.preset, skirt.kind, out PresetResolution skirtFrom);
            Assert.Equal(PresetResolution.File, skirtFrom);
            Assert.Equal(0.6f, skirtParams.connectionStiffness);

            ClothParameters capeParams = ManifestRules.ResolvePreset(m, cape.preset, cape.kind, out PresetResolution capeFrom);
            Assert.Equal(PresetResolution.BuiltIn, capeFrom); // "cape" is not defined in the file
            Assert.Equal(BuiltInPresets.GetOrDefault("cape"), capeParams);

            ManifestCollider head = m.colliders.Single(c => c.name == "head");
            Assert.True(ManifestRules.TryParseShape(head.shape, out ColliderShape sphere) && sphere == ColliderShape.Sphere);
            Assert.Equal("HeadTop_End", head.to_bone);
            Assert.Equal(0.35f, head.t);
        }

        [Fact]
        public void Dtos_RoundTripThroughJson_AndStaySchemaValid()
        {
            ClothManifest first = JsonSerializer.Deserialize<ClothManifest>(ExampleText, s_json);
            string written = JsonSerializer.Serialize(first, s_json);
            EvaluationResults results = Validate(written);
            Assert.True(results.IsValid, Errors(results));

            ClothManifest second = JsonSerializer.Deserialize<ClothManifest>(written, s_json);
            Assert.Equal(written, JsonSerializer.Serialize(second, s_json));
        }

        [Fact]
        public void Dtos_HaveAFieldForEverySchemaProperty()
        {
            JsonNode schema = JsonNode.Parse(File.ReadAllText(SpecFile("cloth-manifest.v1.schema.json")));
            void Check(Type type, JsonNode properties)
            {
                Assert.True(type.IsDefined(typeof(SerializableAttribute), false), type.Name + " must be [Serializable] for JsonUtility");
                foreach (KeyValuePair<string, JsonNode> property in properties.AsObject())
                {
                    FieldInfo field = type.GetField(property.Key, BindingFlags.Public | BindingFlags.Instance);
                    Assert.True(field != null, type.Name + " has no public field \"" + property.Key + "\"");
                }
            }

            Check(typeof(ClothManifest), schema["properties"]);
            Check(typeof(ManifestCalibration), schema["properties"]["calibration"]["properties"]);
            Check(typeof(ManifestPreset), schema["$defs"]["preset"]["properties"]);
            Check(typeof(ManifestGroup), schema["$defs"]["group"]["properties"]);
            Check(typeof(ManifestChain), schema["$defs"]["chain"]["properties"]);
            Check(typeof(ManifestCollider), schema["$defs"]["collider"]["properties"]);
        }

        [Fact]
        public void AbsentFields_ReadAsZeroOrEmpty_LikeJsonUtility()
        {
            // JsonUtility runs no field initializers, and the specification says an absent number is
            // 0 and an absent string is "". The DTOs have no numeric initializers, so this reader
            // (System.Text.Json) sees exactly what Unity sees.
            const string json = "{\"format\":\"autorig.cloth\",\"version\":1,\"units\":\"meters\"," +
                "\"presets\":[{\"name\":\"partial\",\"stiffness\":0.4}]," +
                "\"groups\":[{\"name\":\"g\",\"kind\":\"hair\",\"attach_bone\":\"Head\",\"preset\":\"partial\",\"connection\":\"none\",\"chains\":[{\"bones\":[\"a\",\"b\"]}]}]," +
                "\"colliders\":[{\"name\":\"c\",\"shape\":\"sphere\",\"bone\":\"Head\",\"radius\":0.1}]}";
            ClothManifest m = JsonSerializer.Deserialize<ClothManifest>(json, s_json);

            ClothParameters p = ManifestRules.ResolvePreset(m, "partial", "hair", out PresetResolution from);
            Assert.Equal(PresetResolution.File, from);
            Assert.Equal(0.4f, p.stiffness);
            Assert.Equal(0f, p.gravity);
            Assert.Equal(0f, p.damping);
            Assert.Equal(0f, p.inertiaMove);
            Assert.Equal(0f, p.radius);
            Assert.Equal(p.radius, p.EffectiveRadiusTip); // radius_tip 0 = same as radius

            var warnings = new List<string>();
            ManifestRules.CollectWarnings(m, warnings);
            Assert.Contains(warnings, w => w.Contains("probably written partially"));

            Assert.Equal(0f, m.calibration.distance);   // absent calibration: factor 1
            Assert.Empty(m.groups[0].collider_tags);    // absent list = all colliders
            Assert.Equal(string.Empty, m.colliders[0].to_bone);
            Assert.Equal(string.Empty, m.colliders[0].tag);
            Assert.Equal(0f, m.colliders[0].radius_to); // 0 = same as radius
        }

        [Fact]
        public void UnknownFields_AreIgnored()
        {
            const string json = "{\"format\":\"autorig.cloth\",\"version\":1,\"units\":\"meters\",\"future_field\":{\"x\":[1,2]}," +
                "\"presets\":[{\"name\":\"p\",\"new_knob\":3}],\"groups\":[],\"colliders\":[]}";
            ClothManifest m = JsonSerializer.Deserialize<ClothManifest>(json, s_json);
            Assert.True(ManifestRules.ValidateHeader(m, out _));
            Assert.Equal("p", m.presets[0].name);
        }

        [Fact]
        public void Header_RejectsWrongFormatAndUnknownVersions()
        {
            Assert.False(ManifestRules.ValidateHeader(null, out _));
            Assert.False(ManifestRules.ValidateHeader(new ClothManifest { format = "gltf", version = 1 }, out string wrongFormat));
            Assert.Contains("format", wrongFormat);
            Assert.False(ManifestRules.ValidateHeader(new ClothManifest { format = "autorig.cloth", version = 2 }, out string newer));
            Assert.Contains("newer", newer);
            Assert.False(ManifestRules.ValidateHeader(new ClothManifest { format = "autorig.cloth", version = 0 }, out _));
            Assert.True(ManifestRules.ValidateHeader(new ClothManifest { format = "autorig.cloth", version = 1 }, out _));
        }

        [Fact]
        public void Presets_FileFirst_ThenBuiltIn_ThenDefaults()
        {
            var m = new ClothManifest
            {
                presets = new[] { new ManifestPreset { name = "hair", stiffness = 0.9f } },
            };

            Assert.Equal(0.9f, ManifestRules.ResolvePreset(m, "hair", "hair", out PresetResolution a).stiffness);
            Assert.Equal(PresetResolution.File, a);

            ManifestRules.ResolvePreset(m, "tail", "tail", out PresetResolution b);
            Assert.Equal(PresetResolution.BuiltIn, b);

            Assert.Equal(ClothParameters.Default, ManifestRules.ResolvePreset(m, "unknown", "hair", out PresetResolution c));
            Assert.Equal(PresetResolution.Default, c);

            // An empty preset name picks a built-in preset by kind.
            Assert.Equal(BuiltInPresets.GetOrDefault("skirt"), ManifestRules.ResolvePreset(m, "", "cloth", out PresetResolution d));
            Assert.Equal(PresetResolution.BuiltIn, d);

            foreach (string name in BuiltInPresets.Names)
            {
                Assert.True(BuiltInPresets.TryGet(name, out ClothParameters p), name);
                Assert.Equal(p, p.Sanitized()); // every built-in value is inside the documented ranges
            }
        }

        [Fact]
        public void BoneNames_MatchExactlyOrWithoutNamespace()
        {
            Assert.Equal("Hips", BoneNames.StripNamespace("mixamorig:Hips"));
            Assert.Equal("Hips", BoneNames.StripNamespace("Armature|Hips"));
            Assert.Equal("Hips", BoneNames.StripNamespace("a:b|Hips"));
            Assert.Equal("Hips", BoneNames.StripNamespace("Hips"));
            Assert.Equal(string.Empty, BoneNames.StripNamespace(null));

            Assert.True(BoneNames.Matches("Hips", "Hips"));
            Assert.True(BoneNames.Matches("mixamorig:Hips", "Hips"));
            Assert.True(BoneNames.Matches("Hips", "mixamorig:Hips"));
            Assert.True(BoneNames.Matches("Armature|Hips", "mixamorig:Hips"));
            Assert.False(BoneNames.Matches("hips", "Hips"));
            Assert.False(BoneNames.Matches("LeftHips", "Hips"));
            Assert.False(BoneNames.Matches("Hips", ""));
            Assert.False(BoneNames.Matches("mixamorig:", "Armature|"));
        }

        [Fact]
        public void Calibration_FactorAndTeleportDistance()
        {
            Assert.Equal(2f, ManifestRules.CalibrationFactor(1.24f, 0.62f), 5);
            Assert.Equal(1f, ManifestRules.CalibrationFactor(0f, 0.62f));
            Assert.Equal(1f, ManifestRules.CalibrationFactor(1f, 0f));
            Assert.Equal(1f, ManifestRules.CalibrationFactor(float.NaN, 0.62f));

            Assert.Equal(3f * 0.62f * 2f, ManifestRules.TeleportDistance(0.62f, 2f), 5);
            Assert.Equal(ClothSolver.DefaultTeleportDistance, ManifestRules.TeleportDistance(0f, 1f));
        }

        [Fact]
        public void Enumerations_Parse()
        {
            Assert.True(ManifestRules.TryParseConnection("loop", out ConnectionMode loop));
            Assert.Equal(ConnectionMode.Loop, loop);
            Assert.True(ManifestRules.TryParseConnection(" Open ", out ConnectionMode open));
            Assert.Equal(ConnectionMode.Open, open);
            Assert.False(ManifestRules.TryParseConnection("", out ConnectionMode empty));
            Assert.Equal(ConnectionMode.None, empty);
            Assert.True(ManifestRules.TryParseShape("capsule", out ColliderShape capsule));
            Assert.Equal(ColliderShape.Capsule, capsule);
            Assert.False(ManifestRules.TryParseShape("box", out _));
            Assert.False(ManifestRules.TryParseShape("plane", out _)); // planes exist at runtime only
        }

        [Fact]
        public void Warnings_ReportProblemsThatDoNotPreventLoading()
        {
            ClothManifest m = JsonSerializer.Deserialize<ClothManifest>(ExampleText, s_json);
            m.units = "centimeters";
            m.groups[1].connection = "ring";
            m.groups[2].chains[0].bones = m.groups[2].chains[0].bones.Take(3).ToArray();
            m.groups[0].chains[1].bones = new[] { "lonely" };
            m.colliders[0].radius = 0f;
            m.colliders[1].to_bone = "";
            m.colliders[2].name = m.colliders[3].name;
            m.groups[0].preset = "no_such_preset";
            m.groups[2].preset = "";

            var warnings = new List<string>();
            ManifestRules.CollectWarnings(m, warnings);
            Assert.Contains(warnings, w => w.Contains("Units"));
            Assert.Contains(warnings, w => w.Contains("unknown connection"));
            Assert.Contains(warnings, w => w.Contains("different lengths"));
            Assert.Contains(warnings, w => w.Contains("fewer than two bones"));
            Assert.Contains(warnings, w => w.Contains("radius must be greater than 0"));
            Assert.Contains(warnings, w => w.Contains("needs to_bone"));
            Assert.Contains(warnings, w => w.Contains("more than once"));
            Assert.Contains(warnings, w => w.Contains("neither defined in the file nor built in"));
            Assert.Contains(warnings, w => w.Contains("no preset; using \"skirt\""));
        }
    }
}
