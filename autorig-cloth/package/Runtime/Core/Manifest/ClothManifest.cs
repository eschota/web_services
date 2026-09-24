using System;

// The field names of these types are the manifest's JSON keys, verbatim. Unity's JsonUtility maps
// JSON keys to public fields by exact name, so they deliberately use snake_case.
//
// Absent fields: JsonUtility does not run constructors or field initializers, so a field missing
// from the file reads as 0, null or an empty array. That is also the specification's rule ("an
// absent string is "", an absent number is 0"), and it is why no number below has an initializer:
// every reader sees the same values. Strings and arrays start empty only for convenience when
// manifests are built in code; readers treat null and empty alike.
namespace AutoRig.Cloth.Core
{
    /// <summary>
    /// Root of an AutoRig cloth manifest (<c>&lt;model&gt;.autorig-cloth.json</c>, format
    /// <c>autorig.cloth</c>, version 1). Plain serializable data that Unity's <c>JsonUtility</c>
    /// and <c>System.Text.Json</c> (with fields included) both read. Absent numbers read as 0,
    /// absent strings and lists as empty; unknown fields are ignored.
    /// </summary>
    [Serializable]
    public class ClothManifest
    {
        /// <summary>Always <c>"autorig.cloth"</c>; a reader rejects anything else.</summary>
        public string format = "";

        /// <summary>Format version; this runtime reads version 1.</summary>
        public int version;

        /// <summary>Free text, <c>name/version</c> of the program that wrote the file.</summary>
        public string generator = "";

        /// <summary>Always <c>"meters"</c> in version 1.</summary>
        public string units = "";

        /// <summary>Two bones and their distance in the authored model, used to rescale lengths.</summary>
        public ManifestCalibration calibration = new ManifestCalibration();

        /// <summary>Named parameter sets referenced by groups.</summary>
        public ManifestPreset[] presets = new ManifestPreset[0];

        /// <summary>Simulated parts; one group is one set of chains that move together.</summary>
        public ManifestGroup[] groups = new ManifestGroup[0];

        /// <summary>Body shapes the chains collide with.</summary>
        public ManifestCollider[] colliders = new ManifestCollider[0];
    }

    /// <summary>Manifest <c>calibration</c> object.</summary>
    [Serializable]
    public class ManifestCalibration
    {
        /// <summary>First calibration bone (usually the hips).</summary>
        public string bone_a = "";

        /// <summary>Second calibration bone (usually the head).</summary>
        public string bone_b = "";

        /// <summary>Distance between the two bones in the authored model, in metres.</summary>
        public float distance;
    }

    /// <summary>
    /// Manifest <c>presets[]</c> entry. A field absent from the file reads as 0 (the specification
    /// defaults apply only to preset names that are not found), so producers write every field.
    /// </summary>
    [Serializable]
    public class ManifestPreset
    {
        /// <summary>Name referenced by <see cref="ManifestGroup.preset"/>.</summary>
        public string name = "";

        /// <summary>Multiplier of 9.81 m/s² (0–2).</summary>
        public float gravity;

        /// <summary>Share of velocity removed per 1/60 s (0–1).</summary>
        public float damping;

        /// <summary>Pull back toward the animated pose direction (0–1).</summary>
        public float stiffness;

        /// <summary>Hard limit of deviation from the animated direction in degrees; 0 = no limit.</summary>
        public float angle_limit_deg;

        /// <summary>Allowed stretch of links along a chain; 0 = inextensible.</summary>
        public float stretch;

        /// <summary>Stiffness of links between neighbouring chains (0–1).</summary>
        public float connection_stiffness;

        /// <summary>Collision radius of a chain's first joint, metres.</summary>
        public float radius;

        /// <summary>Collision radius of a chain's last joint, metres; 0 = same as <see cref="radius"/>.</summary>
        public float radius_tip;

        /// <summary>How much of the character's world translation the cloth feels (0–1).</summary>
        public float inertia_move;

        /// <summary>How much of the character's world rotation the cloth feels (0–1).</summary>
        public float inertia_rotate;

        /// <summary>Air drag (0–1).</summary>
        public float drag;

        /// <summary>Influence of wind zones (0–1).</summary>
        public float wind;

        /// <summary>Converts to solver parameters (unscaled; the loader applies calibration).</summary>
        public ClothParameters ToParameters() => new ClothParameters(
            gravity,
            damping,
            stiffness,
            angle_limit_deg,
            stretch,
            connection_stiffness,
            radius,
            radius_tip,
            inertia_move,
            inertia_rotate,
            drag,
            wind);
    }

    /// <summary>Manifest <c>groups[]</c> entry.</summary>
    [Serializable]
    public class ManifestGroup
    {
        /// <summary>Unique name within the file.</summary>
        public string name = "";

        /// <summary><c>hair</c>, <c>cloth</c>, <c>tail</c> or <c>accessory</c>. Informational.</summary>
        public string kind = "";

        /// <summary>The animated bone the group hangs from.</summary>
        public string attach_bone = "";

        /// <summary>A <see cref="ClothManifest.presets"/> name or a built-in preset name.</summary>
        public string preset = "";

        /// <summary><c>none</c>, <c>open</c> or <c>loop</c>.</summary>
        public string connection = "";

        /// <summary>Tags of the colliders that affect the group; empty = all colliders.</summary>
        public string[] collider_tags = new string[0];

        /// <summary>Chains in angular order around the attach bone.</summary>
        public ManifestChain[] chains = new ManifestChain[0];
    }

    /// <summary>Manifest <c>chains[]</c> entry.</summary>
    [Serializable]
    public class ManifestChain
    {
        /// <summary>Bone names from root to tip; the last one is an end joint.</summary>
        public string[] bones = new string[0];
    }

    /// <summary>Manifest <c>colliders[]</c> entry.</summary>
    [Serializable]
    public class ManifestCollider
    {
        /// <summary>Unique name within the file.</summary>
        public string name = "";

        /// <summary>Matched against <see cref="ManifestGroup.collider_tags"/>.</summary>
        public string tag = "";

        /// <summary><c>sphere</c> or <c>capsule</c>.</summary>
        public string shape = "";

        /// <summary>Anchor bone.</summary>
        public string bone = "";

        /// <summary>Second bone: required for a capsule, optional for a sphere.</summary>
        public string to_bone = "";

        /// <summary>Sphere only: centre at <c>lerp(bone, to_bone, t)</c>, or at <c>bone</c> when <c>to_bone</c> is empty.</summary>
        public float t;

        /// <summary>Sphere radius, or capsule radius at <see cref="bone"/>, metres.</summary>
        public float radius;

        /// <summary>Capsule radius at <see cref="to_bone"/>, metres; 0 = same as <see cref="radius"/>.</summary>
        public float radius_to;
    }
}
