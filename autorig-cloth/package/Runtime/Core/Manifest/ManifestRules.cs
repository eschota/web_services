using System;
using System.Collections.Generic;

namespace AutoRig.Cloth.Core
{
    /// <summary>Where <see cref="ManifestRules.ResolvePreset"/> found a group's parameters.</summary>
    public enum PresetResolution
    {
        /// <summary>A preset defined in the manifest's <c>presets</c> array.</summary>
        File = 0,

        /// <summary>A built-in preset (see <see cref="BuiltInPresets"/>).</summary>
        BuiltIn = 1,

        /// <summary>Neither: the specification defaults (<see cref="ClothParameters.Default"/>).</summary>
        Default = 2,
    }

    /// <summary>
    /// Engine-independent rules for reading an AutoRig cloth manifest, version 1: header checks,
    /// enumerations, preset resolution, calibration and the teleport distance.
    /// </summary>
    public static class ManifestRules
    {
        /// <summary>The only accepted <c>format</c> value.</summary>
        public const string Format = "autorig.cloth";

        /// <summary>The manifest version this reader implements.</summary>
        public const int Version = 1;

        /// <summary>The only <c>units</c> value of version 1.</summary>
        public const string Units = "meters";

        /// <summary>The teleport distance is this multiple of the calibrated bone distance.</summary>
        public const float TeleportCalibrationMultiple = 3f;

        /// <summary>
        /// Checks <c>format</c> and <c>version</c>. A file that fails must be rejected as a whole.
        /// </summary>
        /// <returns>True when the file can be read; otherwise false with a reason in <paramref name="error"/>.</returns>
        public static bool ValidateHeader(ClothManifest manifest, out string error)
        {
            if (manifest == null)
            {
                error = "The manifest is empty.";
                return false;
            }

            if (!string.Equals(manifest.format, Format, StringComparison.Ordinal))
            {
                error = "Not an AutoRig cloth manifest: format is \"" + manifest.format + "\", expected \"" + Format + "\".";
                return false;
            }

            if (manifest.version > Version)
            {
                error = "Manifest version " + manifest.version + " is newer than this reader (version " + Version + "). Update AutoRig Cloth.";
                return false;
            }

            if (manifest.version < 1)
            {
                error = "The manifest has no valid version (found " + manifest.version + ").";
                return false;
            }

            error = null;
            return true;
        }

        /// <summary>
        /// Appends a warning for every problem that does not prevent loading: units other than
        /// metres, presets that look partially written, duplicate names, unknown enumerations,
        /// missing or unknown presets, chains too short to simulate, uneven chains in linked
        /// groups, colliders without a usable radius.
        /// </summary>
        public static void CollectWarnings(ClothManifest manifest, List<string> warnings)
        {
            if (manifest == null || warnings == null)
            {
                return;
            }

            if (!string.Equals(manifest.units, Units, StringComparison.Ordinal))
            {
                warnings.Add("Units are \"" + manifest.units + "\"; version 1 is always \"meters\". Lengths are read as metres.");
            }

            ManifestPreset[] presets = manifest.presets ?? new ManifestPreset[0];
            for (int i = 0; i < presets.Length; i++)
            {
                ManifestPreset p = presets[i];
                if (p != null && p.damping == 0f && p.radius == 0f && p.inertia_move == 0f && p.inertia_rotate == 0f)
                {
                    warnings.Add("Preset \"" + p.name + "\" has zero damping, radius and inertia. Absent fields read as 0, so it was probably written partially; write every preset field.");
                }
            }

            var names = new HashSet<string>(StringComparer.Ordinal);
            ManifestGroup[] groups = manifest.groups ?? new ManifestGroup[0];
            for (int g = 0; g < groups.Length; g++)
            {
                ManifestGroup group = groups[g];
                if (group == null)
                {
                    continue;
                }

                string label = "Group \"" + group.name + "\"";
                if (!names.Add(group.name ?? string.Empty))
                {
                    warnings.Add(label + ": the name is used more than once.");
                }

                if (!TryParseConnection(group.connection, out _))
                {
                    warnings.Add(label + ": unknown connection \"" + group.connection + "\"; using \"none\".");
                }

                if (!IsKnownKind(group.kind))
                {
                    warnings.Add(label + ": unknown kind \"" + group.kind + "\".");
                }

                ResolvePreset(manifest, group.preset, group.kind, out PresetResolution resolution);
                if (string.IsNullOrEmpty(group.preset))
                {
                    warnings.Add(label + ": no preset; using \"" + DefaultPresetForKind(group.kind) + "\", chosen by its kind.");
                }
                else if (resolution == PresetResolution.Default)
                {
                    warnings.Add(label + ": preset \"" + group.preset + "\" is neither defined in the file nor built in; the default parameters are used.");
                }

                ManifestChain[] chains = group.chains ?? new ManifestChain[0];
                int shortest = int.MaxValue;
                int longest = 0;
                for (int c = 0; c < chains.Length; c++)
                {
                    int length = chains[c] != null && chains[c].bones != null ? chains[c].bones.Length : 0;
                    if (length < 2)
                    {
                        warnings.Add(label + ": chain " + c + " has fewer than two bones and is ignored.");
                        continue;
                    }

                    shortest = Math.Min(shortest, length);
                    longest = Math.Max(longest, length);
                }

                if (chains.Length == 0)
                {
                    warnings.Add(label + " has no chains.");
                }

                if (TryParseConnection(group.connection, out ConnectionMode mode)
                    && mode != ConnectionMode.None && longest > shortest)
                {
                    warnings.Add(label + ": chains have different lengths; links between chains stop at the shortest chain (" + shortest + " joints).");
                }
            }

            names.Clear();
            ManifestCollider[] colliders = manifest.colliders ?? new ManifestCollider[0];
            for (int c = 0; c < colliders.Length; c++)
            {
                ManifestCollider collider = colliders[c];
                if (collider == null)
                {
                    continue;
                }

                string label = "Collider \"" + collider.name + "\"";
                if (!names.Add(collider.name ?? string.Empty))
                {
                    warnings.Add(label + ": the name is used more than once.");
                }

                if (!TryParseShape(collider.shape, out ColliderShape shape))
                {
                    warnings.Add(label + ": unknown shape \"" + collider.shape + "\"; the collider is skipped.");
                }
                else if (shape == ColliderShape.Capsule && string.IsNullOrEmpty(collider.to_bone))
                {
                    warnings.Add(label + ": a capsule needs to_bone; the collider is skipped.");
                }

                if (!(collider.radius > 0f))
                {
                    warnings.Add(label + ": radius must be greater than 0; the collider is skipped.");
                }
            }
        }

        /// <summary>Parses <c>none</c>, <c>open</c> or <c>loop</c> (case-insensitive).</summary>
        /// <returns>False for anything else; <paramref name="mode"/> is then <see cref="ConnectionMode.None"/>.</returns>
        public static bool TryParseConnection(string value, out ConnectionMode mode)
        {
            string v = value == null ? string.Empty : value.Trim();
            if (string.Equals(v, "none", StringComparison.OrdinalIgnoreCase))
            {
                mode = ConnectionMode.None;
                return true;
            }

            if (string.Equals(v, "open", StringComparison.OrdinalIgnoreCase))
            {
                mode = ConnectionMode.Open;
                return true;
            }

            if (string.Equals(v, "loop", StringComparison.OrdinalIgnoreCase))
            {
                mode = ConnectionMode.Loop;
                return true;
            }

            mode = ConnectionMode.None;
            return false;
        }

        /// <summary>Parses the manifest collider shapes <c>sphere</c> and <c>capsule</c> (case-insensitive).</summary>
        public static bool TryParseShape(string value, out ColliderShape shape)
        {
            string v = value == null ? string.Empty : value.Trim();
            if (string.Equals(v, "sphere", StringComparison.OrdinalIgnoreCase))
            {
                shape = ColliderShape.Sphere;
                return true;
            }

            if (string.Equals(v, "capsule", StringComparison.OrdinalIgnoreCase))
            {
                shape = ColliderShape.Capsule;
                return true;
            }

            shape = ColliderShape.Sphere;
            return false;
        }

        /// <summary>True for the group kinds of version 1: <c>hair</c>, <c>cloth</c>, <c>tail</c>, <c>accessory</c>.</summary>
        public static bool IsKnownKind(string kind) =>
            kind == "hair" || kind == "cloth" || kind == "tail" || kind == "accessory";

        /// <summary>
        /// Built-in preset used for a group whose <c>preset</c> is empty, chosen by its kind:
        /// hair → <c>hair</c>, cloth → <c>skirt</c>, tail → <c>tail</c>, anything else → <c>accessory</c>.
        /// </summary>
        public static string DefaultPresetForKind(string kind)
        {
            switch (kind)
            {
                case "hair":
                    return BuiltInPresets.Hair;
                case "cloth":
                    return BuiltInPresets.Skirt;
                case "tail":
                    return BuiltInPresets.Tail;
                default:
                    return BuiltInPresets.Accessory;
            }
        }

        /// <summary>
        /// Finds the parameters for a preset name: the manifest's own <c>presets</c> first (exact
        /// name), then the built-in presets, then the specification defaults. An empty name picks a
        /// built-in preset by the group's kind. Radii are returned unscaled (authored metres).
        /// </summary>
        public static ClothParameters ResolvePreset(ClothManifest manifest, string presetName, string kind, out PresetResolution resolution)
        {
            string name = string.IsNullOrEmpty(presetName) ? DefaultPresetForKind(kind) : presetName;
            ManifestPreset[] presets = manifest != null && manifest.presets != null ? manifest.presets : new ManifestPreset[0];
            for (int i = 0; i < presets.Length; i++)
            {
                if (presets[i] != null && string.Equals(presets[i].name, name, StringComparison.Ordinal))
                {
                    resolution = PresetResolution.File;
                    return presets[i].ToParameters().Sanitized();
                }
            }

            if (BuiltInPresets.TryGet(name, out ClothParameters builtIn))
            {
                resolution = PresetResolution.BuiltIn;
                return builtIn;
            }

            resolution = PresetResolution.Default;
            return ClothParameters.Default;
        }

        /// <summary>
        /// Calibration factor: measured world distance between the calibration bones divided by the
        /// authored distance. Returns 1 when either value is missing, zero, negative or not finite.
        /// </summary>
        public static float CalibrationFactor(float measuredDistance, float authoredDistance)
        {
            if (!(measuredDistance > 0f) || !(authoredDistance > 0f)
                || float.IsInfinity(measuredDistance) || float.IsInfinity(authoredDistance))
            {
                return 1f;
            }

            return measuredDistance / authoredDistance;
        }

        /// <summary>
        /// Teleport distance in world units: 3 × the calibrated bone distance
        /// (<paramref name="authoredDistance"/> × <paramref name="factor"/>), or
        /// <see cref="ClothSolver.DefaultTeleportDistance"/> × <paramref name="factor"/> when the
        /// authored distance is unusable.
        /// </summary>
        public static float TeleportDistance(float authoredDistance, float factor)
        {
            float f = factor > 0f && !float.IsInfinity(factor) ? factor : 1f;
            if (authoredDistance > 0f && !float.IsInfinity(authoredDistance))
            {
                return TeleportCalibrationMultiple * authoredDistance * f;
            }

            return ClothSolver.DefaultTeleportDistance * f;
        }
    }
}
