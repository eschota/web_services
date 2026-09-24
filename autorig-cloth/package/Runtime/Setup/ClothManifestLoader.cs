using System;
using System.Collections.Generic;
using AutoRig.Cloth.Core;
using UnityEngine;

namespace AutoRig.Cloth
{
    /// <summary>
    /// Builds the cloth setup of a character from an AutoRig cloth manifest
    /// (<c>&lt;model&gt;.autorig-cloth.json</c>, format version 1).
    /// </summary>
    /// <remarks>
    /// <para>
    /// Colliders are added to the bones they are anchored to; groups are created as children of an
    /// <c>AutoRigCloth</c> object under the character root. Every length in the file is multiplied by
    /// the calibration factor (measured distance between the calibration bones divided by the
    /// authored distance). Bones are found by exact name, then with namespace prefixes stripped.
    /// A group with an unresolvable bone is skipped with a warning; the rest still load.
    /// </para>
    /// <para>
    /// Applying is idempotent: it first removes everything a previous application created
    /// (and nothing else), so it can be re-run after the manifest changes.
    /// </para>
    /// </remarks>
    public static class ClothManifestLoader
    {
        /// <summary><see cref="ClothBoneGroup.GeneratedBy"/> / <see cref="ClothCollider.GeneratedBy"/> value of objects made by the loader.</summary>
        public const string GeneratorId = "manifest";

        /// <summary>Parses a manifest with <c>JsonUtility</c>.</summary>
        /// <returns>The manifest, or null with a reason in <paramref name="error"/>.</returns>
        public static ClothManifest Parse(string json, out string error)
        {
            if (string.IsNullOrWhiteSpace(json))
            {
                error = "The manifest text is empty.";
                return null;
            }

            try
            {
                ClothManifest manifest = JsonUtility.FromJson<ClothManifest>(json);
                error = manifest == null ? "The manifest could not be read." : null;
                return manifest;
            }
            catch (ArgumentException e)
            {
                error = "The manifest is not valid JSON: " + e.Message;
                return null;
            }
        }

        /// <summary>Applies a manifest given as JSON text to a character.</summary>
        public static ClothSetupReport Apply(GameObject characterRoot, string json) => Apply(characterRoot, json, null);

        /// <summary>Applies a manifest given as JSON text to a character, creating objects through <paramref name="handler"/>.</summary>
        public static ClothSetupReport Apply(GameObject characterRoot, string json, IClothSetupHandler handler)
        {
            ClothManifest manifest = Parse(json, out string error);
            if (manifest == null)
            {
                var report = new ClothSetupReport();
                report.Errors.Add(error);
                return report;
            }

            return Apply(characterRoot, manifest, handler);
        }

        /// <summary>Applies a parsed manifest to a character, creating objects through <paramref name="handler"/> (null = runtime).</summary>
        public static ClothSetupReport Apply(GameObject characterRoot, ClothManifest manifest, IClothSetupHandler handler)
        {
            var report = new ClothSetupReport();
            if (characterRoot == null)
            {
                report.Errors.Add("No character root object was given.");
                return report;
            }

            if (!ManifestRules.ValidateHeader(manifest, out string error))
            {
                report.Errors.Add(error);
                return report;
            }

            IClothSetupHandler h = handler ?? RuntimeClothSetupHandler.Instance;
            Transform root = characterRoot.transform;
            ManifestRules.CollectWarnings(manifest, report.Warnings);
            var bones = new BoneLookup(root, GeneratedObjects.FindContainer(root));

            ManifestCalibration calibration = manifest.calibration ?? new ManifestCalibration();
            float factor = 1f;
            Transform boneA = bones.Find(calibration.bone_a, report.Warnings);
            Transform boneB = bones.Find(calibration.bone_b, report.Warnings);
            if (boneA == null || boneB == null)
            {
                report.Warnings.Add("Calibration bone '" + (boneA == null ? calibration.bone_a : calibration.bone_b) + "' was not found; lengths are used unscaled.");
            }
            else if (!(calibration.distance > 0f))
            {
                report.Warnings.Add("The calibration distance is missing or not positive; lengths are used unscaled.");
            }
            else
            {
                float measured = Vector3.Distance(boneA.position, boneB.position);
                if (!(measured > 0f))
                {
                    report.Warnings.Add("Calibration bones '" + boneA.name + "' and '" + boneB.name + "' are at the same position; lengths are used unscaled.");
                }

                factor = ManifestRules.CalibrationFactor(measured, calibration.distance);
            }

            report.CalibrationFactor = factor;
            report.TeleportDistance = ManifestRules.TeleportDistance(calibration.distance, factor);
            report.RemovedObjects = GeneratedObjects.Remove(root, GeneratorId, h);

            ManifestCollider[] colliders = manifest.colliders ?? new ManifestCollider[0];
            for (int i = 0; i < colliders.Length; i++)
            {
                if (colliders[i] != null)
                {
                    CreateCollider(colliders[i], bones, factor, h, report);
                }
            }

            ManifestGroup[] groups = manifest.groups ?? new ManifestGroup[0];
            Transform container = null;
            for (int i = 0; i < groups.Length; i++)
            {
                if (groups[i] == null)
                {
                    continue;
                }

                List<Transform[]> chains = ResolveChains(groups[i], bones, report, out Transform attach);
                if (chains == null)
                {
                    continue;
                }

                if (container == null)
                {
                    container = GeneratedObjects.GetOrCreateContainer(root, h);
                }

                CreateGroup(manifest, groups[i], attach, chains, container, factor, report.TeleportDistance, h, report);
            }

            return report;
        }

        private static void CreateCollider(ManifestCollider c, BoneLookup bones, float factor, IClothSetupHandler h, ClothSetupReport report)
        {
            // Unknown shapes, missing to_bone on capsules and bad radii were reported by CollectWarnings.
            if (!ManifestRules.TryParseShape(c.shape, out ColliderShape shape) || !(c.radius > 0f))
            {
                return;
            }

            if (shape == ColliderShape.Capsule && string.IsNullOrEmpty(c.to_bone))
            {
                return;
            }

            Transform bone = bones.Find(c.bone, report.Warnings);
            Transform to = string.IsNullOrEmpty(c.to_bone) ? null : bones.Find(c.to_bone, report.Warnings);
            if (bone == null || (!string.IsNullOrEmpty(c.to_bone) && to == null))
            {
                report.Warnings.Add("Collider '" + c.name + "' skipped: bone '" + (bone == null ? c.bone : c.to_bone) + "' not found.");
                return;
            }

            float toLocal = factor / UnityConversions.MaxAbsScale(bone.lossyScale);
            if (shape == ColliderShape.Sphere)
            {
                ClothSphereCollider sphere = h.AddComponent<ClothSphereCollider>(bone.gameObject);
                sphere.Configure(Vector3.zero, c.radius * toLocal, to, c.t);
                sphere.MarkGenerated(GeneratorId, c.tag);
            }
            else
            {
                float endRadius = c.radius_to > 0f ? c.radius_to : c.radius;
                ClothCapsuleCollider capsule = h.AddComponent<ClothCapsuleCollider>(bone.gameObject);
                capsule.Configure(Vector3.zero, Vector3.zero, to, c.radius * toLocal, endRadius * toLocal);
                capsule.MarkGenerated(GeneratorId, c.tag);
            }

            report.CreatedColliders.Add(c.name);
        }

        // Returns null (and a warning) when the attach bone or any chain bone cannot be found.
        private static List<Transform[]> ResolveChains(ManifestGroup group, BoneLookup bones, ClothSetupReport report, out Transform attach)
        {
            attach = bones.Find(group.attach_bone, report.Warnings);
            var missing = new List<string>();
            if (attach == null)
            {
                missing.Add(group.attach_bone);
            }

            var chains = new List<Transform[]>();
            ManifestChain[] manifestChains = group.chains ?? new ManifestChain[0];
            for (int c = 0; c < manifestChains.Length; c++)
            {
                string[] names = manifestChains[c] != null ? manifestChains[c].bones : null;
                if (names == null || names.Length < 2)
                {
                    continue; // reported by CollectWarnings
                }

                var chain = new Transform[names.Length];
                for (int k = 0; k < names.Length; k++)
                {
                    chain[k] = bones.Find(names[k], report.Warnings);
                    if (chain[k] == null)
                    {
                        missing.Add(names[k]);
                    }
                }

                chains.Add(chain);
            }

            if (missing.Count > 0)
            {
                const int shown = 5;
                string list = string.Join("', '", missing.GetRange(0, Math.Min(shown, missing.Count)));
                report.Warnings.Add("Group '" + group.name + "' skipped: bone(s) not found: '" + list + "'" + (missing.Count > shown ? " and " + (missing.Count - shown) + " more." : "."));
                return null;
            }

            if (chains.Count == 0)
            {
                report.Warnings.Add("Group '" + group.name + "' skipped: it has no usable chain.");
                return null;
            }

            return chains;
        }

        private static void CreateGroup(
            ClothManifest manifest,
            ManifestGroup group,
            Transform attach,
            List<Transform[]> chains,
            Transform container,
            float factor,
            float teleport,
            IClothSetupHandler h,
            ClothSetupReport report)
        {
            // Unknown connections and presets were reported by CollectWarnings.
            ManifestRules.TryParseConnection(group.connection, out ConnectionMode connection);
            ClothParameters parameters = ManifestRules.ResolvePreset(manifest, group.preset, group.kind, out PresetResolution resolution);
            string presetName = string.IsNullOrEmpty(group.preset) ? ManifestRules.DefaultPresetForKind(group.kind) : group.preset;
            GameObject go = h.CreateGameObject(string.IsNullOrEmpty(group.name) ? "ClothGroup" : group.name, container, false);
            ClothBoneGroup component = h.AddComponent<ClothBoneGroup>(go);
            float lossy = UnityConversions.MaxAbsScale(go.transform.lossyScale);
            component.Configure(
                attach,
                chains,
                connection,
                resolution == PresetResolution.BuiltIn ? ClothPresetSource.BuiltIn : ClothPresetSource.Custom,
                presetName,
                parameters,
                group.collider_tags ?? new string[0],
                factor / lossy,
                teleport / lossy,
                GeneratorId);
            go.SetActive(true);
            report.CreatedGroups.Add(go.name);
        }
    }
}
