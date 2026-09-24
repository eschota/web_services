using System;
using System.Collections.Generic;
using System.IO;
using AutoRig.Cloth.Core;
using UnityEditor;
using UnityEngine;

namespace AutoRig.Cloth.EditorTools
{
    /// <summary>
    /// The setup actions shared by the inspectors and the Tools/AutoRig Cloth menu. Each action is a
    /// single Undo step and logs a report.
    /// </summary>
    internal static class ClothEditorActions
    {
        /// <summary>File name suffix of manifests written next to models.</summary>
        public const string ManifestSuffix = ".autorig-cloth.json";

        private static readonly Dictionary<int, string> s_lastReports = new Dictionary<int, string>();

        /// <summary>Last report of an action run on <paramref name="character"/> in this editor session, or null.</summary>
        public static string GetLastReport(GameObject character)
        {
            return character != null && s_lastReports.TryGetValue(character.GetInstanceID(), out string text) ? text : null;
        }

        /// <summary>
        /// The manifest for a character: the one assigned to its <see cref="AutoRigClothSetup"/>, else
        /// <c>&lt;model&gt;.autorig-cloth.json</c> next to the model asset the character comes from.
        /// </summary>
        public static TextAsset FindManifest(GameObject character)
        {
            if (character == null)
            {
                return null;
            }

            AutoRigClothSetup setup = character.GetComponent<AutoRigClothSetup>();
            if (setup != null && setup.Manifest != null)
            {
                return setup.Manifest;
            }

            string modelPath = ModelAssetPath(character);
            if (string.IsNullOrEmpty(modelPath))
            {
                return null;
            }

            string directory = Path.GetDirectoryName(modelPath);
            string stem = Path.GetFileNameWithoutExtension(modelPath);
            string candidate = string.IsNullOrEmpty(directory) ? stem + ManifestSuffix : directory.Replace('\\', '/') + "/" + stem + ManifestSuffix;
            return AssetDatabase.LoadAssetAtPath<TextAsset>(candidate);
        }

        /// <summary>Applies a manifest asset to a character.</summary>
        public static void ApplyManifest(GameObject character, TextAsset manifest)
        {
            if (character == null || manifest == null)
            {
                return;
            }

            ApplyManifestText(character, manifest.text, manifest.name);
        }

        /// <summary>Applies manifest JSON to a character.</summary>
        public static void ApplyManifestText(GameObject character, string json, string sourceName)
        {
            Run("Apply AutoRig Cloth Manifest", character, h => ClothManifestLoader.Apply(character, json, h), "Applying '" + sourceName + "' to '" + character.name + "'");
        }

        /// <summary>Detects chains by bone name and creates groups for them.</summary>
        public static void AutoDetectChains(GameObject character)
        {
            if (character == null)
            {
                return;
            }

            Animator animator = character.GetComponentInChildren<Animator>();
            List<DetectedChainGroup> groups = ChainDetector.Detect(character.transform, animator);
            Run("Auto-Detect Cloth Chains", character, h => ChainDetector.CreateGroups(character.transform, groups, true, animator, h), "Chain detection on '" + character.name + "'");
        }

        /// <summary>Builds humanoid body colliders.</summary>
        public static void BuildHumanoidColliders(GameObject character)
        {
            if (character == null)
            {
                return;
            }

            Animator animator = character.GetComponentInChildren<Animator>();
            Run("Build Humanoid Cloth Colliders", character, h => HumanoidColliderBuilder.Build(animator, h), "Humanoid colliders on '" + character.name + "'");
        }

        /// <summary>Snaps every group under the character to its animated pose (Play mode).</summary>
        public static void ResetSimulation(GameObject character)
        {
            if (character == null)
            {
                return;
            }

            ClothBoneGroup[] groups = character.GetComponentsInChildren<ClothBoneGroup>();
            for (int i = 0; i < groups.Length; i++)
            {
                groups[i].ResetSimulation();
            }
        }

        /// <summary>Builds the chains of groups from their root bones.</summary>
        public static void BuildChainsFromRoots(ClothBoneGroup[] groups)
        {
            const string label = "Build Cloth Chains From Roots";
            Undo.IncrementCurrentGroup();
            Undo.SetCurrentGroupName(label);
            int undoGroup = Undo.GetCurrentGroup();
            var handler = new UndoSetupHandler(label);
            for (int i = 0; i < groups.Length; i++)
            {
                int count = groups[i].BuildChainsFromRoots(groups[i].ChainRoots, groups[i].CreateEndJoints, handler);
                if (count == 0)
                {
                    Debug.LogWarning("[AutoRig Cloth] '" + groups[i].name + "': no chain could be built. Add root bones that have at least one child, or enable Create End Joints.", groups[i]);
                }
            }

            Undo.CollapseUndoOperations(undoGroup);
        }

        /// <summary>Counts the groups and colliders under a character, by the tool that made them.</summary>
        public static string DescribeCharacter(GameObject character)
        {
            ClothBoneGroup[] groups = character.GetComponentsInChildren<ClothBoneGroup>(true);
            ClothCollider[] colliders = character.GetComponentsInChildren<ClothCollider>(true);
            int particles = 0;
            for (int i = 0; i < groups.Length; i++)
            {
                for (int c = 0; c < groups[i].ChainCount; c++)
                {
                    ClothChain chain = groups[i].GetChain(c);
                    particles += chain != null && chain.bones != null ? chain.bones.Length : 0;
                }
            }

            return groups.Length + " group(s) with " + particles + " joint(s), " + colliders.Length + " collider(s).";
        }

        private static void Run(string label, GameObject character, Func<IClothSetupHandler, ClothSetupReport> action, string title)
        {
            Undo.IncrementCurrentGroup();
            Undo.SetCurrentGroupName(label);
            int undoGroup = Undo.GetCurrentGroup();
            var handler = new UndoSetupHandler(label);
            ClothSetupReport report = action(handler);
            report.Warnings.AddRange(handler.Notes);
            Undo.CollapseUndoOperations(undoGroup);
            s_lastReports[character.GetInstanceID()] = report.ToString();
            report.Log(title, character);
        }

        private static string ModelAssetPath(GameObject character)
        {
            GameObject source = PrefabUtility.GetCorrespondingObjectFromOriginalSource(character);
            string path = source != null ? AssetDatabase.GetAssetPath(source) : null;
            if (string.IsNullOrEmpty(path) && EditorUtility.IsPersistent(character))
            {
                path = AssetDatabase.GetAssetPath(character);
            }

            return path;
        }

        /// <summary>Lets the user pick a manifest file when none is found next to the model.</summary>
        public static bool TryPickManifestFile(out string json, out string fileName)
        {
            string path = EditorUtility.OpenFilePanel("Select an AutoRig cloth manifest", Application.dataPath, "json");
            if (string.IsNullOrEmpty(path))
            {
                json = null;
                fileName = null;
                return false;
            }

            json = File.ReadAllText(path);
            fileName = Path.GetFileName(path);
            return true;
        }

        /// <summary>Built-in preset names as an array for popups.</summary>
        public static string[] BuiltInPresetNames()
        {
            var names = new string[BuiltInPresets.Names.Count];
            for (int i = 0; i < names.Length; i++)
            {
                names[i] = BuiltInPresets.Names[i];
            }

            return names;
        }
    }
}
