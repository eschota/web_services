using UnityEditor;
using UnityEngine;

namespace AutoRig.Cloth.EditorTools
{
    /// <summary>The Tools/AutoRig Cloth menu. Every item works on the selected character root.</summary>
    internal static class ClothMenuItems
    {
        private const string Root = "Tools/AutoRig Cloth/";

        [MenuItem(Root + "Apply Manifest to Selected Character", false, 1)]
        private static void ApplyManifest()
        {
            GameObject character = Selection.activeGameObject;
            TextAsset manifest = ClothEditorActions.FindManifest(character);
            if (manifest != null)
            {
                ClothEditorActions.ApplyManifest(character, manifest);
                return;
            }

            if (ClothEditorActions.TryPickManifestFile(out string json, out string fileName))
            {
                ClothEditorActions.ApplyManifestText(character, json, fileName);
            }
        }

        [MenuItem(Root + "Auto-Detect Chains on Selected Character", false, 2)]
        private static void AutoDetectChains()
        {
            ClothEditorActions.AutoDetectChains(Selection.activeGameObject);
        }

        [MenuItem(Root + "Build Humanoid Colliders on Selected Character", false, 3)]
        private static void BuildHumanoidColliders()
        {
            ClothEditorActions.BuildHumanoidColliders(Selection.activeGameObject);
        }

        [MenuItem(Root + "Add Setup Component to Selected Character", false, 20)]
        private static void AddSetup()
        {
            GameObject character = Selection.activeGameObject;
            AutoRigClothSetup setup = character.GetComponent<AutoRigClothSetup>();
            if (setup == null)
            {
                setup = Undo.AddComponent<AutoRigClothSetup>(character);
            }

            if (setup.Manifest == null)
            {
                TextAsset manifest = ClothEditorActions.FindManifest(character);
                if (manifest != null)
                {
                    Undo.RecordObject(setup, "Assign AutoRig Cloth Manifest");
                    setup.Manifest = manifest;
                }
            }

            EditorGUIUtility.PingObject(setup);
        }

        [MenuItem(Root + "Reset Simulation on Selected Character", false, 40)]
        private static void ResetSimulation()
        {
            ClothEditorActions.ResetSimulation(Selection.activeGameObject);
        }

        [MenuItem(Root + "Apply Manifest to Selected Character", true)]
        [MenuItem(Root + "Auto-Detect Chains on Selected Character", true)]
        [MenuItem(Root + "Build Humanoid Colliders on Selected Character", true)]
        [MenuItem(Root + "Add Setup Component to Selected Character", true)]
        private static bool HasSceneSelection()
        {
            GameObject go = Selection.activeGameObject;
            return go != null && !EditorUtility.IsPersistent(go);
        }

        [MenuItem(Root + "Reset Simulation on Selected Character", true)]
        private static bool CanReset() => EditorApplication.isPlaying && Selection.activeGameObject != null;
    }
}
