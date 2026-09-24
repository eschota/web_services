using UnityEditor;
using UnityEngine;

namespace AutoRig.Cloth.EditorTools
{
    [CustomEditor(typeof(AutoRigClothSetup))]
    internal sealed class AutoRigClothSetupEditor : UnityEditor.Editor
    {
        private SerializedProperty _manifest;
        private SerializedProperty _applyOnAwake;

        private void OnEnable()
        {
            _manifest = serializedObject.FindProperty("manifest");
            _applyOnAwake = serializedObject.FindProperty("applyOnAwake");
        }

        public override void OnInspectorGUI()
        {
            var setup = (AutoRigClothSetup)target;
            serializedObject.Update();
            EditorGUILayout.PropertyField(_manifest);
            EditorGUILayout.PropertyField(_applyOnAwake);
            serializedObject.ApplyModifiedProperties();

            if (setup.Manifest == null)
            {
                TextAsset found = ClothEditorActions.FindManifest(setup.gameObject);
                if (found != null)
                {
                    EditorGUILayout.HelpBox("Found '" + found.name + "' next to the model.", MessageType.Info);
                    if (GUILayout.Button("Use It"))
                    {
                        Undo.RecordObject(setup, "Assign AutoRig Cloth Manifest");
                        setup.Manifest = found;
                    }
                }
                else
                {
                    EditorGUILayout.HelpBox("Assign the <model>.autorig-cloth.json file AutoRig wrote next to the model, or set the character up without one with Auto-Detect Chains and Build Humanoid Colliders.", MessageType.Info);
                }
            }

            EditorGUILayout.Space();
            using (new EditorGUI.DisabledScope(setup.Manifest == null))
            {
                if (GUILayout.Button(new GUIContent("Apply Manifest", "Create the groups and colliders the manifest describes, replacing what a previous Apply created.")))
                {
                    ClothEditorActions.ApplyManifest(setup.gameObject, setup.Manifest);
                }
            }

            using (new EditorGUILayout.HorizontalScope())
            {
                if (GUILayout.Button(new GUIContent("Auto-Detect Chains", "Find hair, skirt, cape, tail and similar chains by bone name and create groups for them.")))
                {
                    ClothEditorActions.AutoDetectChains(setup.gameObject);
                }

                if (GUILayout.Button(new GUIContent("Build Humanoid Colliders", "Add head, torso, arm, hand and leg colliders sized from the humanoid avatar.")))
                {
                    ClothEditorActions.BuildHumanoidColliders(setup.gameObject);
                }
            }

            using (new EditorGUI.DisabledScope(!EditorApplication.isPlaying))
            {
                if (GUILayout.Button(new GUIContent("Reset Simulation", "Snap every group to the animated pose (Play mode).")))
                {
                    setup.ResetSimulation();
                }
            }

            EditorGUILayout.Space();
            EditorGUILayout.LabelField(ClothEditorActions.DescribeCharacter(setup.gameObject), EditorStyles.miniLabel);
            string report = ClothEditorActions.GetLastReport(setup.gameObject);
            if (report != null)
            {
                EditorGUILayout.HelpBox(report, MessageType.None);
            }
        }
    }
}
