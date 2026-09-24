using System;
using System.Collections.Generic;
using AutoRig.Cloth.Core;
using UnityEditor;
using UnityEngine;

namespace AutoRig.Cloth.EditorTools
{
    [CustomEditor(typeof(ClothBoneGroup))]
    [CanEditMultipleObjects]
    internal sealed class ClothBoneGroupEditor : UnityEditor.Editor
    {
        private static readonly List<string> s_problems = new List<string>();
        private static string[] s_presetNames;

        private SerializedProperty _attachBone;
        private SerializedProperty _chains;
        private SerializedProperty _connection;
        private SerializedProperty _presetSource;
        private SerializedProperty _builtInPreset;
        private SerializedProperty _presetAsset;
        private SerializedProperty _customSettings;
        private SerializedProperty _colliderTags;
        private SerializedProperty _radiusScale;
        private SerializedProperty _teleportDistance;
        private SerializedProperty _blendWeight;
        private SerializedProperty _chainRoots;
        private SerializedProperty _createEndJoints;
        private int _loadPreset;

        private void OnEnable()
        {
            _attachBone = serializedObject.FindProperty("attachBone");
            _chains = serializedObject.FindProperty("chains");
            _connection = serializedObject.FindProperty("connection");
            _presetSource = serializedObject.FindProperty("presetSource");
            _builtInPreset = serializedObject.FindProperty("builtInPreset");
            _presetAsset = serializedObject.FindProperty("presetAsset");
            _customSettings = serializedObject.FindProperty("customSettings");
            _colliderTags = serializedObject.FindProperty("colliderTags");
            _radiusScale = serializedObject.FindProperty("radiusScale");
            _teleportDistance = serializedObject.FindProperty("teleportDistance");
            _blendWeight = serializedObject.FindProperty("blendWeight");
            _chainRoots = serializedObject.FindProperty("chainRoots");
            _createEndJoints = serializedObject.FindProperty("createEndJoints");
            if (s_presetNames == null)
            {
                s_presetNames = ClothEditorActions.BuiltInPresetNames();
            }
        }

        public override void OnInspectorGUI()
        {
            serializedObject.Update();

            EditorGUILayout.LabelField("Chains", EditorStyles.boldLabel);
            EditorGUILayout.PropertyField(_attachBone);
            EditorGUILayout.PropertyField(_connection);
            EditorGUILayout.PropertyField(_chains, true);

            EditorGUILayout.Space();
            EditorGUILayout.LabelField("Parameters", EditorStyles.boldLabel);
            EditorGUILayout.PropertyField(_presetSource);
            if (!_presetSource.hasMultipleDifferentValues)
            {
                DrawPresetFields((ClothPresetSource)_presetSource.enumValueIndex);
            }

            EditorGUILayout.Space();
            EditorGUILayout.LabelField("Collision and Motion", EditorStyles.boldLabel);
            EditorGUILayout.PropertyField(_colliderTags, true);
            EditorGUILayout.PropertyField(_radiusScale);
            EditorGUILayout.PropertyField(_teleportDistance);
            EditorGUILayout.PropertyField(_blendWeight);

            EditorGUILayout.Space();
            EditorGUILayout.LabelField("Build Chains From Roots", EditorStyles.boldLabel);
            EditorGUILayout.PropertyField(_chainRoots, true);
            EditorGUILayout.PropertyField(_createEndJoints);
            serializedObject.ApplyModifiedProperties();

            if (GUILayout.Button("Build Chains From Roots"))
            {
                ClothEditorActions.BuildChainsFromRoots(Array.ConvertAll(targets, t => (ClothBoneGroup)t));
                serializedObject.Update();
            }

            if (!serializedObject.isEditingMultipleObjects)
            {
                DrawStatus((ClothBoneGroup)target);
            }
        }

        private void DrawPresetFields(ClothPresetSource source)
        {
            switch (source)
            {
                case ClothPresetSource.BuiltIn:
                    if (_builtInPreset.hasMultipleDifferentValues)
                    {
                        EditorGUILayout.PropertyField(_builtInPreset);
                        break;
                    }

                    int index = Array.IndexOf(s_presetNames, _builtInPreset.stringValue);
                    if (index < 0)
                    {
                        EditorGUILayout.PropertyField(_builtInPreset);
                        index = EditorGUILayout.Popup(" ", -1, s_presetNames);
                    }
                    else
                    {
                        index = EditorGUILayout.Popup(new GUIContent("Built-in Preset", _builtInPreset.tooltip), index, s_presetNames);
                    }

                    if (index >= 0 && s_presetNames[index] != _builtInPreset.stringValue)
                    {
                        _builtInPreset.stringValue = s_presetNames[index];
                    }

                    break;
                case ClothPresetSource.Asset:
                    EditorGUILayout.PropertyField(_presetAsset);
                    break;
                default:
                    EditorGUILayout.PropertyField(_customSettings, true);
                    using (new EditorGUILayout.HorizontalScope())
                    {
                        _loadPreset = EditorGUILayout.Popup("Load Built-in Values", _loadPreset, s_presetNames);
                        if (GUILayout.Button("Load", GUILayout.Width(60f)))
                        {
                            serializedObject.ApplyModifiedProperties();
                            ClothParameters values = BuiltInPresets.GetOrDefault(s_presetNames[_loadPreset]);
                            foreach (UnityEngine.Object t in targets)
                            {
                                Undo.RecordObject(t, "Load Built-in Cloth Preset");
                                ((ClothBoneGroup)t).CustomSettings.SetFrom(values);
                                EditorUtility.SetDirty(t);
                            }

                            serializedObject.Update();
                        }
                    }

                    break;
            }
        }

        private static void DrawStatus(ClothBoneGroup group)
        {
            s_problems.Clear();
            group.CollectProblems(s_problems);
            for (int i = 0; i < s_problems.Count; i++)
            {
                EditorGUILayout.HelpBox(s_problems[i], MessageType.Warning);
            }

            if (!string.IsNullOrEmpty(group.GeneratedBy))
            {
                EditorGUILayout.HelpBox("Created by the '" + group.GeneratedBy + "' setup tool. Re-running that tool replaces this group; copy it first if you want to keep manual edits.", MessageType.Info);
            }

            if (!EditorApplication.isPlaying)
            {
                return;
            }

            EditorGUILayout.Space();
            EditorGUILayout.LabelField("Simulation", group.IsSimulating ? group.ParticleCount + " joints simulated" : "not simulated");
            using (new EditorGUILayout.HorizontalScope())
            {
                if (GUILayout.Button("Reset Simulation"))
                {
                    group.ResetSimulation();
                }

                if (GUILayout.Button("Rebuild"))
                {
                    group.Rebuild();
                }
            }
        }
    }
}
