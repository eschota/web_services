using UnityEditor;
using UnityEngine;

namespace AutoRig.Cloth.EditorTools
{
    [CustomEditor(typeof(ClothPreset))]
    [CanEditMultipleObjects]
    internal sealed class ClothPresetEditor : UnityEditor.Editor
    {
        private static string[] s_presetNames;
        private int _selected;

        public override void OnInspectorGUI()
        {
            if (s_presetNames == null)
            {
                s_presetNames = ClothEditorActions.BuiltInPresetNames();
            }

            DrawDefaultInspector();
            EditorGUILayout.Space();
            using (new EditorGUILayout.HorizontalScope())
            {
                _selected = EditorGUILayout.Popup("Load Built-in Values", _selected, s_presetNames);
                if (GUILayout.Button("Load", GUILayout.Width(60f)))
                {
                    foreach (Object t in targets)
                    {
                        Undo.RecordObject(t, "Load Built-in Cloth Preset");
                        ((ClothPreset)t).LoadBuiltIn(s_presetNames[_selected]);
                        EditorUtility.SetDirty(t);
                    }
                }
            }
        }
    }
}
