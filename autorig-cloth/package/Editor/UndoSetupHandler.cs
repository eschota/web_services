using System.Collections.Generic;
using UnityEditor;
using UnityEngine;

namespace AutoRig.Cloth.EditorTools
{
    /// <summary>
    /// Creates and destroys objects through Undo, so a whole setup action can be undone in one step.
    /// </summary>
    internal sealed class UndoSetupHandler : IClothSetupHandler
    {
        private readonly string _label;

        public UndoSetupHandler(string label)
        {
            _label = label;
        }

        /// <summary>Things the handler had to do differently (reported to the user afterwards).</summary>
        public List<string> Notes { get; } = new List<string>();

        public GameObject CreateGameObject(string name, Transform parent, bool active)
        {
            var go = new GameObject(name);
            go.SetActive(active);
            go.transform.SetParent(parent, false);
            Undo.RegisterCreatedObjectUndo(go, _label);
            return go;
        }

        public T AddComponent<T>(GameObject target) where T : Component => Undo.AddComponent<T>(target);

        public void DestroyObject(Object target)
        {
            if (target == null)
            {
                return;
            }

            // Unity 2021 cannot delete a GameObject that comes from a prefab asset out of a prefab
            // instance; remove its generated components instead and leave the empty object.
            if (target is GameObject go && PrefabUtility.IsPartOfPrefabInstance(go) && !PrefabUtility.IsAddedGameObjectOverride(go))
            {
                ClothBoneGroup[] groups = go.GetComponents<ClothBoneGroup>();
                for (int i = 0; i < groups.Length; i++)
                {
                    Undo.DestroyObjectImmediate(groups[i]);
                }

                Notes.Add("'" + go.name + "' belongs to the prefab, so only its components were removed. Re-apply in Prefab Mode to replace it cleanly.");
                return;
            }

            Undo.DestroyObjectImmediate(target);
        }

        public void RecordObject(Object target)
        {
            if (target != null)
            {
                Undo.RecordObject(target, _label);
            }
        }
    }
}
