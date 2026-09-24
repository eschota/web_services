using System.Collections.Generic;
using AutoRig.Cloth.Core;
using UnityEngine;

namespace AutoRig.Cloth
{
    /// <summary>
    /// Finds bones in a character hierarchy by the manifest rules: exact name first, then the name
    /// with any namespace prefix stripped on both sides (<c>mixamorig:Hips</c> matches <c>Hips</c>).
    /// When several transforms match, the first one in depth-first order wins (see
    /// <see cref="BoneNameIndex{T}"/>).
    /// </summary>
    public sealed class BoneLookup
    {
        private readonly BoneNameIndex<Transform> _index = new BoneNameIndex<Transform>();

        /// <summary>
        /// Indexes every transform under <paramref name="root"/> (inclusive), except the subtree of
        /// <paramref name="exclude"/> (for example the generated <c>AutoRigCloth</c> object).
        /// </summary>
        public BoneLookup(Transform root, Transform exclude = null)
        {
            if (root == null)
            {
                return;
            }

            // Pre-order depth-first: children are pushed in reverse so the first child is visited first.
            var stack = new Stack<Transform>();
            stack.Push(root);
            while (stack.Count > 0)
            {
                Transform t = stack.Pop();
                if (exclude != null && t == exclude)
                {
                    continue;
                }

                _index.Add(t.name, t);
                for (int i = t.childCount - 1; i >= 0; i--)
                {
                    stack.Push(t.GetChild(i));
                }
            }
        }

        /// <summary>
        /// Finds a bone. Adds a warning (once per name) to <paramref name="warnings"/> when several
        /// transforms match.
        /// </summary>
        /// <returns>The bone, or null when nothing matches.</returns>
        public Transform Find(string boneName, List<string> warnings = null) => _index.Find(boneName, warnings);
    }
}
