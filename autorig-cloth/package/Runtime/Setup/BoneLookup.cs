using System;
using System.Collections.Generic;
using AutoRig.Cloth.Core;
using UnityEngine;

namespace AutoRig.Cloth
{
    /// <summary>
    /// Finds bones in a character hierarchy by the manifest rules: exact name first, then the name
    /// with any namespace prefix stripped on both sides (<c>mixamorig:Hips</c> matches <c>Hips</c>).
    /// When several transforms share a name, the first one in depth-first order wins.
    /// </summary>
    public sealed class BoneLookup
    {
        private readonly Dictionary<string, Transform> _exact = new Dictionary<string, Transform>(StringComparer.Ordinal);
        private readonly Dictionary<string, Transform> _stripped = new Dictionary<string, Transform>(StringComparer.Ordinal);
        private readonly HashSet<string> _ambiguous = new HashSet<string>(StringComparer.Ordinal);
        private readonly HashSet<string> _reported = new HashSet<string>(StringComparer.Ordinal);

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

            var stack = new Stack<Transform>();
            stack.Push(root);
            while (stack.Count > 0)
            {
                Transform t = stack.Pop();
                if (exclude != null && t == exclude)
                {
                    continue;
                }

                string name = t.name;
                if (!_exact.ContainsKey(name))
                {
                    _exact.Add(name, t);
                }
                else
                {
                    _ambiguous.Add(name);
                }

                string stripped = BoneNames.StripNamespace(name);
                if (stripped.Length > 0)
                {
                    if (!_stripped.ContainsKey(stripped))
                    {
                        _stripped.Add(stripped, t);
                    }
                    else if (_stripped[stripped] != t)
                    {
                        _ambiguous.Add(stripped);
                    }
                }

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
        public Transform Find(string boneName, List<string> warnings = null)
        {
            if (string.IsNullOrEmpty(boneName))
            {
                return null;
            }

            if (_exact.TryGetValue(boneName, out Transform bone))
            {
                Report(boneName, boneName, warnings);
                return bone;
            }

            string stripped = BoneNames.StripNamespace(boneName);
            if (stripped.Length > 0 && _stripped.TryGetValue(stripped, out bone))
            {
                Report(stripped, boneName, warnings);
                return bone;
            }

            return null;
        }

        private void Report(string key, string boneName, List<string> warnings)
        {
            if (warnings != null && _ambiguous.Contains(key) && _reported.Add(key))
            {
                warnings.Add("Several bones match '" + boneName + "'; the first one in the hierarchy is used.");
            }
        }
    }
}
