using System;
using System.Collections.Generic;

namespace AutoRig.Cloth.Core
{
    /// <summary>
    /// Bone name rules of the cloth manifest: a name matches exactly first, then with any namespace
    /// prefix stripped (<c>mixamorig:Hips</c> → <c>Hips</c>, <c>Armature|Hips</c> → <c>Hips</c>).
    /// </summary>
    public static class BoneNames
    {
        private static readonly char[] s_separators = { ':', '|' };

        /// <summary>
        /// Returns the part of <paramref name="name"/> after its last namespace separator
        /// (<c>:</c> or <c>|</c>), or the name itself when it has none. Null becomes empty.
        /// </summary>
        public static string StripNamespace(string name)
        {
            if (string.IsNullOrEmpty(name))
            {
                return string.Empty;
            }

            int index = name.LastIndexOfAny(s_separators);
            return index < 0 ? name : name.Substring(index + 1);
        }

        /// <summary>True when <paramref name="name"/> contains a namespace separator.</summary>
        public static bool HasNamespace(string name) =>
            !string.IsNullOrEmpty(name) && name.IndexOfAny(s_separators) >= 0;

        /// <summary>
        /// True when two bone names refer to the same bone under the manifest rules: equal, or equal
        /// once both have their namespace prefixes stripped. Comparison is ordinal (case-sensitive).
        /// </summary>
        public static bool Matches(string sceneName, string manifestName)
        {
            if (string.IsNullOrEmpty(sceneName) || string.IsNullOrEmpty(manifestName))
            {
                return false;
            }

            if (string.Equals(sceneName, manifestName, StringComparison.Ordinal))
            {
                return true;
            }

            string a = StripNamespace(sceneName);
            return a.Length > 0 && string.Equals(a, StripNamespace(manifestName), StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// Finds bones by the manifest's name rules: the exact name first, then the name with any
    /// namespace prefix stripped on both sides. Add the bones in depth-first hierarchy order: when
    /// several bones match, the first one added wins and <see cref="Find"/> reports it once.
    /// </summary>
    /// <typeparam name="T">The bone type (a Unity <c>Transform</c> in the package).</typeparam>
    public sealed class BoneNameIndex<T>
        where T : class
    {
        private readonly Dictionary<string, T> _exact = new Dictionary<string, T>(StringComparer.Ordinal);
        private readonly Dictionary<string, T> _stripped = new Dictionary<string, T>(StringComparer.Ordinal);

        // Names several bones share, exactly or once stripped. Kept apart so that a unique exact match
        // is never reported as ambiguous; a name leaves its set once its warning has been given.
        private readonly HashSet<string> _ambiguousExact = new HashSet<string>(StringComparer.Ordinal);
        private readonly HashSet<string> _ambiguousStripped = new HashSet<string>(StringComparer.Ordinal);

        /// <summary>Adds a bone. Call in depth-first hierarchy order; empty names are ignored.</summary>
        public void Add(string name, T bone)
        {
            if (string.IsNullOrEmpty(name) || bone == null)
            {
                return;
            }

            if (!_exact.TryGetValue(name, out T first))
            {
                _exact.Add(name, bone);
            }
            else if (!ReferenceEquals(first, bone))
            {
                _ambiguousExact.Add(name);
            }

            string stripped = BoneNames.StripNamespace(name);
            if (stripped.Length == 0)
            {
                return;
            }

            if (!_stripped.TryGetValue(stripped, out first))
            {
                _stripped.Add(stripped, bone);
            }
            else if (!ReferenceEquals(first, bone))
            {
                _ambiguousStripped.Add(stripped);
            }
        }

        /// <summary>
        /// Finds a bone. Adds a warning (once per name) to <paramref name="warnings"/> when several
        /// bones match the way this one was found.
        /// </summary>
        /// <returns>The bone, or null when nothing matches.</returns>
        public T Find(string name, List<string> warnings = null)
        {
            if (string.IsNullOrEmpty(name))
            {
                return null;
            }

            if (_exact.TryGetValue(name, out T bone))
            {
                Report(_ambiguousExact, name, name, warnings);
                return bone;
            }

            string stripped = BoneNames.StripNamespace(name);
            if (stripped.Length > 0 && _stripped.TryGetValue(stripped, out bone))
            {
                Report(_ambiguousStripped, stripped, name, warnings);
                return bone;
            }

            return null;
        }

        private static void Report(HashSet<string> ambiguous, string key, string name, List<string> warnings)
        {
            if (warnings != null && ambiguous.Remove(key))
            {
                warnings.Add("Several bones match '" + name + "'; the first one in the hierarchy is used.");
            }
        }
    }
}
