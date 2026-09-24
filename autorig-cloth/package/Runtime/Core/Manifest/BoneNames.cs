using System;

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
}
