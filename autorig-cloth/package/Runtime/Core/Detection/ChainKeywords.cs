using System;
using System.Collections.Generic;
using System.Text;

namespace AutoRig.Cloth.Core
{
    /// <summary>What a bone name says about the part it belongs to.</summary>
    public enum ChainCategory
    {
        /// <summary>Not a secondary-motion bone.</summary>
        None = 0,

        /// <summary>Hair strands, ponytails, braids, twin tails.</summary>
        Hair,

        /// <summary>Bangs / fringe.</summary>
        Bangs,

        /// <summary>Skirt panels.</summary>
        Skirt,

        /// <summary>Coat tails.</summary>
        Coat,

        /// <summary>Capes and cloaks.</summary>
        Cape,

        /// <summary>Scarf ends.</summary>
        Scarf,

        /// <summary>Ribbons.</summary>
        Ribbon,

        /// <summary>Loose sleeves.</summary>
        Sleeve,

        /// <summary>Animal tails.</summary>
        Tail,

        /// <summary>Animal ears.</summary>
        Ear,
    }

    /// <summary>
    /// Name heuristics for automatic chain detection. Long, unambiguous words (<c>skirt</c>,
    /// <c>ponytail</c>, <c>cape</c>…) match anywhere in the name; short words that hide inside other
    /// words (<c>ear</c> in <i>forearm</i>, <c>tail</c> in <i>detail</i>) must be whole name tokens.
    /// </summary>
    public static class ChainKeywords
    {
        /// <summary>
        /// Classifies a bone by name. The namespace prefix is ignored and matching is
        /// case-insensitive; tokens are split at separators, letter/digit changes and camelCase humps.
        /// </summary>
        public static ChainCategory Classify(string boneName)
        {
            string name = BoneNames.StripNamespace(boneName);
            if (name.Length == 0)
            {
                return ChainCategory.None;
            }

            string lower = name.ToLowerInvariant();
            List<string> tokens = Tokenize(name);

            if (HasToken(tokens, "bang") || HasToken(tokens, "fringe"))
            {
                return ChainCategory.Bangs;
            }

            if (lower.Contains("ponytail") || lower.Contains("twintail") || lower.Contains("braid")
                || (lower.Contains("hair") && !lower.Contains("chair")))
            {
                return ChainCategory.Hair;
            }

            if (lower.Contains("skirt"))
            {
                return ChainCategory.Skirt;
            }

            if (lower.Contains("cape") || lower.Contains("cloak"))
            {
                return ChainCategory.Cape;
            }

            if (lower.Contains("scarf"))
            {
                return ChainCategory.Scarf;
            }

            if (lower.Contains("ribbon"))
            {
                return ChainCategory.Ribbon;
            }

            if (lower.Contains("sleeve"))
            {
                return ChainCategory.Sleeve;
            }

            if (lower.Contains("coattail") || HasToken(tokens, "coat"))
            {
                return ChainCategory.Coat;
            }

            if (HasToken(tokens, "tail"))
            {
                return ChainCategory.Tail;
            }

            if (HasToken(tokens, "ear"))
            {
                return ChainCategory.Ear;
            }

            return ChainCategory.None;
        }

        /// <summary>Manifest group kind for a category: <c>hair</c>, <c>cloth</c>, <c>tail</c> or <c>accessory</c>.</summary>
        public static string KindOf(ChainCategory category)
        {
            switch (category)
            {
                case ChainCategory.Hair:
                case ChainCategory.Bangs:
                    return "hair";
                case ChainCategory.Skirt:
                case ChainCategory.Coat:
                case ChainCategory.Cape:
                case ChainCategory.Sleeve:
                    return "cloth";
                case ChainCategory.Tail:
                    return "tail";
                default:
                    return "accessory";
            }
        }

        /// <summary>Built-in preset name for a category.</summary>
        public static string PresetOf(ChainCategory category)
        {
            switch (category)
            {
                case ChainCategory.Hair:
                    return BuiltInPresets.Hair;
                case ChainCategory.Bangs:
                    return BuiltInPresets.HairStiff;
                case ChainCategory.Skirt:
                case ChainCategory.Coat:
                    return BuiltInPresets.Skirt;
                case ChainCategory.Cape:
                case ChainCategory.Sleeve:
                    return BuiltInPresets.Cape;
                case ChainCategory.Scarf:
                case ChainCategory.Ribbon:
                    return BuiltInPresets.Ribbon;
                case ChainCategory.Tail:
                    return BuiltInPresets.Tail;
                default:
                    return BuiltInPresets.Accessory;
            }
        }

        /// <summary>
        /// Connection for a detected group. Skirts and sleeves become a <c>loop</c> when at least three
        /// chains go all the way around (no angular gap of 120° or more), otherwise <c>open</c>; capes,
        /// coats and scarves are <c>open</c>; everything else (hair, tails, ears, ribbons) is <c>none</c>.
        /// A single chain is always <c>none</c>.
        /// </summary>
        public static ConnectionMode ConnectionFor(ChainCategory category, int chainCount, float largestGapDegrees)
        {
            if (chainCount < 2)
            {
                return ConnectionMode.None;
            }

            switch (category)
            {
                case ChainCategory.Skirt:
                case ChainCategory.Sleeve:
                    return chainCount >= 3 && largestGapDegrees < 120f ? ConnectionMode.Loop : ConnectionMode.Open;
                case ChainCategory.Coat:
                case ChainCategory.Cape:
                case ChainCategory.Scarf:
                    return ConnectionMode.Open;
                default:
                    return ConnectionMode.None;
            }
        }

        /// <summary>Splits a name into lower-case tokens at separators, letter/digit changes and camelCase humps.</summary>
        public static List<string> Tokenize(string name)
        {
            var tokens = new List<string>();
            if (string.IsNullOrEmpty(name))
            {
                return tokens;
            }

            var current = new StringBuilder();
            char previous = '\0';
            for (int i = 0; i < name.Length; i++)
            {
                char c = name[i];
                if (!char.IsLetterOrDigit(c))
                {
                    Flush(tokens, current);
                    previous = '\0';
                    continue;
                }

                bool boundary = current.Length > 0
                    && ((char.IsDigit(c) != char.IsDigit(previous))
                        || (char.IsUpper(c) && char.IsLower(previous)));
                if (boundary)
                {
                    Flush(tokens, current);
                }

                current.Append(char.ToLowerInvariant(c));
                previous = c;
            }

            Flush(tokens, current);
            return tokens;
        }

        private static void Flush(List<string> tokens, StringBuilder current)
        {
            if (current.Length > 0)
            {
                tokens.Add(current.ToString());
                current.Length = 0;
            }
        }

        private static bool HasToken(List<string> tokens, string word)
        {
            for (int i = 0; i < tokens.Count; i++)
            {
                string t = tokens[i];
                if (string.Equals(t, word, StringComparison.Ordinal)
                    || (t.Length == word.Length + 1 && t[t.Length - 1] == 's' && t.StartsWith(word, StringComparison.Ordinal)))
                {
                    return true;
                }
            }

            return false;
        }
    }
}
