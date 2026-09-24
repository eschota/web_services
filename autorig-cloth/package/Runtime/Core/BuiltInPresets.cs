using System;
using System.Collections.Generic;

namespace AutoRig.Cloth.Core
{
    /// <summary>
    /// The preset names every runtime must know even when a manifest does not define them
    /// (<c>hair</c>, <c>hair_stiff</c>, <c>skirt</c>, <c>cape</c>, <c>ribbon</c>, <c>tail</c>,
    /// <c>accessory</c>), with values tuned for an adult-sized character (lengths in metres).
    /// </summary>
    public static class BuiltInPresets
    {
        /// <summary>Long, free-swinging hair strands. Identical to the <c>hair</c> preset in the manifest specification example.</summary>
        public const string Hair = "hair";

        /// <summary>Short or styled hair and bangs that should keep their shape.</summary>
        public const string HairStiff = "hair_stiff";

        /// <summary>Skirts and coat tails: stiff enough to keep the silhouette, linked around the legs.</summary>
        public const string Skirt = "skirt";

        /// <summary>Capes and cloaks: loose, heavy, strongly affected by movement and wind.</summary>
        public const string Cape = "cape";

        /// <summary>Ribbons, scarf ends and strings: very light and loose.</summary>
        public const string Ribbon = "ribbon";

        /// <summary>Animal tails: springy, partly self-supporting.</summary>
        public const string Tail = "tail";

        /// <summary>Earrings, pendants, animal ears and other small danglers.</summary>
        public const string Accessory = "accessory";

        private static readonly string[] s_names = { Hair, HairStiff, Skirt, Cape, Ribbon, Tail, Accessory };

        /// <summary>All built-in preset names, in documentation order.</summary>
        public static IReadOnlyList<string> Names => s_names;

        /// <summary>
        /// Looks up a built-in preset by name (case-insensitive, surrounding white space ignored).
        /// </summary>
        /// <returns>True when <paramref name="name"/> is a built-in preset name.</returns>
        public static bool TryGet(string name, out ClothParameters parameters)
        {
            string key = name == null ? string.Empty : name.Trim();
            //                                   gravity damping stiff  angle stretch conn  radius  tip     inMove inRot drag  wind
            if (Is(key, Hair))
            {
                parameters = new ClothParameters(1.0f, 0.12f, 0.25f, 70f, 0.00f, 0.0f, 0.015f, 0.008f, 0.60f, 0.60f, 0.02f, 1.0f);
            }
            else if (Is(key, HairStiff))
            {
                parameters = new ClothParameters(0.5f, 0.20f, 0.50f, 30f, 0.00f, 0.0f, 0.015f, 0.010f, 0.40f, 0.40f, 0.02f, 0.5f);
            }
            else if (Is(key, Skirt))
            {
                parameters = new ClothParameters(1.0f, 0.15f, 0.30f, 60f, 0.05f, 0.6f, 0.030f, 0.025f, 0.60f, 0.50f, 0.03f, 0.7f);
            }
            else if (Is(key, Cape))
            {
                parameters = new ClothParameters(1.0f, 0.08f, 0.08f, 110f, 0.03f, 0.5f, 0.035f, 0.035f, 0.80f, 0.75f, 0.05f, 1.0f);
            }
            else if (Is(key, Ribbon))
            {
                parameters = new ClothParameters(0.8f, 0.06f, 0.05f, 0f, 0.00f, 0.3f, 0.010f, 0.010f, 0.85f, 0.85f, 0.08f, 1.0f);
            }
            else if (Is(key, Tail))
            {
                parameters = new ClothParameters(0.4f, 0.18f, 0.45f, 45f, 0.00f, 0.0f, 0.040f, 0.015f, 0.50f, 0.45f, 0.02f, 0.2f);
            }
            else if (Is(key, Accessory))
            {
                parameters = new ClothParameters(1.0f, 0.25f, 0.35f, 50f, 0.00f, 0.0f, 0.012f, 0.012f, 0.50f, 0.50f, 0.02f, 0.3f);
            }
            else
            {
                parameters = ClothParameters.Default;
                return false;
            }

            return true;
        }

        /// <summary>
        /// Returns the built-in preset with this name, or <see cref="ClothParameters.Default"/>
        /// when the name is not a built-in preset.
        /// </summary>
        public static ClothParameters GetOrDefault(string name)
        {
            TryGet(name, out ClothParameters parameters);
            return parameters;
        }

        /// <summary>True when <paramref name="name"/> is a built-in preset name.</summary>
        public static bool IsBuiltIn(string name) => TryGet(name, out _);

        private static bool Is(string a, string b) => string.Equals(a, b, StringComparison.OrdinalIgnoreCase);
    }
}
