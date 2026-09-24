using AutoRig.Cloth.Core;
using UnityEngine;

namespace AutoRig.Cloth
{
    /// <summary>
    /// A reusable set of simulation parameters. Assign it to any number of
    /// <see cref="ClothBoneGroup"/> components; edits apply to all of them, also in Play mode.
    /// </summary>
    [CreateAssetMenu(fileName = "ClothPreset", menuName = "AutoRig Cloth/Cloth Preset", order = 400)]
    public sealed class ClothPreset : ScriptableObject
    {
        [SerializeField]
        [Tooltip("Simulation parameters of every group that uses this preset.")]
        private ClothSettings settings = new ClothSettings(BuiltInPresets.GetOrDefault(BuiltInPresets.Hair));

        /// <summary>The parameters stored in this preset.</summary>
        public ClothSettings Settings => settings;

        /// <summary>Converts the stored settings to solver parameters.</summary>
        public ClothParameters ToParameters() => settings.ToParameters();

        /// <summary>Overwrites the stored settings with the values of a built-in preset.</summary>
        /// <returns>False when <paramref name="builtInName"/> is not a built-in preset name.</returns>
        public bool LoadBuiltIn(string builtInName)
        {
            if (!BuiltInPresets.TryGet(builtInName, out ClothParameters parameters))
            {
                return false;
            }

            settings.SetFrom(parameters);
            return true;
        }
    }
}
