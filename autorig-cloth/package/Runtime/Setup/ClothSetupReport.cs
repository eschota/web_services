using System.Collections.Generic;
using System.Text;
using UnityEngine;

namespace AutoRig.Cloth
{
    /// <summary>What a setup tool (manifest loader, humanoid collider builder, chain detector) did.</summary>
    public sealed class ClothSetupReport
    {
        /// <summary>Names of the groups that were created.</summary>
        public List<string> CreatedGroups { get; } = new List<string>();

        /// <summary>Names of the colliders that were created.</summary>
        public List<string> CreatedColliders { get; } = new List<string>();

        /// <summary>Problems that did not stop the setup (for example a group skipped because of a missing bone).</summary>
        public List<string> Warnings { get; } = new List<string>();

        /// <summary>Problems that stopped the setup; nothing was changed when this is not empty.</summary>
        public List<string> Errors { get; } = new List<string>();

        /// <summary>Number of previously generated components and objects that were replaced.</summary>
        public int RemovedObjects { get; internal set; }

        /// <summary>Factor applied to the manifest's lengths (measured / authored calibration distance).</summary>
        public float CalibrationFactor { get; internal set; } = 1f;

        /// <summary>Teleport distance given to the created groups, in world units.</summary>
        public float TeleportDistance { get; internal set; }

        /// <summary>True when the setup ran (possibly with warnings).</summary>
        public bool Succeeded => Errors.Count == 0;

        /// <summary>Multi-line human-readable summary.</summary>
        public override string ToString()
        {
            var sb = new StringBuilder();
            if (!Succeeded)
            {
                sb.Append("Failed: ").Append(string.Join(" ", Errors));
                return sb.ToString();
            }

            sb.Append("Created ").Append(CreatedGroups.Count).Append(" group(s) and ")
              .Append(CreatedColliders.Count).Append(" collider(s)");
            if (RemovedObjects > 0)
            {
                sb.Append(", replaced ").Append(RemovedObjects).Append(" generated object(s)");
            }

            if (!Mathf.Approximately(CalibrationFactor, 1f))
            {
                sb.Append(", length scale ").Append(CalibrationFactor.ToString("0.###"));
            }

            sb.Append('.');
            if (CreatedGroups.Count > 0)
            {
                sb.Append("\nGroups: ").Append(string.Join(", ", CreatedGroups));
            }

            if (CreatedColliders.Count > 0)
            {
                sb.Append("\nColliders: ").Append(string.Join(", ", CreatedColliders));
            }

            for (int i = 0; i < Warnings.Count; i++)
            {
                sb.Append("\nWarning: ").Append(Warnings[i]);
            }

            return sb.ToString();
        }

        /// <summary>Writes the report to the console: an error, a warning or an info message.</summary>
        public void Log(string title, Object context)
        {
            string message = "[AutoRig Cloth] " + title + ": " + this;
            if (!Succeeded)
            {
                Debug.LogError(message, context);
            }
            else if (Warnings.Count > 0)
            {
                Debug.LogWarning(message, context);
            }
            else
            {
                Debug.Log(message, context);
            }
        }
    }
}
