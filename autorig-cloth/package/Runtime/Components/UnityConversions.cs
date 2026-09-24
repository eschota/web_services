using System.Runtime.CompilerServices;
using AutoRig.Cloth.Core;
using UnityEngine;

namespace AutoRig.Cloth
{
    /// <summary>Conversions between Unity math types and the engine-independent core types.</summary>
    internal static class UnityConversions
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static V3 ToV3(this Vector3 v) => new V3(v.x, v.y, v.z);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Vector3 ToVector3(this V3 v) => new Vector3(v.x, v.y, v.z);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Q4 ToQ4(this Quaternion q) => new Q4(q.x, q.y, q.z, q.w);

        /// <summary>Largest absolute component of a scale, or 1 when the scale is degenerate.</summary>
        public static float MaxAbsScale(Vector3 scale)
        {
            float s = Mathf.Max(Mathf.Abs(scale.x), Mathf.Max(Mathf.Abs(scale.y), Mathf.Abs(scale.z)));
            return s > 1e-8f && !float.IsInfinity(s) ? s : 1f;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsFinite(Vector3 v) =>
            !float.IsNaN(v.x) && !float.IsInfinity(v.x)
            && !float.IsNaN(v.y) && !float.IsInfinity(v.y)
            && !float.IsNaN(v.z) && !float.IsInfinity(v.z);
    }
}
