using System;

namespace AutoRig.Cloth.Core
{
    /// <summary>
    /// A rigid transform (position and unit rotation, no scale). The solver uses it for a group's
    /// attach frame: the animated bone the chains hang from.
    /// </summary>
    [Serializable]
    public struct RigidFrame
    {
        /// <summary>World position of the frame origin.</summary>
        public V3 position;

        /// <summary>World rotation of the frame (unit quaternion).</summary>
        public Q4 rotation;

        /// <summary>Creates a frame from a position and a rotation.</summary>
        public RigidFrame(V3 position, Q4 rotation)
        {
            this.position = position;
            this.rotation = rotation;
        }

        /// <summary>The frame at the world origin with identity rotation.</summary>
        public static RigidFrame Identity => new RigidFrame(V3.Zero, Q4.Identity);

        /// <summary>Maps a point from frame space to world space.</summary>
        public V3 TransformPoint(V3 local) => position + rotation * local;

        /// <summary>Maps a point from world space to frame space.</summary>
        public V3 InverseTransformPoint(V3 world) => Q4.Inverse(rotation) * (world - position);

        /// <summary>
        /// Interpolates position linearly and rotation along the shortest arc (normalized lerp).
        /// </summary>
        public static RigidFrame Interpolate(in RigidFrame a, in RigidFrame b, float t) =>
            new RigidFrame(V3.Lerp(a.position, b.position, t), Q4.Nlerp(a.rotation, b.rotation, t));
    }
}
