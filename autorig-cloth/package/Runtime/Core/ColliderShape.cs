namespace AutoRig.Cloth.Core
{
    /// <summary>Collision shapes supported by the solver.</summary>
    public enum ColliderShape
    {
        /// <summary>A sphere: centre and radius.</summary>
        Sphere = 0,

        /// <summary>A capsule between two points, optionally tapered (a radius at each end).</summary>
        Capsule = 1,

        /// <summary>An infinite plane; particles are kept on the side the normal points to.</summary>
        Plane = 2,
    }
}
