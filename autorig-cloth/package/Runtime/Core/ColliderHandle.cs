namespace AutoRig.Cloth.Core
{
    /// <summary>
    /// Identifies a collider inside a <see cref="ClothSolver"/>. Stays valid until the collider is
    /// removed, even when other colliders are added or removed.
    /// </summary>
    public sealed class ColliderHandle
    {
        internal int index = -1;
        internal ClothSolver owner;

        internal ColliderHandle(ClothSolver owner, int index)
        {
            this.owner = owner;
            this.index = index;
        }

        /// <summary>True while the collider exists in its solver.</summary>
        public bool IsValid => index >= 0 && owner != null;
    }
}
