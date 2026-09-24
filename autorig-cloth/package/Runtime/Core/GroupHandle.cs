namespace AutoRig.Cloth.Core
{
    /// <summary>
    /// Identifies a group of chains inside a <see cref="ClothSolver"/>. Stays valid until the group
    /// is removed, even when other groups are added or removed.
    /// </summary>
    public sealed class GroupHandle
    {
        internal int index = -1;
        internal ClothSolver owner;

        internal GroupHandle(ClothSolver owner, int index)
        {
            this.owner = owner;
            this.index = index;
        }

        /// <summary>True while the group exists in its solver.</summary>
        public bool IsValid => index >= 0 && owner != null;
    }
}
