namespace AutoRig.Cloth.Core
{
    /// <summary>How the chains of one group are linked to each other (manifest field <c>connection</c>).</summary>
    public enum ConnectionMode
    {
        /// <summary>Chains are independent, like hair strands.</summary>
        None = 0,

        /// <summary>Joint <i>k</i> of chain <i>i</i> is linked to joint <i>k</i> of chain <i>i + 1</i>, like a cape.</summary>
        Open = 1,

        /// <summary>As <see cref="Open"/>, plus the last chain links back to the first, like a skirt.</summary>
        Loop = 2,
    }
}
