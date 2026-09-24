using AutoRig.Cloth.Core;
using UnityEngine;

namespace AutoRig.Cloth
{
    /// <summary>
    /// An infinite plane through this transform's position (a floor, a wall). Cloth is kept on the
    /// side the normal points to.
    /// </summary>
    [AddComponentMenu("AutoRig Cloth/Cloth Plane Collider")]
    public sealed class ClothPlaneCollider : ClothCollider
    {
        [SerializeField]
        [Tooltip("Plane normal in this transform's local space. Cloth stays on the side it points to.")]
        private Vector3 normal = Vector3.up;

        /// <summary>Plane normal in local space.</summary>
        public Vector3 Normal
        {
            get => normal;
            set => normal = value;
        }

        /// <inheritdoc />
        public override ClothColliderShape GetWorldShape()
        {
            Vector3 n = transform.TransformDirection(normal);
            n = n.sqrMagnitude > 1e-12f ? n.normalized : Vector3.up;
            return new ClothColliderShape { shape = ColliderShape.Plane, pointA = transform.position, pointB = n };
        }
    }
}
