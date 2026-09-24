using AutoRig.Cloth.Core;
using UnityEngine;

namespace AutoRig.Cloth
{
    /// <summary>
    /// A sphere cloth collides with. The centre can sit between this transform and a second bone
    /// (the manifest's <c>to_bone</c> and <c>t</c>), for example a third of the way from the head
    /// bone to the head-top joint.
    /// </summary>
    [AddComponentMenu("AutoRig Cloth/Cloth Sphere Collider")]
    public sealed class ClothSphereCollider : ClothCollider
    {
        [SerializeField]
        [Tooltip("Centre offset in this transform's local space.")]
        private Vector3 center;

        [SerializeField]
        [Min(0f)]
        [Tooltip("Radius in this transform's local units (multiplied by the largest axis of its lossy scale).")]
        private float radius = 0.1f;

        [SerializeField]
        [Tooltip("Optional second bone. When set, the centre is placed between this transform and it.")]
        private Transform towards;

        [SerializeField]
        [Range(0f, 1f)]
        [Tooltip("Where the centre sits between this transform (0) and Towards (1).")]
        private float towardsBlend;

        /// <summary>Centre offset in local space.</summary>
        public Vector3 Center
        {
            get => center;
            set => center = value;
        }

        /// <summary>Radius in local units.</summary>
        public float Radius
        {
            get => radius;
            set => radius = Mathf.Max(0f, value);
        }

        /// <summary>Optional second bone the centre is blended toward.</summary>
        public Transform Towards
        {
            get => towards;
            set => towards = value;
        }

        /// <summary>Blend of the centre between this transform (0) and <see cref="Towards"/> (1).</summary>
        public float TowardsBlend
        {
            get => towardsBlend;
            set => towardsBlend = Mathf.Clamp01(value);
        }

        /// <inheritdoc />
        public override ClothColliderShape GetWorldShape()
        {
            Transform t = transform;
            Vector3 c = towards != null
                ? Vector3.Lerp(t.position, towards.position, towardsBlend) + t.TransformVector(center)
                : t.TransformPoint(center);
            float r = radius * UnityConversions.MaxAbsScale(t.lossyScale);
            return new ClothColliderShape { shape = ColliderShape.Sphere, pointA = c, pointB = c, radiusA = r, radiusB = r };
        }

        internal void Configure(Vector3 localCenter, float localRadius, Transform towardsBone, float blend)
        {
            center = localCenter;
            radius = Mathf.Max(0f, localRadius);
            towards = towardsBone;
            towardsBlend = Mathf.Clamp01(blend);
        }
    }
}
