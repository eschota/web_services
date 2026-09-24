using AutoRig.Cloth.Core;
using UnityEngine;

namespace AutoRig.Cloth
{
    /// <summary>
    /// A capsule cloth collides with, optionally tapered (a different radius at each end). The
    /// second end is either a local offset or follows another bone, which suits limbs: put the
    /// collider on the upper leg and let the second end follow the knee.
    /// </summary>
    [AddComponentMenu("AutoRig Cloth/Cloth Capsule Collider")]
    public sealed class ClothCapsuleCollider : ClothCollider
    {
        [SerializeField]
        [Tooltip("First end, in this transform's local space.")]
        private Vector3 startOffset;

        [SerializeField]
        [Tooltip("Second end, in this transform's local space. Ignored when End Bone is set.")]
        private Vector3 endOffset = new Vector3(0f, 0.25f, 0f);

        [SerializeField]
        [Tooltip("Optional bone whose position is the second end (for a limb, the next joint).")]
        private Transform endBone;

        [SerializeField]
        [Min(0f)]
        [Tooltip("Radius at the first end, in local units (multiplied by the largest axis of this transform's lossy scale).")]
        private float radius = 0.06f;

        [SerializeField]
        [Min(0f)]
        [Tooltip("Radius at the second end; 0 = same as Radius.")]
        private float endRadius;

        /// <summary>First end in local space.</summary>
        public Vector3 StartOffset
        {
            get => startOffset;
            set => startOffset = value;
        }

        /// <summary>Second end in local space (used when <see cref="EndBone"/> is null).</summary>
        public Vector3 EndOffset
        {
            get => endOffset;
            set => endOffset = value;
        }

        /// <summary>Bone whose position is the second end; null = use <see cref="EndOffset"/>.</summary>
        public Transform EndBone
        {
            get => endBone;
            set => endBone = value;
        }

        /// <summary>Radius at the first end, in local units.</summary>
        public float Radius
        {
            get => radius;
            set => radius = Mathf.Max(0f, value);
        }

        /// <summary>Radius at the second end, in local units; 0 = same as <see cref="Radius"/>.</summary>
        public float EndRadius
        {
            get => endRadius;
            set => endRadius = Mathf.Max(0f, value);
        }

        /// <inheritdoc />
        public override ClothColliderShape GetWorldShape()
        {
            Transform t = transform;
            float scale = UnityConversions.MaxAbsScale(t.lossyScale);
            return new ClothColliderShape
            {
                shape = ColliderShape.Capsule,
                pointA = t.TransformPoint(startOffset),
                pointB = endBone != null ? endBone.position : t.TransformPoint(endOffset),
                radiusA = radius * scale,
                radiusB = (endRadius > 0f ? endRadius : radius) * scale,
            };
        }

        internal void Configure(Vector3 localStart, Vector3 localEnd, Transform endTransform, float startRadius, float finishRadius)
        {
            startOffset = localStart;
            endOffset = localEnd;
            endBone = endTransform;
            radius = Mathf.Max(0f, startRadius);
            endRadius = Mathf.Max(0f, finishRadius);
        }
    }
}
