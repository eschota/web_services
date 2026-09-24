using System;
using AutoRig.Cloth.Core;
using UnityEngine;

namespace AutoRig.Cloth
{
    /// <summary>A collider's shape in world space, as handed to the solver.</summary>
    public struct ClothColliderShape
    {
        /// <summary>Sphere, capsule or plane.</summary>
        public ColliderShape shape;

        /// <summary>Sphere centre, capsule first end, or a point on the plane.</summary>
        public Vector3 pointA;

        /// <summary>Capsule second end, or the plane's unit normal.</summary>
        public Vector3 pointB;

        /// <summary>Sphere radius, or capsule radius at <see cref="pointA"/>.</summary>
        public float radiusA;

        /// <summary>Capsule radius at <see cref="pointB"/>.</summary>
        public float radiusB;
    }

    /// <summary>
    /// Base class of the shapes cloth collides with. A collider affects every
    /// <see cref="ClothBoneGroup"/> whose collider tags include its tag (or whose tag list is empty).
    /// The world shape is recomputed every frame, so colliders follow animated bones.
    /// </summary>
    public abstract class ClothCollider : MonoBehaviour
    {
        [SerializeField]
        [Tooltip("Groups whose Collider Tags contain this tag collide with this shape; groups with an empty list collide with every shape.")]
        private string colliderTag = "body";

        [SerializeField]
        [HideInInspector]
        private string generatedBy = string.Empty;

        [NonSerialized]
        private ColliderHandle _handle;

        /// <summary>Tag matched against <see cref="ClothBoneGroup.ColliderTags"/>.</summary>
        public string ColliderTag
        {
            get => colliderTag;
            set => colliderTag = value ?? string.Empty;
        }

        /// <summary>Identifier of the setup tool that created this component, or empty for user-made components.</summary>
        public string GeneratedBy => generatedBy;

        internal ColliderHandle Handle => _handle;

        /// <summary>The collider's current shape in world space.</summary>
        public abstract ClothColliderShape GetWorldShape();

        internal void MarkGenerated(string generator, string tagValue)
        {
            generatedBy = generator ?? string.Empty;
            colliderTag = tagValue ?? string.Empty;
        }

        /// <summary>Registers with the simulation (Play mode only).</summary>
        protected virtual void OnEnable()
        {
            if (!Application.isPlaying)
            {
                return;
            }

            AutoRigClothManager manager = AutoRigClothManager.GetOrCreate();
            if (manager != null)
            {
                _handle = manager.Register(this);
            }
        }

        /// <summary>Leaves the simulation.</summary>
        protected virtual void OnDisable()
        {
            if (_handle == null)
            {
                return;
            }

            AutoRigClothManager manager = AutoRigClothManager.Existing;
            if (manager != null)
            {
                manager.Unregister(this);
            }

            _handle = null;
        }

        /// <summary>Draws the shape when selected.</summary>
        protected virtual void OnDrawGizmosSelected()
        {
            ClothGizmos.DrawShape(GetWorldShape(), ClothGizmos.ColliderColor);
        }

        internal void OnManagerDestroyed()
        {
            _handle = null;
        }

        internal void PushTo(ClothSolver solver, uint tagBit)
        {
            ClothColliderShape s = GetWorldShape();
            switch (s.shape)
            {
                case ColliderShape.Sphere:
                    solver.SetSphere(_handle, s.pointA.ToV3(), s.radiusA, tagBit);
                    break;
                case ColliderShape.Capsule:
                    solver.SetCapsule(_handle, s.pointA.ToV3(), s.pointB.ToV3(), s.radiusA, s.radiusB, tagBit);
                    break;
                default:
                    solver.SetPlane(_handle, s.pointA.ToV3(), s.pointB.ToV3(), tagBit);
                    break;
            }
        }
    }
}
