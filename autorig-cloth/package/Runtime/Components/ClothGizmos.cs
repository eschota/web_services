using AutoRig.Cloth.Core;
using UnityEngine;

namespace AutoRig.Cloth
{
    /// <summary>
    /// Scene-view drawing of cloth groups, colliders and wind zones. Call from
    /// <c>OnDrawGizmos</c>/<c>OnDrawGizmosSelected</c> or a <c>DrawGizmo</c> method.
    /// </summary>
    public static class ClothGizmos
    {
        /// <summary>Colour of collider shapes.</summary>
        public static readonly Color ColliderColor = new Color(0.25f, 0.85f, 1f, 0.9f);

        /// <summary>Colour of free (simulated) joints.</summary>
        public static readonly Color ParticleColor = new Color(1f, 0.62f, 0.15f, 0.9f);

        /// <summary>Colour of chain roots (attached joints).</summary>
        public static readonly Color RootColor = new Color(0.4f, 1f, 0.4f, 0.9f);

        /// <summary>Colour of links along chains.</summary>
        public static readonly Color ChainColor = new Color(1f, 0.9f, 0.3f, 0.8f);

        /// <summary>Colour of links between neighbouring chains.</summary>
        public static readonly Color ConnectionColor = new Color(0.85f, 0.45f, 1f, 0.7f);

        /// <summary>Colour of the attach bone marker and of wind zones.</summary>
        public static readonly Color AccentColor = new Color(1f, 1f, 1f, 0.6f);

        /// <summary>
        /// Draws a group: joints with their collision radii, links along chains and between chains,
        /// and the attach bone. Uses the simulated positions in Play mode.
        /// </summary>
        public static void DrawGroup(ClothBoneGroup group)
        {
            if (group == null)
            {
                return;
            }

            ClothParameters p = group.GetParameters().Sanitized();
            float scale = group.LengthScale;
            float rootRadius = p.radius * scale;
            float tipRadius = p.EffectiveRadiusTip * scale;

            if (group.AttachBone != null)
            {
                Gizmos.color = AccentColor;
                float size = Mathf.Max(rootRadius, 0.01f) * 2.5f;
                Gizmos.DrawWireCube(group.AttachBone.position, new Vector3(size, size, size));
            }

            AutoRigClothManager manager = AutoRigClothManager.Existing;
            if (group.IsSimulating && manager != null)
            {
                DrawSimulated(group, manager.Solver);
                return;
            }

            // Links between chains stop at the shortest chain of the group, as in the solver.
            int shortest = int.MaxValue;
            int valid = 0;
            for (int c = 0; c < group.ChainCount; c++)
            {
                ClothChain chain = group.GetChain(c);
                if (chain != null && chain.bones != null && chain.bones.Length >= 2)
                {
                    shortest = Mathf.Min(shortest, chain.bones.Length);
                    valid++;
                }
            }

            Transform[] previousChain = null;
            Transform[] firstChain = null;
            for (int c = 0; c < group.ChainCount; c++)
            {
                ClothChain chain = group.GetChain(c);
                if (chain == null || chain.bones == null || chain.bones.Length < 2)
                {
                    continue;
                }

                Transform[] bones = chain.bones;
                for (int k = 0; k < bones.Length; k++)
                {
                    if (bones[k] == null)
                    {
                        continue;
                    }

                    float t = bones.Length > 1 ? k / (float)(bones.Length - 1) : 0f;
                    DrawJoint(bones[k].position, Mathf.Lerp(rootRadius, tipRadius, t), k == 0);
                    if (k > 0 && bones[k - 1] != null)
                    {
                        Gizmos.color = ChainColor;
                        Gizmos.DrawLine(bones[k - 1].position, bones[k].position);
                    }
                }

                if (group.Connection != ConnectionMode.None && previousChain != null)
                {
                    DrawConnection(previousChain, bones, shortest);
                }

                previousChain = bones;
                if (firstChain == null)
                {
                    firstChain = bones;
                }
            }

            if (group.Connection == ConnectionMode.Loop && valid >= 3)
            {
                DrawConnection(previousChain, firstChain, shortest);
            }
        }

        /// <summary>Draws a collider's current shape.</summary>
        public static void DrawCollider(ClothCollider collider, Color color)
        {
            if (collider != null)
            {
                DrawShape(collider.GetWorldShape(), color);
            }
        }

        /// <summary>Draws a world-space collider shape.</summary>
        public static void DrawShape(ClothColliderShape shape, Color color)
        {
            Gizmos.color = color;
            switch (shape.shape)
            {
                case ColliderShape.Sphere:
                    Gizmos.DrawWireSphere(shape.pointA, shape.radiusA);
                    break;
                case ColliderShape.Capsule:
                    DrawWireCapsule(shape.pointA, shape.pointB, shape.radiusA, shape.radiusB);
                    break;
                default:
                    DrawPlane(shape.pointA, shape.pointB, 0.5f);
                    break;
            }
        }

        /// <summary>Draws a (possibly tapered) wire capsule.</summary>
        public static void DrawWireCapsule(Vector3 a, Vector3 b, float radiusA, float radiusB)
        {
            Gizmos.DrawWireSphere(a, radiusA);
            Gizmos.DrawWireSphere(b, radiusB);
            Vector3 axis = b - a;
            if (axis.sqrMagnitude < 1e-12f)
            {
                return;
            }

            axis.Normalize();
            Vector3 u = Vector3.Cross(axis, Mathf.Abs(axis.y) < 0.9f ? Vector3.up : Vector3.right).normalized;
            Vector3 v = Vector3.Cross(axis, u);
            Gizmos.DrawLine(a + u * radiusA, b + u * radiusB);
            Gizmos.DrawLine(a - u * radiusA, b - u * radiusB);
            Gizmos.DrawLine(a + v * radiusA, b + v * radiusB);
            Gizmos.DrawLine(a - v * radiusA, b - v * radiusB);
        }

        /// <summary>Draws a square patch of a plane and its normal.</summary>
        public static void DrawPlane(Vector3 point, Vector3 normal, float halfSize)
        {
            Vector3 u = Vector3.Cross(normal, Mathf.Abs(normal.y) < 0.9f ? Vector3.up : Vector3.right).normalized;
            Vector3 v = Vector3.Cross(normal, u);
            for (int i = -2; i <= 2; i++)
            {
                float f = halfSize * i / 2f;
                Gizmos.DrawLine(point + u * f - v * halfSize, point + u * f + v * halfSize);
                Gizmos.DrawLine(point + v * f - u * halfSize, point + v * f + u * halfSize);
            }

            Gizmos.DrawLine(point, point + normal * halfSize);
        }

        /// <summary>Draws a wind zone: its direction and, for sphere zones, its radius.</summary>
        public static void DrawWind(ClothWind wind)
        {
            if (wind == null)
            {
                return;
            }

            Transform t = wind.transform;
            Vector3 p = t.position;
            Gizmos.color = AccentColor;
            if (wind.Mode != ClothWindMode.Global)
            {
                Gizmos.DrawWireSphere(p, wind.Radius * UnityConversions.MaxAbsScale(t.lossyScale));
            }

            if (wind.Mode == ClothWindMode.SphereRadial)
            {
                Vector3[] directions = { Vector3.up, Vector3.down, Vector3.left, Vector3.right, Vector3.forward, Vector3.back };
                for (int i = 0; i < directions.Length; i++)
                {
                    DrawArrow(p + directions[i] * 0.2f, directions[i] * 0.5f);
                }
            }
            else
            {
                DrawArrow(p, t.forward);
            }
        }

        private static void DrawArrow(Vector3 from, Vector3 vector)
        {
            Vector3 to = from + vector;
            Gizmos.DrawLine(from, to);
            Vector3 side = Vector3.Cross(vector, Mathf.Abs(vector.normalized.y) < 0.9f ? Vector3.up : Vector3.right).normalized * (vector.magnitude * 0.15f);
            Vector3 back = to - vector * 0.25f;
            Gizmos.DrawLine(to, back + side);
            Gizmos.DrawLine(to, back - side);
        }

        private static void DrawJoint(Vector3 position, float radius, bool root)
        {
            Gizmos.color = root ? RootColor : ParticleColor;
            if (radius > 0f)
            {
                Gizmos.DrawWireSphere(position, radius);
            }
            else
            {
                Gizmos.DrawSphere(position, 0.004f);
            }
        }

        private static void DrawConnection(Transform[] a, Transform[] b, int shortest)
        {
            Gizmos.color = ConnectionColor;
            int count = Mathf.Min(shortest, Mathf.Min(a.Length, b.Length));
            for (int k = 1; k < count; k++)
            {
                if (a[k] != null && b[k] != null)
                {
                    Gizmos.DrawLine(a[k].position, b[k].position);
                }
            }
        }

        private static void DrawSimulated(ClothBoneGroup group, ClothSolver solver)
        {
            int[] lengths = group.ChainLengths;
            int start = 0;
            for (int c = 0; c < lengths.Length; c++)
            {
                Vector3 previous = Vector3.zero;
                for (int k = 0; k < lengths[c]; k++)
                {
                    if (!group.TryGetParticle(start + k, out Vector3 position, out float radius))
                    {
                        return;
                    }

                    DrawJoint(position, radius, k == 0);
                    if (k > 0)
                    {
                        Gizmos.color = ChainColor;
                        Gizmos.DrawLine(previous, position);
                    }

                    previous = position;
                }

                start += lengths[c];
            }

            Gizmos.color = ConnectionColor;
            int links = solver.GetLinkCount(group.Handle);
            for (int l = 0; l < links; l++)
            {
                solver.GetLink(group.Handle, l, out int a, out int b);
                if (group.TryGetParticle(a, out Vector3 pa, out _) && group.TryGetParticle(b, out Vector3 pb, out _))
                {
                    Gizmos.DrawLine(pa, pb);
                }
            }
        }
    }
}
