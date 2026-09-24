using System;
using System.Runtime.CompilerServices;

namespace AutoRig.Cloth.Core
{
    /// <summary>
    /// Allocation-free geometric helpers used by the solver: collision push-out for spheres,
    /// tapered capsules and planes, penetration queries and the cone clamp of the angle limit.
    /// </summary>
    public static class ClothGeometry
    {
        private const float Epsilon = 1e-9f;

        /// <summary>
        /// Moves a particle of radius <paramref name="particleRadius"/> out of a sphere, along the
        /// line from the sphere centre.
        /// </summary>
        /// <returns>True when the particle was inside and has been moved.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool PushOutOfSphere(ref V3 p, float particleRadius, V3 center, float radius)
        {
            V3 d = p - center;
            float min = radius + particleRadius;
            float sq = d.LengthSquared;
            if (sq >= min * min)
            {
                return false;
            }

            float dist = (float)Math.Sqrt(sq);
            V3 n = dist > Epsilon ? d * (1f / dist) : V3.Up;
            p = center + n * min;
            return true;
        }

        /// <summary>
        /// Moves a particle out of a capsule whose radius varies linearly from
        /// <paramref name="radiusA"/> at <paramref name="a"/> to <paramref name="radiusB"/> at
        /// <paramref name="b"/>. The push uses the closest point on the segment and the radius
        /// interpolated at that point.
        /// </summary>
        /// <returns>True when the particle was inside and has been moved.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool PushOutOfCapsule(ref V3 p, float particleRadius, V3 a, V3 b, float radiusA, float radiusB)
        {
            V3 ab = b - a;
            float t = ClosestPointParameter(p, a, ab);
            V3 q = a + ab * t;
            float min = radiusA + (radiusB - radiusA) * t + particleRadius;
            V3 d = p - q;
            float sq = d.LengthSquared;
            if (sq >= min * min)
            {
                return false;
            }

            float dist = (float)Math.Sqrt(sq);
            V3 n = dist > Epsilon ? d * (1f / dist) : V3.AnyPerpendicular(V3.NormalizeOr(ab, V3.Up));
            p = q + n * min;
            return true;
        }

        /// <summary>
        /// Keeps a particle on the positive side of a plane given by a point and a unit normal.
        /// </summary>
        /// <returns>True when the particle was behind the plane and has been moved.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool PushOutOfPlane(ref V3 p, float particleRadius, V3 point, V3 normal)
        {
            float s = V3.Dot(p - point, normal) - particleRadius;
            if (s >= 0f)
            {
                return false;
            }

            p -= normal * s;
            return true;
        }

        /// <summary>How deep a particle is inside a sphere (positive = penetrating).</summary>
        public static float SpherePenetration(V3 p, float particleRadius, V3 center, float radius) =>
            radius + particleRadius - V3.Distance(p, center);

        /// <summary>How deep a particle is inside a tapered capsule (positive = penetrating).</summary>
        public static float CapsulePenetration(V3 p, float particleRadius, V3 a, V3 b, float radiusA, float radiusB)
        {
            V3 ab = b - a;
            float t = ClosestPointParameter(p, a, ab);
            V3 q = a + ab * t;
            return radiusA + (radiusB - radiusA) * t + particleRadius - V3.Distance(p, q);
        }

        /// <summary>How deep a particle is behind a plane (positive = penetrating).</summary>
        public static float PlanePenetration(V3 p, float particleRadius, V3 point, V3 normal) =>
            particleRadius - V3.Dot(p - point, normal);

        /// <summary>
        /// Parameter in [0, 1] of the point on segment <c>a → a + ab</c> closest to <paramref name="p"/>.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static float ClosestPointParameter(V3 p, V3 a, V3 ab)
        {
            float lenSq = ab.LengthSquared;
            if (lenSq <= Epsilon)
            {
                return 0f;
            }

            float t = V3.Dot(p - a, ab) / lenSq;
            return t < 0f ? 0f : (t > 1f ? 1f : t);
        }

        /// <summary>
        /// Rotates the unit direction <paramref name="direction"/> toward the unit
        /// <paramref name="axis"/> so that the angle between them does not exceed the limit given
        /// by its cosine and sine. Directions already inside the cone are returned unchanged.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static V3 ClampToCone(V3 direction, V3 axis, float cosLimit, float sinLimit)
        {
            float c = V3.Dot(direction, axis);
            if (c >= cosLimit)
            {
                return direction;
            }

            V3 perpendicular = V3.NormalizeOr(direction - axis * c, V3.AnyPerpendicular(axis));
            return axis * cosLimit + perpendicular * sinLimit;
        }
    }
}
