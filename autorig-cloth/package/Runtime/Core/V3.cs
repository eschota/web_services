using System;
using System.Runtime.CompilerServices;

namespace AutoRig.Cloth.Core
{
    /// <summary>
    /// Three-component single-precision vector used by the solver core. It has the same field
    /// layout as <c>UnityEngine.Vector3</c>, so converting between the two is a field copy, and it
    /// keeps the core free of any engine dependency.
    /// </summary>
    [Serializable]
    public struct V3 : IEquatable<V3>
    {
        /// <summary>Squared length below which a vector is treated as zero when normalizing.</summary>
        public const float EpsilonSquared = 1e-18f;

        /// <summary>X component.</summary>
        public float x;

        /// <summary>Y component.</summary>
        public float y;

        /// <summary>Z component.</summary>
        public float z;

        /// <summary>Creates a vector from its components.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public V3(float x, float y, float z)
        {
            this.x = x;
            this.y = y;
            this.z = z;
        }

        /// <summary>(0, 0, 0).</summary>
        public static V3 Zero => default;

        /// <summary>(1, 1, 1).</summary>
        public static V3 One => new V3(1f, 1f, 1f);

        /// <summary>(0, 1, 0).</summary>
        public static V3 Up => new V3(0f, 1f, 0f);

        /// <summary>(0, -1, 0).</summary>
        public static V3 Down => new V3(0f, -1f, 0f);

        /// <summary>(1, 0, 0).</summary>
        public static V3 Right => new V3(1f, 0f, 0f);

        /// <summary>(0, 0, 1).</summary>
        public static V3 Forward => new V3(0f, 0f, 1f);

        /// <summary>Euclidean length.</summary>
        public float Length
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => (float)Math.Sqrt(x * x + y * y + z * z);
        }

        /// <summary>Squared Euclidean length.</summary>
        public float LengthSquared
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => x * x + y * y + z * z;
        }

        /// <summary>This vector scaled to unit length, or zero when it is (nearly) zero.</summary>
        public V3 Normalized
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Normalize(this);
        }

        /// <summary>True when all three components are finite numbers.</summary>
        public bool IsFinite
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => !float.IsNaN(x) && !float.IsInfinity(x)
                && !float.IsNaN(y) && !float.IsInfinity(y)
                && !float.IsNaN(z) && !float.IsInfinity(z);
        }

        /// <summary>Component-wise sum.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static V3 operator +(V3 a, V3 b) => new V3(a.x + b.x, a.y + b.y, a.z + b.z);

        /// <summary>Component-wise difference.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static V3 operator -(V3 a, V3 b) => new V3(a.x - b.x, a.y - b.y, a.z - b.z);

        /// <summary>Negation.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static V3 operator -(V3 a) => new V3(-a.x, -a.y, -a.z);

        /// <summary>Scales a vector.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static V3 operator *(V3 a, float s) => new V3(a.x * s, a.y * s, a.z * s);

        /// <summary>Scales a vector.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static V3 operator *(float s, V3 a) => new V3(a.x * s, a.y * s, a.z * s);

        /// <summary>Divides every component by a scalar.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static V3 operator /(V3 a, float s)
        {
            float inv = 1f / s;
            return new V3(a.x * inv, a.y * inv, a.z * inv);
        }

        /// <summary>Exact component-wise equality.</summary>
        public static bool operator ==(V3 a, V3 b) => a.x == b.x && a.y == b.y && a.z == b.z;

        /// <summary>Exact component-wise inequality.</summary>
        public static bool operator !=(V3 a, V3 b) => !(a == b);

        /// <summary>Dot product.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static float Dot(V3 a, V3 b) => a.x * b.x + a.y * b.y + a.z * b.z;

        /// <summary>Cross product.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static V3 Cross(V3 a, V3 b) =>
            new V3(a.y * b.z - a.z * b.y, a.z * b.x - a.x * b.z, a.x * b.y - a.y * b.x);

        /// <summary>Component-wise product.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static V3 Scale(V3 a, V3 b) => new V3(a.x * b.x, a.y * b.y, a.z * b.z);

        /// <summary>Unclamped linear interpolation <c>a + (b - a) * t</c>.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static V3 Lerp(V3 a, V3 b, float t) =>
            new V3(a.x + (b.x - a.x) * t, a.y + (b.y - a.y) * t, a.z + (b.z - a.z) * t);

        /// <summary>Distance between two points.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static float Distance(V3 a, V3 b) => (a - b).Length;

        /// <summary>Squared distance between two points.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static float DistanceSquared(V3 a, V3 b) => (a - b).LengthSquared;

        /// <summary>Component-wise minimum.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static V3 Min(V3 a, V3 b) =>
            new V3(a.x < b.x ? a.x : b.x, a.y < b.y ? a.y : b.y, a.z < b.z ? a.z : b.z);

        /// <summary>Component-wise maximum.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static V3 Max(V3 a, V3 b) =>
            new V3(a.x > b.x ? a.x : b.x, a.y > b.y ? a.y : b.y, a.z > b.z ? a.z : b.z);

        /// <summary>Returns <paramref name="v"/> scaled to unit length, or zero when it is (nearly) zero.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static V3 Normalize(V3 v)
        {
            float sq = v.x * v.x + v.y * v.y + v.z * v.z;
            if (sq <= EpsilonSquared)
            {
                return default;
            }

            float inv = 1f / (float)Math.Sqrt(sq);
            return new V3(v.x * inv, v.y * inv, v.z * inv);
        }

        /// <summary>
        /// Returns <paramref name="v"/> scaled to unit length, or <paramref name="fallback"/> when
        /// <paramref name="v"/> is (nearly) zero.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static V3 NormalizeOr(V3 v, V3 fallback)
        {
            float sq = v.x * v.x + v.y * v.y + v.z * v.z;
            if (sq <= EpsilonSquared)
            {
                return fallback;
            }

            float inv = 1f / (float)Math.Sqrt(sq);
            return new V3(v.x * inv, v.y * inv, v.z * inv);
        }

        /// <summary>Returns a unit vector perpendicular to the unit vector <paramref name="n"/>.</summary>
        public static V3 AnyPerpendicular(V3 n)
        {
            V3 axis = Math.Abs(n.x) < 0.57735f ? Right : (Math.Abs(n.y) < 0.57735f ? Up : Forward);
            return NormalizeOr(Cross(n, axis), Forward);
        }

        /// <inheritdoc />
        public bool Equals(V3 other) => x.Equals(other.x) && y.Equals(other.y) && z.Equals(other.z);

        /// <inheritdoc />
        public override bool Equals(object obj) => obj is V3 other && Equals(other);

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked
            {
                int h = x.GetHashCode();
                h = (h * 397) ^ y.GetHashCode();
                return (h * 397) ^ z.GetHashCode();
            }
        }

        /// <inheritdoc />
        public override string ToString() =>
            "(" + x.ToString("0.#####", System.Globalization.CultureInfo.InvariantCulture) + ", "
            + y.ToString("0.#####", System.Globalization.CultureInfo.InvariantCulture) + ", "
            + z.ToString("0.#####", System.Globalization.CultureInfo.InvariantCulture) + ")";
    }
}
