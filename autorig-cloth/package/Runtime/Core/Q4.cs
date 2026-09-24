using System;
using System.Runtime.CompilerServices;

namespace AutoRig.Cloth.Core
{
    /// <summary>
    /// Unit quaternion used to describe attach-frame orientations. Same component order and
    /// multiplication convention as <c>UnityEngine.Quaternion</c>: <c>a * b</c> applies
    /// <c>b</c> first, then <c>a</c>.
    /// </summary>
    [Serializable]
    public struct Q4 : IEquatable<Q4>
    {
        /// <summary>X component of the vector part.</summary>
        public float x;

        /// <summary>Y component of the vector part.</summary>
        public float y;

        /// <summary>Z component of the vector part.</summary>
        public float z;

        /// <summary>Scalar part.</summary>
        public float w;

        /// <summary>Creates a quaternion from raw components (not normalized).</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Q4(float x, float y, float z, float w)
        {
            this.x = x;
            this.y = y;
            this.z = z;
            this.w = w;
        }

        /// <summary>The identity rotation.</summary>
        public static Q4 Identity => new Q4(0f, 0f, 0f, 1f);

        /// <summary>True when all four components are finite numbers.</summary>
        public bool IsFinite =>
            !float.IsNaN(x) && !float.IsInfinity(x) && !float.IsNaN(y) && !float.IsInfinity(y)
            && !float.IsNaN(z) && !float.IsInfinity(z) && !float.IsNaN(w) && !float.IsInfinity(w);

        /// <summary>Hamilton product: the rotation <paramref name="b"/> followed by <paramref name="a"/>.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Q4 operator *(Q4 a, Q4 b) => new Q4(
            a.w * b.x + a.x * b.w + a.y * b.z - a.z * b.y,
            a.w * b.y + a.y * b.w + a.z * b.x - a.x * b.z,
            a.w * b.z + a.z * b.w + a.x * b.y - a.y * b.x,
            a.w * b.w - a.x * b.x - a.y * b.y - a.z * b.z);

        /// <summary>Rotates a vector by a unit quaternion.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static V3 operator *(Q4 q, V3 v)
        {
            // t = 2 * cross(q.xyz, v); v' = v + w * t + cross(q.xyz, t)
            float tx = 2f * (q.y * v.z - q.z * v.y);
            float ty = 2f * (q.z * v.x - q.x * v.z);
            float tz = 2f * (q.x * v.y - q.y * v.x);
            return new V3(
                v.x + q.w * tx + (q.y * tz - q.z * ty),
                v.y + q.w * ty + (q.z * tx - q.x * tz),
                v.z + q.w * tz + (q.x * ty - q.y * tx));
        }

        /// <summary>Four-dimensional dot product.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static float Dot(Q4 a, Q4 b) => a.x * b.x + a.y * b.y + a.z * b.z + a.w * b.w;

        /// <summary>Inverse of a unit quaternion (its conjugate).</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Q4 Inverse(Q4 q) => new Q4(-q.x, -q.y, -q.z, q.w);

        /// <summary>Scales to unit length; returns identity for a (nearly) zero quaternion.</summary>
        public static Q4 Normalize(Q4 q)
        {
            float sq = q.x * q.x + q.y * q.y + q.z * q.z + q.w * q.w;
            if (sq <= 1e-18f || float.IsNaN(sq) || float.IsInfinity(sq))
            {
                return Identity;
            }

            float inv = 1f / (float)Math.Sqrt(sq);
            return new Q4(q.x * inv, q.y * inv, q.z * inv, q.w * inv);
        }

        /// <summary>Rotation of <paramref name="degrees"/> around <paramref name="axis"/>.</summary>
        public static Q4 AngleAxis(float degrees, V3 axis)
        {
            V3 n = V3.NormalizeOr(axis, V3.Up);
            double half = degrees * (Math.PI / 360.0);
            float s = (float)Math.Sin(half);
            return new Q4(n.x * s, n.y * s, n.z * s, (float)Math.Cos(half));
        }

        /// <summary>Angle in degrees of the rotation that takes <paramref name="a"/> to <paramref name="b"/>.</summary>
        public static float Angle(Q4 a, Q4 b)
        {
            // atan2 of the relative rotation stays accurate for tiny angles, where acos(dot) does not.
            Q4 d = Inverse(a) * b;
            double sin = Math.Sqrt((double)d.x * d.x + (double)d.y * d.y + (double)d.z * d.z);
            return (float)(2.0 * Math.Atan2(sin, Math.Abs((double)d.w)) * (180.0 / Math.PI));
        }

        /// <summary>Normalized linear interpolation along the shortest arc.</summary>
        public static Q4 Nlerp(Q4 a, Q4 b, float t)
        {
            if (Dot(a, b) < 0f)
            {
                b = new Q4(-b.x, -b.y, -b.z, -b.w);
            }

            return Normalize(new Q4(
                a.x + (b.x - a.x) * t,
                a.y + (b.y - a.y) * t,
                a.z + (b.z - a.z) * t,
                a.w + (b.w - a.w) * t));
        }

        /// <summary>Spherical linear interpolation along the shortest arc.</summary>
        public static Q4 Slerp(Q4 a, Q4 b, float t)
        {
            float cos = Dot(a, b);
            if (cos < 0f)
            {
                cos = -cos;
                b = new Q4(-b.x, -b.y, -b.z, -b.w);
            }

            if (cos > 0.9995f)
            {
                return Nlerp(a, b, t);
            }

            double angle = Math.Acos(cos);
            double sin = Math.Sin(angle);
            float wa = (float)(Math.Sin((1.0 - t) * angle) / sin);
            float wb = (float)(Math.Sin(t * angle) / sin);
            return new Q4(
                a.x * wa + b.x * wb,
                a.y * wa + b.y * wb,
                a.z * wa + b.z * wb,
                a.w * wa + b.w * wb);
        }

        /// <inheritdoc />
        public bool Equals(Q4 other) =>
            x.Equals(other.x) && y.Equals(other.y) && z.Equals(other.z) && w.Equals(other.w);

        /// <inheritdoc />
        public override bool Equals(object obj) => obj is Q4 other && Equals(other);

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked
            {
                int h = x.GetHashCode();
                h = (h * 397) ^ y.GetHashCode();
                h = (h * 397) ^ z.GetHashCode();
                return (h * 397) ^ w.GetHashCode();
            }
        }

        /// <inheritdoc />
        public override string ToString() =>
            "(" + x.ToString("0.#####", System.Globalization.CultureInfo.InvariantCulture) + ", "
            + y.ToString("0.#####", System.Globalization.CultureInfo.InvariantCulture) + ", "
            + z.ToString("0.#####", System.Globalization.CultureInfo.InvariantCulture) + ", "
            + w.ToString("0.#####", System.Globalization.CultureInfo.InvariantCulture) + ")";
    }
}
