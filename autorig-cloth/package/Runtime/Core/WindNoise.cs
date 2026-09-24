using System;

namespace AutoRig.Cloth.Core
{
    /// <summary>
    /// Deterministic smooth value noise used for wind turbulence. Pure function of its inputs, so
    /// it gives identical results on every platform and in tests. Every sample is a convex blend of
    /// lattice values, so the output is always inside [-1, 1].
    /// </summary>
    public static class WindNoise
    {
        /// <summary>The noise repeats every <c>Period</c> units along each axis.</summary>
        public const int Period = 256;

        /// <summary>World-space frequency (per metre) used by <see cref="SampleWind"/>.</summary>
        public const float SpatialFrequency = 0.35f;

        private const float Limit = 1e8f;

        /// <summary>Smooth noise in [-1, 1], periodic with <see cref="Period"/> along every axis.</summary>
        public static float Sample(float x, float y, float z)
        {
            if (!(x > -Limit && x < Limit && y > -Limit && y < Limit && z > -Limit && z < Limit))
            {
                return 0f; // NaN, infinity or absurdly large input
            }

            int xi = (int)Math.Floor(x);
            int yi = (int)Math.Floor(y);
            int zi = (int)Math.Floor(z);
            float u = Smooth(x - xi);
            float v = Smooth(y - yi);
            float w = Smooth(z - zi);

            int x0 = xi & (Period - 1), x1 = (xi + 1) & (Period - 1);
            int y0 = yi & (Period - 1), y1 = (yi + 1) & (Period - 1);
            int z0 = zi & (Period - 1), z1 = (zi + 1) & (Period - 1);

            float a = Lerp(Lattice(x0, y0, z0), Lattice(x1, y0, z0), u);
            float b = Lerp(Lattice(x0, y1, z0), Lattice(x1, y1, z0), u);
            float c = Lerp(Lattice(x0, y0, z1), Lattice(x1, y0, z1), u);
            float d = Lerp(Lattice(x0, y1, z1), Lattice(x1, y1, z1), u);
            float n = Lerp(Lerp(a, b, v), Lerp(c, d, v), w);
            return n < -1f ? -1f : (n > 1f ? 1f : n); // guards against one-ulp rounding overshoot
        }

        /// <summary>Two-dimensional noise in [0, 1], a drop-in for <c>Mathf.PerlinNoise</c>.</summary>
        public static float Sample01(float x, float y) => 0.5f + 0.5f * Sample(x, y, 0.5f);

        /// <summary>
        /// Wind turbulence at a world position and time: a gust factor and a swirl direction whose
        /// components are all inside [-1, 1]. Pass time already wrapped into [0, <see cref="Period"/>)
        /// to keep float precision; the noise is periodic, so the wrap is seamless.
        /// </summary>
        public static void SampleWind(V3 position, float time, out float gust, out V3 swirl)
        {
            float px = position.x * SpatialFrequency;
            float py = position.y * SpatialFrequency;
            float pz = position.z * SpatialFrequency;
            gust = Sample(px + time, py, pz);
            swirl = new V3(
                Sample(px + 31.7f, py + time, pz + 5.3f),
                Sample(px + 11.1f, py + 57.3f, pz + time),
                Sample(px + time, py + 13.1f, pz + 91.9f));
        }

        private static float Smooth(float t) => t * t * (3f - 2f * t);

        private static float Lerp(float a, float b, float t) => a + (b - a) * t;

        private static float Lattice(int x, int y, int z)
        {
            unchecked
            {
                uint h = ((uint)x * 0x8DA6B343u) ^ ((uint)y * 0xD8163841u) ^ ((uint)z * 0xCB1AB31Fu);
                h ^= h >> 15;
                h *= 0x2C1B3C6Du;
                h ^= h >> 12;
                h *= 0x297A2D39u;
                h ^= h >> 15;
                return (h & 0x00FFFFFFu) / 16777215f * 2f - 1f;
            }
        }
    }
}
