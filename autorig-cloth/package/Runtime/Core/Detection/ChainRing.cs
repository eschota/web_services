using System;
using System.Collections.Generic;

namespace AutoRig.Cloth.Core
{
    /// <summary>
    /// Orders the chains of a group by angle around their attach point, as the manifest requires
    /// for <c>open</c> and <c>loop</c> groups (neighbours in the list are neighbours in space).
    /// </summary>
    public static class ChainRing
    {
        /// <summary>
        /// Sorts chain roots by angle around <paramref name="axis"/> through <paramref name="center"/>.
        /// The result is rotated so that the largest angular gap falls between the last and the
        /// first chain, which is where an <c>open</c> group has its free edge.
        /// </summary>
        /// <param name="roots">Root joint positions.</param>
        /// <param name="center">The point the chains go around, usually the attach bone.</param>
        /// <param name="axis">Ring axis, usually the mean hanging direction of the chains.</param>
        /// <param name="largestGapDegrees">Largest angle between consecutive roots (360 for fewer than two roots).</param>
        /// <returns>Indices into <paramref name="roots"/> in angular order.</returns>
        public static int[] OrderAround(IReadOnlyList<V3> roots, V3 center, V3 axis, out float largestGapDegrees)
        {
            int n = roots == null ? 0 : roots.Count;
            var order = new int[n];
            for (int i = 0; i < n; i++)
            {
                order[i] = i;
            }

            largestGapDegrees = 360f;
            if (n < 2)
            {
                return order;
            }

            V3 ax = V3.NormalizeOr(axis, V3.Down);
            V3 e1 = V3.Zero;
            for (int i = 0; i < n && e1.LengthSquared == 0f; i++)
            {
                e1 = V3.Normalize(Project(roots[i] - center, ax));
            }

            if (e1.LengthSquared == 0f)
            {
                e1 = V3.AnyPerpendicular(ax);
            }

            V3 e2 = V3.Cross(ax, e1);
            var angles = new float[n];
            for (int i = 0; i < n; i++)
            {
                V3 p = Project(roots[i] - center, ax);
                double a = Math.Atan2(V3.Dot(p, e2), V3.Dot(p, e1)) * (180.0 / Math.PI);
                angles[i] = (float)(a < 0.0 ? a + 360.0 : a);
            }

            // Insertion sort: tiny inputs, and ties keep their original order (deterministic).
            for (int i = 1; i < n; i++)
            {
                int item = order[i];
                int j = i - 1;
                while (j >= 0 && angles[order[j]] > angles[item])
                {
                    order[j + 1] = order[j];
                    j--;
                }

                order[j + 1] = item;
            }

            int widest = n - 1;
            largestGapDegrees = 0f;
            for (int k = 0; k < n; k++)
            {
                float a = angles[order[k]];
                float b = k == n - 1 ? angles[order[0]] + 360f : angles[order[k + 1]];
                float gap = b - a;
                if (gap > largestGapDegrees)
                {
                    largestGapDegrees = gap;
                    widest = k;
                }
            }

            var rotated = new int[n];
            for (int k = 0; k < n; k++)
            {
                rotated[k] = order[(widest + 1 + k) % n];
            }

            return rotated;
        }

        private static V3 Project(V3 v, V3 axis) => v - axis * V3.Dot(v, axis);
    }
}
