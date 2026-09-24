using System;

namespace AutoRig.Cloth.Core
{
    /// <summary>Global settings of a <see cref="ClothSolver"/>.</summary>
    [Serializable]
    public struct SolverSettings
    {
        /// <summary>Default fixed simulation rate in steps per second.</summary>
        public const float DefaultSubstepRate = 90f;

        /// <summary>Default cap on simulation steps per rendered frame.</summary>
        public const int DefaultMaxSubsteps = 4;

        /// <summary>Default number of constraint iterations per step.</summary>
        public const int DefaultIterations = 4;

        /// <summary>Fixed simulation rate in steps per second (clamped to 30–480).</summary>
        public float substepRate;

        /// <summary>
        /// Maximum steps per <see cref="ClothSolver.Simulate"/> call (clamped to 1–16). When a frame
        /// needs more, the frame is simulated in this many steps and the remaining time is dropped
        /// (slow motion instead of a spiral of death).
        /// </summary>
        public int maxSubsteps;

        /// <summary>Constraint iterations per step (clamped to 1–16).</summary>
        public int iterations;

        /// <summary>Gravity acceleration in world space; each group scales it by its <c>gravity</c> parameter.</summary>
        public V3 gravity;

        /// <summary>90 Hz, at most 4 steps per frame, 4 iterations, gravity (0, -9.81, 0).</summary>
        public static SolverSettings Default => new SolverSettings
        {
            substepRate = DefaultSubstepRate,
            maxSubsteps = DefaultMaxSubsteps,
            iterations = DefaultIterations,
            gravity = new V3(0f, -9.81f, 0f),
        };

        /// <summary>Returns a copy with every value clamped to its valid range.</summary>
        public SolverSettings Sanitized()
        {
            SolverSettings s = this;
            if (float.IsNaN(s.substepRate) || float.IsInfinity(s.substepRate))
            {
                s.substepRate = DefaultSubstepRate;
            }

            s.substepRate = s.substepRate < 30f ? 30f : (s.substepRate > 480f ? 480f : s.substepRate);
            s.maxSubsteps = s.maxSubsteps < 1 ? 1 : (s.maxSubsteps > 16 ? 16 : s.maxSubsteps);
            s.iterations = s.iterations < 1 ? 1 : (s.iterations > 16 ? 16 : s.iterations);
            if (!s.gravity.IsFinite)
            {
                s.gravity = new V3(0f, -9.81f, 0f);
            }

            return s;
        }
    }
}
