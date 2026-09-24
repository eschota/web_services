using AutoRig.Cloth.Core;
using Xunit;

namespace AutoRig.Cloth.Core.Tests
{
    public class MathTests
    {
        private static void Near(V3 expected, V3 actual, float tolerance = 1e-5f)
        {
            Assert.True(V3.Distance(expected, actual) <= tolerance, "expected " + expected + ", got " + actual);
        }

        [Fact]
        public void V3_BasicOperations()
        {
            var a = new V3(1f, 2f, 3f);
            var b = new V3(-2f, 0.5f, 4f);
            Assert.Equal(new V3(-1f, 2.5f, 7f), a + b);
            Assert.Equal(new V3(3f, 1.5f, -1f), a - b);
            Assert.Equal(new V3(2f, 4f, 6f), a * 2f);
            Assert.Equal(11f, V3.Dot(a, b));
            Assert.Equal(V3.Forward, V3.Cross(V3.Right, V3.Up)); // same convention as UnityEngine.Vector3.Cross
            Assert.Equal(V3.Zero, V3.Normalize(V3.Zero));
            Assert.InRange(new V3(3f, 4f, 0f).Normalized.Length, 0.99999f, 1.00001f);
            Assert.Equal(new V3(-0.5f, 1.25f, 3.5f), V3.Lerp(a, b, 0.5f));
            Assert.InRange(V3.Dot(V3.AnyPerpendicular(V3.Up), V3.Up), -1e-6f, 1e-6f);
            Assert.False(new V3(float.NaN, 0f, 0f).IsFinite);
        }

        [Fact]
        public void Q4_MatchesUnityConventions()
        {
            // Quaternion.AngleAxis(90, Vector3.up) * Vector3.right == (0, 0, -1) in Unity.
            Near(new V3(0f, 0f, -1f), Q4.AngleAxis(90f, V3.Up) * V3.Right);

            Q4 a = Q4.AngleAxis(30f, new V3(1f, 2f, 3f));
            Q4 b = Q4.AngleAxis(-70f, new V3(0f, 1f, -1f));
            var v = new V3(0.3f, -1.2f, 2f);
            Near((a * b) * v, a * (b * v)); // a * b applies b first

            Q4 identity = a * Q4.Inverse(a);
            Assert.InRange(identity.w, 0.99999f, 1.00001f);
            Assert.InRange(Q4.Angle(Q4.Identity, Q4.AngleAxis(120f, V3.Up)), 119.99f, 120.01f);
            Assert.InRange(Q4.Angle(Q4.AngleAxis(170f, V3.Up), Q4.AngleAxis(-170f, V3.Up)), 19.99f, 20.01f);

            Q4 half = Q4.Slerp(Q4.Identity, Q4.AngleAxis(90f, V3.Up), 0.5f);
            Assert.InRange(Q4.Angle(half, Q4.AngleAxis(45f, V3.Up)), 0f, 0.01f);
            Q4 nearHalf = Q4.Nlerp(Q4.Identity, Q4.AngleAxis(10f, V3.Up), 0.5f);
            Assert.InRange(Q4.Angle(nearHalf, Q4.AngleAxis(5f, V3.Up)), 0f, 0.01f);
            Assert.Equal(Q4.Identity, Q4.Normalize(new Q4(0f, 0f, 0f, 0f)));
        }

        [Fact]
        public void RigidFrame_TransformsAndInterpolates()
        {
            var frame = new RigidFrame(new V3(1f, 2f, 3f), Q4.AngleAxis(90f, V3.Up));
            V3 world = frame.TransformPoint(V3.Right);
            Near(new V3(1f, 2f, 2f), world);
            Near(V3.Right, frame.InverseTransformPoint(world));

            RigidFrame mid = RigidFrame.Interpolate(RigidFrame.Identity, frame, 0.5f);
            Near(new V3(0.5f, 1f, 1.5f), mid.position);
            Assert.InRange(Q4.Angle(mid.rotation, Q4.AngleAxis(45f, V3.Up)), 0f, 0.01f);
        }

        [Fact]
        public void Settings_AreClampedToValidRanges()
        {
            var solver = new ClothSolver();
            solver.Settings = new SolverSettings { substepRate = 5f, maxSubsteps = 0, iterations = 99, gravity = new V3(float.NaN, 0f, 0f) };
            Assert.Equal(30f, solver.Settings.substepRate);
            Assert.Equal(1, solver.Settings.maxSubsteps);
            Assert.Equal(16, solver.Settings.iterations);
            Assert.Equal(new V3(0f, -9.81f, 0f), solver.Settings.gravity);

            var p = new ClothParameters(5f, -1f, float.NaN, 400f, 2f, 0.5f, -1f, 0f, 1.5f, 0.2f, 0f, 1f).Sanitized();
            Assert.Equal(2f, p.gravity);
            Assert.Equal(0f, p.damping);
            Assert.Equal(ClothParameters.Default.stiffness, p.stiffness);
            Assert.Equal(180f, p.angleLimitDeg);
            Assert.Equal(1f, p.stretch);
            Assert.Equal(0f, p.radius);
            Assert.Equal(1f, p.inertiaMove);
        }
    }
}
