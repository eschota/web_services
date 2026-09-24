using System;
using System.Diagnostics;
using AutoRig.Cloth.Core;
using Xunit;
using Xunit.Abstractions;

namespace AutoRig.Cloth.Core.Tests
{
    public class PerformanceTests
    {
        private readonly ITestOutputHelper _output;

        public PerformanceTests(ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact]
        public void TwoThousandParticles_SixHundredSteps_Complete()
        {
            var solver = new ClothSolver();
            ClothParameters p = BuiltInPresets.GetOrDefault(BuiltInPresets.Hair);

            // 100 chains × 20 joints = 2,000 particles, plus a body made of three colliders.
            ChainScene scene = ChainScene.Comb(solver, new V3(-0.5f, 1.5f, 0f), 100, 20, 0.01f, 0.03f, p);
            Assert.Equal(2000, solver.ParticleCount);
            ColliderHandle head = solver.AddCollider();
            ColliderHandle torso = solver.AddCollider();
            ColliderHandle floor = solver.AddCollider();
            solver.SetSphere(head, new V3(0f, 1.45f, 0.2f), 0.12f, 1u);
            solver.SetCapsule(torso, new V3(0f, 1.3f, 0.3f), new V3(0f, 0.8f, 0.3f), 0.15f, 0.12f, 1u);
            solver.SetPlane(floor, V3.Zero, V3.Up, 1u);
            solver.SetWind(scene.Group, new V3(1f, 0f, 2f), 0.5f, 1f);

            scene.Run(0.2f, 1f / 90f); // warm-up (JIT)
            var watch = Stopwatch.StartNew();
            for (int step = 0; step < 600; step++)
            {
                float t = step / 90f;
                scene.Frame.position = new V3(-0.5f + 0.2f * (float)Math.Sin(2.0 * t), 1.5f, 0.1f * (float)Math.Cos(3.0 * t));
                scene.Step(1f / 90f); // exactly one simulation step per call
            }

            watch.Stop();
            Assert.True(scene.AllFinite());
            double perStep = watch.Elapsed.TotalMilliseconds / 600.0;
            _output.WriteLine("2,000 particles × 600 steps: " + watch.Elapsed.TotalMilliseconds.ToString("0.0") + " ms total, " + perStep.ToString("0.000") + " ms per step");
        }
    }
}
