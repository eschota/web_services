using System;
using AutoRig.Cloth.Core;
using Xunit;

namespace AutoRig.Cloth.Core.Tests
{
    public class CollisionTests
    {
        // One step per frame with the collider at its final position, so penetration can be
        // measured against exactly the shape the solver used.
        private const float OneStep = 1f / 90f;
        private const float Tolerance = 1e-4f;

        private static ClothParameters Params(float radius)
        {
            ClothParameters p = ChainScene.Plain(gravity: 1f, damping: 0.1f);
            p.radius = radius;
            p.radiusTip = radius;
            return p;
        }

        [Fact]
        public void Sphere_NoParticleEndsAStepInside()
        {
            var solver = new ClothSolver();
            ChainScene scene = ChainScene.Straight(solver, new V3(0f, 1f, 0f), V3.Down, 8, 0.08f, Params(0.02f));
            ColliderHandle sphere = solver.AddCollider();
            V3 center = new V3(0.04f, 0.6f, 0.01f);
            solver.SetSphere(sphere, center, 0.15f, 1u);

            float worst = float.MinValue;
            for (int f = 0; f < 400; f++)
            {
                scene.Step(OneStep);
                for (int k = 1; k < scene.Joints; k++)
                {
                    worst = Math.Max(worst, ClothGeometry.SpherePenetration(scene.Position(0, k), 0.02f, center, 0.15f));
                }
            }

            Assert.True(worst <= Tolerance, "penetration " + worst);
            Assert.True(scene.Position(0, 7).y < 0.6f, "the chain should drape around the sphere");
        }

        [Fact]
        public void MovingSphere_PushesTheChainAndNeverLeavesItInside()
        {
            var solver = new ClothSolver();
            ChainScene scene = ChainScene.Straight(solver, new V3(0f, 1f, 0f), V3.Down, 6, 0.1f, Params(0.015f));
            ColliderHandle sphere = solver.AddCollider();
            scene.Run(1f, OneStep);

            float worst = float.MinValue;
            for (int f = 0; f < 180; f++)
            {
                var center = new V3(-0.6f + f * 0.008f, 0.6f, 0f); // sweeps through the chain at 0.7 m/s
                solver.SetSphere(sphere, center, 0.12f, 1u);
                scene.Step(OneStep);
                for (int k = 1; k < scene.Joints; k++)
                {
                    worst = Math.Max(worst, ClothGeometry.SpherePenetration(scene.Position(0, k), 0.015f, center, 0.12f));
                }
            }

            Assert.True(worst <= Tolerance, "penetration " + worst);
        }

        [Fact]
        public void TaperedCapsule_NoParticleEndsAStepInside()
        {
            var solver = new ClothSolver();
            var p = Params(0.01f);
            p.radiusTip = 0.03f;
            ChainScene scene = ChainScene.Straight(solver, new V3(0.05f, 1f, 0.02f), V3.Down, 8, 0.08f, p);
            ColliderHandle capsule = solver.AddCollider();
            var a = new V3(-0.3f, 0.55f, 0f);
            var b = new V3(0.3f, 0.65f, 0f);
            solver.SetCapsule(capsule, a, b, 0.04f, 0.14f, 1u);

            float worst = float.MinValue;
            for (int f = 0; f < 400; f++)
            {
                scene.Step(OneStep);
                for (int k = 1; k < scene.Joints; k++)
                {
                    float r = solver.GetParticleRadius(scene.Group, k);
                    worst = Math.Max(worst, ClothGeometry.CapsulePenetration(scene.Position(0, k), r, a, b, 0.04f, 0.14f));
                }
            }

            Assert.True(worst <= Tolerance, "penetration " + worst);
            Assert.True(scene.Position(0, 7).y < 0.5f, "the chain should slide off the capsule");
        }

        [Fact]
        public void Plane_NoParticleEndsAStepBelow()
        {
            var solver = new ClothSolver();
            ChainScene scene = ChainScene.Straight(solver, new V3(0f, 0.3f, 0f), new V3(0.3f, -1f, 0f), 8, 0.1f, Params(0.02f));
            ColliderHandle floor = solver.AddCollider();
            solver.SetPlane(floor, V3.Zero, V3.Up, 1u);

            float worst = float.MinValue;
            for (int f = 0; f < 400; f++)
            {
                scene.Step(OneStep);
                for (int k = 1; k < scene.Joints; k++)
                {
                    worst = Math.Max(worst, ClothGeometry.PlanePenetration(scene.Position(0, k), 0.02f, V3.Zero, V3.Up));
                }
            }

            Assert.True(worst <= Tolerance, "penetration " + worst);
            Assert.InRange(scene.Position(0, 7).y, 0.02f - Tolerance, 0.03f); // lying on the floor
        }

        [Fact]
        public void ColliderMask_GroupIgnoresCollidersOutsideItsMask()
        {
            float Depth(uint groupMask)
            {
                var solver = new ClothSolver();
                ChainScene scene = ChainScene.Straight(solver, new V3(0f, 1f, 0f), V3.Down, 8, 0.08f, Params(0.02f));
                solver.SetColliderMask(scene.Group, groupMask);
                ColliderHandle sphere = solver.AddCollider();
                solver.SetSphere(sphere, new V3(0.02f, 0.6f, 0f), 0.15f, 1u << 3);
                scene.Run(3f, OneStep);
                float worst = float.MinValue;
                for (int k = 1; k < scene.Joints; k++)
                {
                    worst = Math.Max(worst, ClothGeometry.SpherePenetration(scene.Position(0, k), 0.02f, new V3(0.02f, 0.6f, 0f), 0.15f));
                }

                return worst;
            }

            Assert.True(Depth(1u << 3) <= Tolerance, "a matching tag must collide");
            Assert.True(Depth(1u << 2) > 0.1f, "a group must pass through a collider outside its mask");
            Assert.True(Depth(uint.MaxValue) <= Tolerance, "the default mask matches every tag");
        }

        [Fact]
        public void DisabledCollider_HasNoEffect()
        {
            var solver = new ClothSolver();
            ChainScene scene = ChainScene.Straight(solver, new V3(0f, 1f, 0f), V3.Down, 6, 0.1f, Params(0.02f));
            ColliderHandle sphere = solver.AddCollider();
            solver.SetSphere(sphere, new V3(0f, 0.7f, 0f), 0.1f, 1u);
            solver.SetColliderEnabled(sphere, false);
            scene.Run(2f, OneStep);
            Assert.InRange(scene.Position(0, 5).x, -1e-3f, 1e-3f); // hangs straight through it
            solver.RemoveCollider(sphere);
            Assert.False(sphere.IsValid);
            Assert.Equal(0, solver.ColliderCount);
        }

        [Fact]
        public void Geometry_PushOutPlacesTheParticleOnTheSurface()
        {
            var p = new V3(0.01f, 0.02f, 0f);
            Assert.True(ClothGeometry.PushOutOfSphere(ref p, 0.01f, V3.Zero, 0.1f));
            Assert.InRange(ClothGeometry.SpherePenetration(p, 0.01f, V3.Zero, 0.1f), -1e-6f, 1e-6f);

            var a = V3.Zero;
            var b = new V3(1f, 0f, 0f);
            var q = new V3(0.5f, 0.05f, 0.02f);
            Assert.True(ClothGeometry.PushOutOfCapsule(ref q, 0.01f, a, b, 0.1f, 0.3f));
            Assert.InRange(ClothGeometry.CapsulePenetration(q, 0.01f, a, b, 0.1f, 0.3f), -1e-6f, 1e-6f);
            Assert.InRange(q.x, 0.5f - 1e-6f, 0.5f + 1e-6f); // pushed straight away from the axis

            var onAxis = new V3(0.25f, 0f, 0f);
            Assert.True(ClothGeometry.PushOutOfCapsule(ref onAxis, 0f, a, b, 0.1f, 0.1f));
            Assert.InRange(onAxis.Length, 0.26f, 0.28f); // degenerate case pushes sideways, never NaN

            var below = new V3(0f, -0.5f, 0f);
            Assert.True(ClothGeometry.PushOutOfPlane(ref below, 0.05f, V3.Zero, V3.Up));
            Assert.InRange(below.y, 0.05f - 1e-6f, 0.05f + 1e-6f);
        }

        [Fact]
        public void Geometry_ConeClampRotatesOntoTheLimit()
        {
            V3 axis = V3.Right;
            V3 dir = V3.Down;
            float cos = (float)Math.Cos(Math.PI / 6.0);
            float sin = (float)Math.Sin(Math.PI / 6.0);
            V3 clamped = ClothGeometry.ClampToCone(dir, axis, cos, sin);
            Assert.InRange(clamped.Length, 0.9999f, 1.0001f);
            Assert.InRange(V3.Dot(clamped, axis), cos - 1e-5f, cos + 1e-5f);
            Assert.True(clamped.y < 0f, "stays on the side of the original direction");

            V3 inside = new V3(1f, 0.1f, 0f).Normalized;
            Assert.Equal(inside, ClothGeometry.ClampToCone(inside, axis, cos, sin));

            V3 opposite = ClothGeometry.ClampToCone(-axis, axis, cos, sin);
            Assert.True(opposite.IsFinite);
            Assert.InRange(V3.Dot(opposite, axis), cos - 1e-5f, cos + 1e-5f);
        }
    }
}
