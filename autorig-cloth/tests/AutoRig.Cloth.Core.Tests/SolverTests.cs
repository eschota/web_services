using System;
using System.Collections.Generic;
using System.Linq;
using AutoRig.Cloth.Core;
using Xunit;

namespace AutoRig.Cloth.Core.Tests
{
    public class SolverTests
    {
        private const float Frame60 = 1f / 60f;

        [Fact]
        public void HangingChain_SettlesStraightDown_WithRestLengthsPreserved()
        {
            var solver = new ClothSolver();
            ClothParameters p = ChainScene.Plain(gravity: 1f, damping: 0.1f);
            ChainScene scene = ChainScene.Straight(solver, new V3(0f, 2f, 0f), V3.Right, 6, 0.1f, p);

            for (int f = 0; f < 600; f++)
            {
                scene.Step(Frame60);
                Assert.True(scene.AllFinite(), "NaN or infinity at frame " + f);
                Assert.True(scene.MaxLengthError() < 1e-4f, "length error " + scene.MaxLengthError() + " at frame " + f);
            }

            V3 tip = scene.Position(0, 5);
            Assert.InRange(tip.x, -0.005f, 0.005f);
            Assert.InRange(tip.y, 2f - 0.5f - 0.001f, 2f - 0.5f + 0.001f);
            Assert.InRange(tip.z, -0.005f, 0.005f);
            Assert.True(scene.MaxSpeed() < 0.01f, "still moving: " + scene.MaxSpeed());
        }

        [Fact]
        public void MovingRoot_ChainLagsThenFollows()
        {
            var solver = new ClothSolver();
            ClothParameters p = ChainScene.Plain(gravity: 1f, damping: 0.1f);
            ChainScene scene = ChainScene.Straight(solver, new V3(0f, 2f, 0f), V3.Down, 5, 0.1f, p);
            scene.Run(1f, Frame60);

            // Move the root sideways at 3 m/s for half a second.
            for (int f = 0; f < 30; f++)
            {
                scene.Frame.position += new V3(3f * Frame60, 0f, 0f);
                scene.Step(Frame60);
            }

            V3 root = scene.Position(0, 0);
            V3 tip = scene.Position(0, 4);
            Assert.True(tip.x < root.x - 0.02f, "tip should trail behind the moving root: root " + root + " tip " + tip);

            // Stop and let it settle: the chain hangs under the root again.
            scene.Run(5f, Frame60);
            root = scene.Position(0, 0);
            tip = scene.Position(0, 4);
            Assert.InRange(tip.x - root.x, -0.005f, 0.005f);
            Assert.InRange(tip.y - root.y, -0.401f, -0.399f);
        }

        [Fact]
        public void RandomRootMotion_KineticEnergyStaysBounded()
        {
            var solver = new ClothSolver();
            ClothParameters p = BuiltInPresets.GetOrDefault(BuiltInPresets.Hair);
            ChainScene scene = ChainScene.Straight(solver, new V3(0f, 1.6f, 0f), V3.Down, 6, 0.08f, p);
            var random = new Random(12345);
            V3 velocity = V3.Zero;
            float yaw = 0f;
            float yawSpeed = 0f;
            double maxEnergy = 0.0;
            float maxSpeed = 0f;

            for (int f = 0; f < 600; f++)
            {
                // Smoothed random walk: accelerations up to ~60 m/s², speeds clamped to 4 m/s, turns up to 360°/s.
                velocity += new V3(Rand(random), Rand(random) * 0.5f, Rand(random)) * 1f;
                if (velocity.Length > 4f)
                {
                    velocity = velocity.Normalized * 4f;
                }

                yawSpeed = Math.Clamp(yawSpeed + Rand(random) * 60f, -360f, 360f);
                yaw += yawSpeed * Frame60;
                scene.Frame.position += velocity * Frame60;
                scene.Frame.rotation = Q4.AngleAxis(yaw, V3.Up);
                scene.Step(Frame60);

                Assert.True(scene.AllFinite(), "NaN at frame " + f);
                Assert.True(scene.MaxLengthError() < 1e-4f, "length error at frame " + f);
                maxEnergy = Math.Max(maxEnergy, scene.KineticEnergy());
                maxSpeed = Math.Max(maxSpeed, scene.MaxSpeed());
            }

            // Five free joints; the root never moves faster than 4 m/s and turns at most 360°/s,
            // so anything near these bounds would be an explosion.
            Assert.True(maxSpeed < 25f, "max joint speed " + maxSpeed);
            Assert.True(maxEnergy < 5 * 0.5 * 25.0 * 25.0, "max kinetic energy " + maxEnergy);

            // After the motion stops, the chain comes to rest.
            scene.Run(4f, Frame60);
            Assert.True(scene.KineticEnergy() < 1e-6, "energy after rest " + scene.KineticEnergy());
        }

        [Fact]
        public void InertiaMoveZero_TranslatedChainKeepsItsShapeExactly()
        {
            V3[] Relative(ChainScene s)
            {
                var r = new V3[s.Joints];
                for (int k = 0; k < s.Joints; k++)
                {
                    r[k] = s.Position(0, k) - s.Position(0, 0);
                }

                return r;
            }

            var solver = new ClothSolver();
            ClothParameters p = ChainScene.Plain(gravity: 1f, damping: 0.1f, stiffness: 0.05f);
            p.inertiaMove = 0f;
            ChainScene scene = ChainScene.Straight(solver, new V3(0f, 2f, 0f), new V3(1f, -1f, 0f), 5, 0.1f, p);
            scene.Run(3f, Frame60);
            V3[] before = Relative(scene);

            for (int f = 0; f < 60; f++)
            {
                scene.Frame.position += new V3(2f * Frame60, 0.5f * Frame60, -1f * Frame60);
                scene.Step(Frame60);
            }

            V3[] after = Relative(scene);
            for (int k = 0; k < scene.Joints; k++)
            {
                Assert.True(V3.Distance(before[k], after[k]) < 1e-4f, "joint " + k + " moved relative to the root: " + before[k] + " → " + after[k]);
            }
        }

        [Fact]
        public void InertiaMoveOne_TranslatedChainLags()
        {
            var solver = new ClothSolver();
            ClothParameters p = ChainScene.Plain(gravity: 1f, damping: 0.1f, stiffness: 0.05f);
            p.inertiaMove = 1f;
            ChainScene scene = ChainScene.Straight(solver, new V3(0f, 2f, 0f), new V3(1f, -1f, 0f), 5, 0.1f, p);
            scene.Run(3f, Frame60);
            V3 before = scene.Position(0, 4) - scene.Position(0, 0);

            for (int f = 0; f < 30; f++)
            {
                scene.Frame.position += new V3(2f * Frame60, 0f, 0f);
                scene.Step(Frame60);
            }

            V3 after = scene.Position(0, 4) - scene.Position(0, 0);
            Assert.True(before.x - after.x > 0.02f, "the tip should lag behind: " + before + " → " + after);
        }

        [Fact]
        public void InertiaRotateZero_RotatedChainKeepsItsShape()
        {
            var solver = new ClothSolver();
            ClothParameters p = ChainScene.Plain(gravity: 0f, damping: 0.1f, stiffness: 0f);
            p.inertiaMove = 0f;
            p.inertiaRotate = 0f;
            ChainScene scene = ChainScene.Straight(solver, new V3(0f, 1f, 0f), new V3(1f, 0f, 1f), 5, 0.1f, p);
            scene.Run(0.5f, Frame60);

            for (int f = 0; f < 60; f++)
            {
                scene.Frame.rotation = Q4.AngleAxis(f * 1.5f, V3.Up);
                scene.Step(Frame60);
            }

            for (int k = 0; k < scene.Joints; k++)
            {
                Assert.True(V3.Distance(scene.Position(0, k), scene.Target(0, k)) < 1e-3f, "joint " + k + " did not turn with the frame");
            }
        }

        [Fact]
        public void Teleport_ResetsToTargetsWithZeroVelocity()
        {
            var solver = new ClothSolver();
            ClothParameters p = ChainScene.Plain(gravity: 0f, damping: 0f, stiffness: 0f);
            ChainScene scene = ChainScene.Straight(solver, new V3(0f, 1f, 0f), V3.Right, 5, 0.1f, p);
            solver.SetTeleportDistance(scene.Group, 1f);
            scene.Run(0.5f, Frame60);

            // Jump 5 m in one frame.
            scene.Frame.position += new V3(5f, 0f, 0f);
            scene.Step(Frame60);
            for (int k = 0; k < scene.Joints; k++)
            {
                Assert.True(V3.Distance(scene.Position(0, k), scene.Target(0, k)) < 1e-5f, "joint " + k + " not reset");
                Assert.True(scene.Velocity(0, k).Length < 1e-3f, "joint " + k + " kept velocity " + scene.Velocity(0, k));
            }

            // Turn 120° in one frame.
            scene.Frame.rotation = Q4.AngleAxis(120f, V3.Up);
            scene.Step(Frame60);
            for (int k = 0; k < scene.Joints; k++)
            {
                Assert.True(V3.Distance(scene.Position(0, k), scene.Target(0, k)) < 1e-5f, "joint " + k + " not reset after rotation");
                Assert.True(scene.Velocity(0, k).Length < 1e-3f);
            }
        }

        [Fact]
        public void MovesBelowTheTeleportDistance_AreSimulated()
        {
            var solver = new ClothSolver();
            ClothParameters p = ChainScene.Plain(gravity: 0f, damping: 0f, stiffness: 0f);
            ChainScene scene = ChainScene.Straight(solver, new V3(0f, 1f, 0f), V3.Right, 5, 0.1f, p);
            solver.SetTeleportDistance(scene.Group, 1f);
            scene.Run(0.5f, Frame60);

            scene.Frame.position += new V3(0f, 0.5f, 0f);
            scene.Step(Frame60);
            Assert.True(V3.Distance(scene.Position(0, 4), scene.Target(0, 4)) > 0.05f, "a 0.5 m move must not teleport");
            Assert.True(scene.MaxLengthError() < 1e-4f);
        }

        [Fact]
        public void AngleLimit_IsRespected()
        {
            var solver = new ClothSolver();
            ClothParameters p = ChainScene.Plain(gravity: 2f, damping: 0.05f, stiffness: 0f);
            p.angleLimitDeg = 30f;
            ChainScene scene = ChainScene.Straight(solver, new V3(0f, 2f, 0f), V3.Right, 6, 0.1f, p);

            float worst = 0f;
            for (int f = 0; f < 300; f++)
            {
                scene.Step(Frame60);
                for (int k = 1; k < scene.Joints; k++)
                {
                    V3 dir = (scene.Position(0, k) - scene.Position(0, k - 1)).Normalized;
                    float angle = (float)(Math.Acos(Math.Clamp(V3.Dot(dir, V3.Right), -1f, 1f)) * 180.0 / Math.PI);
                    worst = Math.Max(worst, angle);
                }
            }

            Assert.InRange(worst, 29f, 30.01f); // gravity pushes every link onto the limit, never past it
        }

        [Fact]
        public void LoopRing_StaysRoughlyCircularWhileSwinging()
        {
            var solver = new ClothSolver();
            ClothParameters p = BuiltInPresets.GetOrDefault(BuiltInPresets.Skirt);
            p.radius = 0f;
            p.radiusTip = 0f;
            ChainScene scene = ChainScene.Ring(solver, new V3(0f, 1f, 0f), 8, 5, 0.18f, 0.1f, 20f, ConnectionMode.Loop, p);
            Assert.Equal(8 * 4, solver.GetLinkCount(scene.Group)); // 8 neighbour pairs × 4 free joints

            float worstRatio = 0f;
            for (int f = 0; f < 300; f++)
            {
                float t = f * Frame60;
                scene.Frame.position = new V3(0.35f * (float)Math.Sin(2.0 * Math.PI * 1.2 * t), 1f, 0.15f * (float)Math.Sin(2.0 * Math.PI * 0.7 * t));
                scene.Frame.rotation = Q4.AngleAxis(35f * (float)Math.Sin(2.0 * Math.PI * 0.9 * t), V3.Up);
                scene.Step(Frame60);

                for (int c = 0; c < scene.Chains; c++)
                {
                    int next = (c + 1) % scene.Chains;
                    for (int k = 1; k < scene.Joints; k++)
                    {
                        float length = V3.Distance(scene.Position(c, k), scene.Position(next, k));
                        float rest = V3.Distance(scene.Target(c, k), scene.Target(next, k));
                        worstRatio = Math.Max(worstRatio, Math.Abs(length / rest - 1f));
                    }
                }
            }

            Assert.True(worstRatio < 0.3f, "horizontal links deviated by " + worstRatio);
        }

        [Fact]
        public void OpenGroup_LinksNeighboursOnly_AndUnevenChainsStopAtTheShortest()
        {
            var solver = new ClothSolver();
            GroupHandle open = solver.AddGroup(new[] { 5, 4, 6 }, ConnectionMode.Open);
            Assert.Equal(2 * 3, solver.GetLinkCount(open)); // 2 pairs × (4 − 1) linked free joints

            // The shortest chain of the whole group bounds every pair, not just the pairs it is in.
            GroupHandle unevenOpen = solver.AddGroup(new[] { 4, 6, 6 }, ConnectionMode.Open);
            Assert.Equal(2 * 3, solver.GetLinkCount(unevenOpen));
            GroupHandle unevenLoop = solver.AddGroup(new[] { 6, 4, 6 }, ConnectionMode.Loop);
            Assert.Equal(3 * 3, solver.GetLinkCount(unevenLoop));
            static int JointOf(int particle) => particle >= 10 ? particle - 10 : (particle >= 6 ? particle - 6 : particle); // chains start at 0, 6, 10
            for (int l = 0; l < solver.GetLinkCount(unevenLoop); l++)
            {
                solver.GetLink(unevenLoop, l, out int first, out int second);
                Assert.Equal(JointOf(first), JointOf(second));
                Assert.InRange(JointOf(first), 1, 3);
            }

            GroupHandle loop = solver.AddGroup(new[] { 3, 3, 3 }, ConnectionMode.Loop);
            Assert.Equal(3 * 2, solver.GetLinkCount(loop));
            solver.GetLink(loop, 5, out int a, out int b);
            Assert.Equal(2 * 3 + 2, a); // last chain, joint 2 …
            Assert.Equal(2, b);         // … linked to the first chain, joint 2

            GroupHandle two = solver.AddGroup(new[] { 3, 3 }, ConnectionMode.Loop);
            Assert.Equal(2, solver.GetLinkCount(two)); // a loop of two chains has no duplicate closing link

            GroupHandle none = solver.AddGroup(new[] { 3, 3 }, ConnectionMode.None);
            Assert.Equal(0, solver.GetLinkCount(none));
        }

        [Fact]
        public void Stiffness_RecoversItsShareOfTheAnglePerSixtiethOfASecond_AtAnyStepRate()
        {
            // stiffness is the share of the angle back to the animated direction recovered per 1/60 s;
            // it turns the link and never changes its length. No gravity, no inertia, and damping 1 so
            // that only the restoration moves the joint.
            const float stiffness = 0.2f;
            const float startDegrees = 10f;
            const float segment = 0.5f;
            V3 hanging = V3.Down * segment;
            V3 turned = Q4.AngleAxis(startDegrees, V3.Forward) * hanging;
            var remaining = new List<float>();
            foreach (float rate in new[] { 60f, 120f, 240f, 480f })
            {
                var solver = new ClothSolver { Settings = new SolverSettings { substepRate = rate, maxSubsteps = 16, iterations = 4, gravity = V3.Zero } };
                GroupHandle group = solver.AddGroup(new[] { 2 }, ConnectionMode.None);
                solver.SetParameters(group, ChainScene.Plain(gravity: 0f, damping: 1f, stiffness: stiffness));
                solver.SetGroupFrame(group, RigidFrame.Identity);
                solver.SetTarget(group, 0, V3.Zero);
                solver.SetTarget(group, 1, hanging);
                solver.Simulate(Frame60); // starts on the animated pose

                // Turn the animated pose; a paused frame makes it the pose of the whole next frame.
                solver.SetTarget(group, 1, turned);
                solver.Simulate(0f);
                solver.Simulate(Frame60);

                V3 link = solver.GetPosition(group, 1) - solver.GetPosition(group, 0);
                Assert.Equal(segment, link.Length, 5);
                double cos = Math.Min(1.0, V3.Dot(link.Normalized, turned.Normalized));
                remaining.Add((float)(Math.Acos(cos) * 180.0 / Math.PI));
            }

            float expected = (1f - stiffness) * startDegrees;
            Assert.All(remaining, angle => Assert.InRange(angle, expected * 0.99f, expected * 1.01f));
            Assert.True(remaining.Max() - remaining.Min() < 0.01f, "depends on the step rate: " + string.Join(", ", remaining));
        }

        [Fact]
        public void RadiusTipZero_MeansTheSameAsRadius()
        {
            var solver = new ClothSolver();
            GroupHandle group = solver.AddGroup(new[] { 3 }, ConnectionMode.None);
            solver.SetLengthScale(group, 2f);
            solver.SetParameters(group, ClothParameters.Default); // radius 0.02, radius_tip 0
            for (int k = 0; k < 3; k++)
            {
                Assert.Equal(0.04f, solver.GetParticleRadius(group, k), 6);
            }

            ClothParameters tapered = ClothParameters.Default;
            tapered.radiusTip = 0.01f;
            solver.SetParameters(group, tapered);
            Assert.Equal(0.04f, solver.GetParticleRadius(group, 0), 6);
            Assert.Equal(0.03f, solver.GetParticleRadius(group, 1), 6); // interpolated along the chain
            Assert.Equal(0.02f, solver.GetParticleRadius(group, 2), 6);
        }

        [Fact]
        public void Stretch_AllowsLongerLinksWithinTheAllowance()
        {
            var solver = new ClothSolver();
            ClothParameters p = ChainScene.Plain(gravity: 2f, damping: 0.2f);
            p.stretch = 0.2f;
            ChainScene scene = ChainScene.Straight(solver, new V3(0f, 2f, 0f), V3.Down, 4, 0.1f, p);
            solver.SetWind(scene.Group, new V3(0f, -60f, 0f), 0f, 0f); // wind pulls straight down
            ClothParameters withWind = p;
            withWind.wind = 1f;
            solver.SetParameters(scene.Group, withWind);
            scene.Run(2f, Frame60);

            for (int k = 1; k < scene.Joints; k++)
            {
                float length = V3.Distance(scene.Position(0, k), scene.Position(0, k - 1));
                Assert.InRange(length, 0.1f, 0.12f + 1e-5f);
            }

            Assert.True(V3.Distance(scene.Position(0, 3), scene.Position(0, 2)) > 0.105f, "the links should stretch under load");
        }

        [Fact]
        public void FixedSteps_ConsumeFrameTime_AndCapSlowFrames()
        {
            var solver = new ClothSolver();
            ChainScene scene = ChainScene.Straight(solver, new V3(0f, 1f, 0f), V3.Down, 4, 0.1f, BuiltInPresets.GetOrDefault("hair"));
            float h = solver.StepDeltaTime;
            Assert.Equal(1f / 90f, h);

            scene.Run(1f, Frame60); // 60 frames of 1/60 s = 90 steps of 1/90 s
            Assert.InRange(solver.SimulationTime, 1.0 - h, 1.0 + 1e-6);

            double before = solver.SimulationTime;
            scene.Step(0.5f); // a hitch: capped at 4 steps, the rest of the time is dropped
            Assert.InRange(solver.SimulationTime - before, 4.0 * h - 1e-6, 4.0 * h + 1e-6);

            before = solver.SimulationTime;
            scene.Step(1f / 240f); // shorter than a step: nothing is simulated yet
            scene.Step(1f / 240f);
            Assert.Equal(before, solver.SimulationTime);
            scene.Step(1f / 240f); // the accumulated time now covers one step
            Assert.InRange(solver.SimulationTime - before, h - 1e-6, h + 1e-6);

            solver.Settings = new SolverSettings { substepRate = 60f, maxSubsteps = 2, iterations = 2, gravity = new V3(0f, -9.81f, 0f) };
            before = solver.SimulationTime;
            scene.Step(0.1f);
            Assert.InRange(solver.SimulationTime - before, 2.0 / 60.0 - 1e-6, 2.0 / 60.0 + 1e-6);
            Assert.True(scene.AllFinite());
        }

        [Fact]
        public void Determinism_TwoIdenticalRunsAreBitIdentical()
        {
            float[] a = RunScenario();
            float[] b = RunScenario();
            Assert.Equal(a.Length, b.Length);
            for (int i = 0; i < a.Length; i++)
            {
                Assert.Equal(BitConverter.SingleToInt32Bits(a[i]), BitConverter.SingleToInt32Bits(b[i]));
            }
        }

        [Fact]
        public void PausedFrames_DoNotStep_AndCarryTheClothWithTheFrame()
        {
            var solver = new ClothSolver();
            ClothParameters p = ChainScene.Plain(gravity: 1f, damping: 0.1f);
            ChainScene scene = ChainScene.Straight(solver, new V3(0f, 1f, 0f), new V3(1f, -1f, 0f), 5, 0.1f, p);
            scene.Run(0.3f, Frame60); // mid-swing
            double time = solver.SimulationTime;
            var local = new V3[scene.Joints];
            for (int k = 0; k < scene.Joints; k++)
            {
                local[k] = scene.Frame.InverseTransformPoint(scene.Position(0, k));
            }

            // The paused character is moved and turned (for example dragged in the editor).
            scene.Frame.position += new V3(0.7f, 0f, 0.2f);
            scene.Frame.rotation = Q4.AngleAxis(40f, V3.Up);
            scene.Step(0f);
            scene.Step(0f);

            Assert.Equal(time, solver.SimulationTime);
            for (int k = 0; k < scene.Joints; k++)
            {
                V3 now = scene.Frame.InverseTransformPoint(scene.Position(0, k));
                Assert.True(V3.Distance(now, local[k]) < 1e-5f, "joint " + k + " did not follow the paused frame");
            }

            Assert.Equal(scene.Target(0, 0), solver.GetOutput(scene.Group, 0));

            // Resuming does not whip the chain: without the rigid follow the jump would be a 40 m/s kick.
            scene.Step(Frame60);
            Assert.True(scene.MaxSpeed() < 5f, "speed after resume " + scene.MaxSpeed());
        }

        [Fact]
        public void Output_RootSitsOnItsAnimatedPosition_AtAnyFrameRate()
        {
            foreach (float dt in new[] { 1f / 30f, 1f / 60f, 1f / 144f, 1f / 240f })
            {
                var solver = new ClothSolver();
                ChainScene scene = ChainScene.Straight(solver, new V3(0f, 1f, 0f), V3.Down, 4, 0.1f, BuiltInPresets.GetOrDefault("hair"));
                for (int f = 0; f < 120; f++)
                {
                    scene.Frame.position += new V3(2f * dt, 0f, 0f);
                    scene.Step(dt);
                    Assert.Equal(scene.Target(0, 0), solver.GetOutput(scene.Group, 0));
                    for (int k = 1; k < scene.Joints; k++)
                    {
                        float length = V3.Distance(solver.GetOutput(scene.Group, k), solver.GetOutput(scene.Group, k - 1));
                        Assert.InRange(length, 0.09f, 0.1001f); // interpolated between two exact states
                    }
                }
            }
        }

        [Fact]
        public void RemovingAGroup_LeavesOtherGroupsUntouched()
        {
            var withTwo = new ClothSolver();
            var alone = new ClothSolver();
            ClothParameters p = BuiltInPresets.GetOrDefault("cape");
            ChainScene first = ChainScene.Ring(withTwo, new V3(0f, 1f, 0f), 6, 4, 0.2f, 0.1f, 10f, ConnectionMode.Loop, p);
            ChainScene second = ChainScene.Ring(withTwo, new V3(2f, 1f, 0f), 5, 5, 0.2f, 0.1f, 15f, ConnectionMode.Open, p);
            ChainScene reference = ChainScene.Ring(alone, new V3(2f, 1f, 0f), 5, 5, 0.2f, 0.1f, 15f, ConnectionMode.Open, p);

            for (int f = 0; f < 240; f++)
            {
                if (f == 100)
                {
                    withTwo.RemoveGroup(first.Group);
                    Assert.False(first.Group.IsValid);
                }

                float t = f * Frame60;
                V3 offset = new V3(0.3f * (float)Math.Sin(3.0 * t), 0f, 0f);
                second.Frame.position = new V3(2f, 1f, 0f) + offset;
                reference.Frame.position = second.Frame.position;
                if (f < 100)
                {
                    first.Frame.position = new V3(0f, 1f, 0f) + offset;
                    first.Push();
                }

                second.Push();
                reference.Push();
                withTwo.Simulate(Frame60);
                alone.Simulate(Frame60);
            }

            for (int i = 0; i < second.ParticleCount; i++)
            {
                Assert.Equal(alone.GetPosition(reference.Group, i), withTwo.GetPosition(second.Group, i));
            }
        }

        [Fact]
        public void ExtremeInput_NeverProducesNaN()
        {
            var solver = new ClothSolver();
            var p = new ClothParameters(2f, 0f, 1f, 180f, 1f, 1f, 0.05f, 0.01f, 1f, 1f, 1f, 1f);
            ChainScene scene = ChainScene.Ring(solver, V3.Zero, 3, 6, 0.1f, 0.05f, 0f, ConnectionMode.Loop, p);
            ColliderHandle sphere = solver.AddCollider();
            solver.SetSphere(sphere, new V3(0f, -0.1f, 0f), 0.08f, 1u);
            solver.SetWind(scene.Group, new V3(500f, 0f, 0f), 1f, 50f);

            var random = new Random(7);
            for (int f = 0; f < 200; f++)
            {
                scene.Frame.position = new V3(Rand(random), Rand(random), Rand(random)) * 1.5f; // just under 2 m jumps
                scene.Frame.rotation = Q4.AngleAxis(Rand(random) * 80f, new V3(Rand(random), 1f, Rand(random)));
                float dt = f % 7 == 0 ? 0.5f : (f % 5 == 0 ? 0f : 1f / (20f + 200f * Math.Abs(Rand(random))));
                scene.Step(dt);
                Assert.True(scene.AllFinite(), "NaN at frame " + f);
            }

            // Bad input: a non-finite animated pose resets the group instead of poisoning it.
            solver.SetTarget(scene.Group, 3, new V3(float.NaN, 0f, 0f));
            solver.Simulate(Frame60);
            scene.Step(Frame60);
            Assert.True(scene.AllFinite());
        }

        internal static float Rand(Random random) => (float)(random.NextDouble() * 2.0 - 1.0);

        private static float[] RunScenario()
        {
            var solver = new ClothSolver();
            ChainScene skirt = ChainScene.Ring(solver, new V3(0f, 1f, 0f), 8, 5, 0.18f, 0.1f, 15f, ConnectionMode.Loop, BuiltInPresets.GetOrDefault("skirt"));
            ChainScene hair = ChainScene.Straight(solver, new V3(0f, 1.6f, 0f), new V3(0f, -1f, -0.3f), 5, 0.07f, BuiltInPresets.GetOrDefault("hair"));
            ColliderHandle leg = solver.AddCollider();
            ColliderHandle head = solver.AddCollider();
            ColliderHandle floor = solver.AddCollider();
            solver.SetPlane(floor, V3.Zero, V3.Up, 1u);
            solver.SetWind(hair.Group, new V3(2f, 0f, 1f), 0.6f, 0.8f);
            var random = new Random(99);
            for (int f = 0; f < 400; f++)
            {
                float t = f * Frame60;
                V3 body = new V3((float)Math.Sin(t) * 0.5f, 0f, t * 0.8f);
                skirt.Frame.position = body + new V3(0f, 1f, 0f);
                skirt.Frame.rotation = Q4.AngleAxis(20f * (float)Math.Sin(2.0 * t), V3.Up);
                hair.Frame.position = body + new V3(0f, 1.6f, 0f);
                hair.Frame.rotation = skirt.Frame.rotation;
                solver.SetCapsule(leg, body + new V3(0.1f, 0.9f, 0.2f * (float)Math.Sin(4.0 * t)), body + new V3(0.1f, 0.5f, 0.3f * (float)Math.Sin(4.0 * t)), 0.07f, 0.05f, 1u);
                solver.SetSphere(head, body + new V3(0f, 1.7f, 0f), 0.1f, 1u);
                skirt.Push();
                hair.Push();
                solver.Simulate(Frame60 * (0.8f + 0.4f * (float)random.NextDouble()));
            }

            var result = new float[solver.ParticleCount * 6];
            int n = 0;
            foreach (ChainScene scene in new[] { skirt, hair })
            {
                for (int i = 0; i < scene.ParticleCount; i++)
                {
                    V3 position = solver.GetPosition(scene.Group, i);
                    V3 output = solver.GetOutput(scene.Group, i);
                    result[n++] = position.x;
                    result[n++] = position.y;
                    result[n++] = position.z;
                    result[n++] = output.x;
                    result[n++] = output.y;
                    result[n++] = output.z;
                }
            }

            return result;
        }
    }
}
