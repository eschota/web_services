using System;
using AutoRig.Cloth.Core;

namespace AutoRig.Cloth.Core.Tests
{
    /// <summary>
    /// A group of chains with an animated pose defined in the space of a movable attach frame,
    /// the way the Unity layer feeds the solver: every frame the frame and the animated joint
    /// positions are pushed, then <see cref="ClothSolver.Simulate"/> runs.
    /// </summary>
    internal sealed class ChainScene
    {
        public readonly ClothSolver Solver;
        public readonly GroupHandle Group;
        public readonly int Chains;
        public readonly int Joints;
        public RigidFrame Frame;
        private readonly V3[] _localPose;

        private ChainScene(ClothSolver solver, V3[] localPose, int chains, int joints, ConnectionMode connection, ClothParameters parameters, RigidFrame frame)
        {
            Solver = solver;
            Chains = chains;
            Joints = joints;
            _localPose = localPose;
            Frame = frame;
            var lengths = new int[chains];
            for (int c = 0; c < chains; c++)
            {
                lengths[c] = joints;
            }

            Group = solver.AddGroup(lengths, connection);
            solver.SetParameters(Group, parameters);
            Push();
        }

        public int ParticleCount => Chains * Joints;

        /// <summary>One chain whose animated pose is a straight line from the frame origin.</summary>
        public static ChainScene Straight(ClothSolver solver, V3 root, V3 direction, int joints, float segment, ClothParameters parameters)
        {
            var pose = new V3[joints];
            V3 d = direction.Normalized;
            for (int k = 0; k < joints; k++)
            {
                pose[k] = d * (segment * k);
            }

            return new ChainScene(solver, pose, 1, joints, ConnectionMode.None, parameters, new RigidFrame(root, Q4.Identity));
        }

        /// <summary>A ring of chains around the frame origin (a skirt), hanging down with a flare.</summary>
        public static ChainScene Ring(ClothSolver solver, V3 center, int chains, int joints, float ringRadius, float segment, float flareDegrees, ConnectionMode connection, ClothParameters parameters)
        {
            var pose = new V3[chains * joints];
            double flare = flareDegrees * Math.PI / 180.0;
            for (int c = 0; c < chains; c++)
            {
                double a = 2.0 * Math.PI * c / chains;
                var outward = new V3((float)Math.Cos(a), 0f, (float)Math.Sin(a));
                V3 root = outward * ringRadius;
                V3 dir = (outward * (float)Math.Sin(flare) + V3.Down * (float)Math.Cos(flare)).Normalized;
                for (int k = 0; k < joints; k++)
                {
                    pose[c * joints + k] = root + dir * (segment * k);
                }
            }

            return new ChainScene(solver, pose, chains, joints, connection, parameters, new RigidFrame(center, Q4.Identity));
        }

        /// <summary>Several independent straight chains side by side (for performance tests).</summary>
        public static ChainScene Comb(ClothSolver solver, V3 origin, int chains, int joints, float spacing, float segment, ClothParameters parameters)
        {
            var pose = new V3[chains * joints];
            for (int c = 0; c < chains; c++)
            {
                var root = new V3(spacing * c, 0f, 0f);
                for (int k = 0; k < joints; k++)
                {
                    pose[c * joints + k] = root + new V3(0f, 0f, segment * k);
                }
            }

            return new ChainScene(solver, pose, chains, joints, ConnectionMode.None, parameters, new RigidFrame(origin, Q4.Identity));
        }

        /// <summary>Pushes the attach frame and the animated pose (local pose mapped through the frame).</summary>
        public void Push()
        {
            Solver.SetGroupFrame(Group, Frame);
            for (int i = 0; i < _localPose.Length; i++)
            {
                Solver.SetTarget(Group, i, Frame.TransformPoint(_localPose[i]));
            }
        }

        public void Step(float deltaTime)
        {
            Push();
            Solver.Simulate(deltaTime);
        }

        public void Run(float seconds, float deltaTime)
        {
            int frames = (int)Math.Round(seconds / deltaTime);
            for (int f = 0; f < frames; f++)
            {
                Step(deltaTime);
            }
        }

        public V3 Position(int chain, int joint) => Solver.GetPosition(Group, chain * Joints + joint);

        public V3 Target(int chain, int joint) => Solver.GetTarget(Group, chain * Joints + joint);

        public V3 Velocity(int chain, int joint) => Solver.GetVelocity(Group, chain * Joints + joint);

        /// <summary>Largest |length − rest length| over all links along the chains.</summary>
        public float MaxLengthError()
        {
            float worst = 0f;
            for (int c = 0; c < Chains; c++)
            {
                for (int k = 1; k < Joints; k++)
                {
                    float length = V3.Distance(Position(c, k), Position(c, k - 1));
                    float rest = V3.Distance(Target(c, k), Target(c, k - 1));
                    worst = Math.Max(worst, Math.Abs(length - rest));
                }
            }

            return worst;
        }

        /// <summary>Largest speed of any free particle over the last step.</summary>
        public float MaxSpeed()
        {
            float worst = 0f;
            for (int c = 0; c < Chains; c++)
            {
                for (int k = 1; k < Joints; k++)
                {
                    worst = Math.Max(worst, Velocity(c, k).Length);
                }
            }

            return worst;
        }

        /// <summary>Kinetic energy of the free particles (unit mass each).</summary>
        public double KineticEnergy()
        {
            double e = 0.0;
            for (int c = 0; c < Chains; c++)
            {
                for (int k = 1; k < Joints; k++)
                {
                    e += 0.5 * Velocity(c, k).LengthSquared;
                }
            }

            return e;
        }

        public bool AllFinite()
        {
            for (int i = 0; i < ParticleCount; i++)
            {
                if (!Solver.GetPosition(Group, i).IsFinite || !Solver.GetOutput(Group, i).IsFinite)
                {
                    return false;
                }
            }

            return true;
        }

        /// <summary>Parameters with every effect switched off except what a test turns on.</summary>
        public static ClothParameters Plain(float gravity = 1f, float damping = 0.1f, float stiffness = 0f)
        {
            return new ClothParameters(gravity, damping, stiffness, 0f, 0f, 0.5f, 0f, 0f, 1f, 1f, 0f, 0f);
        }
    }
}
