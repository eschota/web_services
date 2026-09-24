using System;
using System.Collections.Generic;

namespace AutoRig.Cloth.Core
{
    /// <summary>
    /// Position-based Verlet solver for chains of bone joints (hair strands, skirt panels, capes,
    /// tails). It works purely with positions: the caller supplies the animated joint positions
    /// and the attach frame of each group every frame, calls <see cref="Simulate"/>, and turns the
    /// simulated positions returned by <see cref="GetOutput"/> back into bone rotations.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Data is stored as a struct of arrays. The particles of one group are contiguous, chain by
    /// chain, from root to tip, so a particle's parent always has a lower index and every pass
    /// runs root to tip. <see cref="Simulate"/> does not allocate.
    /// </para>
    /// <para>
    /// The first joint of every chain is kinematic: it follows its animated position. All other
    /// joints are free particles. Results are deterministic: the same sequence of calls with the
    /// same arguments produces bit-identical positions.
    /// </para>
    /// </remarks>
    public sealed partial class ClothSolver
    {
        private const float Epsilon = 1e-9f;

        /// <summary>
        /// Scale that maps the 0–1 <c>drag</c> parameter to a quadratic drag coefficient per metre.
        /// At drag 0.02 a joint moving at 5 m/s loses about 40 % of its speed per 1/10 s.
        /// </summary>
        public const float DragCoefficientScale = 20f;

        /// <summary>
        /// Share of each shape-restoration correction that does not become velocity. The remaining
        /// 20 % makes the chain spring back with a soft bounce; applying all of it as velocity would
        /// turn <c>stiffness</c> into a stiff, buzzing spring.
        /// </summary>
        public const float RestorationVelocityAttenuation = 0.8f;

        /// <summary>Default teleport distance in world units (about 3 × an adult's hips-to-head distance).</summary>
        public const float DefaultTeleportDistance = 2f;

        private SolverSettings _settings = SolverSettings.Default;

        // Particles.
        private int _particleCount;
        private V3[] _position;
        private V3[] _previous;
        private V3[] _stepStart;
        private V3[] _output;
        private V3[] _targetPrevious;
        private V3[] _target;
        private V3[] _stepTarget;
        private V3[] _animDirection;
        private float[] _inverseMass;
        private float[] _restLength;
        private float[] _depth;
        private int[] _parent;
        private int[] _chainRoot;

        // Links between neighbouring chains.
        private int _linkCount;
        private int[] _linkA;
        private int[] _linkB;
        private float[] _linkRest;

        // Groups.
        private int _groupCount;
        private GroupState[] _groups;
        private GroupHandle[] _groupHandles;

        // Colliders.
        private int _colliderCount;
        private ColliderState[] _colliders;
        private ColliderHandle[] _colliderHandles;
        private int[] _candidates;

        private float _accumulator;
        private double _time;

        /// <summary>Creates a solver with room for 256 particles and 16 colliders (it grows as needed).</summary>
        public ClothSolver()
            : this(256, 16)
        {
        }

        /// <summary>Creates a solver with the given initial capacities (it grows as needed).</summary>
        public ClothSolver(int particleCapacity, int colliderCapacity)
        {
            int pc = Math.Max(8, particleCapacity);
            _position = new V3[pc];
            _previous = new V3[pc];
            _stepStart = new V3[pc];
            _output = new V3[pc];
            _targetPrevious = new V3[pc];
            _target = new V3[pc];
            _stepTarget = new V3[pc];
            _animDirection = new V3[pc];
            _inverseMass = new float[pc];
            _restLength = new float[pc];
            _depth = new float[pc];
            _parent = new int[pc];
            _chainRoot = new int[pc];

            _linkA = new int[pc];
            _linkB = new int[pc];
            _linkRest = new float[pc];

            _groups = new GroupState[8];
            _groupHandles = new GroupHandle[8];

            int cc = Math.Max(4, colliderCapacity);
            _colliders = new ColliderState[cc];
            _colliderHandles = new ColliderHandle[cc];
            _candidates = new int[cc];
        }

        /// <summary>Global settings. Values are clamped to their valid ranges when assigned.</summary>
        public SolverSettings Settings
        {
            get => _settings;
            set => _settings = value.Sanitized();
        }

        /// <summary>Number of particles over all groups.</summary>
        public int ParticleCount => _particleCount;

        /// <summary>Number of groups.</summary>
        public int GroupCount => _groupCount;

        /// <summary>Number of colliders.</summary>
        public int ColliderCount => _colliderCount;

        /// <summary>Duration of one simulation step in seconds.</summary>
        public float StepDeltaTime => 1f / _settings.substepRate;

        /// <summary>Simulated time in seconds (sum of all steps taken).</summary>
        public double SimulationTime => _time;

        /// <summary>
        /// Adds a group of chains. Chain <c>c</c> gets <c>chainLengths[c]</c> particles; particle
        /// indices of the group run chain by chain from root to tip. Links between chains are
        /// created from <paramref name="connection"/> (joint <i>k</i> of neighbouring chains, up to
        /// the shortest chain's length; the kinematic roots are not linked).
        /// </summary>
        /// <param name="chainLengths">Joint count of every chain, root and end joint included; each at least 2.</param>
        /// <param name="connection">How neighbouring chains are linked.</param>
        /// <returns>A handle identifying the group.</returns>
        public GroupHandle AddGroup(IReadOnlyList<int> chainLengths, ConnectionMode connection)
        {
            if (chainLengths == null || chainLengths.Count == 0)
            {
                throw new ArgumentException("A group needs at least one chain.", nameof(chainLengths));
            }

            int total = 0;
            int shortest = int.MaxValue;
            for (int c = 0; c < chainLengths.Count; c++)
            {
                int length = chainLengths[c];
                if (length < 2)
                {
                    throw new ArgumentException("Every chain needs at least two joints (a root and a tip).", nameof(chainLengths));
                }

                total += length;
                shortest = Math.Min(shortest, length);
            }

            int chainCount = chainLengths.Count;
            int pairCount = 0;
            if (connection != ConnectionMode.None && chainCount >= 2)
            {
                pairCount = chainCount - 1;
                if (connection == ConnectionMode.Loop && chainCount >= 3)
                {
                    pairCount++;
                }
            }

            int linksToAdd = pairCount * (shortest - 1);
            EnsureParticleCapacity(_particleCount + total);
            EnsureLinkCapacity(_linkCount + linksToAdd);
            EnsureGroupCapacity(_groupCount + 1);

            int start = _particleCount;
            int p = start;
            for (int c = 0; c < chainCount; c++)
            {
                int length = chainLengths[c];
                int root = p;
                for (int k = 0; k < length; k++, p++)
                {
                    _parent[p] = k == 0 ? -1 : p - 1;
                    _chainRoot[p] = root;
                    _inverseMass[p] = k == 0 ? 0f : 1f;
                    _depth[p] = k / (float)(length - 1);
                    _restLength[p] = 0f;
                    _position[p] = V3.Zero;
                    _previous[p] = V3.Zero;
                    _stepStart[p] = V3.Zero;
                    _output[p] = V3.Zero;
                    _target[p] = V3.Zero;
                    _targetPrevious[p] = V3.Zero;
                    _stepTarget[p] = V3.Zero;
                    _animDirection[p] = V3.Down;
                }
            }

            _particleCount += total;

            int linkStart = _linkCount;
            for (int pair = 0; pair < pairCount; pair++)
            {
                int chainA = pair;
                int chainB = (pair + 1) % chainCount;
                int rootA = ChainStart(start, chainLengths, chainA);
                int rootB = ChainStart(start, chainLengths, chainB);
                for (int k = 1; k < shortest; k++)
                {
                    _linkA[_linkCount] = rootA + k;
                    _linkB[_linkCount] = rootB + k;
                    _linkRest[_linkCount] = 0f;
                    _linkCount++;
                }
            }

            int index = _groupCount++;
            _groups[index] = new GroupState
            {
                particleStart = start,
                particleCount = total,
                linkStart = linkStart,
                linkCount = _linkCount - linkStart,
                chainCount = chainCount,
                connection = connection,
                parameters = ClothParameters.Default,
                colliderMask = uint.MaxValue,
                lengthScale = 1f,
                teleportDistance = DefaultTeleportDistance,
                framePrevious = RigidFrame.Identity,
                frameCurrent = RigidFrame.Identity,
                frameStep = RigidFrame.Identity,
            };

            var handle = new GroupHandle(this, index);
            _groupHandles[index] = handle;
            return handle;
        }

        /// <summary>
        /// Removes a group. Other groups keep their handles and their simulation state.
        /// </summary>
        public void RemoveGroup(GroupHandle handle)
        {
            int gi = CheckGroup(handle);
            GroupState g = _groups[gi];

            int s = g.particleStart;
            int c = g.particleCount;
            int tail = _particleCount - (s + c);
            if (tail > 0)
            {
                ShiftLeft(_position, s + c, c, tail);
                ShiftLeft(_previous, s + c, c, tail);
                ShiftLeft(_stepStart, s + c, c, tail);
                ShiftLeft(_output, s + c, c, tail);
                ShiftLeft(_targetPrevious, s + c, c, tail);
                ShiftLeft(_target, s + c, c, tail);
                ShiftLeft(_stepTarget, s + c, c, tail);
                ShiftLeft(_animDirection, s + c, c, tail);
                ShiftLeft(_inverseMass, s + c, c, tail);
                ShiftLeft(_restLength, s + c, c, tail);
                ShiftLeft(_depth, s + c, c, tail);
                ShiftLeft(_parent, s + c, c, tail);
                ShiftLeft(_chainRoot, s + c, c, tail);
                for (int i = s; i < s + tail; i++)
                {
                    if (_parent[i] >= 0)
                    {
                        _parent[i] -= c;
                    }

                    _chainRoot[i] -= c;
                }
            }

            _particleCount -= c;

            int ls = g.linkStart;
            int lc = g.linkCount;
            int linkTail = _linkCount - (ls + lc);
            if (linkTail > 0)
            {
                ShiftLeft(_linkA, ls + lc, lc, linkTail);
                ShiftLeft(_linkB, ls + lc, lc, linkTail);
                ShiftLeft(_linkRest, ls + lc, lc, linkTail);
                for (int l = ls; l < ls + linkTail; l++)
                {
                    _linkA[l] -= c;
                    _linkB[l] -= c;
                }
            }

            _linkCount -= lc;

            for (int k = gi + 1; k < _groupCount; k++)
            {
                GroupState moved = _groups[k];
                moved.particleStart -= c;
                moved.linkStart -= lc;
                _groups[k - 1] = moved;
                _groupHandles[k - 1] = _groupHandles[k];
                _groupHandles[k - 1].index = k - 1;
            }

            _groupCount--;
            _groups[_groupCount] = default;
            _groupHandles[_groupCount] = null;
            handle.index = -1;
            handle.owner = null;
        }

        /// <summary>Number of particles (joints) in a group.</summary>
        public int GetParticleCount(GroupHandle handle) => _groups[CheckGroup(handle)].particleCount;

        /// <summary>Number of chains in a group.</summary>
        public int GetChainCount(GroupHandle handle) => _groups[CheckGroup(handle)].chainCount;

        /// <summary>Number of links between neighbouring chains in a group.</summary>
        public int GetLinkCount(GroupHandle handle) => _groups[CheckGroup(handle)].linkCount;

        /// <summary>The two particles (group-local indices) joined by link <paramref name="link"/> of a group.</summary>
        public void GetLink(GroupHandle handle, int link, out int particleA, out int particleB)
        {
            GroupState g = _groups[CheckGroup(handle)];
            if ((uint)link >= (uint)g.linkCount)
            {
                throw new ArgumentOutOfRangeException(nameof(link));
            }

            particleA = _linkA[g.linkStart + link] - g.particleStart;
            particleB = _linkB[g.linkStart + link] - g.particleStart;
        }

        /// <summary>Group-local index of a particle's parent, or -1 for a chain root.</summary>
        public int GetParent(GroupHandle handle, int particle)
        {
            int i = ParticleIndex(handle, particle, out int start);
            int p = _parent[i];
            return p < 0 ? -1 : p - start;
        }

        /// <summary>
        /// Sets this frame's attach frame of a group: the animated bone its chains hang from. Used
        /// for inertia and teleport detection.
        /// </summary>
        public void SetGroupFrame(GroupHandle handle, RigidFrame frame)
        {
            int gi = CheckGroup(handle);
            if (!frame.position.IsFinite || !frame.rotation.IsFinite)
            {
                return;
            }

            frame.rotation = Q4.Normalize(frame.rotation);
            _groups[gi].frameCurrent = frame;
        }

        /// <summary>Sets this frame's animated position of a particle (group-local index).</summary>
        public void SetTarget(GroupHandle handle, int particle, V3 position)
        {
            _target[ParticleIndex(handle, particle, out _)] = position;
        }

        /// <summary>The animated position of a particle as last set by <see cref="SetTarget"/>.</summary>
        public V3 GetTarget(GroupHandle handle, int particle) => _target[ParticleIndex(handle, particle, out _)];

        /// <summary>Sets the simulation parameters of a group. Values are clamped to their valid ranges.</summary>
        public void SetParameters(GroupHandle handle, in ClothParameters parameters)
        {
            _groups[CheckGroup(handle)].parameters = parameters.Sanitized();
        }

        /// <summary>The (sanitized) simulation parameters of a group.</summary>
        public ClothParameters GetParameters(GroupHandle handle) => _groups[CheckGroup(handle)].parameters;

        /// <summary>
        /// Sets which colliders affect a group: a collider is used when
        /// <c>(colliderTags &amp; mask) != 0</c>. The default is all bits set.
        /// </summary>
        public void SetColliderMask(GroupHandle handle, uint mask)
        {
            _groups[CheckGroup(handle)].colliderMask = mask;
        }

        /// <summary>
        /// Sets the factor applied to the group's length parameters (collision radii); the drag
        /// coefficient is divided by it so the motion looks the same at any scale. Default 1.
        /// </summary>
        public void SetLengthScale(GroupHandle handle, float scale)
        {
            _groups[CheckGroup(handle)].lengthScale = scale > 0f && !float.IsInfinity(scale) ? scale : 1f;
        }

        /// <summary>
        /// Sets the teleport distance in world units: when the attach frame moves farther than this
        /// (or rotates more than 90°) between two frames, the group snaps to the animated pose with
        /// zero velocity. 0 or less disables the distance test (the rotation test stays).
        /// </summary>
        public void SetTeleportDistance(GroupHandle handle, float distance)
        {
            _groups[CheckGroup(handle)].teleportDistance = float.IsNaN(distance) ? DefaultTeleportDistance : distance;
        }

        /// <summary>
        /// Sets the wind acting on a group this frame.
        /// </summary>
        /// <param name="handle">The group.</param>
        /// <param name="acceleration">Mean wind acceleration in world units per second squared (scaled by the group's <c>wind</c> parameter).</param>
        /// <param name="turbulence">Relative strength of the noise, 0–1.</param>
        /// <param name="frequency">How fast the noise changes, in cycles per second.</param>
        public void SetWind(GroupHandle handle, V3 acceleration, float turbulence, float frequency)
        {
            int gi = CheckGroup(handle);
            _groups[gi].windAcceleration = acceleration.IsFinite ? acceleration : V3.Zero;
            _groups[gi].windTurbulence = turbulence > 0f ? (turbulence < 1f ? turbulence : 1f) : 0f;
            _groups[gi].windFrequency = frequency > 0f && !float.IsInfinity(frequency) ? frequency : 0f;
        }

        /// <summary>
        /// Requests a reset: on the next <see cref="Simulate"/> the group snaps to its animated pose
        /// with zero velocity.
        /// </summary>
        public void ResetGroup(GroupHandle handle)
        {
            _groups[CheckGroup(handle)].resetRequested = true;
        }

        /// <summary>True once the group has been simulated (reset to its animated pose) at least once.</summary>
        public bool IsInitialized(GroupHandle handle) => _groups[CheckGroup(handle)].initialized;

        /// <summary>
        /// Position to render a particle at after the last <see cref="Simulate"/>: the simulated state
        /// interpolated between the last two steps, translated so that the chain root sits exactly
        /// at its current animated position.
        /// </summary>
        public V3 GetOutput(GroupHandle handle, int particle) => _output[ParticleIndex(handle, particle, out _)];

        /// <summary>Raw simulated position of a particle after the last step.</summary>
        public V3 GetPosition(GroupHandle handle, int particle) => _position[ParticleIndex(handle, particle, out _)];

        /// <summary>Velocity of a particle over the last step, in world units per second.</summary>
        public V3 GetVelocity(GroupHandle handle, int particle)
        {
            int i = ParticleIndex(handle, particle, out _);
            return (_position[i] - _previous[i]) * _settings.substepRate;
        }

        /// <summary>World collision radius of a particle with the group's current parameters and length scale.</summary>
        public float GetParticleRadius(GroupHandle handle, int particle)
        {
            int gi = CheckGroup(handle);
            int i = ParticleIndex(handle, particle, out _);
            GroupState g = _groups[gi];
            float r0 = g.parameters.radius;
            float r1 = g.parameters.EffectiveRadiusTip;
            return (r0 + (r1 - r0) * _depth[i]) * g.lengthScale;
        }

        /// <summary>Adds a collider. It has no effect until one of the <c>Set…</c> methods gives it a shape.</summary>
        public ColliderHandle AddCollider()
        {
            if (_colliderCount == _colliders.Length)
            {
                int capacity = _colliders.Length * 2;
                Array.Resize(ref _colliders, capacity);
                Array.Resize(ref _colliderHandles, capacity);
                Array.Resize(ref _candidates, capacity);
            }

            int index = _colliderCount++;
            _colliders[index] = new ColliderState { enabled = true };
            var handle = new ColliderHandle(this, index);
            _colliderHandles[index] = handle;
            return handle;
        }

        /// <summary>Removes a collider. Other colliders keep their handles.</summary>
        public void RemoveCollider(ColliderHandle handle)
        {
            int i = CheckCollider(handle);
            int last = _colliderCount - 1;
            if (i != last)
            {
                _colliders[i] = _colliders[last];
                _colliderHandles[i] = _colliderHandles[last];
                _colliderHandles[i].index = i;
            }

            _colliders[last] = default;
            _colliderHandles[last] = null;
            _colliderCount--;
            handle.index = -1;
            handle.owner = null;
        }

        /// <summary>Makes a collider a sphere for this frame.</summary>
        /// <param name="handle">The collider.</param>
        /// <param name="center">World centre.</param>
        /// <param name="radius">World radius.</param>
        /// <param name="tags">Tag bits matched against each group's collider mask.</param>
        public void SetSphere(ColliderHandle handle, V3 center, float radius, uint tags)
        {
            SetShape(CheckCollider(handle), ColliderShape.Sphere, center, center, radius, radius, tags);
        }

        /// <summary>Makes a collider a (possibly tapered) capsule for this frame.</summary>
        /// <param name="handle">The collider.</param>
        /// <param name="a">World position of the first end.</param>
        /// <param name="b">World position of the second end.</param>
        /// <param name="radiusA">World radius at <paramref name="a"/>.</param>
        /// <param name="radiusB">World radius at <paramref name="b"/>.</param>
        /// <param name="tags">Tag bits matched against each group's collider mask.</param>
        public void SetCapsule(ColliderHandle handle, V3 a, V3 b, float radiusA, float radiusB, uint tags)
        {
            SetShape(CheckCollider(handle), ColliderShape.Capsule, a, b, radiusA, radiusB, tags);
        }

        /// <summary>Makes a collider an infinite plane for this frame; particles stay on the side <paramref name="normal"/> points to.</summary>
        /// <param name="handle">The collider.</param>
        /// <param name="point">Any world point on the plane.</param>
        /// <param name="normal">World normal (normalized internally).</param>
        /// <param name="tags">Tag bits matched against each group's collider mask.</param>
        public void SetPlane(ColliderHandle handle, V3 point, V3 normal, uint tags)
        {
            SetShape(CheckCollider(handle), ColliderShape.Plane, point, V3.NormalizeOr(normal, V3.Up), 0f, 0f, tags);
        }

        /// <summary>Enables or disables a collider without removing it.</summary>
        public void SetColliderEnabled(ColliderHandle handle, bool enabled)
        {
            int i = CheckCollider(handle);
            if (enabled && !_colliders[i].enabled)
            {
                _colliders[i].hasPrevious = false; // do not sweep from where it was when disabled
            }

            _colliders[i].enabled = enabled;
        }

        private void SetShape(int i, ColliderShape shape, V3 a, V3 b, float ra, float rb, uint tags)
        {
            ref ColliderState c = ref _colliders[i];
            if (!a.IsFinite || !b.IsFinite || float.IsNaN(ra) || float.IsNaN(rb))
            {
                c.valid = false;
                return;
            }

            if (!c.valid || c.shape != shape)
            {
                c.hasPrevious = false;
            }

            c.shape = shape;
            c.a = a;
            c.b = b;
            c.ra = ra > 0f ? ra : 0f;
            c.rb = rb > 0f ? rb : 0f;
            c.tags = tags;
            c.valid = true;
        }

        private int CheckGroup(GroupHandle handle)
        {
            if (handle == null || handle.owner != this || handle.index < 0)
            {
                throw new ArgumentException("The group handle is not valid for this solver.", nameof(handle));
            }

            return handle.index;
        }

        private int CheckCollider(ColliderHandle handle)
        {
            if (handle == null || handle.owner != this || handle.index < 0)
            {
                throw new ArgumentException("The collider handle is not valid for this solver.", nameof(handle));
            }

            return handle.index;
        }

        private int ParticleIndex(GroupHandle handle, int particle, out int start)
        {
            GroupState g = _groups[CheckGroup(handle)];
            if ((uint)particle >= (uint)g.particleCount)
            {
                throw new ArgumentOutOfRangeException(nameof(particle));
            }

            start = g.particleStart;
            return start + particle;
        }

        private static int ChainStart(int groupStart, IReadOnlyList<int> chainLengths, int chain)
        {
            int start = groupStart;
            for (int c = 0; c < chain; c++)
            {
                start += chainLengths[c];
            }

            return start;
        }

        private static void ShiftLeft<T>(T[] array, int from, int by, int count)
        {
            Array.Copy(array, from, array, from - by, count);
        }

        private void EnsureParticleCapacity(int required)
        {
            if (required <= _position.Length)
            {
                return;
            }

            int capacity = Math.Max(required, _position.Length * 2);
            Array.Resize(ref _position, capacity);
            Array.Resize(ref _previous, capacity);
            Array.Resize(ref _stepStart, capacity);
            Array.Resize(ref _output, capacity);
            Array.Resize(ref _targetPrevious, capacity);
            Array.Resize(ref _target, capacity);
            Array.Resize(ref _stepTarget, capacity);
            Array.Resize(ref _animDirection, capacity);
            Array.Resize(ref _inverseMass, capacity);
            Array.Resize(ref _restLength, capacity);
            Array.Resize(ref _depth, capacity);
            Array.Resize(ref _parent, capacity);
            Array.Resize(ref _chainRoot, capacity);
        }

        private void EnsureLinkCapacity(int required)
        {
            if (required <= _linkA.Length)
            {
                return;
            }

            int capacity = Math.Max(required, _linkA.Length * 2);
            Array.Resize(ref _linkA, capacity);
            Array.Resize(ref _linkB, capacity);
            Array.Resize(ref _linkRest, capacity);
        }

        private void EnsureGroupCapacity(int required)
        {
            if (required <= _groups.Length)
            {
                return;
            }

            int capacity = Math.Max(required, _groups.Length * 2);
            Array.Resize(ref _groups, capacity);
            Array.Resize(ref _groupHandles, capacity);
        }

        private struct GroupState
        {
            public int particleStart;
            public int particleCount;
            public int linkStart;
            public int linkCount;
            public int chainCount;
            public ConnectionMode connection;
            public ClothParameters parameters;
            public uint colliderMask;
            public float lengthScale;
            public float teleportDistance;
            public V3 windAcceleration;
            public float windTurbulence;
            public float windFrequency;

            public RigidFrame framePrevious;
            public RigidFrame frameCurrent;
            public RigidFrame frameStep;
            public bool initialized;
            public bool resetRequested;

            // Derived once per Simulate call.
            public float dampingKeep;
            public float stiffnessPerIteration;
            public float connectionPerIteration;
            public bool angleLimited;
            public float cosLimit;
            public float sinLimit;
            public float stretchMin;
            public float stretchMax;
            public float dragCoefficient;
            public float radiusRoot;
            public float radiusTip;
            public float reach;
            public float translateShare;
            public float rotateShare;
            public V3 gravity;
        }

        private struct ColliderState
        {
            public ColliderShape shape;
            public uint tags;
            public bool enabled;
            public bool valid;
            public bool hasPrevious;
            public V3 a;
            public V3 b;
            public float ra;
            public float rb;
            public V3 aPrevious;
            public V3 bPrevious;
            public float raPrevious;
            public float rbPrevious;
            public V3 aStep;
            public V3 bStep;
            public float raStep;
            public float rbStep;
            public V3 boundsMin;
            public V3 boundsMax;
        }
    }
}
