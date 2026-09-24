using System;
using System.Collections.Generic;
using AutoRig.Cloth.Core;
using UnityEngine;

namespace AutoRig.Cloth
{
    /// <summary>
    /// Runs the cloth simulation of every enabled <see cref="ClothBoneGroup"/>, collider and wind
    /// zone. Created automatically in the DontDestroyOnLoad scene the first time a group, collider or
    /// wind zone is enabled in Play mode; there is no need to add it by hand.
    /// </summary>
    /// <remarks>
    /// Frame order: in <c>Update</c> every chain bone is restored to its rest local pose, so an
    /// Animator that does not animate the chain bones cannot build up feedback (one that does
    /// overwrites them anyway). The Animator then evaluates. In <c>LateUpdate</c>, which runs after
    /// other scripts' <c>LateUpdate</c> (execution order 32000), the animated pose is read, the
    /// solver advances by <c>Time.deltaTime</c> and bone rotations are written root to tip.
    /// Nothing is allocated per frame once groups are registered.
    /// </remarks>
    [DefaultExecutionOrder(ExecutionOrder)]
    [DisallowMultipleComponent]
    [AddComponentMenu("")]
    public sealed class AutoRigClothManager : MonoBehaviour
    {
        /// <summary>Script execution order of the manager: after almost everything else.</summary>
        public const int ExecutionOrder = 32000;

        /// <summary>Maximum number of distinct collider tags; further tags share the last bit.</summary>
        public const int MaxColliderTags = 32;

        private static AutoRigClothManager s_instance;
        private static bool s_quitting;

        private readonly ClothSolver _solver = new ClothSolver();
        private readonly List<ClothBoneGroup> _groups = new List<ClothBoneGroup>();
        private readonly List<ClothCollider> _colliders = new List<ClothCollider>();
        private readonly List<ClothWind> _winds = new List<ClothWind>();
        private readonly Dictionary<string, uint> _tagBits = new Dictionary<string, uint>(StringComparer.Ordinal);
        private bool _tagOverflowReported;

        /// <summary>The manager, or null when none exists (outside Play mode, before any group was enabled, or while quitting).</summary>
        public static AutoRigClothManager Existing => s_instance;

        /// <summary>The solver shared by all groups. Use it for inspection; groups and colliders manage their own entries.</summary>
        public ClothSolver Solver => _solver;

        /// <summary>Global solver settings: step rate (90 Hz), steps per frame (4), iterations (4) and gravity.</summary>
        public SolverSettings Settings
        {
            get => _solver.Settings;
            set => _solver.Settings = value;
        }

        /// <summary>Number of simulated groups.</summary>
        public int GroupCount => _groups.Count;

        /// <summary>Number of registered colliders.</summary>
        public int ColliderCount => _colliders.Count;

        /// <summary>Number of simulated joints over all groups.</summary>
        public int ParticleCount => _solver.ParticleCount;

        /// <summary>
        /// Returns the manager, creating it if needed. Returns null outside Play mode and while the
        /// application is quitting.
        /// </summary>
        public static AutoRigClothManager GetOrCreate()
        {
            if (s_instance != null)
            {
                return s_instance;
            }

            if (s_quitting || !Application.isPlaying)
            {
                return null;
            }

            var go = new GameObject("AutoRig Cloth Manager");
            DontDestroyOnLoad(go);
            s_instance = go.AddComponent<AutoRigClothManager>();
            return s_instance;
        }

        // Statics survive Play mode when domain reload is disabled; start every session clean.
        [RuntimeInitializeOnLoadMethod(RuntimeInitializeLoadType.SubsystemRegistration)]
        private static void ResetStatics()
        {
            s_instance = null;
            s_quitting = false;
        }

        private static void OnQuitting()
        {
            s_quitting = true;
        }

        private void Awake()
        {
            if (s_instance != null && s_instance != this)
            {
                Destroy(this);
                return;
            }

            s_instance = this;
            Application.quitting += OnQuitting;
        }

        private void OnDestroy()
        {
            if (s_instance != this)
            {
                return;
            }

            Application.quitting -= OnQuitting;
            for (int i = 0; i < _groups.Count; i++)
            {
                if (_groups[i] != null)
                {
                    _groups[i].OnManagerDestroyed();
                }
            }

            for (int i = 0; i < _colliders.Count; i++)
            {
                if (_colliders[i] != null)
                {
                    _colliders[i].OnManagerDestroyed();
                }
            }

            _groups.Clear();
            _colliders.Clear();
            _winds.Clear();
            s_instance = null;
        }

        internal GroupHandle Register(ClothBoneGroup group)
        {
            if (group.Handle != null && group.Handle.IsValid)
            {
                return group.Handle;
            }

            GroupHandle handle = _solver.AddGroup(group.ChainLengths, group.Connection);
            _groups.Add(group);
            return handle;
        }

        internal void Unregister(ClothBoneGroup group)
        {
            int index = _groups.IndexOf(group);
            if (index >= 0)
            {
                _groups.RemoveAt(index);
            }

            if (group.Handle != null && group.Handle.IsValid)
            {
                _solver.RemoveGroup(group.Handle);
            }
        }

        internal ColliderHandle Register(ClothCollider collider)
        {
            if (collider.Handle != null && collider.Handle.IsValid)
            {
                return collider.Handle;
            }

            _colliders.Add(collider);
            return _solver.AddCollider();
        }

        internal void Unregister(ClothCollider collider)
        {
            int index = _colliders.IndexOf(collider);
            if (index >= 0)
            {
                _colliders.RemoveAt(index);
            }

            if (collider.Handle != null && collider.Handle.IsValid)
            {
                _solver.RemoveCollider(collider.Handle);
            }
        }

        internal void Register(ClothWind wind)
        {
            if (!_winds.Contains(wind))
            {
                _winds.Add(wind);
            }
        }

        internal void Unregister(ClothWind wind)
        {
            _winds.Remove(wind);
        }

        /// <summary>
        /// Bit assigned to a collider tag. Tags get bits in order of first use; the 32nd and later
        /// distinct tags share the last bit.
        /// </summary>
        public uint GetTagBit(string colliderTag)
        {
            string key = colliderTag ?? string.Empty;
            if (_tagBits.TryGetValue(key, out uint bit))
            {
                return bit;
            }

            int count = _tagBits.Count;
            if (count >= MaxColliderTags && !_tagOverflowReported)
            {
                _tagOverflowReported = true;
                Debug.LogWarning("[AutoRig Cloth] More than " + MaxColliderTags + " collider tags are in use; the extra tags share one bit and may match groups they should not.");
            }

            bit = 1u << Math.Min(count, MaxColliderTags - 1);
            _tagBits.Add(key, bit);
            return bit;
        }

        /// <summary>Collider mask for a list of tags: all bits for an empty list, otherwise the union of the tags' bits.</summary>
        public uint GetTagMask(string[] colliderTags)
        {
            if (colliderTags == null || colliderTags.Length == 0)
            {
                return uint.MaxValue;
            }

            uint mask = 0u;
            for (int i = 0; i < colliderTags.Length; i++)
            {
                mask |= GetTagBit(colliderTags[i]);
            }

            return mask;
        }

        private void Update()
        {
            for (int i = 0; i < _groups.Count; i++)
            {
                ClothBoneGroup group = _groups[i];
                if (group != null)
                {
                    group.RestoreRestPose();
                }
            }
        }

        private void LateUpdate()
        {
            for (int i = _colliders.Count - 1; i >= 0; i--)
            {
                ClothCollider collider = _colliders[i];
                ColliderHandle colliderHandle = ReferenceEquals(collider, null) ? null : collider.Handle;
                if (collider == null || colliderHandle == null || !colliderHandle.IsValid)
                {
                    // Destroyed without OnDisable (should not happen, but never leave a stale entry).
                    _colliders.RemoveAt(i);
                    if (colliderHandle != null && colliderHandle.IsValid)
                    {
                        _solver.RemoveCollider(colliderHandle);
                    }

                    continue;
                }

                collider.PushTo(_solver, GetTagBit(collider.ColliderTag));
            }

            for (int i = _groups.Count - 1; i >= 0; i--)
            {
                ClothBoneGroup group = _groups[i];
                GroupHandle groupHandle = ReferenceEquals(group, null) ? null : group.Handle;
                if (group == null || groupHandle == null || !groupHandle.IsValid)
                {
                    _groups.RemoveAt(i);
                    if (groupHandle != null && groupHandle.IsValid)
                    {
                        _solver.RemoveGroup(groupHandle);
                    }

                    continue;
                }

                if (!group.PushTo(_solver))
                {
                    _groups.RemoveAt(i);
                    _solver.RemoveGroup(group.Handle);
                    group.OnBonesLost();
                    continue;
                }

                _solver.SetColliderMask(group.Handle, GetTagMask(group.ColliderTags));
                EvaluateWind(group.AttachPosition, out Vector3 wind, out float turbulence, out float frequency);
                _solver.SetWind(group.Handle, wind.ToV3(), turbulence, frequency);
            }

            // Time.deltaTime is 0 while Time.timeScale is 0: the solver then only keeps the cloth attached.
            _solver.Simulate(Time.deltaTime);

            for (int i = 0; i < _groups.Count; i++)
            {
                _groups[i].PullFrom(_solver);
            }
        }

        // Sums every wind zone at a point; turbulence and frequency are strength-weighted averages.
        private void EvaluateWind(Vector3 position, out Vector3 acceleration, out float turbulence, out float frequency)
        {
            acceleration = Vector3.zero;
            turbulence = 0f;
            frequency = 0f;
            float weight = 0f;
            for (int i = _winds.Count - 1; i >= 0; i--)
            {
                ClothWind zone = _winds[i];
                if (zone == null)
                {
                    _winds.RemoveAt(i);
                    continue;
                }

                if (!zone.Evaluate(position, out Vector3 a))
                {
                    continue;
                }

                float m = a.magnitude;
                acceleration += a;
                turbulence += zone.Turbulence * m;
                frequency += zone.Frequency * m;
                weight += m;
            }

            if (weight > 0f)
            {
                turbulence /= weight;
                frequency /= weight;
            }
        }
    }
}
