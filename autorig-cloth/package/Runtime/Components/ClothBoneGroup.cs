using System;
using System.Collections.Generic;
using AutoRig.Cloth.Core;
using UnityEngine;

namespace AutoRig.Cloth
{
    /// <summary>Where a <see cref="ClothBoneGroup"/> takes its simulation parameters from.</summary>
    public enum ClothPresetSource
    {
        /// <summary>A built-in preset, by name (<c>hair</c>, <c>skirt</c>, …).</summary>
        BuiltIn = 0,

        /// <summary>A <see cref="ClothPreset"/> asset.</summary>
        Asset = 1,

        /// <summary>The group's own settings.</summary>
        Custom = 2,
    }

    /// <summary>One chain of bones, from the root (attached) to the end joint (tip).</summary>
    [Serializable]
    public sealed class ClothChain
    {
        /// <summary>Bones from root to end joint. Each bone must be a descendant of the previous one.</summary>
        [Tooltip("Bones from the root to the end joint. The root keeps its animated position; the end joint's rotation is never written.")]
        public Transform[] bones = new Transform[0];

        /// <summary>Creates an empty chain.</summary>
        public ClothChain()
        {
        }

        /// <summary>Creates a chain from bones ordered root to tip.</summary>
        public ClothChain(Transform[] bones)
        {
            this.bones = bones ?? new Transform[0];
        }
    }

    /// <summary>
    /// Simulates a set of bone chains that move together: hair strands, a skirt, a cape, a tail.
    /// Every chain hangs from the animated pose; the first bone of a chain keeps its animated
    /// position and gets its rotation simulated, later joints are free particles, and the last bone
    /// (the end joint) is simulated but never rotated. The simulation runs in Play mode, driven by
    /// <see cref="AutoRigClothManager"/>.
    /// </summary>
    [AddComponentMenu("AutoRig Cloth/Cloth Bone Group")]
    public sealed class ClothBoneGroup : MonoBehaviour
    {
        [SerializeField]
        [Tooltip("The animated bone the chains hang from (for example Head for hair, Hips for a skirt). Its motion drives inertia and teleport detection. Empty = the parent of the first chain root.")]
        private Transform attachBone;

        [SerializeField]
        [Tooltip("Bone chains, root to end joint. For Open and Loop groups list them in order around the attach bone.")]
        private List<ClothChain> chains = new List<ClothChain>();

        [SerializeField]
        [Tooltip("None: independent chains (hair). Open: joint k of each chain is linked to joint k of the next (a cape). Loop: Open plus last-to-first (a skirt).")]
        private ConnectionMode connection = ConnectionMode.None;

        [SerializeField]
        [Tooltip("Where the simulation parameters come from.")]
        private ClothPresetSource presetSource = ClothPresetSource.BuiltIn;

        [SerializeField]
        [Tooltip("Built-in preset name: hair, hair_stiff, skirt, cape, ribbon, tail or accessory.")]
        private string builtInPreset = BuiltInPresets.Hair;

        [SerializeField]
        [Tooltip("Preset asset shared by several groups.")]
        private ClothPreset presetAsset;

        [SerializeField]
        [Tooltip("This group's own parameters.")]
        private ClothSettings customSettings = new ClothSettings();

        [SerializeField]
        [Tooltip("Only colliders with one of these tags affect this group. Empty = every collider.")]
        private string[] colliderTags = new string[0];

        [SerializeField]
        [Min(0.0001f)]
        [Tooltip("Multiplies the preset radii. Radii are measured in this object's space, so they also follow its scale. The manifest loader sets this from the manifest's calibration.")]
        private float radiusScale = 1f;

        [SerializeField]
        [Min(0f)]
        [Tooltip("If the attach bone moves farther than this in one frame (or turns more than 90°), the cloth snaps to the animated pose instead of flying after it. In this object's units; 0 disables the distance test.")]
        private float teleportDistance = ClothSolver.DefaultTeleportDistance;

        [SerializeField]
        [Range(0f, 1f)]
        [Tooltip("Blend between the animated pose (0) and the simulated pose (1).")]
        private float blendWeight = 1f;

        [SerializeField]
        [Tooltip("Root bones for 'Build Chains From Roots': each chain follows single-child descendants from its root.")]
        private List<Transform> chainRoots = new List<Transform>();

        [SerializeField]
        [Tooltip("When building chains from roots, add an end joint to chains whose last bone has no child, so that bone's rotation can be simulated.")]
        private bool createEndJoints = true;

        [SerializeField]
        [HideInInspector]
        private string generatedBy = string.Empty;

        [SerializeField]
        [HideInInspector]
        private bool ownsGameObject;

        [NonSerialized] private GroupHandle _handle;
        [NonSerialized] private Transform[] _bones;
        [NonSerialized] private int[] _chainLengths;
        [NonSerialized] private bool[] _directChild;
        [NonSerialized] private Vector3[] _childOffset;
        [NonSerialized] private Quaternion[] _restRotation;
        [NonSerialized] private Vector3[] _restPosition;
        [NonSerialized] private bool _presetCached;
        [NonSerialized] private string _cachedPresetName;
        [NonSerialized] private ClothParameters _cachedPreset;

        /// <summary>The animated bone the chains hang from; null = the parent of the first chain root.</summary>
        public Transform AttachBone
        {
            get => attachBone;
            set => attachBone = value;
        }

        /// <summary>Number of chains (including invalid ones that will be skipped).</summary>
        public int ChainCount => chains.Count;

        /// <summary>How neighbouring chains are linked. Changing it while simulating rebuilds the group.</summary>
        public ConnectionMode Connection
        {
            get => connection;
            set
            {
                if (connection != value)
                {
                    connection = value;
                    Rebuild();
                }
            }
        }

        /// <summary>Where the parameters come from.</summary>
        public ClothPresetSource PresetSource
        {
            get => presetSource;
            set => presetSource = value;
        }

        /// <summary>Built-in preset name used when <see cref="PresetSource"/> is <see cref="ClothPresetSource.BuiltIn"/>.</summary>
        public string BuiltInPreset
        {
            get => builtInPreset;
            set => builtInPreset = value;
        }

        /// <summary>Preset asset used when <see cref="PresetSource"/> is <see cref="ClothPresetSource.Asset"/>.</summary>
        public ClothPreset PresetAsset
        {
            get => presetAsset;
            set => presetAsset = value;
        }

        /// <summary>Settings used when <see cref="PresetSource"/> is <see cref="ClothPresetSource.Custom"/>.</summary>
        public ClothSettings CustomSettings => customSettings;

        /// <summary>Tags of the colliders that affect this group; empty = all colliders.</summary>
        public string[] ColliderTags
        {
            get => colliderTags;
            set => colliderTags = value ?? new string[0];
        }

        /// <summary>Multiplier of the preset radii (in this object's space).</summary>
        public float RadiusScale
        {
            get => radiusScale;
            set => radiusScale = Mathf.Max(0.0001f, value);
        }

        /// <summary>Teleport distance in this object's units.</summary>
        public float TeleportDistance
        {
            get => teleportDistance;
            set => teleportDistance = Mathf.Max(0f, value);
        }

        /// <summary>Blend between animated (0) and simulated (1) pose.</summary>
        public float BlendWeight
        {
            get => blendWeight;
            set => blendWeight = Mathf.Clamp01(value);
        }

        /// <summary>Root bones used by <see cref="BuildChainsFromRoots()"/>.</summary>
        public List<Transform> ChainRoots => chainRoots;

        /// <summary>Whether <see cref="BuildChainsFromRoots()"/> adds missing end joints.</summary>
        public bool CreateEndJoints
        {
            get => createEndJoints;
            set => createEndJoints = value;
        }

        /// <summary>World size of one length unit of the parameters: radius scale × this object's largest lossy scale.</summary>
        public float LengthScale => radiusScale * UnityConversions.MaxAbsScale(transform.lossyScale);

        /// <summary>True while the group is registered with the simulation.</summary>
        public bool IsSimulating => _handle != null && _handle.IsValid;

        /// <summary>Number of simulated joints (0 when not simulating).</summary>
        public int ParticleCount => IsSimulating ? _bones.Length : 0;

        /// <summary>Identifier of the setup tool that created this component, or empty for user-made components.</summary>
        public string GeneratedBy => generatedBy;

        internal GroupHandle Handle => _handle;

        internal int[] ChainLengths => _chainLengths;

        internal bool OwnsGameObject => ownsGameObject;

        /// <summary>Returns chain <paramref name="index"/>.</summary>
        public ClothChain GetChain(int index) => chains[index];

        /// <summary>Replaces the chains (each ordered root to end joint) and rebuilds the group if it is simulating.</summary>
        public void SetChains(IEnumerable<Transform[]> boneChains)
        {
            chains.Clear();
            if (boneChains != null)
            {
                foreach (Transform[] bones in boneChains)
                {
                    chains.Add(new ClothChain(bones));
                }
            }

            Rebuild();
        }

        /// <summary>The parameters currently in effect (preset asset, built-in preset or custom settings).</summary>
        public ClothParameters GetParameters()
        {
            switch (presetSource)
            {
                case ClothPresetSource.Asset:
                    return presetAsset != null ? presetAsset.ToParameters() : ClothParameters.Default;
                case ClothPresetSource.Custom:
                    return customSettings != null ? customSettings.ToParameters() : ClothParameters.Default;
                default:
                    if (!_presetCached || !ReferenceEquals(_cachedPresetName, builtInPreset))
                    {
                        _cachedPreset = BuiltInPresets.GetOrDefault(builtInPreset);
                        _cachedPresetName = builtInPreset;
                        _presetCached = true;
                    }

                    return _cachedPreset;
            }
        }

        /// <summary>Snaps the cloth to the animated pose with zero velocity on the next frame.</summary>
        public void ResetSimulation()
        {
            AutoRigClothManager manager = AutoRigClothManager.Existing;
            if (IsSimulating && manager != null)
            {
                manager.Solver.ResetGroup(_handle);
            }
        }

        /// <summary>
        /// Re-reads the chains and restarts the simulation of this group. Call after changing
        /// chains or bones at runtime. Does nothing outside Play mode or while disabled.
        /// </summary>
        public void Rebuild()
        {
            if (!Application.isPlaying || !isActiveAndEnabled)
            {
                return;
            }

            Deactivate();
            Activate();
        }

        /// <summary>
        /// Rebuilds <see cref="ChainRoots"/> into chains (see <see cref="BuildChainsFromRoots(IList{Transform}, bool, IClothSetupHandler)"/>).
        /// </summary>
        public int BuildChainsFromRoots() => BuildChainsFromRoots(chainRoots, createEndJoints, null);

        /// <summary>
        /// Replaces the chains with one chain per root: the root followed by its single-child
        /// descendants. When <paramref name="createMissingEndJoints"/> is set and a chain's last
        /// bone has no child (and is not already an <c>…_end</c> joint), an end joint is created at
        /// the extrapolated tip so the last real bone's rotation can be simulated.
        /// </summary>
        /// <returns>Number of chains built (chains need at least two joints).</returns>
        public int BuildChainsFromRoots(IList<Transform> roots, bool createMissingEndJoints, IClothSetupHandler handler)
        {
            IClothSetupHandler h = handler ?? RuntimeClothSetupHandler.Instance;
            h.RecordObject(this);
            chains.Clear();
            if (roots != null)
            {
                for (int r = 0; r < roots.Count; r++)
                {
                    if (roots[r] == null)
                    {
                        continue;
                    }

                    List<Transform> chain = ChainBuilder.FollowSingleChildren(roots[r]);
                    Transform last = chain[chain.Count - 1];
                    if (createMissingEndJoints && ChainBuilder.NeedsEndJoint(last))
                    {
                        chain.Add(ChainBuilder.CreateEndJoint(last, chain.Count >= 2 ? chain[chain.Count - 2] : null, h));
                    }

                    if (chain.Count >= 2)
                    {
                        chains.Add(new ClothChain(chain.ToArray()));
                    }
                }
            }

            Rebuild();
            return chains.Count;
        }

        /// <summary>Appends a description of every configuration problem to <paramref name="problems"/>.</summary>
        public void CollectProblems(List<string> problems)
        {
            if (problems == null)
            {
                return;
            }

            if (chains.Count == 0)
            {
                problems.Add("No chains. Add chains or build them from root bones.");
            }

            var seen = new HashSet<Transform>();
            for (int c = 0; c < chains.Count; c++)
            {
                string reason = CheckChain(chains[c], seen);
                if (reason != null)
                {
                    problems.Add("Chain " + c + " is skipped: " + reason);
                }
            }

            Transform attach = attachBone;
            if (attach == null)
            {
                problems.Add("No attach bone: the parent of the first chain root is used.");
            }
            else
            {
                for (int c = 0; c < chains.Count; c++)
                {
                    Transform[] bones = chains[c] != null ? chains[c].bones : null;
                    if (bones != null && bones.Length > 0 && bones[0] != null && !bones[0].IsChildOf(attach))
                    {
                        problems.Add("Chain " + c + " does not hang from the attach bone '" + attach.name + "'.");
                    }
                }
            }

            if (presetSource == ClothPresetSource.Asset && presetAsset == null)
            {
                problems.Add("No preset asset assigned: default parameters are used.");
            }

            if (presetSource == ClothPresetSource.BuiltIn && !BuiltInPresets.IsBuiltIn(builtInPreset))
            {
                problems.Add("'" + builtInPreset + "' is not a built-in preset: default parameters are used.");
            }
        }

        /// <summary>
        /// Position and collision radius of simulated joint <paramref name="index"/> (flattened over
        /// all valid chains, root to tip). Only available while simulating.
        /// </summary>
        public bool TryGetParticle(int index, out Vector3 position, out float radius)
        {
            AutoRigClothManager manager = AutoRigClothManager.Existing;
            if (!IsSimulating || manager == null || index < 0 || index >= _bones.Length)
            {
                position = default;
                radius = 0f;
                return false;
            }

            position = manager.Solver.GetOutput(_handle, index).ToVector3();
            radius = manager.Solver.GetParticleRadius(_handle, index);
            return true;
        }

        internal void Configure(
            Transform attach,
            IList<Transform[]> boneChains,
            ConnectionMode mode,
            ClothPresetSource source,
            string builtInName,
            ClothParameters custom,
            string[] tags,
            float scale,
            float teleport,
            string generator)
        {
            attachBone = attach;
            chains.Clear();
            for (int c = 0; c < boneChains.Count; c++)
            {
                chains.Add(new ClothChain(boneChains[c]));
            }

            connection = mode;
            presetSource = source;
            builtInPreset = builtInName ?? string.Empty;
            customSettings = new ClothSettings(custom);
            colliderTags = tags ?? new string[0];
            radiusScale = Mathf.Max(0.0001f, scale);
            teleportDistance = Mathf.Max(0f, teleport);
            generatedBy = generator ?? string.Empty;
            ownsGameObject = true;
            _presetCached = false;
        }

        private void OnEnable()
        {
            if (Application.isPlaying)
            {
                Activate();
            }
        }

        private void OnDisable()
        {
            Deactivate();
        }

        private void OnValidate()
        {
            radiusScale = Mathf.Max(0.0001f, radiusScale);
            teleportDistance = Mathf.Max(0f, teleportDistance);
            _presetCached = false;
        }

        private void Activate()
        {
            if (IsSimulating)
            {
                return;
            }

            if (!BuildRuntimeData())
            {
                Debug.LogWarning("[AutoRig Cloth] '" + name + "' has no valid chain and is not simulated. See the Cloth Bone Group inspector for details.", this);
                return;
            }

            AutoRigClothManager manager = AutoRigClothManager.GetOrCreate();
            if (manager != null)
            {
                _handle = manager.Register(this);
            }
        }

        private void Deactivate()
        {
            if (_handle != null)
            {
                AutoRigClothManager manager = AutoRigClothManager.Existing;
                if (manager != null)
                {
                    manager.Unregister(this);
                }

                _handle = null;
                RestoreRestPose();
            }
        }

        // Called by the manager when a chain bone has been destroyed.
        internal void OnBonesLost()
        {
            Debug.LogWarning("[AutoRig Cloth] A bone of '" + name + "' was destroyed; the group stops simulating. Call Rebuild() after fixing its chains.", this);
            _handle = null;
            RestoreRestPose();
        }

        // Called by the manager when it is destroyed.
        internal void OnManagerDestroyed()
        {
            _handle = null;
        }

        private bool BuildRuntimeData()
        {
            var bones = new List<Transform>();
            var lengths = new List<int>();
            var seen = new HashSet<Transform>();
            for (int c = 0; c < chains.Count; c++)
            {
                if (CheckChain(chains[c], seen) != null)
                {
                    continue;
                }

                int count = 0;
                Transform[] chain = chains[c].bones;
                for (int j = 0; j < chain.Length; j++)
                {
                    if (chain[j] != null)
                    {
                        bones.Add(chain[j]);
                        count++;
                    }
                }

                lengths.Add(count);
            }

            if (lengths.Count == 0)
            {
                return false;
            }

            _bones = bones.ToArray();
            _chainLengths = lengths.ToArray();
            int n = _bones.Length;
            _directChild = new bool[n];
            _childOffset = new Vector3[n];
            _restRotation = new Quaternion[n];
            _restPosition = new Vector3[n];
            int start = 0;
            for (int c = 0; c < _chainLengths.Length; c++)
            {
                for (int j = 1; j < _chainLengths[c]; j++)
                {
                    int i = start + j;
                    _directChild[i] = _bones[i].parent == _bones[i - 1];
                    _childOffset[i] = _bones[i - 1].InverseTransformPoint(_bones[i].position);
                }

                start += _chainLengths[c];
            }

            for (int i = 0; i < n; i++)
            {
                _restRotation[i] = _bones[i].localRotation;
                _restPosition[i] = _bones[i].localPosition;
            }

            return true;
        }

        // Returns null for a usable chain, otherwise the reason it is skipped. Adds its bones to seen.
        private static string CheckChain(ClothChain chain, HashSet<Transform> seen)
        {
            if (chain == null || chain.bones == null)
            {
                return "it has no bones.";
            }

            Transform previous = null;
            int count = 0;
            for (int j = 0; j < chain.bones.Length; j++)
            {
                Transform bone = chain.bones[j];
                if (bone == null)
                {
                    continue;
                }

                if (previous != null && (bone == previous || !bone.IsChildOf(previous)))
                {
                    return "'" + bone.name + "' is not a descendant of '" + previous.name + "'.";
                }

                if (seen.Contains(bone))
                {
                    return "'" + bone.name + "' is already used by another chain.";
                }

                previous = bone;
                count++;
            }

            if (count < 2)
            {
                return "a chain needs at least two bones (add an end joint).";
            }

            for (int j = 0; j < chain.bones.Length; j++)
            {
                if (chain.bones[j] != null)
                {
                    seen.Add(chain.bones[j]);
                }
            }

            return null;
        }

        private Transform ResolveAttach()
        {
            if (attachBone != null)
            {
                return attachBone;
            }

            Transform parent = _bones != null && _bones.Length > 0 && _bones[0] != null ? _bones[0].parent : null;
            return parent != null ? parent : transform;
        }

        // Update: undo last frame's simulated rotations so an Animator that does not animate these
        // bones cannot build up feedback (one that does overwrites them anyway).
        internal void RestoreRestPose()
        {
            if (_bones == null)
            {
                return;
            }

            for (int i = 0; i < _bones.Length; i++)
            {
                Transform bone = _bones[i];
                if (bone != null)
                {
                    bone.localRotation = _restRotation[i];
                    bone.localPosition = _restPosition[i];
                }
            }
        }

        internal Vector3 AttachPosition => ResolveAttach().position;

        // LateUpdate, before the solver: feeds the animated pose. False when a bone is gone.
        internal bool PushTo(ClothSolver solver)
        {
            for (int i = 0; i < _bones.Length; i++)
            {
                if (_bones[i] == null)
                {
                    return false;
                }
            }

            Transform attach = ResolveAttach();
            solver.SetGroupFrame(_handle, new RigidFrame(attach.position.ToV3(), attach.rotation.ToQ4()));
            for (int i = 0; i < _bones.Length; i++)
            {
                solver.SetTarget(_handle, i, _bones[i].position.ToV3());
            }

            float lossy = UnityConversions.MaxAbsScale(transform.lossyScale);
            solver.SetParameters(_handle, GetParameters());
            solver.SetLengthScale(_handle, radiusScale * lossy);
            solver.SetTeleportDistance(_handle, teleportDistance * lossy);
            return true;
        }

        // LateUpdate, after the solver: writes rotations root to tip. Each bone is turned so that it
        // points at its simulated child; the end joint is never rotated.
        internal void PullFrom(ClothSolver solver)
        {
            if (blendWeight <= 0f)
            {
                return;
            }

            int start = 0;
            for (int c = 0; c < _chainLengths.Length; c++)
            {
                int length = _chainLengths[c];
                for (int j = 0; j < length - 1; j++)
                {
                    int i = start + j;
                    Transform bone = _bones[i];
                    Transform child = _bones[i + 1];
                    Quaternion current = bone.rotation;
                    Vector3 animated = current * (_directChild[i + 1] ? child.localPosition : _childOffset[i + 1]);
                    Vector3 simulated = solver.GetOutput(_handle, i + 1).ToVector3() - bone.position;
                    if (animated.sqrMagnitude < 1e-12f || simulated.sqrMagnitude < 1e-12f || !UnityConversions.IsFinite(simulated))
                    {
                        continue;
                    }

                    Quaternion delta = Quaternion.FromToRotation(animated, simulated);
                    if (blendWeight < 1f)
                    {
                        delta = Quaternion.Slerp(Quaternion.identity, delta, blendWeight);
                    }

                    bone.rotation = delta * current;
                }

                start += length;
            }
        }

        private void OnDrawGizmosSelected()
        {
            ClothGizmos.DrawGroup(this);
        }
    }
}
