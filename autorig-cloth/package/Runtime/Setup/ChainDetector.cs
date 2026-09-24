using System.Collections.Generic;
using AutoRig.Cloth.Core;
using UnityEngine;

namespace AutoRig.Cloth
{
    /// <summary>A group of chains found by <see cref="ChainDetector.Detect"/>.</summary>
    public sealed class DetectedChainGroup
    {
        /// <summary>Suggested group name, for example <c>hair_Head</c>.</summary>
        public string Name;

        /// <summary>What the bone names say the chains are.</summary>
        public ChainCategory Category;

        /// <summary>Manifest kind: <c>hair</c>, <c>cloth</c>, <c>tail</c> or <c>accessory</c>.</summary>
        public string Kind;

        /// <summary>Suggested built-in preset.</summary>
        public string Preset;

        /// <summary>Suggested connection (skirt-like rings are <c>Loop</c>, capes <c>Open</c>, hair <c>None</c>).</summary>
        public ConnectionMode Connection;

        /// <summary>The bone the chains hang from.</summary>
        public Transform AttachBone;

        /// <summary>Chains ordered around the attach bone, each root to tip. A tip may still lack an end joint.</summary>
        public List<Transform[]> Chains = new List<Transform[]>();

        /// <summary>Largest angle between neighbouring chain roots around the attach bone.</summary>
        public float LargestGapDegrees;
    }

    /// <summary>
    /// Finds hair, skirts, capes, tails and other secondary-motion chains from bone names (hair,
    /// ponytail, braid, bang, skirt, cape, cloak, coat, tail, ribbon, scarf, sleeve, ear) and turns
    /// them into <see cref="ClothBoneGroup"/> components. Only skeleton bones are considered: bones
    /// mapped by a humanoid avatar, objects with renderers (a mesh called "Hair" is not a bone) and
    /// the generated <c>AutoRigCloth</c> object are never used.
    /// </summary>
    public static class ChainDetector
    {
        /// <summary><see cref="ClothBoneGroup.GeneratedBy"/> value of groups made by <see cref="CreateGroups"/>.</summary>
        public const string GeneratorId = "autodetect";

        // Hips-to-head distance of the adult the built-in presets are tuned for.
        private const float ReferenceBodyLength = 0.62f;

        /// <summary>
        /// Scans the hierarchy under <paramref name="root"/>. A chain starts at the topmost bone of a
        /// category (or at every same-category child of a branching bone) and follows single child
        /// bones, preferring the one same-category child at a branch. Chains are grouped by attach
        /// bone and category and ordered around the attach bone.
        /// </summary>
        /// <param name="root">Character root.</param>
        /// <param name="animator">Optional humanoid Animator; its mapped bones are excluded.</param>
        public static List<DetectedChainGroup> Detect(Transform root, Animator animator = null)
        {
            if (root == null)
            {
                return new List<DetectedChainGroup>();
            }

            var scan = new Scan(root, animator);
            scan.Run();
            for (int g = 0; g < scan.Result.Count; g++)
            {
                Finish(scan.Result[g]);
            }

            return scan.Result;
        }

        /// <summary>
        /// Creates a <see cref="ClothBoneGroup"/> for every detected group under the character's
        /// <c>AutoRigCloth</c> object, replacing groups a previous run created. Radii and the
        /// teleport distance are scaled by the character's size (hips-to-head distance of a humanoid).
        /// </summary>
        /// <param name="characterRoot">Character root.</param>
        /// <param name="groups">Output of <see cref="Detect"/>, possibly edited.</param>
        /// <param name="createEndJoints">Add an end joint where a chain's last bone has no child bone (otherwise that bone is not rotated).</param>
        /// <param name="animator">Optional humanoid Animator used to measure the character.</param>
        /// <param name="handler">Object creation handler; null = runtime.</param>
        public static ClothSetupReport CreateGroups(
            Transform characterRoot,
            IList<DetectedChainGroup> groups,
            bool createEndJoints = true,
            Animator animator = null,
            IClothSetupHandler handler = null)
        {
            var report = new ClothSetupReport();
            if (characterRoot == null)
            {
                report.Errors.Add("No character root was given.");
                return report;
            }

            IClothSetupHandler h = handler ?? RuntimeClothSetupHandler.Instance;
            report.RemovedObjects = GeneratedObjects.Remove(characterRoot, GeneratorId, h);
            float bodyScale = MeasureScale(animator);
            report.CalibrationFactor = bodyScale;
            report.TeleportDistance = ClothSolver.DefaultTeleportDistance * bodyScale;
            if (groups == null || groups.Count == 0)
            {
                report.Warnings.Add("No hair, cloth, tail or accessory chains were found by bone name.");
                return report;
            }

            Transform container = GeneratedObjects.GetOrCreateContainer(characterRoot, h);
            for (int g = 0; g < groups.Count; g++)
            {
                DetectedChainGroup detected = groups[g];
                var chains = new List<Transform[]>();
                for (int c = 0; c < detected.Chains.Count; c++)
                {
                    var chain = new List<Transform>(detected.Chains[c]);
                    Transform last = chain[chain.Count - 1];
                    if (createEndJoints && ChainBuilder.NeedsEndJoint(last))
                    {
                        chain.Add(ChainBuilder.CreateEndJoint(last, chain.Count >= 2 ? chain[chain.Count - 2] : null, h));
                    }

                    if (chain.Count >= 2)
                    {
                        chains.Add(chain.ToArray());
                    }
                }

                if (chains.Count == 0)
                {
                    report.Warnings.Add("'" + detected.Name + "' skipped: its chains have a single bone and no end joint.");
                    continue;
                }

                GameObject go = h.CreateGameObject(detected.Name, container, false);
                ClothBoneGroup group = h.AddComponent<ClothBoneGroup>(go);
                float lossy = UnityConversions.MaxAbsScale(go.transform.lossyScale);
                group.Configure(
                    detected.AttachBone,
                    chains,
                    detected.Connection,
                    ClothPresetSource.BuiltIn,
                    detected.Preset,
                    BuiltInPresets.GetOrDefault(detected.Preset),
                    new string[0],
                    bodyScale / lossy,
                    report.TeleportDistance / lossy,
                    GeneratorId);
                go.SetActive(true);
                report.CreatedGroups.Add(detected.Name + " (" + chains.Count + " chains, " + detected.Preset + ", " + detected.Connection + ")");
            }

            return report;
        }

        private static float MeasureScale(Animator animator)
        {
            if (animator == null || !animator.isHuman)
            {
                return 1f;
            }

            Transform hips = animator.GetBoneTransform(HumanBodyBones.Hips);
            Transform head = animator.GetBoneTransform(HumanBodyBones.Head);
            if (hips == null || head == null)
            {
                return 1f;
            }

            return ManifestRules.CalibrationFactor(Vector3.Distance(hips.position, head.position), ReferenceBodyLength);
        }

        // Orders the chains around the attach bone and picks the connection.
        private static void Finish(DetectedChainGroup group)
        {
            var roots = new List<V3>(group.Chains.Count);
            V3 hang = V3.Zero;
            for (int c = 0; c < group.Chains.Count; c++)
            {
                Transform[] chain = group.Chains[c];
                roots.Add(chain[0].position.ToV3());
                if (chain.Length >= 2)
                {
                    hang += (chain[chain.Length - 1].position - chain[0].position).ToV3();
                }
            }

            int[] order = ChainRing.OrderAround(roots, group.AttachBone.position.ToV3(), hang, out float gap);
            var sorted = new List<Transform[]>(order.Length);
            for (int i = 0; i < order.Length; i++)
            {
                sorted.Add(group.Chains[order[i]]);
            }

            group.Chains = sorted;
            group.LargestGapDegrees = gap;
            group.Connection = ChainKeywords.ConnectionFor(group.Category, sorted.Count, gap);
        }

        // One detection pass over a character.
        private sealed class Scan
        {
            public readonly List<DetectedChainGroup> Result = new List<DetectedChainGroup>();

            private readonly Transform _root;
            private readonly Transform _container;
            private readonly HashSet<Transform> _mapped = new HashSet<Transform>();
            private readonly HashSet<Transform> _skeleton;
            private readonly HashSet<Transform> _used = new HashSet<Transform>();
            private readonly Dictionary<(Transform, ChainCategory), DetectedChainGroup> _byKey = new Dictionary<(Transform, ChainCategory), DetectedChainGroup>();

            public Scan(Transform root, Animator animator)
            {
                _root = root;
                _container = GeneratedObjects.FindContainer(root);
                _skeleton = CollectSkeleton(root);
                if (animator != null && animator.isHuman)
                {
                    for (int b = 0; b < (int)HumanBodyBones.LastBone; b++)
                    {
                        Transform t = animator.GetBoneTransform((HumanBodyBones)b);
                        if (t != null)
                        {
                            _mapped.Add(t);
                        }
                    }
                }
            }

            public void Run()
            {
                var stack = new Stack<Transform>();
                var work = new Stack<(Transform start, Transform attach)>();
                for (int i = _root.childCount - 1; i >= 0; i--)
                {
                    stack.Push(_root.GetChild(i));
                }

                while (stack.Count > 0)
                {
                    Transform t = stack.Pop();
                    if (t == _container)
                    {
                        continue;
                    }

                    ChainCategory category = CategoryOf(t);
                    if (category != ChainCategory.None && !_used.Contains(t) && CategoryOf(t.parent) != category)
                    {
                        work.Push((t, t.parent != null ? t.parent : _root));
                        while (work.Count > 0)
                        {
                            (Transform start, Transform attach) = work.Pop();
                            if (SameCategoryChildren(start, category) >= 2)
                            {
                                // A hub (a "Skirt" bone with one child per panel, a forking braid):
                                // its same-category children start chains that hang from it.
                                PushBranches(work, start, category);
                                continue;
                            }

                            Transform[] chain = FollowChain(start, category);
                            _used.UnionWith(chain);
                            Add(attach, category, chain);
                            Transform end = chain[chain.Length - 1];
                            if (SameCategoryChildren(end, category) >= 2)
                            {
                                PushBranches(work, end, category);
                            }
                        }
                    }

                    // Always descend: other categories may hang off a chain (a ribbon tied to a braid).
                    // Bones already in a chain never start another one.
                    for (int i = t.childCount - 1; i >= 0; i--)
                    {
                        stack.Push(t.GetChild(i));
                    }
                }
            }

            // Bones referenced by skinned meshes plus their non-rendering descendants (end joints,
            // unweighted helpers). Null when the character has no skinned mesh: then every
            // transform without a renderer counts as a bone.
            private static HashSet<Transform> CollectSkeleton(Transform root)
            {
                SkinnedMeshRenderer[] meshes = root.GetComponentsInChildren<SkinnedMeshRenderer>(true);
                if (meshes.Length == 0)
                {
                    return null;
                }

                var skeleton = new HashSet<Transform>();
                var stack = new Stack<Transform>();
                for (int m = 0; m < meshes.Length; m++)
                {
                    Transform[] bones = meshes[m].bones;
                    for (int b = 0; b < bones.Length; b++)
                    {
                        if (bones[b] != null && skeleton.Add(bones[b]))
                        {
                            stack.Push(bones[b]);
                        }
                    }
                }

                while (stack.Count > 0)
                {
                    Transform t = stack.Pop();
                    for (int c = 0; c < t.childCount; c++)
                    {
                        Transform child = t.GetChild(c);
                        if (!ChainBuilder.IsAttachment(child) && skeleton.Add(child))
                        {
                            stack.Push(child);
                        }
                    }
                }

                return skeleton;
            }

            private bool IsBone(Transform t)
            {
                if (t == null || t == _container || _mapped.Contains(t))
                {
                    return false;
                }

                return _skeleton != null ? _skeleton.Contains(t) : !ChainBuilder.IsAttachment(t);
            }

            private ChainCategory CategoryOf(Transform t) => IsBone(t) ? ChainKeywords.Classify(t.name) : ChainCategory.None;

            private int SameCategoryChildren(Transform t, ChainCategory category)
            {
                int count = 0;
                for (int c = 0; c < t.childCount; c++)
                {
                    if (CategoryOf(t.GetChild(c)) == category)
                    {
                        count++;
                    }
                }

                return count;
            }

            private void PushBranches(Stack<(Transform, Transform)> work, Transform hub, ChainCategory category)
            {
                for (int c = hub.childCount - 1; c >= 0; c--)
                {
                    Transform child = hub.GetChild(c);
                    if (CategoryOf(child) == category)
                    {
                        work.Push((child, hub));
                    }
                }
            }

            // Root followed by its only child bone; at a branch, continues into the one child bone of
            // the same category, if there is exactly one. A bone of another category ends the chain.
            private Transform[] FollowChain(Transform root, ChainCategory category)
            {
                var chain = new List<Transform> { root };
                Transform current = root;
                while (true)
                {
                    Transform only = null;
                    Transform sameCategory = null;
                    int bones = 0;
                    int sameCategoryCount = 0;
                    for (int i = 0; i < current.childCount; i++)
                    {
                        Transform child = current.GetChild(i);
                        if (!IsBone(child))
                        {
                            continue;
                        }

                        bones++;
                        only = child;
                        if (ChainKeywords.Classify(child.name) == category)
                        {
                            sameCategoryCount++;
                            sameCategory = child;
                        }
                    }

                    Transform next = null;
                    if (bones == 1)
                    {
                        ChainCategory c = ChainKeywords.Classify(only.name);
                        next = c == category || c == ChainCategory.None ? only : null;
                    }
                    else if (sameCategoryCount == 1)
                    {
                        next = sameCategory;
                    }

                    if (next == null)
                    {
                        return chain.ToArray();
                    }

                    chain.Add(next);
                    current = next;
                }
            }

            private void Add(Transform attach, ChainCategory category, Transform[] chain)
            {
                if (!_byKey.TryGetValue((attach, category), out DetectedChainGroup group))
                {
                    group = new DetectedChainGroup
                    {
                        Name = category.ToString().ToLowerInvariant() + "_" + BoneNames.StripNamespace(attach.name),
                        Category = category,
                        Kind = ChainKeywords.KindOf(category),
                        Preset = ChainKeywords.PresetOf(category),
                        AttachBone = attach,
                    };
                    _byKey.Add((attach, category), group);
                    Result.Add(group);
                }

                group.Chains.Add(chain);
            }
        }
    }
}
