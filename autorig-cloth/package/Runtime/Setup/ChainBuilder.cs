using System;
using System.Collections.Generic;
using UnityEngine;

namespace AutoRig.Cloth
{
    /// <summary>Helpers that turn bone hierarchies into chains.</summary>
    public static class ChainBuilder
    {
        /// <summary>Suffix of end joints created by <see cref="CreateEndJoint"/>.</summary>
        public const string EndJointSuffix = "_end";

        /// <summary>
        /// True for transforms that render something (mesh objects, props parented to bones): they
        /// are attachments, not bones, and never become part of a chain.
        /// </summary>
        public static bool IsAttachment(Transform t) => t != null && t.GetComponent<Renderer>() != null;

        /// <summary>
        /// Returns <paramref name="root"/> followed by its single-child descendants: the chain
        /// continues while the current bone has exactly one child that is not an attachment.
        /// </summary>
        public static List<Transform> FollowSingleChildren(Transform root)
        {
            var chain = new List<Transform>();
            if (root == null)
            {
                return chain;
            }

            Transform current = root;
            chain.Add(current);
            while (true)
            {
                Transform next = null;
                for (int i = 0; i < current.childCount; i++)
                {
                    Transform child = current.GetChild(i);
                    if (IsAttachment(child))
                    {
                        continue;
                    }

                    if (next != null)
                    {
                        return chain; // a branch: the chain ends here
                    }

                    next = child;
                }

                if (next == null)
                {
                    return chain;
                }

                chain.Add(next);
                current = next;
            }
        }

        /// <summary>
        /// True when <paramref name="bone"/> is named like an end joint (<c>…_end</c>, any case), as
        /// AutoRig, Blender and <see cref="CreateEndJoint"/> name them.
        /// </summary>
        public static bool IsEndJoint(Transform bone) =>
            bone != null && bone.name.EndsWith(EndJointSuffix, StringComparison.OrdinalIgnoreCase);

        /// <summary>
        /// True when a chain ending in <paramref name="last"/> needs an end joint so that
        /// <paramref name="last"/> can be rotated: it has no child bone (attachments do not count)
        /// and is not an end joint itself.
        /// </summary>
        public static bool NeedsEndJoint(Transform last)
        {
            if (last == null || IsEndJoint(last))
            {
                return false;
            }

            for (int i = 0; i < last.childCount; i++)
            {
                if (!IsAttachment(last.GetChild(i)))
                {
                    return false;
                }
            }

            return true;
        }

        /// <summary>
        /// Creates an end joint as a child of <paramref name="last"/> at the extrapolated tip of the
        /// chain: one more segment along <paramref name="previous"/> → <paramref name="last"/>, or,
        /// for a single-bone chain, half the parent-bone distance along the bone's local Y axis (the
        /// bone axis of FBX files from Blender and Mixamo).
        /// </summary>
        /// <returns>The new end joint, named after <paramref name="last"/> with <see cref="EndJointSuffix"/>.</returns>
        public static Transform CreateEndJoint(Transform last, Transform previous, IClothSetupHandler handler)
        {
            if (last == null)
            {
                return null;
            }

            Vector3 tip;
            if (previous != null && (last.position - previous.position).sqrMagnitude > 1e-12f)
            {
                tip = last.position + (last.position - previous.position);
            }
            else
            {
                float length = last.parent != null ? Vector3.Distance(last.parent.position, last.position) * 0.5f : 0f;
                tip = last.position + last.up * (length > 1e-4f ? length : 0.05f);
            }

            GameObject end = (handler ?? RuntimeClothSetupHandler.Instance).CreateGameObject(last.name + EndJointSuffix, last, true);
            end.transform.position = tip;
            return end.transform;
        }
    }
}
