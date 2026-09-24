using UnityEngine;

namespace AutoRig.Cloth
{
    /// <summary>
    /// Adds a body made of cloth colliders to a humanoid character: a head sphere, chest, spine and
    /// hips capsules, tapered capsules for the upper and lower arms and legs, and hand spheres, all
    /// tagged <see cref="BodyTag"/>. Radii are derived from the character's own bone lengths, so the
    /// result fits any proportions and scale.
    /// </summary>
    /// <remarks>
    /// Radii are fractions of bone lengths tuned to sit slightly inside an average body mesh (for a
    /// hips-to-head distance H: head 0.15 H, chest 0.19 H, spine 0.175 H, hips 0.17 H; upper arm
    /// 0.17 → 0.14 of its length, forearm 0.16 → 0.10, upper leg 0.19 → 0.13, lower leg 0.13 → 0.09,
    /// hand 0.18 of the forearm length). Running the builder again replaces the colliders it made
    /// before and leaves every other collider alone.
    /// </remarks>
    public static class HumanoidColliderBuilder
    {
        /// <summary>Tag of the created colliders.</summary>
        public const string BodyTag = "body";

        /// <summary><see cref="ClothCollider.GeneratedBy"/> value of colliders made by this builder.</summary>
        public const string GeneratorId = "humanoid";

        /// <summary>Builds the colliders with the runtime handler (no Undo).</summary>
        public static ClothSetupReport Build(Animator animator) => Build(animator, null);

        /// <summary>Builds the colliders, creating components through <paramref name="handler"/> (null = runtime).</summary>
        public static ClothSetupReport Build(Animator animator, IClothSetupHandler handler)
        {
            var report = new ClothSetupReport();
            if (animator == null || !animator.isHuman || animator.avatar == null)
            {
                report.Errors.Add("A humanoid Animator (with a Humanoid avatar) is required to build body colliders.");
                return report;
            }

            Transform hips = animator.GetBoneTransform(HumanBodyBones.Hips);
            Transform head = animator.GetBoneTransform(HumanBodyBones.Head);
            if (hips == null || head == null)
            {
                report.Errors.Add("The avatar has no hips or head bone.");
                return report;
            }

            float h = Vector3.Distance(hips.position, head.position);
            if (h < 1e-4f)
            {
                report.Errors.Add("The hips and head bones are at the same position.");
                return report;
            }

            IClothSetupHandler setup = handler ?? RuntimeClothSetupHandler.Instance;
            report.RemovedObjects = GeneratedObjects.Remove(animator.transform, GeneratorId, setup);

            Transform spine = Bone(animator, HumanBodyBones.Spine, hips);
            Transform chest = Bone(animator, HumanBodyBones.Chest, spine);
            Transform upperChest = Bone(animator, HumanBodyBones.UpperChest, chest);
            Transform neck = Bone(animator, HumanBodyBones.Neck, head);

            Vector3 up = (head.position - (neck != head ? neck.position : upperChest.position)).normalized;
            Sphere(setup, report, "head", head, head.position + up * (0.12f * h), 0.15f * h);
            Capsule(setup, report, "chest", upperChest, neck, 0.19f * h, 0.10f * h);
            Capsule(setup, report, "spine", hips, chest, 0.175f * h, 0.175f * h);

            Transform leftLeg = animator.GetBoneTransform(HumanBodyBones.LeftUpperLeg);
            Transform rightLeg = animator.GetBoneTransform(HumanBodyBones.RightUpperLeg);
            if (leftLeg != null && rightLeg != null)
            {
                // Across the pelvis, between the hip joints; hosted by the hips bone.
                ClothCapsuleCollider pelvis = setup.AddComponent<ClothCapsuleCollider>(hips.gameObject);
                float local = 1f / UnityConversions.MaxAbsScale(hips.lossyScale);
                pelvis.Configure(hips.InverseTransformPoint(leftLeg.position), hips.InverseTransformPoint(rightLeg.position), null, 0.17f * h * local, 0f);
                pelvis.MarkGenerated(GeneratorId, BodyTag);
                report.CreatedColliders.Add("hips");
            }

            Limb(setup, report, animator, "upper_arm_l", HumanBodyBones.LeftUpperArm, HumanBodyBones.LeftLowerArm, 0.17f, 0.14f);
            Limb(setup, report, animator, "upper_arm_r", HumanBodyBones.RightUpperArm, HumanBodyBones.RightLowerArm, 0.17f, 0.14f);
            Limb(setup, report, animator, "forearm_l", HumanBodyBones.LeftLowerArm, HumanBodyBones.LeftHand, 0.16f, 0.10f);
            Limb(setup, report, animator, "forearm_r", HumanBodyBones.RightLowerArm, HumanBodyBones.RightHand, 0.16f, 0.10f);
            Limb(setup, report, animator, "upper_leg_l", HumanBodyBones.LeftUpperLeg, HumanBodyBones.LeftLowerLeg, 0.19f, 0.13f);
            Limb(setup, report, animator, "upper_leg_r", HumanBodyBones.RightUpperLeg, HumanBodyBones.RightLowerLeg, 0.19f, 0.13f);
            Limb(setup, report, animator, "lower_leg_l", HumanBodyBones.LeftLowerLeg, HumanBodyBones.LeftFoot, 0.13f, 0.09f);
            Limb(setup, report, animator, "lower_leg_r", HumanBodyBones.RightLowerLeg, HumanBodyBones.RightFoot, 0.13f, 0.09f);
            Hand(setup, report, animator, "hand_l", HumanBodyBones.LeftLowerArm, HumanBodyBones.LeftHand);
            Hand(setup, report, animator, "hand_r", HumanBodyBones.RightLowerArm, HumanBodyBones.RightHand);
            return report;
        }

        private static Transform Bone(Animator animator, HumanBodyBones bone, Transform fallback)
        {
            Transform t = animator.GetBoneTransform(bone);
            return t != null ? t : fallback;
        }

        private static void Sphere(IClothSetupHandler setup, ClothSetupReport report, string label, Transform bone, Vector3 worldCenter, float worldRadius)
        {
            float local = 1f / UnityConversions.MaxAbsScale(bone.lossyScale);
            ClothSphereCollider sphere = setup.AddComponent<ClothSphereCollider>(bone.gameObject);
            sphere.Configure(bone.InverseTransformPoint(worldCenter), worldRadius * local, null, 0f);
            sphere.MarkGenerated(GeneratorId, BodyTag);
            report.CreatedColliders.Add(label);
        }

        private static void Capsule(IClothSetupHandler setup, ClothSetupReport report, string label, Transform from, Transform to, float worldRadius, float worldEndRadius)
        {
            if (from == null || to == null || from == to)
            {
                report.Warnings.Add("Collider '" + label + "' skipped: the avatar lacks the bones for it.");
                return;
            }

            float local = 1f / UnityConversions.MaxAbsScale(from.lossyScale);
            ClothCapsuleCollider capsule = setup.AddComponent<ClothCapsuleCollider>(from.gameObject);
            capsule.Configure(Vector3.zero, Vector3.zero, to, worldRadius * local, worldEndRadius * local);
            capsule.MarkGenerated(GeneratorId, BodyTag);
            report.CreatedColliders.Add(label);
        }

        private static void Limb(IClothSetupHandler setup, ClothSetupReport report, Animator animator, string label, HumanBodyBones from, HumanBodyBones to, float startFraction, float endFraction)
        {
            Transform a = animator.GetBoneTransform(from);
            Transform b = animator.GetBoneTransform(to);
            if (a == null || b == null)
            {
                report.Warnings.Add("Collider '" + label + "' skipped: the avatar lacks the bones for it.");
                return;
            }

            float length = Vector3.Distance(a.position, b.position);
            Capsule(setup, report, label, a, b, startFraction * length, endFraction * length);
        }

        private static void Hand(IClothSetupHandler setup, ClothSetupReport report, Animator animator, string label, HumanBodyBones lowerArm, HumanBodyBones hand)
        {
            Transform forearm = animator.GetBoneTransform(lowerArm);
            Transform palm = animator.GetBoneTransform(hand);
            if (forearm == null || palm == null)
            {
                report.Warnings.Add("Collider '" + label + "' skipped: the avatar lacks the bones for it.");
                return;
            }

            Vector3 direction = palm.position - forearm.position;
            float length = direction.magnitude;
            Sphere(setup, report, label, palm, palm.position + direction * 0.3f, 0.18f * length);
        }
    }
}
