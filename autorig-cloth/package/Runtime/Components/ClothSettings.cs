using System;
using AutoRig.Cloth.Core;
using UnityEngine;

namespace AutoRig.Cloth
{
    /// <summary>
    /// Inspector-friendly set of simulation parameters (the manifest's preset fields). Used by
    /// <see cref="ClothPreset"/> assets and by the custom settings of a <see cref="ClothBoneGroup"/>.
    /// Lengths are in metres before the group's radius scale is applied.
    /// </summary>
    [Serializable]
    public sealed class ClothSettings
    {
        /// <summary>Multiplier of gravity (9.81 m/s² downwards by default).</summary>
        [Tooltip("Multiplier of gravity (9.81 m/s² downwards by default).")]
        [Range(0f, 2f)]
        public float gravity = 1f;

        /// <summary>Share of velocity removed per 1/60 s.</summary>
        [Tooltip("Share of velocity removed per 1/60 s. Higher values calm the motion down faster.")]
        [Range(0f, 1f)]
        public float damping = 0.1f;

        /// <summary>Pull back toward the animated pose direction, per 1/60 s.</summary>
        [Tooltip("Pull back toward the animated pose, as a share of the deviation removed per 1/60 s. 0 = hangs freely, 1 = follows the animation exactly.")]
        [Range(0f, 1f)]
        public float stiffness = 0.2f;

        /// <summary>Hard limit of deviation from the animated direction, in degrees; 0 = no limit.</summary>
        [Tooltip("Hard limit, in degrees, of how far a joint may swing away from its animated direction. 0 = no limit.")]
        [Range(0f, 180f)]
        public float angleLimit;

        /// <summary>Allowed stretch of links along a chain; 0 = inextensible.</summary>
        [Tooltip("How much links along a chain may stretch or compress while simulating (bones keep their length on screen). 0 = inextensible.")]
        [Range(0f, 1f)]
        public float stretch;

        /// <summary>Stiffness of the links between neighbouring chains (open and loop groups).</summary>
        [Tooltip("Stiffness of the links between neighbouring chains in Open and Loop groups, per 1/60 s.")]
        [Range(0f, 1f)]
        public float connectionStiffness = 0.5f;

        /// <summary>Collision radius of the first joint of every chain, in metres.</summary>
        [Tooltip("Collision radius of the first joint of every chain, in metres (times the group's radius scale).")]
        [Min(0f)]
        public float radius = 0.02f;

        /// <summary>Collision radius of the last joint; 0 = same as <see cref="radius"/>.</summary>
        [Tooltip("Collision radius of the last joint, in metres; joints in between are interpolated. 0 = same as Radius.")]
        [Min(0f)]
        public float radiusTip;

        /// <summary>Share of the attach bone's translation the cloth feels.</summary>
        [Tooltip("How much of the attach bone's movement the cloth feels. 1 = pure world-space physics (trails behind), 0 = moves rigidly with the character.")]
        [Range(0f, 1f)]
        public float inertiaMove = 0.7f;

        /// <summary>Share of the attach bone's rotation the cloth feels.</summary>
        [Tooltip("How much of the attach bone's rotation the cloth feels. 1 = pure world-space physics, 0 = turns rigidly with the character.")]
        [Range(0f, 1f)]
        public float inertiaRotate = 0.7f;

        /// <summary>Quadratic air drag.</summary>
        [Tooltip("Air drag. Grows with speed, so it tames fast whipping while leaving slow sway lively.")]
        [Range(0f, 1f)]
        public float drag = 0.02f;

        /// <summary>Influence of wind zones.</summary>
        [Tooltip("How strongly Cloth Wind zones push this cloth.")]
        [Range(0f, 1f)]
        public float wind = 1f;

        /// <summary>Creates settings with the manifest specification defaults.</summary>
        public ClothSettings()
        {
        }

        /// <summary>Creates settings holding the given parameters.</summary>
        public ClothSettings(ClothParameters parameters)
        {
            SetFrom(parameters);
        }

        /// <summary>Converts to solver parameters.</summary>
        public ClothParameters ToParameters() => new ClothParameters(
            gravity,
            damping,
            stiffness,
            angleLimit,
            stretch,
            connectionStiffness,
            radius,
            radiusTip,
            inertiaMove,
            inertiaRotate,
            drag,
            wind);

        /// <summary>Copies every value from solver parameters.</summary>
        public void SetFrom(ClothParameters parameters)
        {
            gravity = parameters.gravity;
            damping = parameters.damping;
            stiffness = parameters.stiffness;
            angleLimit = parameters.angleLimitDeg;
            stretch = parameters.stretch;
            connectionStiffness = parameters.connectionStiffness;
            radius = parameters.radius;
            radiusTip = parameters.radiusTip;
            inertiaMove = parameters.inertiaMove;
            inertiaRotate = parameters.inertiaRotate;
            drag = parameters.drag;
            wind = parameters.wind;
        }
    }
}
