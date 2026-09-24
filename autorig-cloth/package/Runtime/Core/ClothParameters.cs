using System;

namespace AutoRig.Cloth.Core
{
    /// <summary>
    /// Simulation parameters of one group, matching the manifest's <c>presets[]</c> entries.
    /// All values are dimensionless unless noted; lengths are in the group's length units
    /// (metres times the group's length scale).
    /// </summary>
    [Serializable]
    public struct ClothParameters : IEquatable<ClothParameters>
    {
        /// <summary>Multiplier of the solver gravity (9.81 m/s² downwards by default). Range 0–2.</summary>
        public float gravity;

        /// <summary>Share of velocity removed per 1/60 s. Range 0–1.</summary>
        public float damping;

        /// <summary>
        /// Pull back toward the animated pose direction: share of the deviation removed per 1/60 s.
        /// Range 0–1; 1 makes the chain follow the animation exactly.
        /// </summary>
        public float stiffness;

        /// <summary>Hard limit, in degrees, of a joint's deviation from its animated direction; 0 = no limit. Range 0–180.</summary>
        public float angleLimitDeg;

        /// <summary>Allowed relative stretch (and compression) of links along a chain; 0 = inextensible. Range 0–1.</summary>
        public float stretch;

        /// <summary>Stiffness of the links between neighbouring chains, as a share of the error removed per 1/60 s. Range 0–1.</summary>
        public float connectionStiffness;

        /// <summary>Collision radius of a chain's first joint (length units, ≥ 0).</summary>
        public float radius;

        /// <summary>Collision radius of a chain's last joint; 0 = same as <see cref="radius"/>.</summary>
        public float radiusTip;

        /// <summary>
        /// Share of the attach frame's world translation the cloth feels. 1 = pure world-space
        /// physics, 0 = the cloth moves rigidly with the attach frame. Range 0–1.
        /// </summary>
        public float inertiaMove;

        /// <summary>Same as <see cref="inertiaMove"/> for the attach frame's rotation. Range 0–1.</summary>
        public float inertiaRotate;

        /// <summary>Quadratic air drag: slows fast motion much more than slow sway. Range 0–1.</summary>
        public float drag;

        /// <summary>Influence of wind zones. Range 0–1.</summary>
        public float wind;

        /// <summary>Creates a parameter set from every value.</summary>
        public ClothParameters(
            float gravity,
            float damping,
            float stiffness,
            float angleLimitDeg,
            float stretch,
            float connectionStiffness,
            float radius,
            float radiusTip,
            float inertiaMove,
            float inertiaRotate,
            float drag,
            float wind)
        {
            this.gravity = gravity;
            this.damping = damping;
            this.stiffness = stiffness;
            this.angleLimitDeg = angleLimitDeg;
            this.stretch = stretch;
            this.connectionStiffness = connectionStiffness;
            this.radius = radius;
            this.radiusTip = radiusTip;
            this.inertiaMove = inertiaMove;
            this.inertiaRotate = inertiaRotate;
            this.drag = drag;
            this.wind = wind;
        }

        /// <summary>
        /// The defaults the manifest specification prescribes for a preset name a runtime does not find:
        /// gravity 1, damping 0.1, stiffness 0.2, no angle limit, no stretch, connection stiffness 0.5,
        /// radius 0.02 (tip the same), inertia 0.7 / 0.7, drag 0.02, wind 1.
        /// </summary>
        public static ClothParameters Default =>
            new ClothParameters(1f, 0.1f, 0.2f, 0f, 0f, 0.5f, 0.02f, 0f, 0.7f, 0.7f, 0.02f, 1f);

        /// <summary>Radius of a chain's last joint after applying the "0 = same as radius" rule.</summary>
        public float EffectiveRadiusTip => radiusTip > 0f ? radiusTip : radius;

        /// <summary>
        /// Returns a copy with every value clamped to its documented range. Non-finite values are
        /// replaced by the corresponding <see cref="Default"/> value.
        /// </summary>
        public ClothParameters Sanitized()
        {
            ClothParameters d = Default;
            return new ClothParameters(
                Clamp(gravity, 0f, 2f, d.gravity),
                Clamp(damping, 0f, 1f, d.damping),
                Clamp(stiffness, 0f, 1f, d.stiffness),
                Clamp(angleLimitDeg, 0f, 180f, d.angleLimitDeg),
                Clamp(stretch, 0f, 1f, d.stretch),
                Clamp(connectionStiffness, 0f, 1f, d.connectionStiffness),
                Clamp(radius, 0f, 1000f, d.radius),
                Clamp(radiusTip, 0f, 1000f, d.radiusTip),
                Clamp(inertiaMove, 0f, 1f, d.inertiaMove),
                Clamp(inertiaRotate, 0f, 1f, d.inertiaRotate),
                Clamp(drag, 0f, 1f, d.drag),
                Clamp(wind, 0f, 1f, d.wind));
        }

        private static float Clamp(float value, float min, float max, float fallback)
        {
            if (float.IsNaN(value) || float.IsInfinity(value))
            {
                return fallback;
            }

            return value < min ? min : (value > max ? max : value);
        }

        /// <inheritdoc />
        public bool Equals(ClothParameters other) =>
            gravity.Equals(other.gravity) && damping.Equals(other.damping)
            && stiffness.Equals(other.stiffness) && angleLimitDeg.Equals(other.angleLimitDeg)
            && stretch.Equals(other.stretch) && connectionStiffness.Equals(other.connectionStiffness)
            && radius.Equals(other.radius) && radiusTip.Equals(other.radiusTip)
            && inertiaMove.Equals(other.inertiaMove) && inertiaRotate.Equals(other.inertiaRotate)
            && drag.Equals(other.drag) && wind.Equals(other.wind);

        /// <inheritdoc />
        public override bool Equals(object obj) => obj is ClothParameters other && Equals(other);

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked
            {
                int h = gravity.GetHashCode();
                h = (h * 397) ^ damping.GetHashCode();
                h = (h * 397) ^ stiffness.GetHashCode();
                h = (h * 397) ^ angleLimitDeg.GetHashCode();
                h = (h * 397) ^ stretch.GetHashCode();
                h = (h * 397) ^ connectionStiffness.GetHashCode();
                h = (h * 397) ^ radius.GetHashCode();
                h = (h * 397) ^ radiusTip.GetHashCode();
                h = (h * 397) ^ inertiaMove.GetHashCode();
                h = (h * 397) ^ inertiaRotate.GetHashCode();
                h = (h * 397) ^ drag.GetHashCode();
                return (h * 397) ^ wind.GetHashCode();
            }
        }
    }
}
