using UnityEngine;

namespace AutoRig.Cloth
{
    /// <summary>Shape of a <see cref="ClothWind"/> zone.</summary>
    public enum ClothWindMode
    {
        /// <summary>Blows along the transform's forward axis everywhere.</summary>
        Global = 0,

        /// <summary>Blows along the forward axis inside a sphere, fading to zero at its edge.</summary>
        SphereDirectional = 1,

        /// <summary>Blows outward from the centre inside a sphere, fading to zero at its edge (a fan, a blast).</summary>
        SphereRadial = 2,
    }

    /// <summary>
    /// A wind zone for cloth. Every group is pushed by the sum of all zones at its attach bone,
    /// scaled by its preset's <c>wind</c> value. Turbulence adds gusts and swirl from a
    /// deterministic noise field that varies over space and time.
    /// </summary>
    [AddComponentMenu("AutoRig Cloth/Cloth Wind")]
    public sealed class ClothWind : MonoBehaviour
    {
        [SerializeField]
        [Tooltip("Global: blows along this object's forward axis everywhere. Sphere Directional: forward, inside the radius. Sphere Radial: outward from this object, inside the radius.")]
        private ClothWindMode mode = ClothWindMode.Global;

        [SerializeField]
        [Min(0f)]
        [Tooltip("Wind acceleration in m/s² at full strength. About 2–4 is a breeze, 10 or more a storm.")]
        private float strength = 3f;

        [SerializeField]
        [Range(0f, 1f)]
        [Tooltip("How gusty and swirling the wind is.")]
        private float turbulence = 0.5f;

        [SerializeField]
        [Min(0f)]
        [Tooltip("How fast the gusts change, in cycles per second.")]
        private float frequency = 0.7f;

        [SerializeField]
        [Min(0.01f)]
        [Tooltip("Radius of a sphere zone, in this object's units. The wind fades linearly to zero at the edge.")]
        private float radius = 5f;

        /// <summary>Shape of the zone.</summary>
        public ClothWindMode Mode
        {
            get => mode;
            set => mode = value;
        }

        /// <summary>Wind acceleration at full strength, m/s².</summary>
        public float Strength
        {
            get => strength;
            set => strength = Mathf.Max(0f, value);
        }

        /// <summary>Gustiness, 0–1.</summary>
        public float Turbulence
        {
            get => turbulence;
            set => turbulence = Mathf.Clamp01(value);
        }

        /// <summary>Gust frequency in cycles per second.</summary>
        public float Frequency
        {
            get => frequency;
            set => frequency = Mathf.Max(0f, value);
        }

        /// <summary>Radius of a sphere zone.</summary>
        public float Radius
        {
            get => radius;
            set => radius = Mathf.Max(0.01f, value);
        }

        /// <summary>Mean wind acceleration this zone applies at a world position.</summary>
        /// <returns>False when the position is outside the zone.</returns>
        public bool Evaluate(Vector3 position, out Vector3 acceleration)
        {
            Transform t = transform;
            if (mode == ClothWindMode.Global)
            {
                acceleration = t.forward * strength;
                return strength > 0f;
            }

            Vector3 offset = position - t.position;
            float r = radius * UnityConversions.MaxAbsScale(t.lossyScale);
            float d = offset.magnitude;
            if (d >= r || strength <= 0f)
            {
                acceleration = Vector3.zero;
                return false;
            }

            Vector3 direction = mode == ClothWindMode.SphereRadial && d > 1e-5f ? offset / d : t.forward;
            acceleration = direction * (strength * (1f - d / r));
            return true;
        }

        private void OnEnable()
        {
            if (!Application.isPlaying)
            {
                return;
            }

            AutoRigClothManager manager = AutoRigClothManager.GetOrCreate();
            if (manager != null)
            {
                manager.Register(this);
            }
        }

        private void OnDisable()
        {
            AutoRigClothManager manager = AutoRigClothManager.Existing;
            if (manager != null)
            {
                manager.Unregister(this);
            }
        }

        private void OnDrawGizmosSelected()
        {
            ClothGizmos.DrawWind(this);
        }
    }
}
