using System.Collections.Generic;
using AutoRig.Cloth.Core;
using UnityEngine;

namespace AutoRig.Cloth.Samples
{
    /// <summary>
    /// Builds a procedural character at runtime (capsule body, swinging legs, head) wearing a skirt
    /// (a loop group), long hair (independent strands) and a cape (an open group), then walks it
    /// around a circle with turns, stops and jumps so the cloth simulation can be watched in an
    /// empty scene. Add it to an empty GameObject and press Play.
    /// </summary>
    [AddComponentMenu("AutoRig Cloth/Samples/Cloth Demo")]
    public sealed class ClothDemo : MonoBehaviour
    {
        private const string BodyTag = "body";

        [SerializeField]
        [Min(0f)]
        [Tooltip("Walking speed in m/s.")]
        private float walkSpeed = 2.2f;

        [SerializeField]
        [Min(0.5f)]
        [Tooltip("Radius of the walking circle, in metres.")]
        private float circleRadius = 3f;

        [SerializeField]
        [Min(0.8f)]
        [Tooltip("Seconds between jumps.")]
        private float jumpInterval = 3.5f;

        [SerializeField]
        [Min(0f)]
        [Tooltip("Jump height in metres.")]
        private float jumpHeight = 0.5f;

        [SerializeField]
        [Tooltip("Stop and start again every few seconds, so the cloth swings forward.")]
        private bool stopAndGo = true;

        [SerializeField]
        [Tooltip("Add a gusty wind zone.")]
        private bool addWind = true;

        [SerializeField]
        [Tooltip("Create a follow camera, a light and a floor when the scene has none.")]
        private bool setUpScene = true;

        private readonly List<Material> _materials = new List<Material>();
        private Transform _character;
        private Transform _leftLeg;
        private Transform _rightLeg;
        private Camera _camera;
        private float _angle;
        private float _legPhase;
        private float _time;

        private void Start()
        {
            Material template = DefaultMaterial();
            BuildCharacter(template);
            if (addWind)
            {
                var wind = new GameObject("Cloth Wind").AddComponent<ClothWind>();
                wind.transform.SetParent(transform, false);
                wind.transform.rotation = Quaternion.LookRotation(new Vector3(1f, 0f, 0.4f));
                wind.Strength = 2.5f;
                wind.Turbulence = 0.6f;
                wind.Frequency = 0.5f;
            }

            if (setUpScene)
            {
                SetUpScene(template);
            }
        }

        private void Update()
        {
            float dt = Time.deltaTime;
            _time += dt;

            // Walk around the circle, pausing briefly every few seconds.
            float pace = stopAndGo ? Mathf.SmoothStep(0f, 1f, Mathf.Clamp01(Mathf.Abs(Mathf.Sin(_time * 0.55f)) * 3f - 0.6f)) : 1f;
            float speed = walkSpeed * pace;
            _angle += speed / circleRadius * dt;
            var position = new Vector3(Mathf.Cos(_angle), 0f, Mathf.Sin(_angle)) * circleRadius;
            var forward = new Vector3(-Mathf.Sin(_angle), 0f, Mathf.Cos(_angle));

            // Jump: a 0.7 s parabola every few seconds.
            float jump = (_time % jumpInterval) / 0.7f;
            if (jump < 1f)
            {
                position.y = jumpHeight * 4f * jump * (1f - jump);
            }

            _character.SetPositionAndRotation(position, Quaternion.LookRotation(forward));

            _legPhase += speed * dt * 3.2f;
            float swing = Mathf.Sin(_legPhase) * 35f * Mathf.Clamp01(speed / Mathf.Max(walkSpeed, 0.01f));
            _leftLeg.localRotation = Quaternion.Euler(swing, 0f, 0f);
            _rightLeg.localRotation = Quaternion.Euler(-swing, 0f, 0f);
        }

        private void LateUpdate()
        {
            if (_camera == null)
            {
                return;
            }

            Vector3 target = _character.position + Vector3.up * 1.2f;
            Vector3 desired = target - _character.forward * 3.2f + _character.right * 1.6f + Vector3.up * 0.5f;
            Transform cameraTransform = _camera.transform;
            cameraTransform.position = Vector3.Lerp(cameraTransform.position, desired, 1f - Mathf.Exp(-3f * Time.deltaTime));
            cameraTransform.LookAt(target);
        }

        private void OnDestroy()
        {
            for (int i = 0; i < _materials.Count; i++)
            {
                Destroy(_materials[i]);
            }
        }

        private void BuildCharacter(Material template)
        {
            Material skin = Tint(template, new Color(0.87f, 0.74f, 0.62f));
            Material cloth = Tint(template, new Color(0.3f, 0.32f, 0.38f));

            _character = new GameObject("Demo Character").transform;
            _character.SetParent(transform, false);

            // Skeleton (every bone starts with identity rotation, so local offsets are world offsets).
            Transform hips = Bone("Hips", _character, new Vector3(0f, 1f, 0f));
            Transform spine = Bone("Spine", hips, new Vector3(0f, 0.25f, 0f));
            Transform chest = Bone("Chest", spine, new Vector3(0f, 0.22f, 0f));
            Transform neck = Bone("Neck", chest, new Vector3(0f, 0.13f, 0f));
            Transform head = Bone("Head", neck, new Vector3(0f, 0.08f, 0f));
            _leftLeg = Leg("Left", hips, -0.1f, cloth);
            _rightLeg = Leg("Right", hips, 0.1f, cloth);

            Visual(PrimitiveType.Capsule, spine, new Vector3(0f, 0.05f, 0f), new Vector3(0.32f, 0.36f, 0.26f), cloth);
            Visual(PrimitiveType.Sphere, head, new Vector3(0f, 0.1f, 0f), new Vector3(0.26f, 0.28f, 0.26f), skin);

            // Body colliders, all tagged "body".
            var torso = spine.gameObject.AddComponent<ClothCapsuleCollider>();
            torso.StartOffset = new Vector3(0f, -0.25f, 0f);
            torso.EndOffset = new Vector3(0f, 0.36f, 0f);
            torso.Radius = 0.16f;
            torso.EndRadius = 0.15f;
            var skull = head.gameObject.AddComponent<ClothSphereCollider>();
            skull.Center = new Vector3(0f, 0.1f, 0f);
            skull.Radius = 0.14f;

            BuildSkirt(hips, template);
            BuildHair(head, template);
            BuildCape(chest, template);
        }

        private Transform Leg(string side, Transform hips, float x, Material material)
        {
            Transform upper = Bone(side + "UpLeg", hips, new Vector3(x, -0.05f, 0f));
            Transform knee = Bone(side + "Leg", upper, new Vector3(0f, -0.42f, 0f));
            Transform foot = Bone(side + "Foot", knee, new Vector3(0f, -0.43f, 0f));
            Visual(PrimitiveType.Capsule, upper, new Vector3(0f, -0.21f, 0f), new Vector3(0.15f, 0.24f, 0.15f), material);
            Visual(PrimitiveType.Capsule, knee, new Vector3(0f, -0.21f, 0f), new Vector3(0.11f, 0.24f, 0.11f), material);

            var thigh = upper.gameObject.AddComponent<ClothCapsuleCollider>();
            thigh.EndBone = knee;
            thigh.Radius = 0.075f;
            thigh.EndRadius = 0.06f;
            var shin = knee.gameObject.AddComponent<ClothCapsuleCollider>();
            shin.EndBone = foot;
            shin.Radius = 0.06f;
            shin.EndRadius = 0.045f;
            return upper;
        }

        // Eight chains of four bones plus an end joint around the hips, linked into a ring.
        private void BuildSkirt(Transform hips, Material template)
        {
            const int count = 8;
            var chains = new Transform[count][];
            for (int c = 0; c < count; c++)
            {
                float a = 2f * Mathf.PI * c / count;
                var outward = new Vector3(Mathf.Cos(a), 0f, Mathf.Sin(a));
                Vector3 direction = (outward * Mathf.Sin(15f * Mathf.Deg2Rad) + Vector3.down * Mathf.Cos(15f * Mathf.Deg2Rad)) * 0.12f;
                chains[c] = Chain("Skirt_" + c, hips, outward * 0.2f + new Vector3(0f, -0.02f, 0f), direction, 4);
            }

            Group("Skirt", hips, chains, ConnectionMode.Loop, BuiltInPresets.Skirt);
            Strip("Skirt Mesh", chains, true, Tint(template, new Color(0.78f, 0.2f, 0.27f)));
        }

        // Ten independent strands over the back of the head.
        private void BuildHair(Transform head, Material template)
        {
            const int count = 10;
            Material material = Tint(template, new Color(0.28f, 0.17f, 0.1f));
            var chains = new Transform[count][];
            for (int c = 0; c < count; c++)
            {
                float a = Mathf.Lerp(-80f, 80f, c / (count - 1f)) * Mathf.Deg2Rad;
                var around = new Vector3(Mathf.Sin(a), 0f, -Mathf.Cos(a));
                Vector3 root = new Vector3(0f, 0.12f, 0f) + around * 0.15f;
                Vector3 direction = (around * 0.35f + Vector3.down).normalized * 0.085f;
                chains[c] = Chain("Hair_" + c, head, root, direction, 4);
                for (int k = 0; k < chains[c].Length - 1; k++)
                {
                    Segment(chains[c][k], chains[c][k + 1], 0.012f, material);
                }
            }

            Group("Hair", head, chains, ConnectionMode.None, BuiltInPresets.Hair);
        }

        // Five chains of five bones plus an end joint across the upper back, linked side by side.
        private void BuildCape(Transform chest, Material template)
        {
            const int count = 5;
            var chains = new Transform[count][];
            for (int c = 0; c < count; c++)
            {
                float x = Mathf.Lerp(-0.17f, 0.17f, c / (count - 1f));
                Vector3 direction = new Vector3(x * 0.25f, -1f, -0.12f).normalized * 0.15f;
                chains[c] = Chain("Cape_" + c, chest, new Vector3(x, 0.08f, -0.2f), direction, 5);
            }

            Group("Cape", chest, chains, ConnectionMode.Open, BuiltInPresets.Cape);
            Strip("Cape Mesh", chains, false, Tint(template, new Color(0.2f, 0.3f, 0.7f)));
        }

        // A chain of `bones` bones plus an end joint, each a child of the previous.
        private static Transform[] Chain(string name, Transform parent, Vector3 rootOffset, Vector3 segment, int bones)
        {
            var chain = new Transform[bones + 1];
            chain[0] = Bone(name + "_0", parent, rootOffset);
            for (int k = 1; k <= bones; k++)
            {
                chain[k] = Bone(k == bones ? name + "_end" : name + "_" + k, chain[k - 1], segment);
            }

            return chain;
        }

        private void Group(string name, Transform attach, Transform[][] chains, ConnectionMode connection, string preset)
        {
            var go = new GameObject(name);
            go.SetActive(false); // configure before OnEnable registers it
            go.transform.SetParent(_character, false);
            var group = go.AddComponent<ClothBoneGroup>();
            group.AttachBone = attach;
            group.SetChains(chains);
            group.Connection = connection;
            group.PresetSource = ClothPresetSource.BuiltIn;
            group.BuiltInPreset = preset;
            group.ColliderTags = new[] { BodyTag };
            go.SetActive(true);
        }

        private void Strip(string name, Transform[][] chains, bool loop, Material material)
        {
            var go = new GameObject(name, typeof(MeshFilter), typeof(MeshRenderer), typeof(ChainStripMesh));
            go.transform.SetParent(_character, false);
            go.GetComponent<ChainStripMesh>().Initialize(chains, loop, material);
        }

        private void SetUpScene(Material template)
        {
            _camera = Camera.main;
            if (_camera == null)
            {
                var cameraObject = new GameObject("Demo Camera") { tag = "MainCamera" };
                _camera = cameraObject.AddComponent<Camera>();
                cameraObject.transform.position = new Vector3(0f, 2f, -circleRadius - 4f);
            }

            if (RenderSettings.sun == null)
            {
                var light = new GameObject("Demo Light").AddComponent<Light>();
                light.type = LightType.Directional;
                light.shadows = LightShadows.Soft;
                light.transform.rotation = Quaternion.Euler(50f, -30f, 0f);
                RenderSettings.sun = light;
            }

            GameObject floor = GameObject.CreatePrimitive(PrimitiveType.Plane);
            floor.name = "Demo Floor";
            floor.transform.SetParent(transform, false);
            floor.transform.localScale = new Vector3(1.5f, 1f, 1.5f);
            Destroy(floor.GetComponent<Collider>());
            floor.GetComponent<Renderer>().sharedMaterial = Tint(template, new Color(0.55f, 0.57f, 0.6f));
            floor.AddComponent<ClothPlaneCollider>().ColliderTag = BodyTag;
        }

        private static Transform Bone(string name, Transform parent, Vector3 localPosition)
        {
            Transform bone = new GameObject(name).transform;
            bone.SetParent(parent, false);
            bone.localPosition = localPosition;
            return bone;
        }

        private static void Visual(PrimitiveType type, Transform parent, Vector3 localPosition, Vector3 localScale, Material material)
        {
            GameObject go = GameObject.CreatePrimitive(type);
            go.name = parent.name + " Visual";
            Destroy(go.GetComponent<Collider>());
            go.transform.SetParent(parent, false);
            go.transform.localPosition = localPosition;
            go.transform.localScale = localScale;
            go.GetComponent<Renderer>().sharedMaterial = material;
        }

        // A thin cylinder from a bone to its child; it rotates with the bone.
        private static void Segment(Transform bone, Transform child, float radius, Material material)
        {
            Vector3 offset = child.localPosition;
            GameObject go = GameObject.CreatePrimitive(PrimitiveType.Cylinder);
            go.name = bone.name + " Visual";
            Destroy(go.GetComponent<Collider>());
            go.transform.SetParent(bone, false);
            go.transform.localPosition = offset * 0.5f;
            go.transform.localRotation = Quaternion.FromToRotation(Vector3.up, offset);
            go.transform.localScale = new Vector3(radius * 2f, offset.magnitude * 0.5f, radius * 2f);
            go.GetComponent<Renderer>().sharedMaterial = material;
        }

        // The render pipeline's default lit material (works with the built-in pipeline, URP and HDRP).
        private static Material DefaultMaterial()
        {
            GameObject probe = GameObject.CreatePrimitive(PrimitiveType.Quad);
            Material material = probe.GetComponent<Renderer>().sharedMaterial;
            Destroy(probe);
            return material;
        }

        private Material Tint(Material template, Color color)
        {
            var material = new Material(template) { color = color };
            _materials.Add(material);
            return material;
        }
    }
}
