using UnityEngine;

namespace AutoRig.Cloth
{
    /// <summary>
    /// One-stop setup for an AutoRig character: holds the <c>.autorig-cloth.json</c> manifest written
    /// next to the model and builds the cloth from it, either in the editor (inspector button) or
    /// when the object awakes. Put it on the character root.
    /// </summary>
    [AddComponentMenu("AutoRig Cloth/AutoRig Cloth Setup")]
    [DisallowMultipleComponent]
    public sealed class AutoRigClothSetup : MonoBehaviour
    {
        [SerializeField]
        [Tooltip("The <model>.autorig-cloth.json manifest AutoRig writes next to the rigged model.")]
        private TextAsset manifest;

        [SerializeField]
        [Tooltip("Build the cloth from the manifest when this object awakes (for characters spawned at runtime). Turn off when the setup was applied in the editor and saved with the prefab.")]
        private bool applyOnAwake = true;

        /// <summary>The manifest to apply.</summary>
        public TextAsset Manifest
        {
            get => manifest;
            set => manifest = value;
        }

        /// <summary>Whether <see cref="Apply()"/> runs in <c>Awake</c>.</summary>
        public bool ApplyOnAwake
        {
            get => applyOnAwake;
            set => applyOnAwake = value;
        }

        /// <summary>Result of the last <see cref="Apply()"/> in this session, or null.</summary>
        public ClothSetupReport LastReport { get; private set; }

        /// <summary>Builds the cloth from <see cref="Manifest"/> (runtime object creation, no Undo).</summary>
        public ClothSetupReport Apply() => Apply(null);

        /// <summary>Builds the cloth from <see cref="Manifest"/>, creating objects through <paramref name="handler"/>.</summary>
        public ClothSetupReport Apply(IClothSetupHandler handler)
        {
            if (manifest == null)
            {
                LastReport = new ClothSetupReport();
                LastReport.Errors.Add("No manifest is assigned.");
                return LastReport;
            }

            LastReport = ClothManifestLoader.Apply(gameObject, manifest.text, handler);
            return LastReport;
        }

        /// <summary>Snaps every cloth group of this character to its animated pose (after a teleport or a cut).</summary>
        public void ResetSimulation()
        {
            ClothBoneGroup[] groups = GetComponentsInChildren<ClothBoneGroup>();
            for (int i = 0; i < groups.Length; i++)
            {
                groups[i].ResetSimulation();
            }
        }

        private void Awake()
        {
            if (!applyOnAwake || !Application.isPlaying || manifest == null)
            {
                return;
            }

            ClothSetupReport report = Apply();
            if (!report.Succeeded || report.Warnings.Count > 0)
            {
                report.Log("Applying manifest '" + manifest.name + "' to '" + name + "'", this);
            }
        }
    }
}
