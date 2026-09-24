using UnityEngine;

namespace AutoRig.Cloth
{
    /// <summary>
    /// Creates and destroys objects on behalf of the setup tools (manifest loader, humanoid
    /// collider builder, chain detector). The editor supplies an implementation that records
    /// Undo; at runtime <see cref="RuntimeClothSetupHandler"/> is used.
    /// </summary>
    public interface IClothSetupHandler
    {
        /// <summary>Creates a GameObject under <paramref name="parent"/> (local transform reset).</summary>
        GameObject CreateGameObject(string name, Transform parent, bool active);

        /// <summary>Adds a component to <paramref name="target"/>.</summary>
        T AddComponent<T>(GameObject target) where T : Component;

        /// <summary>Destroys a GameObject or a component.</summary>
        void DestroyObject(Object target);

        /// <summary>Called before an existing object is modified.</summary>
        void RecordObject(Object target);
    }

    /// <summary>
    /// Plain runtime implementation of <see cref="IClothSetupHandler"/>: no Undo. Objects destroyed
    /// in Play mode are deactivated first, so they stop simulating immediately.
    /// </summary>
    public sealed class RuntimeClothSetupHandler : IClothSetupHandler
    {
        /// <summary>The shared instance.</summary>
        public static readonly RuntimeClothSetupHandler Instance = new RuntimeClothSetupHandler();

        private RuntimeClothSetupHandler()
        {
        }

        /// <inheritdoc />
        public GameObject CreateGameObject(string name, Transform parent, bool active)
        {
            var go = new GameObject(name);
            go.SetActive(active);
            go.transform.SetParent(parent, false);
            return go;
        }

        /// <inheritdoc />
        public T AddComponent<T>(GameObject target) where T : Component => target.AddComponent<T>();

        /// <inheritdoc />
        public void DestroyObject(Object target)
        {
            if (target == null)
            {
                return;
            }

            if (!Application.isPlaying)
            {
                Object.DestroyImmediate(target);
                return;
            }

            if (target is GameObject go)
            {
                go.SetActive(false);
            }
            else if (target is Behaviour behaviour)
            {
                behaviour.enabled = false;
            }

            Object.Destroy(target);
        }

        /// <inheritdoc />
        public void RecordObject(Object target)
        {
        }
    }
}
