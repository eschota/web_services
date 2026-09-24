using UnityEngine;

namespace AutoRig.Cloth
{
    /// <summary>
    /// Bookkeeping for objects created by the setup tools. Every generated group and collider
    /// records which tool made it, so re-running a tool replaces exactly its own output and never
    /// touches components made by hand.
    /// </summary>
    public static class GeneratedObjects
    {
        /// <summary>Name of the child object that holds generated groups.</summary>
        public const string ContainerName = "AutoRigCloth";

        /// <summary>The character's <see cref="ContainerName"/> child, or null.</summary>
        public static Transform FindContainer(Transform characterRoot)
        {
            for (int i = 0; i < characterRoot.childCount; i++)
            {
                Transform child = characterRoot.GetChild(i);
                if (child.name == ContainerName)
                {
                    return child;
                }
            }

            return null;
        }

        /// <summary>Returns the character's <see cref="ContainerName"/> child, creating it when missing.</summary>
        public static Transform GetOrCreateContainer(Transform characterRoot, IClothSetupHandler handler)
        {
            Transform container = FindContainer(characterRoot);
            return container != null ? container : handler.CreateGameObject(ContainerName, characterRoot, true).transform;
        }

        /// <summary>
        /// Destroys every group and collider under <paramref name="characterRoot"/> (inactive ones
        /// included) that <paramref name="generator"/> created. A group's GameObject is destroyed too
        /// when the tool created it and nothing else lives on it.
        /// </summary>
        /// <returns>Number of destroyed objects.</returns>
        public static int Remove(Transform characterRoot, string generator, IClothSetupHandler handler)
        {
            int removed = 0;
            ClothCollider[] colliders = characterRoot.GetComponentsInChildren<ClothCollider>(true);
            for (int i = 0; i < colliders.Length; i++)
            {
                if (colliders[i].GeneratedBy == generator)
                {
                    handler.DestroyObject(colliders[i]);
                    removed++;
                }
            }

            ClothBoneGroup[] groups = characterRoot.GetComponentsInChildren<ClothBoneGroup>(true);
            for (int i = 0; i < groups.Length; i++)
            {
                ClothBoneGroup group = groups[i];
                if (group.GeneratedBy != generator)
                {
                    continue;
                }

                GameObject go = group.gameObject;
                bool destroyObject = group.OwnsGameObject && go.transform.childCount == 0 && go.GetComponents<Component>().Length == 2;
                handler.DestroyObject(destroyObject ? (Object)go : group);
                removed++;
            }

            return removed;
        }
    }
}
