using UnityEditor;

namespace AutoRig.Cloth.EditorTools
{
    /// <summary>
    /// Scene-view helpers: selecting a character's <see cref="AutoRigClothSetup"/> shows all of its
    /// cloth groups (labelled at their attach bones) and colliders at once, so the whole setup can be
    /// checked at a glance.
    /// </summary>
    internal static class ClothGizmoDrawers
    {
        [DrawGizmo(GizmoType.Selected | GizmoType.Active)]
        private static void DrawCharacter(AutoRigClothSetup setup, GizmoType gizmoType)
        {
            ClothCollider[] colliders = setup.GetComponentsInChildren<ClothCollider>();
            for (int i = 0; i < colliders.Length; i++)
            {
                ClothGizmos.DrawCollider(colliders[i], ClothGizmos.ColliderColor);
            }

            ClothBoneGroup[] groups = setup.GetComponentsInChildren<ClothBoneGroup>();
            for (int i = 0; i < groups.Length; i++)
            {
                ClothGizmos.DrawGroup(groups[i]);
                if (groups[i].AttachBone != null)
                {
                    Handles.Label(groups[i].AttachBone.position, groups[i].name);
                }
            }
        }
    }
}
