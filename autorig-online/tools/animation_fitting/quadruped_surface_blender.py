"""Read evaluated mesh positions in actor-local coordinates without Python vectors per vertex."""
import bpy
import numpy as np


def actor_local_points(arm, mesh_names):
    graph = bpy.context.evaluated_depsgraph_get()
    inverse = arm.matrix_world.inverted()
    chunks = []
    for name in mesh_names:
        obj = bpy.data.objects[name].evaluated_get(graph)
        mesh = obj.to_mesh()
        try:
            values = np.empty(len(mesh.vertices)*3, dtype=np.float32)
            mesh.vertices.foreach_get('co', values)
            matrix = np.asarray(inverse @ obj.matrix_world, dtype=np.float64)
            chunks.append(values.reshape(-1,3) @ matrix[:3,:3].T + matrix[:3,3])
        finally:
            obj.to_mesh_clear()
    if not chunks or sum(len(c) for c in chunks) == 0:
        raise ValueError('No evaluated actor surface')
    return np.concatenate(chunks)
