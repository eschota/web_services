"""Rest-relative escape screening, shared NumPy/CuPy math; never a whole-rig acceptance.

A neighborhood residual removes local coherent motion. Allowance includes twice
rest separation (rotation of any magnitude) plus an explicit motion margin.
Small disconnected components use rest-near anchors in OTHER components;
component radius grants articulation room. Root/world travel cancels in both.
No geometry is removed. Reports retain vertex, mesh, primitive, time and allowance.
"""
from __future__ import annotations
import itertools
import numpy as np

SCHEMA = 'autorig.deformation-probe/1'
DEFAULTS = {'edge_ratio': 4.0, 'growth_fraction': .005,
            'vertex_motion_fraction': .03, 'component_motion_fraction': .08,
            'contact_fraction': .02}


def validate_layout(document):
    """Do not silently truncate float bone indices or accept missing bind bytes."""
    accessors = document.get('accessors', [])
    def get(index):
        if not isinstance(index,int) or not 0 <= index < len(accessors):
            raise ValueError('invalid_accessor_index')
        return accessors[index]
    for skin in document.get('skins', []):
        ibm = get(skin.get('inverseBindMatrices'))
        if ibm.get('type') != 'MAT4' or ibm.get('componentType') != 5126 or ibm.get('normalized'):
            raise ValueError('invalid_inverse_bind_accessor')
    for node in document.get('nodes', []):
        if 'skin' not in node or 'mesh' not in node:
            continue
        for prim in document['meshes'][node['mesh']]['primitives']:
            attrs = prim.get('attributes', {})
            joints = get(attrs.get('JOINTS_0')); weights = get(attrs.get('WEIGHTS_0'))
            if joints.get('componentType') not in (5121,5123) or joints.get('type') != 'VEC4' or joints.get('normalized'):
                raise ValueError('invalid_joint_accessor_type')
            if weights.get('type') != 'VEC4' or not (weights.get('componentType') == 5126 or
                    weights.get('componentType') in (5121,5123) and weights.get('normalized') is True):
                raise ValueError('invalid_weight_accessor_type')


def components(count, edges):
    parent = np.arange(count)
    def root(i):
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = int(parent[i])
        return i
    for a, b in edges:
        a, b = root(int(a)), root(int(b))
        if a != b:
            parent[b] = a
    labels = np.array([root(i) for i in range(count)])
    return np.unique(labels, return_inverse=True)[1]


class Probe:
    def __init__(self, rows, scale, *, backend='numpy', parameters=None):
        self.parameters = {**DEFAULTS, **(parameters or {})}
        if set(self.parameters) != set(DEFAULTS) or any(not np.isfinite(v) or v <= 0 for v in self.parameters.values()):
            raise ValueError('positive finite explicit probe thresholds required')
        if self.parameters['edge_ratio'] <= 1:
            raise ValueError('edge_ratio must exceed one')
        self.backend, self.scale, self.rows = backend, float(scale), rows
        self.xp = np
        self.device = 'CPU'
        if backend == 'cupy':
            import cupy as cp
            self.xp = cp
            self.device = cp.cuda.runtime.getDeviceProperties(cp.cuda.Device().id)['name'].decode()
        elif backend != 'numpy':
            raise ValueError('unsupported probe backend')
        self.rest = np.concatenate([r['rest'] for r in rows])
        self.height = float(np.ptp(self.rest[:, 1])) or self.scale
        self.vertex_layer = np.concatenate([np.full(len(r['rest']), k) for k, r in enumerate(rows)])
        self.edges = np.concatenate([r['edges'] + r['offset'] for r in rows])
        self.lengths = np.linalg.norm(self.rest[self.edges[:, 0]] - self.rest[self.edges[:, 1]], axis=1)
        self.adj = np.concatenate([self.edges, self.edges[:, ::-1]])
        self.degree = np.bincount(self.adj[:, 0], minlength=len(self.rest))
        self.radius = np.zeros(len(self.rest))
        np.maximum.at(self.radius, self.adj[:, 0], np.linalg.norm(self.rest[self.adj[:, 0]]-self.rest[self.adj[:, 1]], axis=1))
        # Components remain distinct across primitives/material seams. Rest-near
        # anchor pairs make touching seams harmless unless they actually separate.
        self.comp = []
        self.vertex_comp = np.zeros(len(self.rest), int)
        for row in rows:
            labels = components(len(row['rest']), row['edges'])
            for label in np.unique(labels):
                ids = np.flatnonzero(labels == label) + row['offset']
                center = self.rest[ids].mean(0)
                radius = float(np.linalg.norm(self.rest[ids] - center, axis=1).max(initial=0))
                self.vertex_comp[ids] = len(self.comp)
                self.comp.append({'ids': ids, 'radius': radius, 'row': int(self.vertex_layer[ids[0]])})
        cell = self.parameters['contact_fraction'] * self.scale
        grid = {}
        keys = np.floor(self.rest / cell).astype(int)
        for i, key in enumerate(keys):
            grid.setdefault(tuple(key), []).append(i)
        self.contacts = []
        for ci, comp in enumerate(self.comp):
            # Bounded representatives; full edge/vertex pass remains exhaustive.
            ids = comp['ids']
            pick = ids[np.unique(np.linspace(0, len(ids)-1, min(64, len(ids))).astype(int))]
            pairs = []
            for v in pick:
                candidates = [u for delta in itertools.product((-1,0,1), repeat=3)
                              for u in grid.get(tuple(keys[v]+delta), []) if self.vertex_comp[u] != ci]
                if not candidates:
                    continue
                dst = np.linalg.norm(self.rest[candidates]-self.rest[v], axis=1)
                j = int(np.argmin(dst))
                if dst[j] <= cell:
                    pairs.append((int(v), candidates[j], float(dst[j])))
            if pairs:
                # Several anchors, not one lucky point; preserve pair attribution.
                self.contacts.append((ci, pairs))
        xp = self.xp
        self.g_edges = xp.asarray(self.edges)
        self.g_lengths = xp.asarray(self.lengths)
        self.g_adj = xp.asarray(self.adj)
        self.g_degree = xp.asarray(np.maximum(self.degree, 1))
        self.g_rest = xp.asarray(self.rest)
        self.g_radius = xp.asarray(self.radius)
        self.g_rows = [(xp.asarray(r['hom']), xp.asarray(r['weights']), xp.asarray(r['joints']),
                        xp.asarray(r['ibms'])) for r in rows]
        pairs = [(ci,v,u,d) for ci, contacts in self.contacts for v,u,d in contacts]
        self.contact_comp = np.array([ci for ci,v,u,d in pairs], dtype=int)
        self.g_contact_comp = xp.asarray(self.contact_comp)
        self.g_contact_a = xp.asarray([v for ci,v,u,d in pairs], dtype=int)
        self.g_contact_b = xp.asarray([u for ci,v,u,d in pairs], dtype=int)
        self.g_contact_rest = xp.asarray([d for ci,v,u,d in pairs])
        self.g_contact_limit = xp.asarray([3*d+2*self.comp[ci]['radius']+self.parameters['component_motion_fraction']*self.scale for ci,v,u,d in pairs])

    def host(self, value):
        return self.xp.asnumpy(value) if self.backend == 'cupy' else np.asarray(value)

    def pose(self, worlds):
        xp = self.xp
        out = []
        for row, (hom, weights, joints, ibms) in zip(self.rows, self.g_rows):
            matrices = xp.asarray([worlds[n] for n in row['joint_nodes']]) @ ibms
            pos = xp.zeros((len(hom), 3))
            for slot in range(4):
                pos += weights[:, slot, None] * xp.einsum('nij,nj->ni', matrices[joints[:,slot]], hom)[:, :3]
            out.append(pos)
        return xp.concatenate(out)

    def proof(self, vertex, pos):
        layer = int(self.vertex_layer[vertex]); row = self.rows[layer]; local = int(vertex-row['offset'])
        return {'global_vertex': int(vertex), 'vertex': local, 'mesh': row['mesh_id'],
                'node': row['node'], 'primitive': row['primitive_id'],
                'rest': self.rest[vertex].tolist(), 'posed': pos[vertex].tolist(),
                'joints': row['joints'][local].tolist(), 'weights': row['weights'][local].tolist(),
                'joint_names': [row['joint_names'][j] for j in row['joints'][local]]}

    def check(self, pos, *, time=0.0, frame=None, details=True):
        xp = self.xp; par = self.parameters; edges = self.g_edges
        finite = xp.isfinite(pos).all(1)
        lengths = xp.linalg.norm(pos[edges[:,0]]-pos[edges[:,1]], axis=1)
        growth = lengths-self.g_lengths
        # Exact old rig_judge threshold for parity, INCLUDING its short-edge filter.
        old = (self.g_lengths > .001*self.height) & (lengths > 4*self.g_lengths) & (growth > .005*self.height)
        # Tiny rest edges can produce a huge fin too; absolute growth guards noise.
        bad_edges = (lengths > par['edge_ratio']*self.g_lengths) & (growth > par['growth_fraction']*self.scale)
        movement = pos-self.g_rest
        mean = xp.zeros_like(movement)
        xp.add.at(mean, self.g_adj[:,0], movement[self.g_adj[:,1]])
        mean /= self.g_degree[:,None]
        residual = xp.linalg.norm(movement-mean, axis=1)
        allowance = 2*self.g_radius + par['vertex_motion_fraction']*self.scale
        outliers = (residual > allowance) & xp.asarray(self.degree > 0)
        # Distance tests cancel common translation/rotation without a world AABB.
        detached = []
        if len(self.contact_comp):
            gaps = xp.linalg.norm(pos[self.g_contact_a]-pos[self.g_contact_b], axis=1)
            margins = gaps-self.g_contact_limit
            minimum = xp.full(len(self.comp), xp.inf)
            xp.minimum.at(minimum, self.g_contact_comp, margins)
            hm = self.host(minimum)
            hits = np.flatnonzero(np.isfinite(hm) & (hm > 0))
            if len(hits):
                hg, hl, hr, hv = map(self.host, (gaps,self.g_contact_limit,self.g_contact_rest,margins))
                ha, hb = map(self.host, (self.g_contact_a,self.g_contact_b))
                for ci in hits:
                    ids = np.flatnonzero(self.contact_comp == ci)
                    k = int(ids[np.argmax(hv[ids])])
                    detached.append({'component': int(ci), 'mesh': self.rows[self.comp[ci]['row']]['mesh_id'],
                                     'vertices': len(self.comp[ci]['ids']), 'anchor_vertices': [int(ha[k]),int(hb[k])],
                                     'gap': float(hg[k]), 'rest_gap': float(hr[k]), 'allowed_gap': float(hl[k])})
        eids = self.host(xp.flatnonzero(bad_edges)).astype(int)
        vids = self.host(xp.flatnonzero(outliers | ~finite)).astype(int)
        suspects = np.unique(np.concatenate([vids, self.edges[eids].ravel()]))
        result = {'time': float(time), 'frame': frame, 'nonfinite_vertices': int(self.host((~finite).sum())),
                  'old_stretched_edges': int(self.host(old.sum())), 'stretched_edges': len(eids),
                  'outlier_vertices': len(vids), 'detached_components': detached,
                  'affected_vertices': len(suspects), 'vertex_ids': suspects.tolist(), 'passed': not (len(eids) or len(vids) or detached)}
        result['meshes'] = []
        for ri, row in enumerate(self.rows):
            lo, hi = row['offset'], row['offset'] + len(row['rest'])
            count = int(((suspects >= lo) & (suspects < hi)).sum())
            ec = int(((self.edges[eids,0] >= lo) & (self.edges[eids,0] < hi)).sum())
            result['meshes'].append({'mesh': row['mesh_id'], 'node': row['node'],
                                     'primitive': row['primitive_id'], 'affected_vertices': count,
                                     'stretched_edges': ec})
        if details and len(suspects):
            hp = self.host(pos)
            result['vertices'] = [{**self.proof(int(v),hp), 'residual': float(self.host(residual[v])),
                                   'allowed_residual': float(self.host(allowance[v]))} for v in suspects]
        return result
