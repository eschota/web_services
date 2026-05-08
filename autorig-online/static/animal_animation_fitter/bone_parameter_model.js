/**
 * Heuristic bone grouping for quadruped / common animal rigs.
 * MVP: only rotation, enabled subset by quality.
 */

const GROUPS = {
    core: ['root', 'pelvis', 'hip', 'spine', 'chest', 'neck', 'head'],
    tail: ['tail'],
    leg: ['leg', 'thigh', 'calf', 'knee', 'ankle', 'foot', 'paw', 'toe', 'femur', 'tibia', 'clavicle', 'scapula', 'arm', 'elbow', 'wrist', 'shoulder', 'humerus', 'radius', 'ulna', 'claw'],
    ear: ['ear'],
    jaw: ['jaw', 'mouth'],
    wing: ['wing'],
};

function lower(s) {
    return String(s || '').toLowerCase();
}

function detectGroup(boneName) {
    const n = lower(boneName);
    for (const g of Object.keys(GROUPS)) {
        for (const token of GROUPS[g]) {
            if (n.includes(token)) return g;
        }
    }
    return 'other';
}

export class BoneParameterModel {
    /**
     * @param {import('three').SkinnedMesh} skinned
     * @param {{ quality_level_string?: string }} options
     */
    constructor(skinned, options = {}) {
        this.skinned = skinned;
        this.skeleton = skinned.skeleton;
        this.bones = this.skeleton?.bones || [];
        this.quality = options.quality_level_string || 'balanced';
        this.parameters = [];
        this._build();
    }

    _limitForGroup(group) {
        const rad = Math.PI * 0.35;
        return {
            rotation_x_min_float: -rad,
            rotation_x_max_float: rad,
            rotation_y_min_float: -rad,
            rotation_y_max_float: rad,
            rotation_z_min_float: -rad,
            rotation_z_max_float: rad,
        };
    }

    _build() {
        const quality = this.quality;
        this.parameters = [];
        for (let i = 0; i < this.bones.length; i++) {
            const bone = this.bones[i];
            const name = bone.name || `Bone_${i}`;
            const group = detectGroup(name);
            let enabled = false;
            if (quality === 'high') {
                enabled = group !== 'other' || i < 40;
            } else if (quality === 'balanced') {
                enabled = ['core', 'tail', 'leg'].includes(group);
            } else {
                enabled = ['core', 'tail'].includes(group);
            }
            // MVP force spine/head/tail always try core+tail
            if (group === 'core' || group === 'tail') enabled = true;

            const lim = this._limitForGroup(group);
            this.parameters.push({
                bone_name_string: name,
                bone_ref: bone,
                parent_name_string: bone.parent?.name || '',
                group_string: group,
                enabled_bool: enabled,
                weight_float: group === 'core' ? 1 : group === 'tail' ? 0.85 : 0.7,
                default_rotation_euler: bone.rotation.clone(),
                current_rotation_euler: bone.rotation.clone(),
                ...lim,
            });
        }
    }

    /** MVP subset */
    getActiveBoneParameters() {
        return this.parameters.filter((p) => p.enabled_bool);
    }
}
