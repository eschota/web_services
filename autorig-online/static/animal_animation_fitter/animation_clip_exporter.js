import * as THREE from 'three';

/**
 * @param {Array<{ bones: Object, time_seconds_float: number }>} poseStates
 * @param {import('three').SkinnedMesh} skinned
 * @param {object} meta
 */
export function exportAnimationJson(poseStates, skinned, meta = {}) {
    const bones = skinned.skeleton.bones;
    const tracks = [];
    for (const bone of bones) {
        const name = bone.name;
        if (!name) continue;
        const times = [];
        const values = [];
        for (const ps of poseStates) {
            const br = ps.bones?.[name];
            if (!br) continue;
            times.push(ps.time_seconds_float || 0);
            const e = new THREE.Euler(
                br.rotation_x_float || 0,
                br.rotation_y_float || 0,
                br.rotation_z_float || 0,
                'XYZ'
            );
            const q = new THREE.Quaternion().setFromEuler(e);
            values.push(q.x, q.y, q.z, q.w);
        }
        if (times.length < 2) continue;
        tracks.push({
            bone_name_string: name,
            property_string: 'rotation',
            times_float_array: times,
            values_float_array: values,
        });
    }
    const duration = poseStates.length ? poseStates[poseStates.length - 1].time_seconds_float : 0;
    return {
        version_string: 'animal_idle_fit_v0001',
        fps_int: meta.fps_int || 24,
        duration_seconds_float: duration,
        source_video_url_string: meta.source_video_url_string || '',
        convergence_percent_float: meta.convergence_percent_float || 0,
        bone_tracks: tracks,
    };
}

/**
 * @returns {import('three').AnimationClip}
 */
export function buildThreeAnimationClip(poseStates, skinned, name = 'auto_fitted_idle') {
    const bones = skinned.skeleton.bones;
    const tracks = [];
    for (const bone of bones) {
        const bname = bone.name;
        if (!bname) continue;
        const times = [];
        const values = [];
        for (const ps of poseStates) {
            const br = ps.bones?.[bname];
            if (!br) continue;
            times.push(ps.time_seconds_float || 0);
            const e = new THREE.Euler(
                br.rotation_x_float || 0,
                br.rotation_y_float || 0,
                br.rotation_z_float || 0,
                'XYZ'
            );
            const q = new THREE.Quaternion().setFromEuler(e);
            values.push(q.x, q.y, q.z, q.w);
        }
        if (times.length < 2) continue;
        const track = new THREE.QuaternionKeyframeTrack(
            `${bname}.quaternion`,
            times,
            values
        );
        tracks.push(track);
    }
    const duration = poseStates.length ? Math.max(...poseStates.map((p) => p.time_seconds_float || 0)) : 0;
    return new THREE.AnimationClip(name, duration, tracks);
}
