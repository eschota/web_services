import { fitBrowserAnimation } from './animation-fitting-browser-core.js';

const OBSERVATION_SCHEMA = 'autorig-fitting-observations.v1';
const SKELETON_SCHEMA = 'autorig-browser-fitting-skeleton.v1';
const BRIDGE_SCHEMA = 'autorig-browser-rgb-observation-bridge.v1';
const TAPNEXT_BACKEND = 'google-deepmind-tapnextpp-online';
const SHA256_PATTERN = /^[0-9a-f]{64}$/;

function object(value, field) {
    if (!value || typeof value !== 'object' || Array.isArray(value)) {
        throw new Error(`${field} must be an object`);
    }
    return value;
}

function finite(value, field) {
    const number = Number(value);
    if (!Number.isFinite(number)) throw new Error(`${field} must be finite`);
    return number;
}

function positiveInteger(value, field, minimum = 1) {
    if (!Number.isInteger(value) || value < minimum) {
        throw new Error(`${field} must be an integer of at least ${minimum}`);
    }
    return value;
}

function nonEmptyString(value, field) {
    if (typeof value !== 'string' || !value) throw new Error(`${field} must be a non-empty string`);
    return value;
}

function sha256(value, field) {
    const result = nonEmptyString(value, field);
    if (!SHA256_PATTERN.test(result)) throw new Error(`${field} must be a lowercase SHA-256`);
    return result;
}

function resolution(value, field) {
    if (!Array.isArray(value) || value.length !== 2) {
        throw new Error(`${field} must be a width/height pair`);
    }
    return value.map((item, index) => positiveInteger(item, `${field}[${index}]`));
}

function samePair(first, second) {
    return first[0] === second[0] && first[1] === second[1];
}

function normalizePoint(pointValue, field, frameCount) {
    const point = object(pointValue, field);
    const frame = point.frame;
    if (!Number.isInteger(frame) || frame < 0 || frame >= frameCount) {
        throw new Error(`${field}.frame is outside [0, ${frameCount - 1}]`);
    }
    if (typeof point.visible !== 'boolean') throw new Error(`${field}.visible must be boolean`);
    const confidence = finite(point.confidence ?? (point.visible ? 1 : 0), `${field}.confidence`);
    if (confidence < 0) throw new Error(`${field}.confidence must not be negative`);
    return {
        frame,
        x: finite(point.x, `${field}.x`),
        y: finite(point.y, `${field}.y`),
        visible: point.visible,
        confidence,
    };
}

function splitRigAnchor(anchorId, field) {
    const value = nonEmptyString(anchorId, field);
    const separator = value.lastIndexOf(':');
    if (separator <= 0 || !/^\d+$/.test(value.slice(separator + 1))) {
        throw new Error(`${field} must use the immutable bone:vertex anchor format`);
    }
    return { bone: value.slice(0, separator), vertex: Number(value.slice(separator + 1)) };
}

function normalizeTracks(value, frameCount) {
    if (!Array.isArray(value) || !value.length) throw new Error('observations.tracks must not be empty');
    const trackIds = new Set();
    const anchorIds = new Set();
    const tracks = value.map((trackValue, trackIndex) => {
        const field = `observations.tracks[${trackIndex}]`;
        const track = object(trackValue, field);
        const id = nonEmptyString(track.id, `${field}.id`);
        if (trackIds.has(id)) throw new Error(`duplicate observation track ID ${id}`);
        trackIds.add(id);
        const anchorId = nonEmptyString(track.anchor_id, `${field}.anchor_id`);
        if (anchorIds.has(anchorId)) throw new Error(`duplicate observation anchor ID ${anchorId}`);
        anchorIds.add(anchorId);
        const sourceAnchor = splitRigAnchor(anchorId, `${field}.anchor_id`);
        const queryFrame = track.query_frame;
        if (!Number.isInteger(queryFrame) || queryFrame < 0 || queryFrame >= frameCount) {
            throw new Error(`${field}.query_frame is outside [0, ${frameCount - 1}]`);
        }
        if (!Array.isArray(track.points) || track.points.length !== frameCount) {
            throw new Error(`${field}.points must contain exactly ${frameCount} frames`);
        }
        const byFrame = new Map();
        track.points.forEach((point, pointIndex) => {
            const normalized = normalizePoint(point, `${field}.points[${pointIndex}]`, frameCount);
            if (byFrame.has(normalized.frame)) {
                throw new Error(`${field}.points repeats frame ${normalized.frame}`);
            }
            byFrame.set(normalized.frame, normalized);
        });
        const points = Array.from({ length: frameCount }, (_, frame) => {
            const point = byFrame.get(frame);
            if (!point) throw new Error(`${field}.points is missing frame ${frame}`);
            return point;
        });
        return { id, anchorId, sourceAnchor, queryFrame, points };
    });
    return { tracks, anchorIds };
}

function normalizeContacts(value, frameCount, sourceAnchorIds) {
    if (value == null) return [];
    if (!Array.isArray(value)) throw new Error('observations.contacts must be an array');
    return value.map((contactValue, contactIndex) => {
        const field = `observations.contacts[${contactIndex}]`;
        const contact = object(contactValue, field);
        const anchorId = nonEmptyString(contact.anchor_id, `${field}.anchor_id`);
        if (!sourceAnchorIds.has(anchorId)) {
            throw new Error(`${field}.anchor_id does not reference an observation track`);
        }
        if (!Array.isArray(contact.frames) || !contact.frames.length) {
            throw new Error(`${field}.frames must not be empty`);
        }
        const frames = [...new Set(contact.frames.map((frame) => {
            if (!Number.isInteger(frame) || frame < 0 || frame >= frameCount) {
                throw new Error(`${field}.frames contains an invalid frame`);
            }
            return frame;
        }))].sort((first, second) => first - second);
        const result = { anchorId, frames };
        if ('ground_height' in contact) result.groundHeight = finite(contact.ground_height, `${field}.ground_height`);
        if ('weight' in contact) {
            result.weight = finite(contact.weight, `${field}.weight`);
            if (result.weight <= 0) throw new Error(`${field}.weight must be positive`);
        }
        return result;
    });
}

function normalizeCameraContract(value, skeleton, observations) {
    const contract = object(value, 'cameraContract');
    const projection = object(skeleton.projection, 'skeleton.projection');
    const skeletonResolution = resolution(projection.outputResolution, 'skeleton.projection.outputResolution');
    const contractResolution = resolution(
        contract.outputResolution ?? skeletonResolution,
        'cameraContract.outputResolution',
    );
    if (!samePair(contractResolution, skeletonResolution)) {
        throw new Error('cameraContract.outputResolution does not match the Three adapter projection');
    }
    const observationResolution = [observations.width, observations.height];
    if (!samePair(observationResolution, skeletonResolution)) {
        throw new Error(
            `observation camera ${observationResolution.join('x')} does not match adapter output ${skeletonResolution.join('x')}`,
        );
    }
    const bundleSha256 = sha256(contract.bundleSha256, 'cameraContract.bundleSha256');
    const immutableManifestSha256 = sha256(
        contract.immutableManifestSha256,
        'cameraContract.immutableManifestSha256',
    );
    const provenance = object(observations.provenance, 'observations.provenance');
    if (provenance.bundle_sha256 !== bundleSha256) {
        throw new Error('observations bundle SHA-256 does not match the pinned camera contract');
    }
    if (provenance.immutable_manifest_sha256 !== immutableManifestSha256) {
        throw new Error('observations immutable-manifest SHA-256 does not match the pinned camera contract');
    }
    return { outputResolution: skeletonResolution, bundleSha256, immutableManifestSha256 };
}

function requiredSemanticTracks(skeleton) {
    const limbs = object(skeleton.limbs, 'skeleton.limbs');
    const requirements = [];
    const semanticIds = new Set();
    Object.entries(limbs).forEach(([label, limbValue]) => {
        const limb = object(limbValue, `skeleton.limbs.${label}`);
        if (!Array.isArray(limb.sourceBoneChain) || limb.sourceBoneChain.length < 3) {
            throw new Error(`skeleton limb ${label} needs the Three adapter sourceBoneChain`);
        }
        const sourceBoneChain = limb.sourceBoneChain.map((bone, index) => (
            nonEmptyString(bone, `skeleton.limbs.${label}.sourceBoneChain[${index}]`)
        ));
        const trackedJointIndex = limb.trackedJointIndex;
        if (!Number.isInteger(trackedJointIndex) || trackedJointIndex < 1 || trackedJointIndex >= sourceBoneChain.length - 1) {
            throw new Error(`skeleton limb ${label} has an incompatible trackedJointIndex`);
        }
        const terminalBone = nonEmptyString(limb.terminalBone, `skeleton.limbs.${label}.terminalBone`);
        if (terminalBone !== sourceBoneChain.at(-1)) {
            throw new Error(`skeleton limb ${label} terminalBone does not match sourceBoneChain`);
        }
        [
            [nonEmptyString(limb.proximalTrack, `skeleton.limbs.${label}.proximalTrack`), sourceBoneChain[0]],
            [nonEmptyString(limb.jointTrack, `skeleton.limbs.${label}.jointTrack`), sourceBoneChain[trackedJointIndex]],
            [nonEmptyString(limb.hoofTrack, `skeleton.limbs.${label}.hoofTrack`), terminalBone],
        ].forEach(([semanticId, sourceBone]) => {
            if (semanticIds.has(semanticId)) throw new Error(`duplicate skeleton semantic track ${semanticId}`);
            semanticIds.add(semanticId);
            requirements.push({ label, semanticId, sourceBone });
        });
    });
    if (!requirements.length) throw new Error('skeleton.limbs must not be empty');
    return requirements;
}

function selectTracks(requirements, tracks, minimumVisiblePoints) {
    const selectedSourceAnchors = new Set();
    return requirements.map((requirement) => {
        const matches = tracks.filter((track) => track.sourceAnchor.bone === requirement.sourceBone);
        if (matches.length !== 1) {
            throw new Error(
                `${requirement.semanticId} requires exactly one RGB anchor on ${requirement.sourceBone}; found ${matches.length}`,
            );
        }
        const track = matches[0];
        if (selectedSourceAnchors.has(track.anchorId)) {
            throw new Error(`RGB anchor ${track.anchorId} is assigned to more than one semantic track`);
        }
        selectedSourceAnchors.add(track.anchorId);
        if (!track.points[track.queryFrame].visible) {
            throw new Error(`RGB track ${track.id} is not visible on its query frame`);
        }
        const visiblePoints = track.points.filter((point) => point.visible).length;
        if (visiblePoints < minimumVisiblePoints) {
            throw new Error(`RGB track ${track.id} has only ${visiblePoints} visible frames`);
        }
        return { ...requirement, track };
    });
}

/**
 * Convert the immutable-bundle TAPNext++ RGB tracks into the semantic track IDs
 * consumed by fitBrowserAnimation. The Three adapter's sourceBoneChain is the
 * only mapping authority; no color-marker or name-order heuristic is used.
 */
export function prepareRgbObservationsForBrowser({
    observations: observationValue,
    skeleton: skeletonValue,
    cameraContract,
    minimumVisiblePoints = 2,
} = {}) {
    const observations = object(observationValue, 'observations');
    if (observations.schema !== OBSERVATION_SCHEMA) {
        throw new Error(`observations.schema must be ${OBSERVATION_SCHEMA}`);
    }
    const skeleton = object(skeletonValue, 'skeleton');
    if (skeleton.schema !== SKELETON_SCHEMA) {
        throw new Error(`skeleton.schema must be ${SKELETON_SCHEMA}`);
    }
    const frameCount = positiveInteger(observations.frame_count, 'observations.frame_count', 2);
    const width = positiveInteger(observations.width, 'observations.width');
    const height = positiveInteger(observations.height, 'observations.height');
    const fps = finite(observations.fps, 'observations.fps');
    if (fps <= 0) throw new Error('observations.fps must be positive');
    const minimumVisible = positiveInteger(minimumVisiblePoints, 'minimumVisiblePoints');
    if (minimumVisible > frameCount) throw new Error('minimumVisiblePoints exceeds frame_count');

    const provenance = object(observations.provenance, 'observations.provenance');
    const tracker = object(provenance.tracker, 'observations.provenance.tracker');
    if (tracker.backend !== TAPNEXT_BACKEND) {
        throw new Error(`observations tracker backend must be ${TAPNEXT_BACKEND}`);
    }
    const camera = normalizeCameraContract(cameraContract, skeleton, { ...observations, width, height });
    const normalized = normalizeTracks(observations.tracks, frameCount);
    const contacts = normalizeContacts(observations.contacts, frameCount, normalized.anchorIds);
    const selected = selectTracks(
        requiredSemanticTracks(skeleton),
        normalized.tracks,
        minimumVisible,
    );
    const semanticBySourceAnchor = new Map(selected.map((item) => [item.track.anchorId, item.semanticId]));

    return {
        schema: OBSERVATION_SCHEMA,
        frame_count: frameCount,
        width,
        height,
        fps,
        tracks: selected.map(({ semanticId, track }) => ({
            id: track.id,
            anchor_id: semanticId,
            query_frame: track.queryFrame,
            points: track.points.map((point) => ({ ...point })),
        })),
        silhouettes: [],
        depth: [],
        contacts: contacts
            .filter((contact) => semanticBySourceAnchor.has(contact.anchorId))
            .map((contact) => ({
                anchor_id: semanticBySourceAnchor.get(contact.anchorId),
                frames: [...contact.frames],
                ...(contact.groundHeight == null ? {} : { ground_height: contact.groundHeight }),
                ...(contact.weight == null ? {} : { weight: contact.weight }),
            })),
        provenance: {
            ...provenance,
            browser_rgb_bridge: {
                schema: BRIDGE_SCHEMA,
                trackerBackend: TAPNEXT_BACKEND,
                camera,
                mappings: selected.map(({ label, semanticId, sourceBone, track }) => ({
                    limb: label,
                    semanticAnchorId: semanticId,
                    sourceBone,
                    sourceTrackId: track.id,
                    sourceAnchorId: track.anchorId,
                })),
            },
        },
    };
}

/** Validate/bridge the RGB observations before invoking the pure browser solver. */
export function fitRgbObservationsInBrowser({
    observations,
    skeleton,
    cameraContract,
    minimumVisiblePoints = 2,
    options = {},
} = {}) {
    const prepared = prepareRgbObservationsForBrowser({
        observations,
        skeleton,
        cameraContract,
        minimumVisiblePoints,
    });
    return fitBrowserAnimation({ skeleton, observations: prepared, options });
}

export const RGB_OBSERVATION_BRIDGE_CONTRACT = Object.freeze({
    schema: BRIDGE_SCHEMA,
    observations: OBSERVATION_SCHEMA,
    skeleton: SKELETON_SCHEMA,
    trackerBackend: TAPNEXT_BACKEND,
});
