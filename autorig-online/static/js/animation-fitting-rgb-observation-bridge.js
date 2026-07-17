import { fitBrowserAnimation } from './animation-fitting-browser-core.js?v=3';

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

function optionalUnitInterval(value, field) {
    if (value == null) return null;
    const number = finite(value, field);
    if (number < 0 || number > 1) throw new Error(`${field} must be between 0 and 1`);
    return number;
}

function confidenceThresholdsByChain(value, skeleton) {
    if (value == null) return {};
    const source = object(value, 'minimumVisibleConfidenceByChain');
    const labels = new Set([
        ...Object.keys(skeleton.limbs || {}),
        ...Object.keys(skeleton.auxiliaryChains || {}),
    ]);
    const result = {};
    Object.entries(source).forEach(([label, threshold]) => {
        if (!labels.has(label)) throw new Error(`minimumVisibleConfidenceByChain has unknown chain ${label}`);
        result[label] = optionalUnitInterval(
            threshold,
            `minimumVisibleConfidenceByChain.${label}`,
        );
        if (result[label] == null) {
            throw new Error(`minimumVisibleConfidenceByChain.${label} must not be null`);
        }
    });
    return result;
}

function optionalMaximumScale(value, field) {
    if (value == null) return null;
    const number = finite(value, field);
    if (number < 1) throw new Error(`${field} must be at least 1`);
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

function point2(value, field) {
    if (!Array.isArray(value) || value.length !== 2) {
        throw new Error(`${field} must be an x/y pair`);
    }
    return value.map((item, index) => finite(item, `${field}[${index}]`));
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
    const auxiliaryChains = skeleton.auxiliaryChains == null
        ? {}
        : object(skeleton.auxiliaryChains, 'skeleton.auxiliaryChains');
    const requirements = [];
    const semanticIds = new Set();
    const records = [
        ...Object.entries(limbs).map(([label, value]) => ({ collection: 'limbs', label, value })),
        ...Object.entries(auxiliaryChains).map(([label, value]) => ({ collection: 'auxiliaryChains', label, value })),
    ];
    const recordByLabel = new Map(records.map((record) => [record.label, record]));
    if (recordByLabel.size !== records.length) throw new Error('skeleton chain labels must be globally unique');
    records.forEach(({ collection, label, value: limbValue }) => {
        const limb = object(limbValue, `skeleton.${collection}.${label}`);
        if (!Array.isArray(limb.sourceBoneChain) || limb.sourceBoneChain.length < 2) {
            throw new Error(`skeleton chain ${label} needs the Three adapter sourceBoneChain`);
        }
        const sourceBoneChain = limb.sourceBoneChain.map((bone, index) => (
            nonEmptyString(bone, `skeleton.${collection}.${label}.sourceBoneChain[${index}]`)
        ));
        const sourceAnchorIds = limb.sourceAnchorIds == null
            ? null
            : limb.sourceAnchorIds.map((anchorId, index) => {
                const normalizedAnchorId = nonEmptyString(
                    anchorId,
                    `skeleton.${collection}.${label}.sourceAnchorIds[${index}]`,
                );
                const normalized = splitRigAnchor(
                    normalizedAnchorId,
                    `skeleton.${collection}.${label}.sourceAnchorIds[${index}]`,
                );
                if (normalized.bone !== sourceBoneChain[index]) {
                    throw new Error(`skeleton chain ${label} sourceAnchorIds do not match sourceBoneChain`);
                }
                return normalizedAnchorId;
            });
        if (sourceAnchorIds && sourceAnchorIds.length !== sourceBoneChain.length) {
            throw new Error(`skeleton chain ${label} sourceAnchorIds do not match sourceBoneChain`);
        }
        const trackedJointIndex = limb.trackedJointIndex;
        if (sourceBoneChain.length === 2) {
            if (trackedJointIndex != null) {
                throw new Error(`one-joint skeleton chain ${label} must use a null trackedJointIndex`);
            }
        } else if (!Number.isInteger(trackedJointIndex)
            || trackedJointIndex < 1
            || trackedJointIndex >= sourceBoneChain.length - 1) {
            throw new Error(`skeleton chain ${label} has an incompatible trackedJointIndex`);
        }
        const terminalBone = nonEmptyString(limb.terminalBone, `skeleton.${collection}.${label}.terminalBone`);
        if (terminalBone !== sourceBoneChain.at(-1)) {
            throw new Error(`skeleton chain ${label} terminalBone does not match sourceBoneChain`);
        }
        if (!Array.isArray(limb.joints) || limb.joints.length + 1 !== sourceBoneChain.length) {
            throw new Error(`skeleton chain ${label} joints do not match sourceBoneChain heads`);
        }
        const proximalTrack = nonEmptyString(
            limb.proximalTrack,
            `skeleton.${collection}.${label}.proximalTrack`,
        );
        const jointTrack = nonEmptyString(limb.jointTrack, `skeleton.${collection}.${label}.jointTrack`);
        const hoofTrack = nonEmptyString(limb.hoofTrack, `skeleton.${collection}.${label}.hoofTrack`);
        let branchConnector = null;
        if (limb.branchConnector != null) {
            const connector = object(limb.branchConnector, `skeleton.${collection}.${label}.branchConnector`);
            branchConnector = {
                schema: nonEmptyString(connector.schema, `${label}.branchConnector.schema`),
                bone: nonEmptyString(connector.bone, `${label}.branchConnector.bone`),
                fromChain: nonEmptyString(connector.fromChain, `${label}.branchConnector.fromChain`),
                fromHeadIndex: positiveInteger(connector.fromHeadIndex + 1, `${label}.branchConnector.fromHeadIndex+1`) - 1,
                toHeadIndex: positiveInteger(connector.toHeadIndex + 1, `${label}.branchConnector.toHeadIndex+1`) - 1,
            };
            const sourceRecord = recordByLabel.get(branchConnector.fromChain);
            const sourceBones = sourceRecord?.value?.sourceBoneChain;
            const isExactHeadEarConnector = collection === 'auxiliaryChains'
                && label === 'head_left_ear'
                && branchConnector.schema === 'autorig-browser-fitting-branch-connector.v1'
                && branchConnector.bone === 'head.x'
                && branchConnector.fromChain === 'body_neck_head'
                && branchConnector.toHeadIndex === 0
                && sourceBoneChain[0] === branchConnector.bone
                && Array.isArray(sourceBones)
                && branchConnector.fromHeadIndex === sourceBones.length - 1
                && sourceBones[branchConnector.fromHeadIndex] === branchConnector.bone;
            if (!isExactHeadEarConnector) {
                throw new Error(`unsupported skeleton branch connector on ${label}`);
            }
        }
        const orderedHeads = sourceBoneChain.map((sourceBone, headIndex) => {
            let semanticId = `${label}.deformHead.${headIndex}`;
            if (headIndex === 0) semanticId = proximalTrack;
            else if (headIndex === sourceBoneChain.length - 1) semanticId = hoofTrack;
            else if (headIndex === trackedJointIndex) semanticId = jointTrack;
            const restPoint = headIndex === sourceBoneChain.length - 1
                ? point2(limb.joints.at(-1)?.restEnd, `skeleton.limbs.${label}.joints[last].restEnd`)
                : point2(
                    limb.joints[headIndex]?.restStart,
                    `skeleton.limbs.${label}.joints[${headIndex}].restStart`,
                );
            return {
                label,
                semanticId,
                sourceBone,
                headIndex,
                orderedHeadCount: sourceBoneChain.length,
                restPoint,
                collection,
                branchConnector: branchConnector && headIndex === branchConnector.toHeadIndex
                    ? branchConnector
                    : null,
                sourceAnchorId: sourceAnchorIds?.[headIndex] || null,
            };
        });
        orderedHeads.forEach(({
            semanticId, sourceBone, headIndex, orderedHeadCount, restPoint,
            collection: chainCollection, branchConnector: connector, sourceAnchorId,
        }) => {
            if (semanticIds.has(semanticId)) throw new Error(`duplicate skeleton semantic track ${semanticId}`);
            semanticIds.add(semanticId);
            requirements.push({
                label,
                collection: chainCollection,
                semanticId,
                sourceBone,
                headIndex,
                orderedHeadCount,
                restPoint,
                branchConnector: connector,
                sourceAnchorId,
            });
        });
    });
    if (!requirements.length) throw new Error('skeleton.limbs must not be empty');
    return requirements;
}

function selectTracks(requirements, tracks) {
    const selectedSourceAnchors = new Map();
    return requirements.map((requirement) => {
        const matches = requirement.sourceAnchorId
            ? tracks.filter((track) => track.anchorId === requirement.sourceAnchorId)
            : tracks.filter((track) => track.sourceAnchor.bone === requirement.sourceBone);
        if (matches.length !== 1) {
            throw new Error(
                requirement.sourceAnchorId
                    ? `${requirement.semanticId} requires pinned RGB anchor ${requirement.sourceAnchorId}; found ${matches.length}`
                    : `${requirement.semanticId} requires exactly one RGB anchor on ${requirement.sourceBone}; found ${matches.length}`,
            );
        }
        const track = matches[0];
        const previous = selectedSourceAnchors.get(track.anchorId);
        if (previous) {
            const connector = requirement.branchConnector;
            const allowed = connector
                && connector.bone === requirement.sourceBone
                && connector.fromChain === previous.label
                && previous.sourceBone === requirement.sourceBone;
            if (!allowed) {
                throw new Error(`RGB anchor ${track.anchorId} is assigned to more than one semantic track`);
            }
        }
        if (!previous) selectedSourceAnchors.set(track.anchorId, requirement);
        return { ...requirement, track };
    });
}

function enforceSelectedTrackAvailability(selected, minimumVisiblePoints) {
    selected.forEach(({ track }) => {
        if (!track.points[track.queryFrame].visible) {
            throw new Error(`RGB track ${track.id} is not visible on its query frame`);
        }
        const visiblePoints = track.points.filter((point) => point.visible).length;
        if (visiblePoints < minimumVisiblePoints) {
            throw new Error(`RGB track ${track.id} has only ${visiblePoints} visible frames`);
        }
    });
}

function filterSelectedByVisibleConfidence(selected, minimumVisibleConfidence, confidenceByChain) {
    return selected.map((item) => {
        const threshold = confidenceByChain[item.label] ?? minimumVisibleConfidence;
        const track = item.track;
        let sourceVisiblePointCount = 0;
        let filteredLowConfidencePointCount = 0;
        const points = track.points.map((point) => {
            if (point.visible) sourceVisiblePointCount += 1;
            const filtered = point.visible
                && threshold != null
                && point.confidence < threshold;
            if (filtered) filteredLowConfidencePointCount += 1;
            return {
                ...point,
                visible: point.visible && !filtered,
            };
        });
        return {
            ...item,
            track: {
                ...track,
                points,
                confidenceFilterCounts: {
                    minimumVisibleConfidence: threshold,
                    sourceVisiblePointCount,
                    filteredLowConfidencePointCount,
                    retainedVisiblePointCount: sourceVisiblePointCount - filteredLowConfidencePointCount,
                },
            },
        };
    });
}

function translatedObservationPoint(item, point) {
    const query = item.track.points[item.track.queryFrame];
    return [
        item.restPoint[0] + point.x - query.x,
        item.restPoint[1] + point.y - query.y,
    ];
}

function distance2(first, second) {
    return Math.hypot(second[0] - first[0], second[1] - first[1]);
}

function filterSelectedByRestSegmentConsistency(selected, maximumRestSegmentScale) {
    const filtered = selected.map((item) => ({
        ...item,
        track: {
            ...item.track,
            points: item.track.points.map((point) => ({ ...point })),
        },
        restSegmentFilterCounts: {
            maximumRestSegmentScale,
            projectedRestSegmentLengthPx: null,
            restSegmentEvaluatedPointCount: 0,
            filteredRestSegmentOutlierPointCount: 0,
            postObservationQaVisiblePointCount: item.track.points.filter((point) => point.visible).length,
            maximumObservedRestSegmentScale: null,
        },
    }));
    if (maximumRestSegmentScale == null) return filtered;

    const filteredBySemanticId = new Map(filtered.map((item) => [item.semanticId, item]));
    const byLimb = new Map();
    selected.forEach((item) => {
        if (!byLimb.has(item.label)) byLimb.set(item.label, []);
        byLimb.get(item.label).push(item);
    });
    byLimb.forEach((items) => {
        const ordered = [...items].sort((first, second) => first.headIndex - second.headIndex);
        if (!ordered.some((item) => item.orderedHeadCount > 3)) return;
        for (let index = 1; index < ordered.length; index += 1) {
            const proximal = ordered[index - 1];
            const distal = ordered[index];
            const filteredDistal = filteredBySemanticId.get(distal.semanticId);
            const restSegmentLength = distance2(proximal.restPoint, distal.restPoint);
            if (!(restSegmentLength > 0)) {
                throw new Error(`ordered rest segment ${proximal.semanticId} -> ${distal.semanticId} has zero projected length`);
            }
            filteredDistal.restSegmentFilterCounts.projectedRestSegmentLengthPx = restSegmentLength;
            distal.track.points.forEach((distalPoint, frame) => {
                const proximalPoint = proximal.track.points[frame];
                if (!proximalPoint.visible || !distalPoint.visible) return;
                const observedSegmentLength = distance2(
                    translatedObservationPoint(proximal, proximalPoint),
                    translatedObservationPoint(distal, distalPoint),
                );
                const observedScale = observedSegmentLength / restSegmentLength;
                const counts = filteredDistal.restSegmentFilterCounts;
                counts.restSegmentEvaluatedPointCount += 1;
                counts.maximumObservedRestSegmentScale = Math.max(
                    counts.maximumObservedRestSegmentScale ?? 0,
                    observedScale,
                );
                if (observedScale > maximumRestSegmentScale) {
                    filteredDistal.track.points[frame].visible = false;
                    counts.filteredRestSegmentOutlierPointCount += 1;
                }
            });
        }
    });
    filtered.forEach((item) => {
        item.restSegmentFilterCounts.postObservationQaVisiblePointCount = item.track.points
            .filter((point) => point.visible).length;
    });
    return filtered;
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
    minimumVisibleConfidence = null,
    minimumVisibleConfidenceByChain = null,
    maximumRestSegmentScale = null,
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
    const confidenceThreshold = optionalUnitInterval(
        minimumVisibleConfidence,
        'minimumVisibleConfidence',
    );
    const confidenceByChain = confidenceThresholdsByChain(
        minimumVisibleConfidenceByChain,
        skeleton,
    );
    const restSegmentScaleThreshold = optionalMaximumScale(
        maximumRestSegmentScale,
        'maximumRestSegmentScale',
    );

    const provenance = object(observations.provenance, 'observations.provenance');
    const tracker = object(provenance.tracker, 'observations.provenance.tracker');
    if (tracker.backend !== TAPNEXT_BACKEND) {
        throw new Error(`observations tracker backend must be ${TAPNEXT_BACKEND}`);
    }
    const camera = normalizeCameraContract(cameraContract, skeleton, { ...observations, width, height });
    const normalized = normalizeTracks(observations.tracks, frameCount);
    const contacts = normalizeContacts(observations.contacts, frameCount, normalized.anchorIds);
    const selectedRaw = selectTracks(
        requiredSemanticTracks(skeleton),
        normalized.tracks,
    );
    const selectedByConfidence = filterSelectedByVisibleConfidence(
        selectedRaw,
        confidenceThreshold,
        confidenceByChain,
    );
    const selected = filterSelectedByRestSegmentConsistency(
        selectedByConfidence,
        restSegmentScaleThreshold,
    );
    enforceSelectedTrackAvailability(selected, minimumVisible);
    const selectionBySourceAnchor = new Map(selected.map((item) => [item.track.anchorId, item]));
    const selectedSourceAnchorIds = new Set(selected.map((item) => item.track.anchorId));
    const unusedSourceTracks = normalized.tracks
        .filter((track) => !selectedSourceAnchorIds.has(track.anchorId))
        .map((track) => ({ id: track.id, anchorId: track.anchorId }));
    const confidenceFilterCounts = selected.reduce((counts, { track }) => ({
        sourceVisiblePointCount:
            counts.sourceVisiblePointCount + track.confidenceFilterCounts.sourceVisiblePointCount,
        filteredLowConfidencePointCount:
            counts.filteredLowConfidencePointCount + track.confidenceFilterCounts.filteredLowConfidencePointCount,
        retainedVisiblePointCount:
            counts.retainedVisiblePointCount + track.confidenceFilterCounts.retainedVisiblePointCount,
    }), {
        sourceVisiblePointCount: 0,
        filteredLowConfidencePointCount: 0,
        retainedVisiblePointCount: 0,
    });
    const restSegmentFilterCounts = selected.reduce((counts, item) => {
        const itemMaximum = item.restSegmentFilterCounts.maximumObservedRestSegmentScale;
        return {
            evaluatedAdjacentSegmentSampleCount:
                counts.evaluatedAdjacentSegmentSampleCount
                + item.restSegmentFilterCounts.restSegmentEvaluatedPointCount,
            filteredDistalPointCount:
                counts.filteredDistalPointCount
                + item.restSegmentFilterCounts.filteredRestSegmentOutlierPointCount,
            retainedVisiblePointCount:
                counts.retainedVisiblePointCount
                + item.restSegmentFilterCounts.postObservationQaVisiblePointCount,
            maximumObservedRestSegmentScale: itemMaximum == null
                ? counts.maximumObservedRestSegmentScale
                : Math.max(counts.maximumObservedRestSegmentScale ?? itemMaximum, itemMaximum),
        };
    }, {
        evaluatedAdjacentSegmentSampleCount: 0,
        filteredDistalPointCount: 0,
        retainedVisiblePointCount: 0,
        maximumObservedRestSegmentScale: null,
    });

    return {
        schema: OBSERVATION_SCHEMA,
        frame_count: frameCount,
        width,
        height,
        fps,
        tracks: selected.map(({ semanticId, restPoint, track }) => {
            const query = track.points[track.queryFrame];
            return {
                id: track.id,
                anchor_id: semanticId,
                query_frame: track.queryFrame,
                points: track.points.map((point) => ({
                    ...point,
                    x: restPoint[0] + point.x - query.x,
                    y: restPoint[1] + point.y - query.y,
                })),
            };
        }),
        silhouettes: [],
        depth: [],
        contacts: contacts
            .filter((contact) => selectionBySourceAnchor.has(contact.anchorId))
            .map((contact) => {
                const selectedTrack = selectionBySourceAnchor.get(contact.anchorId);
                const query = selectedTrack.track.points[selectedTrack.track.queryFrame];
                return {
                    anchor_id: selectedTrack.semanticId,
                    frames: [...contact.frames],
                    ...(contact.groundHeight == null ? {} : {
                        ground_height: selectedTrack.restPoint[1] + contact.groundHeight - query.y,
                    }),
                    ...(contact.weight == null ? {} : { weight: contact.weight }),
                };
            }),
        provenance: {
            ...provenance,
            browser_rgb_bridge: {
                schema: BRIDGE_SCHEMA,
                trackerBackend: TAPNEXT_BACKEND,
                coordinateMode: 'rest_head_plus_query_displacement',
                camera,
                mappingMode: selected.some((item) => item.orderedHeadCount > 3)
                    ? 'ordered_deform_heads'
                    : 'legacy_three_track',
                confidenceFilter: {
                    enabled: confidenceThreshold != null || Object.keys(confidenceByChain).length > 0,
                    minimumVisibleConfidence: confidenceThreshold,
                    ...(Object.keys(confidenceByChain).length ? {
                        minimumVisibleConfidenceByChain: { ...confidenceByChain },
                    } : {}),
                    ...confidenceFilterCounts,
                },
                selection: {
                    selectedSemanticTrackCount: selected.length,
                    selectedUniqueSourceTrackCount: selectedSourceAnchorIds.size,
                    unusedSourceTrackCount: unusedSourceTracks.length,
                    unusedSourceTracks,
                },
                restSegmentConsistencyFilter: {
                    enabled: restSegmentScaleThreshold != null,
                    maximumRestSegmentScale: restSegmentScaleThreshold,
                    ...restSegmentFilterCounts,
                },
                mappings: selected.map(({
                    label,
                    collection,
                    semanticId,
                    sourceBone,
                    sourceAnchorId,
                    headIndex,
                    orderedHeadCount,
                    restPoint,
                    track,
                    restSegmentFilterCounts: mappingRestSegmentFilterCounts,
                }) => ({
                    limb: label,
                    collection,
                    semanticAnchorId: semanticId,
                    sourceBone,
                    sourceAnchorPin: sourceAnchorId,
                    headIndex,
                    orderedHeadCount,
                    restPoint: [...restPoint],
                    queryToRestOffsetPx: [
                        restPoint[0] - track.points[track.queryFrame].x,
                        restPoint[1] - track.points[track.queryFrame].y,
                    ],
                    sourceTrackId: track.id,
                    sourceAnchorId: track.anchorId,
                    ...track.confidenceFilterCounts,
                    ...mappingRestSegmentFilterCounts,
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
    minimumVisibleConfidence = null,
    maximumRestSegmentScale = null,
    options = {},
} = {}) {
    const prepared = prepareRgbObservationsForBrowser({
        observations,
        skeleton,
        cameraContract,
        minimumVisiblePoints,
        minimumVisibleConfidence,
        maximumRestSegmentScale,
    });
    return fitBrowserAnimation({ skeleton, observations: prepared, options });
}

export const RGB_OBSERVATION_BRIDGE_CONTRACT = Object.freeze({
    schema: BRIDGE_SCHEMA,
    observations: OBSERVATION_SCHEMA,
    skeleton: SKELETON_SCHEMA,
    trackerBackend: TAPNEXT_BACKEND,
});
