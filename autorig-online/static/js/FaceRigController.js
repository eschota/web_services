import * as THREE from 'three';

function clamp01(value) {
    return Math.max(0, Math.min(1, value));
}

function clampSigned01(value) {
    return Math.max(-1, Math.min(1, value));
}

function smoothstep(edge0, edge1, x) {
    if (edge0 === edge1) return x < edge0 ? 0 : 1;
    const t = clamp01((x - edge0) / (edge1 - edge0));
    return t * t * (3 - 2 * t);
}

function pickFirstBoneIndex(mesh, predicate) {
    const bones = mesh?.skeleton?.bones || [];
    return bones.findIndex((bone) => predicate(String(bone?.name || '').toLowerCase()));
}

function collectBoneIndices(mesh, predicates) {
    const bones = mesh?.skeleton?.bones || [];
    const indices = new Set();
    bones.forEach((bone, index) => {
        const name = String(bone?.name || '').toLowerCase();
        if (predicates.some((predicate) => predicate(name))) {
            indices.add(index);
        }
    });
    return [...indices];
}

function countMaskEntries(mask, threshold = 0.001) {
    if (!mask) return 0;
    let total = 0;
    for (let i = 0; i < mask.length; i += 1) {
        if (mask[i] > threshold) total += 1;
    }
    return total;
}

function cloneZoneConfig(zone) {
    return {
        ...zone,
        center: { ...(zone?.center || {}) },
        radii: { ...(zone?.radii || {}) },
    };
}

function cloneManualMaskConfig(config) {
    const zones = {};
    Object.entries(config?.zones || {}).forEach(([key, zone]) => {
        zones[key] = cloneZoneConfig(zone);
    });
    return {
        version: Number(config?.version || 1),
        coordinateSpace: config?.coordinateSpace || 'headBoundsNormalized',
        mode: config?.mode || 'parametric',
        zones,
    };
}

function cloneMaskMap(maskMap) {
    const clone = {};
    Object.entries(maskMap || {}).forEach(([key, mask]) => {
        clone[key] = mask ? Float32Array.from(mask) : null;
    });
    return clone;
}

export class FaceRigController {
    constructor() {
        this.mesh = null;
        this.geometry = null;
        this.vertexCount = 0;
        this.basePositions = null;
        this.headBounds = null;
        this.headMask = null;
        this.autoRegions = null;
        this.regions = null;
        this.endpointPaintMasks = null;
        this.endpointRegions = null;
        this.endpointActiveZones = new Set();
        this.manualMaskEnabled = false;
        this.manualMaskConfig = null;
        this.pendingPaintMasks = null;
        this.appliedPaintMasks = null;
        this.channelMap = {};
        this.enabled = true;
        this.blinkEnabled = true;
        this.intensity = 0.75;
        this.emotion = 'neutral';
        this.time = 0;
        this.blinkTimer = 0;
        this.blinkDuration = 0.12;
        this.activeBlink = 0;
        this.usesExistingMorphTargets = false;
        this.speechMode = 'autoTalk';
        this.speechActive = true;
        this.speechTarget = 0;
        this.speechEnvelope = 0;
        this.speechAttack = 16;
        this.speechRelease = 7;
        this.autoTalkPhase = Math.random() * Math.PI * 2;
        this.manualMouthTest = 0;
        this.debugInfo = {
            mode: 'uninitialized',
            hasSkeleton: false,
            vertexCount: 0,
        };
    }

    init(mesh) {
        if (!mesh?.geometry?.attributes?.position) {
            throw new Error('FaceRigController: mesh with position attribute is required');
        }

        this.mesh = mesh;
        this.geometry = mesh.geometry.clone();
        this.mesh.geometry = this.geometry;
        this.vertexCount = this.geometry.attributes.position.count;
        this.basePositions = Float32Array.from(this.geometry.attributes.position.array);
        this.debugInfo.vertexCount = this.vertexCount;
        this.debugInfo.hasSkeleton = !!mesh.skeleton;
        this.debugInfo.meshName = String(mesh.name || '').trim() || '(unnamed)';

        this.geometry.computeBoundingBox();
        this.geometry.computeBoundingSphere();

        this.detectHead();

        const hasExistingMorphs = this.mesh.morphTargetDictionary && Object.keys(this.mesh.morphTargetDictionary).length > 0;
        if (hasExistingMorphs) {
            this.channelMap = this.mapExistingMorphTargets();
            this.usesExistingMorphTargets = Object.keys(this.channelMap).length >= 52;
        }

        if (!this.usesExistingMorphTargets) {
            this.createMorphTargets();
            this.channelMap = this.buildGeneratedChannelMap();
        }

        this.debugInfo.morphSource = this.usesExistingMorphTargets ? 'existing' : 'generated';
        this.debugInfo.morphChannelCount = Object.keys(this.channelMap).length;

        if (typeof this.mesh.updateMorphTargets === 'function') {
            this.mesh.updateMorphTargets();
        }

        return this;
    }

    detectHead() {
        const position = this.geometry.attributes.position;
        let headMask = null;
        let mode = 'skeleton-required';
        let relevantBoneNames = [];
        if (!this.mesh.isSkinnedMesh || !this.mesh.skeleton || !this.geometry.attributes.skinIndex || !this.geometry.attributes.skinWeight) {
            throw new Error('FaceRigController: worker rig skinned mesh is required for face preview');
        }
        const headIndex = pickFirstBoneIndex(this.mesh, (name) => name.includes('head'));
        const neckIndex = pickFirstBoneIndex(this.mesh, (name) => name.includes('neck'));
        const relevantBoneIndices = collectBoneIndices(this.mesh, [
            (name) => name.includes('head'),
            (name) => name.includes('jaw'),
            (name) => name.includes('face'),
            (name) => name.includes('eye')
        ]);
        relevantBoneNames = relevantBoneIndices.map((index) => String(this.mesh.skeleton?.bones?.[index]?.name || '')).filter(Boolean);
        if (headIndex < 0) {
            throw new Error('FaceRigController: worker rig does not expose a head bone');
        }

        const skinIndex = this.geometry.attributes.skinIndex;
        const skinWeight = this.geometry.attributes.skinWeight;
        const buildMaskForBones = (boneIndices, threshold = 0.05) => {
            const mask = new Float32Array(this.vertexCount);
            const relevantSet = new Set(boneIndices.filter((index) => index >= 0));
            for (let i = 0; i < this.vertexCount; i += 1) {
                let weightSum = 0;
                for (let j = 0; j < 4; j += 1) {
                    if (j === 0 && relevantSet.has(skinIndex.getX(i))) weightSum += skinWeight.getX(i);
                    if (j === 1 && relevantSet.has(skinIndex.getY(i))) weightSum += skinWeight.getY(i);
                    if (j === 2 && relevantSet.has(skinIndex.getZ(i))) weightSum += skinWeight.getZ(i);
                    if (j === 3 && relevantSet.has(skinIndex.getW(i))) weightSum += skinWeight.getW(i);
                }
                mask[i] = weightSum > threshold ? clamp01(weightSum) : 0;
            }
            return mask;
        };
        const countSelected = (mask) => {
            let selected = 0;
            for (let i = 0; i < mask.length; i += 1) {
                if (mask[i] > 0) selected += 1;
            }
            return selected;
        };

        const primaryBoneIndices = relevantBoneIndices.length
            ? relevantBoneIndices
            : [headIndex];
        headMask = buildMaskForBones(primaryBoneIndices, 0.08);
        let selected = countSelected(headMask);

        if (selected <= Math.max(48, Math.floor(this.vertexCount * 0.0035)) && neckIndex >= 0) {
            headMask = buildMaskForBones([headIndex, neckIndex], 0.06);
            selected = countSelected(headMask);
            mode = 'skeleton-head-neck';
        } else {
            mode = 'skeleton-head-bone';
        }
        if (selected <= Math.max(48, Math.floor(this.vertexCount * 0.005))) {
            throw new Error('FaceRigController: worker rig head mask is too small or missing');
        }

        const headBounds = new THREE.Box3();
        let hasHeadVertex = false;
        for (let i = 0; i < this.vertexCount; i += 1) {
            if (headMask[i] <= 0.001) continue;
            hasHeadVertex = true;
            headBounds.expandByPoint(new THREE.Vector3(position.getX(i), position.getY(i), position.getZ(i)));
        }

        if (!hasHeadVertex) {
            throw new Error('FaceRigController: worker rig head bounds could not be computed');
        }

        this.headMask = headMask;
        this.headBounds = headBounds;
        this.autoRegions = this.computeFaceRegions(headBounds, headMask);
        this.endpointPaintMasks = this.createEmptyPaintMaskMap();
        this.endpointRegions = null;
        this.manualMaskConfig = this.createDefaultManualMaskConfig();
        this.pendingPaintMasks = this.createEmptyPaintMaskMap();
        this.appliedPaintMasks = this.createEmptyPaintMaskMap();
        this.regions = this.getEffectiveRegions();
        this.debugInfo.mode = mode;
        this.debugInfo.headVertexCount = countMaskEntries(headMask);
        this.debugInfo.headBoundsSize = {
            width: Math.round((headBounds.max.x - headBounds.min.x) * 1000) / 1000,
            height: Math.round((headBounds.max.y - headBounds.min.y) * 1000) / 1000,
            depth: Math.round((headBounds.max.z - headBounds.min.z) * 1000) / 1000,
        };
        this.debugInfo.relevantBones = relevantBoneNames.slice(0, 12);
    }

    computeFaceRegions(headBounds, headMask) {
        const position = this.geometry.attributes.position;
        const width = Math.max(1e-4, headBounds.max.x - headBounds.min.x);
        const height = Math.max(1e-4, headBounds.max.y - headBounds.min.y);
        const depth = Math.max(1e-4, headBounds.max.z - headBounds.min.z);
        const centerX = (headBounds.min.x + headBounds.max.x) * 0.5;
        const centerY = (headBounds.min.y + headBounds.max.y) * 0.5;
        const centerZ = (headBounds.min.z + headBounds.max.z) * 0.5;
        const eyeLine = headBounds.max.y - height * 0.28;
        const browLine = headBounds.max.y - height * 0.18;
        const mouthCenterLine = headBounds.min.y + height * 0.24;
        const mouthUpperLine = headBounds.min.y + height * 0.38;
        const noseLine = headBounds.min.y + height * 0.5;
        const chinInfluenceLine = headBounds.min.y + height * 0.12;

        const regions = {
            eyeLeft: new Float32Array(this.vertexCount),
            eyeRight: new Float32Array(this.vertexCount),
            mouth: new Float32Array(this.vertexCount),
            mouthLeft: new Float32Array(this.vertexCount),
            mouthRight: new Float32Array(this.vertexCount),
            lowerFace: new Float32Array(this.vertexCount),
            browLeft: new Float32Array(this.vertexCount),
            browRight: new Float32Array(this.vertexCount),
        };

        for (let i = 0; i < this.vertexCount; i += 1) {
            const headWeight = headMask[i] || 0;
            if (headWeight <= 0.001) continue;

            const x = position.getX(i);
            const y = position.getY(i);
            const z = position.getZ(i);
            const normX = (x - centerX) / (width * 0.5);
            const normZ = (z - centerZ) / (depth * 0.5);
            const depthFocus = 1 - smoothstep(0.06, 0.45, Math.abs(normZ));
            const faceWidthFocus = 1 - smoothstep(0.14, 0.62, Math.abs(normX));
            const jawWidthFocus = 1 - smoothstep(0.22, 0.76, Math.abs(normX));

            const eyeBand = smoothstep(eyeLine - height * 0.12, eyeLine + height * 0.04, y) *
                (1 - smoothstep(eyeLine + height * 0.04, headBounds.max.y, y));
            const mouthBand = smoothstep(mouthCenterLine - height * 0.05, mouthCenterLine + height * 0.02, y) *
                (1 - smoothstep(mouthUpperLine - height * 0.02, mouthUpperLine + height * 0.06, y));
            const jawBand = smoothstep(chinInfluenceLine, mouthCenterLine + height * 0.02, y) *
                (1 - smoothstep(mouthUpperLine + height * 0.03, noseLine, y));
            const lowerFaceBand = jawBand * (1 - smoothstep(noseLine, headBounds.max.y, y));
            const browBand = smoothstep(browLine - height * 0.08, browLine + height * 0.04, y) *
                (1 - smoothstep(browLine + height * 0.04, headBounds.max.y, y));

            const eyeSideFocusLeft = 1 - smoothstep(-0.05, 0.7, normX);
            const eyeSideFocusRight = smoothstep(0.05, 0.7, normX);
            const mouthSideWindow = 1 - smoothstep(0.06, 0.4, Math.abs(normX));
            const cornerFocusLeft = (1 - smoothstep(-0.35, 0.38, normX)) * mouthSideWindow;
            const cornerFocusRight = smoothstep(-0.38, 0.35, normX) * mouthSideWindow;
            const mouthCore = depthFocus * faceWidthFocus;
            const browSideFocusLeft = 1 - smoothstep(-0.08, 0.72, normX);
            const browSideFocusRight = smoothstep(0.08, 0.72, normX);

            regions.eyeLeft[i] = headWeight * eyeBand * eyeSideFocusLeft * depthFocus;
            regions.eyeRight[i] = headWeight * eyeBand * eyeSideFocusRight * depthFocus;
            regions.mouth[i] = headWeight * mouthBand * mouthCore;
            regions.mouthLeft[i] = headWeight * mouthBand * depthFocus * cornerFocusLeft;
            regions.mouthRight[i] = headWeight * mouthBand * depthFocus * cornerFocusRight;
            regions.lowerFace[i] = headWeight * lowerFaceBand * depthFocus * jawWidthFocus;
            regions.browLeft[i] = headWeight * browBand * browSideFocusLeft * depthFocus;
            regions.browRight[i] = headWeight * browBand * browSideFocusRight * depthFocus;
        }

        return {
            ...regions,
            centerX,
            centerY,
            centerZ,
            width,
            height,
            depth,
        };
    }

    createDefaultManualMaskConfig() {
        return {
            version: 1,
            coordinateSpace: 'headBoundsNormalized',
            mode: 'parametric',
            zones: {
                eyeLeft: {
                    type: 'ellipse',
                    label: 'Eye Left',
                    center: { x: -0.28, y: 0.34, z: 0.18 },
                    radii: { x: 0.2, y: 0.16, z: 0.22 },
                    weight: 1,
                },
                eyeRight: {
                    type: 'ellipse',
                    label: 'Eye Right',
                    center: { x: 0.28, y: 0.34, z: 0.18 },
                    radii: { x: 0.2, y: 0.16, z: 0.22 },
                    weight: 1,
                },
                browLeft: {
                    type: 'ellipse',
                    label: 'Brow Left',
                    center: { x: -0.25, y: 0.56, z: 0.16 },
                    radii: { x: 0.24, y: 0.1, z: 0.2 },
                    weight: 0.85,
                },
                browRight: {
                    type: 'ellipse',
                    label: 'Brow Right',
                    center: { x: 0.25, y: 0.56, z: 0.16 },
                    radii: { x: 0.24, y: 0.1, z: 0.2 },
                    weight: 0.85,
                },
                mouth: {
                    type: 'ellipse',
                    label: 'Mouth',
                    center: { x: 0, y: -0.34, z: 0.24 },
                    radii: { x: 0.26, y: 0.13, z: 0.22 },
                    weight: 1,
                },
                lowerFace: {
                    type: 'ellipse',
                    label: 'Chin / Lower Face',
                    center: { x: 0, y: -0.58, z: 0.12 },
                    radii: { x: 0.3, y: 0.22, z: 0.2 },
                    weight: 1,
                },
            },
        };
    }

    createEmptyPaintMaskMap() {
        return {
            eyeLeft: new Float32Array(this.vertexCount),
            eyeRight: new Float32Array(this.vertexCount),
            mouth: new Float32Array(this.vertexCount),
            lowerFace: new Float32Array(this.vertexCount),
            browLeft: new Float32Array(this.vertexCount),
            browRight: new Float32Array(this.vertexCount),
        };
    }

    hasPaintMaskContent(maskMap) {
        return Object.values(maskMap || {}).some((mask) => countMaskEntries(mask) > 0);
    }

    normalizedToLocalPoint(center) {
        const bounds = this.headBounds;
        const halfWidth = Math.max(1e-4, (bounds.max.x - bounds.min.x) * 0.5);
        const halfHeight = Math.max(1e-4, (bounds.max.y - bounds.min.y) * 0.5);
        const halfDepth = Math.max(1e-4, (bounds.max.z - bounds.min.z) * 0.5);
        const centerPoint = bounds.getCenter(new THREE.Vector3());
        return new THREE.Vector3(
            centerPoint.x + halfWidth * clampSigned01(Number(center?.x) || 0),
            centerPoint.y + halfHeight * clampSigned01(Number(center?.y) || 0),
            centerPoint.z + halfDepth * clampSigned01(Number(center?.z) || 0)
        );
    }

    localToNormalizedPoint(point) {
        const bounds = this.headBounds;
        const halfWidth = Math.max(1e-4, (bounds.max.x - bounds.min.x) * 0.5);
        const halfHeight = Math.max(1e-4, (bounds.max.y - bounds.min.y) * 0.5);
        const halfDepth = Math.max(1e-4, (bounds.max.z - bounds.min.z) * 0.5);
        const centerPoint = bounds.getCenter(new THREE.Vector3());
        return {
            x: clampSigned01((point.x - centerPoint.x) / halfWidth),
            y: clampSigned01((point.y - centerPoint.y) / halfHeight),
            z: clampSigned01((point.z - centerPoint.z) / halfDepth),
        };
    }

    normalizedRadiiToLocal(radii) {
        const bounds = this.headBounds;
        const halfWidth = Math.max(1e-4, (bounds.max.x - bounds.min.x) * 0.5);
        const halfHeight = Math.max(1e-4, (bounds.max.y - bounds.min.y) * 0.5);
        const halfDepth = Math.max(1e-4, (bounds.max.z - bounds.min.z) * 0.5);
        return new THREE.Vector3(
            Math.max(1e-4, halfWidth * clamp01(Number(radii?.x) || 0)),
            Math.max(1e-4, halfHeight * clamp01(Number(radii?.y) || 0)),
            Math.max(1e-4, halfDepth * clamp01(Number(radii?.z) || 0))
        );
    }

    localRadiiToNormalized(radii) {
        const bounds = this.headBounds;
        const halfWidth = Math.max(1e-4, (bounds.max.x - bounds.min.x) * 0.5);
        const halfHeight = Math.max(1e-4, (bounds.max.y - bounds.min.y) * 0.5);
        const halfDepth = Math.max(1e-4, (bounds.max.z - bounds.min.z) * 0.5);
        return {
            x: clamp01((Number(radii?.x) || 0) / halfWidth),
            y: clamp01((Number(radii?.y) || 0) / halfHeight),
            z: clamp01((Number(radii?.z) || 0) / halfDepth),
        };
    }

    getManualMaskConfig() {
        return cloneManualMaskConfig(this.manualMaskConfig || this.createDefaultManualMaskConfig());
    }

    getManualMaskSceneData() {
        const config = this.getManualMaskConfig();
        const zones = {};
        Object.entries(config.zones || {}).forEach(([key, zone]) => {
            zones[key] = {
                ...cloneZoneConfig(zone),
                localCenter: this.normalizedToLocalPoint(zone.center),
                localRadii: this.normalizedRadiiToLocal(zone.radii),
            };
        });
        return {
            enabled: this.manualMaskEnabled,
            version: config.version,
            coordinateSpace: config.coordinateSpace,
            mode: config.mode,
            zones,
        };
    }

    getManualPaintMasks() {
        return {
            pending: cloneMaskMap(this.pendingPaintMasks),
            applied: cloneMaskMap(this.appliedPaintMasks),
        };
    }

    getEndpointPaintMasks() {
        return cloneMaskMap(this.endpointPaintMasks);
    }

    setEndpointPaintMasks(maskMap) {
        this.endpointPaintMasks = cloneMaskMap(maskMap || this.createEmptyPaintMaskMap());
        this.endpointActiveZones = new Set();
        const zoneKeys = ['mouth', 'eyeLeft', 'eyeRight', 'browLeft', 'browRight', 'lowerFace'];
        zoneKeys.forEach((key) => {
            if (countMaskEntries(this.endpointPaintMasks[key]) > 0) {
                this.endpointActiveZones.add(key);
            }
        });
        this.endpointRegions = this.hasPaintMaskContent(this.endpointPaintMasks)
            ? this.buildRegionsFromPaintMasks(this.endpointPaintMasks)
            : null;
        this.refreshEffectiveRegions(!this.usesExistingMorphTargets && !!this.endpointRegions);
    }

    clearEndpointPaintMasks() {
        this.endpointPaintMasks = this.createEmptyPaintMaskMap();
        this.endpointRegions = null;
        this.endpointActiveZones = new Set();
        this.refreshEffectiveRegions(!this.usesExistingMorphTargets);
    }

    setManualMaskEnabled(enabled) {
        this.manualMaskEnabled = !!enabled;
        this.refreshEffectiveRegions(!this.usesExistingMorphTargets);
    }

    paintManualMaskAt(localPoint, zoneKey, radius, strength, options = {}) {
        if (!this.pendingPaintMasks?.[zoneKey] || !localPoint) return false;
        const headSize = this.headBounds?.getSize?.(new THREE.Vector3()) || new THREE.Vector3(1, 1, 1);
        const headScale = Math.max(headSize.x, headSize.y, headSize.z, 1e-4);
        const radiusValue = Math.max(1e-4, (Number(radius) || 0.1) * headScale);
        const opacity = clamp01(Number(strength) || 0.5);
        const erase = !!options.erase;
        const position = this.geometry.attributes.position;
        const targetMask = this.pendingPaintMasks[zoneKey];
        let changed = false;
        for (let i = 0; i < this.vertexCount; i += 1) {
            if ((this.headMask?.[i] || 0) <= 0.001) continue;
            const dx = position.getX(i) - localPoint.x;
            const dy = position.getY(i) - localPoint.y;
            const dz = position.getZ(i) - localPoint.z;
            const distance = Math.sqrt(dx * dx + dy * dy + dz * dz);
            if (distance > radiusValue) continue;
            const falloff = 1 - smoothstep(0, radiusValue, distance);
            const delta = falloff * opacity;
            const nextValue = erase
                ? Math.max(0, targetMask[i] - delta)
                : Math.min(1, Math.max(targetMask[i], targetMask[i] + delta * (1 - targetMask[i])));
            if (Math.abs(nextValue - targetMask[i]) > 1e-5) {
                targetMask[i] = nextValue;
                changed = true;
            }
        }
        return changed;
    }

    commitManualPaintMasks() {
        this.appliedPaintMasks = cloneMaskMap(this.pendingPaintMasks);
        this.refreshEffectiveRegions(!this.usesExistingMorphTargets);
    }

    resetPendingPaintZone(zoneKey) {
        if (!this.pendingPaintMasks?.[zoneKey]) return;
        this.pendingPaintMasks[zoneKey].fill(0);
    }

    resetPendingPaintMasks() {
        Object.values(this.pendingPaintMasks || {}).forEach((mask) => mask?.fill?.(0));
    }

    clearAppliedPaintMasks() {
        Object.values(this.appliedPaintMasks || {}).forEach((mask) => mask?.fill?.(0));
        this.refreshEffectiveRegions(!this.usesExistingMorphTargets);
    }

    setManualZone(zoneKey, patch = {}) {
        if (!this.manualMaskConfig?.zones?.[zoneKey]) return;
        const current = this.manualMaskConfig.zones[zoneKey];
        this.manualMaskConfig.zones[zoneKey] = {
            ...current,
            ...patch,
            center: {
                ...(current.center || {}),
                ...(patch.center || {}),
            },
            radii: {
                ...(current.radii || {}),
                ...(patch.radii || {}),
            },
        };
        this.refreshEffectiveRegions(!this.usesExistingMorphTargets);
    }

    setManualZoneFromLocalState(zoneKey, localCenter, localRadii, extra = {}) {
        if (!this.manualMaskConfig?.zones?.[zoneKey]) return;
        const patch = {
            ...extra,
            center: localCenter ? this.localToNormalizedPoint(localCenter) : undefined,
            radii: localRadii ? this.localRadiiToNormalized(localRadii) : undefined,
        };
        this.setManualZone(zoneKey, patch);
    }

    resetManualZone(zoneKey) {
        const defaults = this.createDefaultManualMaskConfig();
        if (!defaults.zones?.[zoneKey]) return;
        this.manualMaskConfig.zones[zoneKey] = cloneZoneConfig(defaults.zones[zoneKey]);
        this.refreshEffectiveRegions(!this.usesExistingMorphTargets);
    }

    resetManualZones() {
        this.manualMaskConfig = this.createDefaultManualMaskConfig();
        this.refreshEffectiveRegions(!this.usesExistingMorphTargets);
    }

    buildManualRegions() {
        if (this.hasPaintMaskContent(this.appliedPaintMasks)) {
            return this.buildRegionsFromPaintMasks(this.appliedPaintMasks);
        }
        const baseRegions = this.endpointRegions || this.autoRegions || this.computeFaceRegions(this.headBounds, this.headMask);
        const position = this.geometry.attributes.position;
        const effective = {
            eyeLeft: new Float32Array(this.vertexCount),
            eyeRight: new Float32Array(this.vertexCount),
            mouth: new Float32Array(this.vertexCount),
            mouthLeft: new Float32Array(this.vertexCount),
            mouthRight: new Float32Array(this.vertexCount),
            lowerFace: new Float32Array(this.vertexCount),
            browLeft: new Float32Array(this.vertexCount),
            browRight: new Float32Array(this.vertexCount),
            centerX: baseRegions.centerX,
            centerY: baseRegions.centerY,
            centerZ: baseRegions.centerZ,
            width: baseRegions.width,
            height: baseRegions.height,
            depth: baseRegions.depth,
        };
        const zoneEntries = Object.entries(this.manualMaskConfig?.zones || {});
        if (!zoneEntries.length) {
            return effective;
        }
        const zoneCache = zoneEntries.map(([zoneKey, zone]) => ({
            zoneKey,
            weight: Math.max(0, Number(zone.weight) || 1),
            localCenter: this.normalizedToLocalPoint(zone.center),
            localRadii: this.normalizedRadiiToLocal(zone.radii),
        }));
        for (let i = 0; i < this.vertexCount; i += 1) {
            if ((this.headMask?.[i] || 0) <= 0.001) continue;
            const x = position.getX(i);
            const y = position.getY(i);
            const z = position.getZ(i);
            zoneCache.forEach(({ zoneKey, weight, localCenter, localRadii }) => {
                const dx = (x - localCenter.x) / Math.max(localRadii.x, 1e-4);
                const dy = (y - localCenter.y) / Math.max(localRadii.y, 1e-4);
                const dz = (z - localCenter.z) / Math.max(localRadii.z, 1e-4);
                const distance = Math.sqrt(dx * dx + dy * dy + dz * dz);
                if (distance >= 1) return;
                const zoneWeight = clamp01((1 - smoothstep(0.35, 1, distance)) * weight);
                if (zoneKey === 'mouth') {
                    effective.mouth[i] = Math.max(effective.mouth[i], zoneWeight);
                    const sideNorm = (x - localCenter.x) / Math.max(localRadii.x, 1e-4);
                    const leftBias = clamp01(0.25 + Math.max(0, -sideNorm));
                    const rightBias = clamp01(0.25 + Math.max(0, sideNorm));
                    effective.mouthLeft[i] = Math.max(effective.mouthLeft[i], zoneWeight * leftBias);
                    effective.mouthRight[i] = Math.max(effective.mouthRight[i], zoneWeight * rightBias);
                    return;
                }
                if (zoneKey === 'eyeLeft') effective.eyeLeft[i] = Math.max(effective.eyeLeft[i], zoneWeight);
                if (zoneKey === 'eyeRight') effective.eyeRight[i] = Math.max(effective.eyeRight[i], zoneWeight);
                if (zoneKey === 'lowerFace') effective.lowerFace[i] = Math.max(effective.lowerFace[i], zoneWeight);
                if (zoneKey === 'browLeft') effective.browLeft[i] = Math.max(effective.browLeft[i], zoneWeight);
                if (zoneKey === 'browRight') effective.browRight[i] = Math.max(effective.browRight[i], zoneWeight);
            });
        }
        return effective;
    }

    buildRegionsFromPaintMasks(maskMap) {
        const baseRegions = this.endpointRegions || this.autoRegions || this.computeFaceRegions(this.headBounds, this.headMask);
        const effective = {
            eyeLeft: Float32Array.from(maskMap?.eyeLeft || new Float32Array(this.vertexCount)),
            eyeRight: Float32Array.from(maskMap?.eyeRight || new Float32Array(this.vertexCount)),
            mouth: Float32Array.from(maskMap?.mouth || new Float32Array(this.vertexCount)),
            mouthLeft: new Float32Array(this.vertexCount),
            mouthRight: new Float32Array(this.vertexCount),
            lowerFace: Float32Array.from(maskMap?.lowerFace || new Float32Array(this.vertexCount)),
            browLeft: Float32Array.from(maskMap?.browLeft || new Float32Array(this.vertexCount)),
            browRight: Float32Array.from(maskMap?.browRight || new Float32Array(this.vertexCount)),
            centerX: baseRegions.centerX,
            centerY: baseRegions.centerY,
            centerZ: baseRegions.centerZ,
            width: baseRegions.width,
            height: baseRegions.height,
            depth: baseRegions.depth,
        };
        const position = this.geometry.attributes.position;
        for (let i = 0; i < this.vertexCount; i += 1) {
            const mouth = effective.mouth[i] || 0;
            if (mouth <= 0.001) continue;
            const sideNorm = (position.getX(i) - baseRegions.centerX) / Math.max(baseRegions.width * 0.5, 1e-4);
            const leftBias = clamp01(0.25 + Math.max(0, -sideNorm));
            const rightBias = clamp01(0.25 + Math.max(0, sideNorm));
            effective.mouthLeft[i] = mouth * leftBias;
            effective.mouthRight[i] = mouth * rightBias;
        }
        return effective;
    }

    getPreviewRegions() {
        if (this.manualMaskEnabled && this.hasPaintMaskContent(this.pendingPaintMasks)) {
            return this.buildRegionsFromPaintMasks(this.pendingPaintMasks);
        }
        return this.regions;
    }

    getEffectiveRegions() {
        if (!this.manualMaskEnabled && this.endpointRegions) {
            return this.endpointRegions;
        }
        if (!this.manualMaskEnabled) {
            return this.autoRegions;
        }
        return this.buildManualRegions();
    }

    refreshEffectiveRegions(rebuildMorphTargets = false) {
        this.regions = this.getEffectiveRegions();
        this.debugInfo.manualMaskEnabled = this.manualMaskEnabled;
        this.debugInfo.manualZones = Object.keys(this.manualMaskConfig?.zones || {});
        this.debugInfo.endpointActiveZones = [...this.endpointActiveZones];
        if (rebuildMorphTargets && !this.usesExistingMorphTargets) {
            try {
                this.createMorphTargets();
                this.channelMap = this.buildGeneratedChannelMap();
                this.debugInfo.morphSource = this.endpointRegions ? 'generated (endpoint)' : 'generated';
                this.debugInfo.morphChannelCount = Object.keys(this.channelMap).length;
                if (typeof this.mesh.updateMorphTargets === 'function') {
                    this.mesh.updateMorphTargets();
                }
            } catch (err) {
                console.error('[FaceRigController] morph rebuild failed:', err);
            }
        }
    }

    mapExistingMorphTargets() {
        const dictionary = this.mesh.morphTargetDictionary || {};
        const normalized = {};
        Object.entries(dictionary).forEach(([name, index]) => {
            normalized[String(name).toLowerCase()] = index;
        });

        const aliases = {
            jawOpen: ['jawopen', 'jaw_open', 'blendshape1.jawopen', 'openjaw', 'mouthopen', 'mouth_open'],
            eyeBlinkLeft: ['eyeblinkleft', 'eyeblink_l', 'blendshape1.eyeblinkleft', 'blink_l'],
            eyeBlinkRight: ['eyeblinkright', 'eyeblink_r', 'blendshape1.eyeblinkright', 'blink_r'],
            mouthSmileLeft: ['mouthsmileleft', 'mouthsmile_l', 'blendshape1.mouthsmileleft', 'smile_l'],
            mouthSmileRight: ['mouthsmileright', 'mouthsmile_r', 'blendshape1.mouthsmileright', 'smile_r'],
            mouthFrownLeft: ['mouthfrownleft', 'mouthfrown_l', 'blendshape1.mouthfrownleft', 'frown_l'],
            mouthFrownRight: ['mouthfrownright', 'mouthfrown_r', 'blendshape1.mouthfrownright', 'frown_r'],
            mouthPucker: ['mouthpucker', 'blendshape1.mouthpucker', 'pucker'],
            mouthFunnel: ['mouthfunnel', 'mouth_funnel'],
            browDownLeft: ['browdownleft', 'browdown_l'],
            browDownRight: ['browdownright', 'browdown_r'],
            browInnerUp: ['browinnerup'],
            browOuterUpLeft: ['browouterupleft', 'browouterup_l'],
            browOuterUpRight: ['browouterupright', 'browouterup_r'],
            eyeSquintLeft: ['eyesquintleft', 'eyesquint_l'],
            eyeSquintRight: ['eyesquintright', 'eyesquint_r'],
            eyeWideLeft: ['eyewideleft', 'eyewide_l'],
            eyeWideRight: ['eyewideright', 'eyewide_r'],
        };

        const map = {};
        Object.entries(aliases).forEach(([channel, keys]) => {
            const match = keys.find((key) => normalized[key] !== undefined);
            if (match) map[channel] = normalized[match];
        });
        this.debugInfo.availableMorphTargets = Object.keys(dictionary).slice(0, 24);
        this.debugInfo.mappedMorphChannels = Object.keys(map);
        return map;
    }

    buildGeneratedChannelMap() {
        const map = {};
        (this.geometry.morphAttributes.position || []).forEach((attribute, index) => {
            if (attribute?.name) map[attribute.name] = index;
        });
        return map;
    }

    createMorphTargets() {
        const position = this.geometry.attributes.position;
        const regions = this.regions;
        const width = regions.width;
        const height = regions.height;
        const depth = regions.depth;
        const centerX = regions.centerX;
        const eyeLine = this.headBounds.max.y - height * 0.28;
        const mouthCenterLine = this.headBounds.min.y + height * 0.24;
        const noseLine = this.headBounds.min.y + height * 0.5;
        const baseMorphs = {
            browDownLeft: new Float32Array(this.vertexCount * 3),
            browDownRight: new Float32Array(this.vertexCount * 3),
            browInnerUp: new Float32Array(this.vertexCount * 3),
            browOuterUpLeft: new Float32Array(this.vertexCount * 3),
            browOuterUpRight: new Float32Array(this.vertexCount * 3),
            cheekPuff: new Float32Array(this.vertexCount * 3),
            cheekSquintLeft: new Float32Array(this.vertexCount * 3),
            cheekSquintRight: new Float32Array(this.vertexCount * 3),
            eyeBlinkLeft: new Float32Array(this.vertexCount * 3),
            eyeBlinkRight: new Float32Array(this.vertexCount * 3),
            eyeLookDownLeft: new Float32Array(this.vertexCount * 3),
            eyeLookDownRight: new Float32Array(this.vertexCount * 3),
            eyeLookInLeft: new Float32Array(this.vertexCount * 3),
            eyeLookInRight: new Float32Array(this.vertexCount * 3),
            eyeLookOutLeft: new Float32Array(this.vertexCount * 3),
            eyeLookOutRight: new Float32Array(this.vertexCount * 3),
            eyeLookUpLeft: new Float32Array(this.vertexCount * 3),
            eyeLookUpRight: new Float32Array(this.vertexCount * 3),
            eyeSquintLeft: new Float32Array(this.vertexCount * 3),
            eyeSquintRight: new Float32Array(this.vertexCount * 3),
            eyeWideLeft: new Float32Array(this.vertexCount * 3),
            eyeWideRight: new Float32Array(this.vertexCount * 3),
            jawForward: new Float32Array(this.vertexCount * 3),
            jawLeft: new Float32Array(this.vertexCount * 3),
            jawOpen: new Float32Array(this.vertexCount * 3),
            jawRight: new Float32Array(this.vertexCount * 3),
            mouthClose: new Float32Array(this.vertexCount * 3),
            mouthDimpleLeft: new Float32Array(this.vertexCount * 3),
            mouthDimpleRight: new Float32Array(this.vertexCount * 3),
            mouthFrownLeft: new Float32Array(this.vertexCount * 3),
            mouthFrownRight: new Float32Array(this.vertexCount * 3),
            mouthFunnel: new Float32Array(this.vertexCount * 3),
            mouthLeft: new Float32Array(this.vertexCount * 3),
            mouthLowerDownLeft: new Float32Array(this.vertexCount * 3),
            mouthLowerDownRight: new Float32Array(this.vertexCount * 3),
            mouthPressLeft: new Float32Array(this.vertexCount * 3),
            mouthPressRight: new Float32Array(this.vertexCount * 3),
            mouthPucker: new Float32Array(this.vertexCount * 3),
            mouthRight: new Float32Array(this.vertexCount * 3),
            mouthRollLower: new Float32Array(this.vertexCount * 3),
            mouthRollUpper: new Float32Array(this.vertexCount * 3),
            mouthShrugLower: new Float32Array(this.vertexCount * 3),
            mouthShrugUpper: new Float32Array(this.vertexCount * 3),
            mouthSmileLeft: new Float32Array(this.vertexCount * 3),
            mouthSmileRight: new Float32Array(this.vertexCount * 3),
            mouthStretchLeft: new Float32Array(this.vertexCount * 3),
            mouthStretchRight: new Float32Array(this.vertexCount * 3),
            mouthUpperUpLeft: new Float32Array(this.vertexCount * 3),
            mouthUpperUpRight: new Float32Array(this.vertexCount * 3),
            noseSneerLeft: new Float32Array(this.vertexCount * 3),
            noseSneerRight: new Float32Array(this.vertexCount * 3),
            tongueOut: new Float32Array(this.vertexCount * 3),
        };

        for (let i = 0; i < this.vertexCount; i += 1) {
            const x = position.getX(i);
            const y = position.getY(i);
            const lowerWeight = regions.lowerFace[i];
            const mouthWeight = regions.mouth[i];
            const mouthLeftWeight = regions.mouthLeft[i];
            const mouthRightWeight = regions.mouthRight[i];
            const eyeLeftWeight = regions.eyeLeft[i];
            const eyeRightWeight = regions.eyeRight[i];
            const browLeftWeight = regions.browLeft[i];
            const browRightWeight = regions.browRight[i];
            const dyToEyeCenter = (y - (this.headBounds.max.y - height * 0.28)) / Math.max(height * 0.12, 1e-4);
            const mouthVertical = (y - mouthCenterLine) / Math.max(height * 0.14, 1e-4);
            const jawBase = i * 3;
            const eyeBase = i * 3;
            const noseBand = smoothstep(noseLine - height * 0.08, noseLine + height * 0.05, y) *
                (1 - smoothstep(noseLine + height * 0.05, this.headBounds.max.y, y));
            const cheekLeftWeight = eyeLeftWeight * (1 - smoothstep(eyeLine - height * 0.1, eyeLine + height * 0.16, y));
            const cheekRightWeight = eyeRightWeight * (1 - smoothstep(eyeLine - height * 0.1, eyeLine + height * 0.16, y));
            const upperLipWeight = mouthWeight * clamp01(0.5 - mouthVertical * 0.8);
            const lowerLipWeight = mouthWeight * clamp01(0.5 + mouthVertical * 0.8);
            const centerLipWeight = mouthWeight * (1 - smoothstep(0.2, 0.9, Math.abs((x - centerX) / Math.max(width * 0.25, 1e-4))));

            baseMorphs.jawOpen[jawBase + 1] = -height * (0.14 * lowerWeight + 0.05 * mouthWeight);
            baseMorphs.jawOpen[jawBase + 2] = depth * 0.055 * mouthWeight;
            baseMorphs.jawForward[jawBase + 2] = depth * 0.045 * (lowerWeight + mouthWeight * 0.6);
            baseMorphs.jawLeft[jawBase] = -width * 0.035 * lowerWeight;
            baseMorphs.jawRight[jawBase] = width * 0.035 * lowerWeight;
            baseMorphs.mouthClose[jawBase + 1] = height * 0.06 * lowerLipWeight;

            baseMorphs.eyeBlinkLeft[eyeBase + 1] = -height * 0.05 * eyeLeftWeight * (dyToEyeCenter >= 0 ? 1 : -0.6);
            baseMorphs.eyeBlinkRight[eyeBase + 1] = -height * 0.05 * eyeRightWeight * (dyToEyeCenter >= 0 ? 1 : -0.6);
            baseMorphs.eyeSquintLeft[eyeBase + 1] = -height * 0.025 * eyeLeftWeight * (dyToEyeCenter >= 0 ? 1 : 0.5);
            baseMorphs.eyeSquintRight[eyeBase + 1] = -height * 0.025 * eyeRightWeight * (dyToEyeCenter >= 0 ? 1 : 0.5);
            baseMorphs.eyeWideLeft[eyeBase + 1] = height * 0.02 * eyeLeftWeight * (dyToEyeCenter >= 0 ? 1 : -0.3);
            baseMorphs.eyeWideRight[eyeBase + 1] = height * 0.02 * eyeRightWeight * (dyToEyeCenter >= 0 ? 1 : -0.3);
            baseMorphs.eyeLookUpLeft[eyeBase + 1] = height * 0.015 * eyeLeftWeight;
            baseMorphs.eyeLookUpRight[eyeBase + 1] = height * 0.015 * eyeRightWeight;
            baseMorphs.eyeLookDownLeft[eyeBase + 1] = -height * 0.015 * eyeLeftWeight;
            baseMorphs.eyeLookDownRight[eyeBase + 1] = -height * 0.015 * eyeRightWeight;
            baseMorphs.eyeLookInLeft[eyeBase] = width * 0.01 * eyeLeftWeight;
            baseMorphs.eyeLookInRight[eyeBase] = -width * 0.01 * eyeRightWeight;
            baseMorphs.eyeLookOutLeft[eyeBase] = -width * 0.01 * eyeLeftWeight;
            baseMorphs.eyeLookOutRight[eyeBase] = width * 0.01 * eyeRightWeight;

            baseMorphs.browDownLeft[jawBase + 1] = -height * 0.04 * browLeftWeight;
            baseMorphs.browDownRight[jawBase + 1] = -height * 0.04 * browRightWeight;
            baseMorphs.browInnerUp[jawBase + 1] = height * 0.04 * (browLeftWeight + browRightWeight) * 0.5;
            baseMorphs.browOuterUpLeft[jawBase + 1] = height * 0.038 * browLeftWeight;
            baseMorphs.browOuterUpRight[jawBase + 1] = height * 0.038 * browRightWeight;
            baseMorphs.cheekPuff[jawBase + 2] = depth * 0.04 * (cheekLeftWeight + cheekRightWeight + mouthWeight * 0.5);
            baseMorphs.cheekSquintLeft[jawBase + 1] = -height * 0.02 * cheekLeftWeight;
            baseMorphs.cheekSquintRight[jawBase + 1] = -height * 0.02 * cheekRightWeight;
            baseMorphs.noseSneerLeft[jawBase + 1] = height * 0.014 * noseBand * (x < centerX ? 1 : 0);
            baseMorphs.noseSneerRight[jawBase + 1] = height * 0.014 * noseBand * (x >= centerX ? 1 : 0);

            const smileLY = i * 3 + 1;
            const smileLX = i * 3;
            baseMorphs.mouthSmileLeft[smileLY] = height * 0.065 * mouthLeftWeight;
            baseMorphs.mouthSmileLeft[smileLX] = -width * 0.05 * mouthLeftWeight;
            baseMorphs.mouthSmileRight[smileLY] = height * 0.065 * mouthRightWeight;
            baseMorphs.mouthSmileRight[smileLX] = width * 0.05 * mouthRightWeight;

            baseMorphs.mouthFrownLeft[smileLY] = -height * 0.055 * mouthLeftWeight;
            baseMorphs.mouthFrownLeft[smileLX] = width * 0.02 * mouthLeftWeight;
            baseMorphs.mouthFrownRight[smileLY] = -height * 0.055 * mouthRightWeight;
            baseMorphs.mouthFrownRight[smileLX] = -width * 0.02 * mouthRightWeight;

            const towardCenter = x >= centerX ? -1 : 1;
            const puckerBase = i * 3;
            baseMorphs.mouthPucker[puckerBase] = towardCenter * width * 0.045 * mouthWeight;
            baseMorphs.mouthPucker[puckerBase + 2] = depth * 0.075 * mouthWeight;
            baseMorphs.mouthPucker[puckerBase + 1] = -height * 0.018 * mouthWeight;
            baseMorphs.mouthFunnel[puckerBase] = towardCenter * width * 0.032 * centerLipWeight;
            baseMorphs.mouthFunnel[puckerBase + 2] = depth * 0.085 * centerLipWeight;
            baseMorphs.mouthLeft[puckerBase] = -width * 0.04 * mouthWeight;
            baseMorphs.mouthRight[puckerBase] = width * 0.04 * mouthWeight;
            baseMorphs.mouthDimpleLeft[puckerBase] = -width * 0.025 * mouthLeftWeight;
            baseMorphs.mouthDimpleRight[puckerBase] = width * 0.025 * mouthRightWeight;
            baseMorphs.mouthStretchLeft[puckerBase] = -width * 0.055 * mouthLeftWeight;
            baseMorphs.mouthStretchRight[puckerBase] = width * 0.055 * mouthRightWeight;
            baseMorphs.mouthPressLeft[puckerBase + 2] = -depth * 0.02 * mouthLeftWeight;
            baseMorphs.mouthPressRight[puckerBase + 2] = -depth * 0.02 * mouthRightWeight;
            baseMorphs.mouthRollUpper[puckerBase + 1] = -height * 0.02 * upperLipWeight;
            baseMorphs.mouthRollLower[puckerBase + 1] = height * 0.02 * lowerLipWeight;
            baseMorphs.mouthShrugUpper[puckerBase + 1] = height * 0.03 * upperLipWeight;
            baseMorphs.mouthShrugLower[puckerBase + 1] = -height * 0.018 * lowerLipWeight;
            baseMorphs.mouthUpperUpLeft[puckerBase + 1] = height * 0.04 * upperLipWeight * (x < centerX ? 1 : 0.5);
            baseMorphs.mouthUpperUpRight[puckerBase + 1] = height * 0.04 * upperLipWeight * (x >= centerX ? 1 : 0.5);
            baseMorphs.mouthLowerDownLeft[puckerBase + 1] = -height * 0.04 * lowerLipWeight * (x < centerX ? 1 : 0.5);
            baseMorphs.mouthLowerDownRight[puckerBase + 1] = -height * 0.04 * lowerLipWeight * (x >= centerX ? 1 : 0.5);
            baseMorphs.tongueOut[puckerBase + 2] = depth * 0.01 * lowerLipWeight;
        }

        this.geometry.morphTargetsRelative = true;
        this.geometry.morphAttributes.position = Object.entries(baseMorphs).map(([name, array]) => {
            const attribute = new THREE.Float32BufferAttribute(array, 3);
            attribute.name = name;
            return attribute;
        });
        this.debugInfo.mappedMorphChannels = Object.keys(baseMorphs);
    }

    setEmotion(type) {
        this.emotion = ['neutral', 'happy', 'sad'].includes(type) ? type : 'neutral';
    }

    setIntensity(value) {
        this.intensity = clamp01(Number(value));
    }

    setBlinkEnabled(enabled) {
        this.blinkEnabled = !!enabled;
    }

    setEnabled(enabled) {
        this.enabled = !!enabled;
        if (!this.enabled) {
            this.speechEnvelope = 0;
            this.speechTarget = 0;
            Object.keys(this.channelMap).forEach((channel) => this.setInfluence(channel, 0));
        }
    }

    setSpeechMode(mode) {
        const nextMode = ['idle', 'autoTalk', 'mic'].includes(String(mode)) ? String(mode) : 'autoTalk';
        this.speechMode = nextMode;
        this.debugInfo.speechMode = nextMode;
    }

    setSpeechEnvelope(value) {
        this.speechTarget = clamp01(Number(value) || 0);
    }

    setSpeechActive(enabled) {
        this.speechActive = !!enabled;
        if (!this.speechActive) {
            this.speechTarget = 0;
        }
    }

    setManualMouthTest(value) {
        this.manualMouthTest = clamp01(Number(value) || 0);
        this.debugInfo.manualMouthTest = this.manualMouthTest;
    }

    getHeadBounds() {
        return this.headBounds ? this.headBounds.clone() : null;
    }

    getMesh() {
        return this.mesh;
    }

    getDebugVisualizationData() {
        return {
            vertexCount: this.vertexCount,
            headMask: this.headMask,
            regions: this.getPreviewRegions(),
            headBounds: this.getHeadBounds(),
            manualMaskConfig: this.getManualMaskConfig(),
            manualMaskEnabled: this.manualMaskEnabled,
            endpointPaintMasks: this.getEndpointPaintMasks(),
            endpointActiveZones: [...this.endpointActiveZones],
            pendingPaintMasks: this.getManualPaintMasks().pending,
            appliedPaintMasks: this.getManualPaintMasks().applied,
        };
    }

    getMaskWeightRange(mask) {
        if (!mask) return { min: 0, max: 0 };
        let min = Infinity;
        let max = -Infinity;
        let hasValue = false;
        for (let i = 0; i < mask.length; i += 1) {
            if (mask[i] > 0.001) {
                hasValue = true;
                if (mask[i] < min) min = mask[i];
                if (mask[i] > max) max = mask[i];
            }
        }
        return hasValue
            ? { min: Math.round(min * 1000) / 1000, max: Math.round(max * 1000) / 1000 }
            : { min: 0, max: 0 };
    }

    getDebugInfo() {
        const mouthWeightRange = this.getMaskWeightRange(this.regions?.mouth);
        return {
            ...this.debugInfo,
            usesExistingMorphTargets: this.usesExistingMorphTargets,
            channels: Object.keys(this.channelMap),
            speechMode: this.speechMode,
            speechActive: this.speechActive,
            manualMaskEnabled: this.manualMaskEnabled,
            mouthVertexCount: countMaskEntries(this.regions?.mouth),
            mouthWeightRange,
            lowerFaceVertexCount: countMaskEntries(this.regions?.lowerFace),
            eyeLeftVertexCount: countMaskEntries(this.regions?.eyeLeft),
            eyeRightVertexCount: countMaskEntries(this.regions?.eyeRight),
            browLeftVertexCount: countMaskEntries(this.regions?.browLeft),
            browRightVertexCount: countMaskEntries(this.regions?.browRight),
            endpointMaskPainted: this.hasPaintMaskContent(this.endpointPaintMasks),
            endpointActiveZones: [...this.endpointActiveZones],
            pendingMaskPainted: this.hasPaintMaskContent(this.pendingPaintMasks),
            appliedMaskPainted: this.hasPaintMaskContent(this.appliedPaintMasks),
        };
    }

    setInfluence(channel, value) {
        const index = this.channelMap[channel];
        const influences = this.mesh?.morphTargetInfluences;
        if (index === undefined || !influences || typeof influences.length !== 'number') return;
        influences[index] = clamp01(value);
    }

    updateBlink(deltaTime) {
        if (!this.blinkEnabled) {
            this.activeBlink = 0;
            return 0;
        }

        this.blinkTimer -= deltaTime;
        if (this.blinkTimer <= 0) {
            this.activeBlink = this.blinkDuration;
            this.blinkTimer = 2 + Math.random() * 3.5;
        }

        if (this.activeBlink > 0) {
            this.activeBlink = Math.max(0, this.activeBlink - deltaTime);
            const phase = 1 - this.activeBlink / this.blinkDuration;
            return Math.sin(Math.PI * phase);
        }
        return 0;
    }

    getAutoTalkEnvelope() {
        const cadence = Math.sin(this.time * 2.35 + this.autoTalkPhase) * 0.5 + 0.5;
        const syllables = Math.max(0, Math.sin(this.time * 9.2 + this.autoTalkPhase * 1.7));
        const articulation = Math.sin(this.time * 5.4 + 1.3 + this.autoTalkPhase * 0.35) * 0.5 + 0.5;
        const gate = smoothstep(0.18, 0.84, cadence);
        return clamp01((syllables * 0.72 + articulation * 0.28) * gate);
    }

    updateSpeechEnvelope(deltaTime) {
        const dt = Math.max(0, Number(deltaTime) || 0);
        let target = 0;
        if (this.enabled && this.speechActive) {
            if (this.speechMode === 'autoTalk') {
                target = this.getAutoTalkEnvelope();
            } else if (this.speechMode === 'mic') {
                target = this.speechTarget;
            }
        }
        const followRate = target > this.speechEnvelope ? this.speechAttack : this.speechRelease;
        const blend = clamp01(dt * followRate);
        this.speechEnvelope += (target - this.speechEnvelope) * blend;
        return this.speechEnvelope;
    }

    isZoneActive(zone) {
        if (!this.endpointRegions || this.endpointActiveZones.size === 0) return true;
        return this.endpointActiveZones.has(zone);
    }

    applyAnimation(deltaTime) {
        if (!this.mesh || !this.channelMap) return;
        this.time += Math.max(0, deltaTime || 0);

        if (!this.enabled) {
            Object.keys(this.channelMap).forEach((channel) => this.setInfluence(channel, 0));
            return;
        }

        const mouthActive = this.isZoneActive('mouth');
        const eyesActive = this.isZoneActive('eyeLeft') || this.isZoneActive('eyeRight');
        const browsActive = this.isZoneActive('browLeft') || this.isZoneActive('browRight');
        const lowerFaceActive = this.isZoneActive('lowerFace');

        const blink = eyesActive ? this.updateBlink(deltaTime) * this.intensity : 0;
        const speechEnvelope = this.updateSpeechEnvelope(deltaTime) * this.intensity;
        const talkArticulation = Math.sin(this.time * 6.6 + 0.6) * 0.5 + 0.5;
        const talkPuckerWave = Math.sin(this.time * 4.8 + 1.2) * 0.5 + 0.5;
        const generatedBoost = this.usesExistingMorphTargets ? 1 : 1.55;
        const manualTest = this.manualMouthTest;
        const idleJaw = this.speechMode === 'idle'
            ? 0
            : (Math.sin(this.time * 2.15) * 0.5 + 0.5) * 0.04 * this.intensity;
        const jawSpeech = speechEnvelope * (0.52 + talkArticulation * 0.55) * generatedBoost;
        const jawValue = mouthActive ? clamp01(Math.max(idleJaw, jawSpeech, manualTest * 0.95)) : 0;
        const puckerValue = mouthActive ? clamp01(Math.max(
            speechEnvelope * (0.18 + talkPuckerWave * 0.22) * generatedBoost,
            manualTest * 0.34
        )) : 0;
        const speechSmileBias = this.speechMode === 'autoTalk' ? speechEnvelope * 0.08 : 0;
        const speechFrownBias = this.speechMode === 'mic' ? speechEnvelope * 0.04 : 0;

        let smileL = 0;
        let smileR = 0;
        let frownL = 0;
        let frownR = 0;
        let browLift = 0;
        let browDown = 0;

        if (this.emotion === 'happy') {
            smileL = mouthActive ? 0.7 * this.intensity : 0;
            smileR = mouthActive ? 0.7 * this.intensity : 0;
            browLift = browsActive ? 0.14 * this.intensity : 0;
        } else if (this.emotion === 'sad') {
            frownL = mouthActive ? 0.6 * this.intensity : 0;
            frownR = mouthActive ? 0.6 * this.intensity : 0;
            browDown = browsActive ? 0.24 * this.intensity : 0;
        }

        if (mouthActive) {
            smileL = clamp01(smileL + speechSmileBias);
            smileR = clamp01(smileR + speechSmileBias);
            frownL = clamp01(frownL + speechFrownBias * 0.5);
            frownR = clamp01(frownR + speechFrownBias * 0.5);
            if (manualTest > 0) {
                smileL = clamp01(smileL + manualTest * 0.08);
                smileR = clamp01(smileR + manualTest * 0.08);
            }
        }

        this.setInfluence('jawOpen', jawValue);
        this.setInfluence('jawForward', mouthActive ? clamp01(jawValue * 0.18) : 0);
        this.setInfluence('mouthClose', mouthActive ? clamp01((1 - jawValue) * 0.18) : 0);

        this.setInfluence('eyeBlinkLeft', blink);
        this.setInfluence('eyeBlinkRight', blink);
        this.setInfluence('eyeSquintLeft', eyesActive ? clamp01(smileL * 0.28) : 0);
        this.setInfluence('eyeSquintRight', eyesActive ? clamp01(smileR * 0.28) : 0);
        this.setInfluence('eyeWideLeft', eyesActive ? clamp01((1 - blink) * 0.08 + browLift * 0.12) : 0);
        this.setInfluence('eyeWideRight', eyesActive ? clamp01((1 - blink) * 0.08 + browLift * 0.12) : 0);

        this.setInfluence('mouthSmileLeft', smileL);
        this.setInfluence('mouthSmileRight', smileR);
        this.setInfluence('mouthFrownLeft', frownL);
        this.setInfluence('mouthFrownRight', frownR);
        this.setInfluence('mouthPucker', mouthActive ? puckerValue * (this.emotion === 'sad' ? 0.72 : 1) : 0);
        this.setInfluence('mouthFunnel', mouthActive ? clamp01(puckerValue * 0.72 + speechEnvelope * 0.16) : 0);
        this.setInfluence('mouthStretchLeft', mouthActive ? clamp01(speechEnvelope * 0.18 + smileL * 0.12) : 0);
        this.setInfluence('mouthStretchRight', mouthActive ? clamp01(speechEnvelope * 0.18 + smileR * 0.12) : 0);
        this.setInfluence('mouthShrugUpper', mouthActive ? clamp01(speechEnvelope * 0.16) : 0);
        this.setInfluence('mouthShrugLower', mouthActive ? clamp01(speechEnvelope * 0.08) : 0);
        this.setInfluence('mouthRollUpper', mouthActive ? clamp01(puckerValue * 0.16) : 0);
        this.setInfluence('mouthRollLower', mouthActive ? clamp01(puckerValue * 0.12) : 0);
        this.setInfluence('mouthUpperUpLeft', mouthActive ? clamp01(smileL * 0.22) : 0);
        this.setInfluence('mouthUpperUpRight', mouthActive ? clamp01(smileR * 0.22) : 0);
        this.setInfluence('mouthLowerDownLeft', mouthActive ? clamp01(jawValue * 0.44) : 0);
        this.setInfluence('mouthLowerDownRight', mouthActive ? clamp01(jawValue * 0.44) : 0);
        this.setInfluence('mouthDimpleLeft', mouthActive ? clamp01(smileL * 0.2) : 0);
        this.setInfluence('mouthDimpleRight', mouthActive ? clamp01(smileR * 0.2) : 0);
        this.setInfluence('cheekPuff', mouthActive ? clamp01(puckerValue * 0.22) : 0);
        this.setInfluence('cheekSquintLeft', mouthActive ? clamp01(smileL * 0.2) : 0);
        this.setInfluence('cheekSquintRight', mouthActive ? clamp01(smileR * 0.2) : 0);
        this.setInfluence('noseSneerLeft', mouthActive ? clamp01(smileL * 0.12) : 0);
        this.setInfluence('noseSneerRight', mouthActive ? clamp01(smileR * 0.12) : 0);
        this.setInfluence('browInnerUp', browLift);
        this.setInfluence('browOuterUpLeft', browLift);
        this.setInfluence('browOuterUpRight', browLift);
        this.setInfluence('browDownLeft', browDown);
        this.setInfluence('browDownRight', browDown);
    }
}

export default FaceRigController;
