const UV_TEXTURE_KEYS = [
    'map',
    'alphaMap',
    'aoMap',
    'bumpMap',
    'clearcoatMap',
    'clearcoatNormalMap',
    'clearcoatRoughnessMap',
    'displacementMap',
    'emissiveMap',
    'iridescenceMap',
    'iridescenceThicknessMap',
    'lightMap',
    'metalnessMap',
    'normalMap',
    'roughnessMap',
    'sheenColorMap',
    'sheenRoughnessMap',
    'specularColorMap',
    'specularIntensityMap',
    'specularMap',
    'thicknessMap',
    'transmissionMap',
];

function exactMeshName(value) {
    return String(value || '').trim();
}

/**
 * Normalize common Blender/FBX exporter name drift without guessing semantic
 * mesh roles. In particular, `textured_mesh.obj` and
 * `textured_mesh.obj.002` resolve to the same key.
 */
export function normalizeAppearanceMeshName(value) {
    let name = exactMeshName(value).normalize('NFKD').replace(/[\u0300-\u036f]/g, '').toLowerCase();
    const namespaceParts = name.split(/[|:]/).filter(Boolean);
    if (namespaceParts.length) name = namespaceParts[namespaceParts.length - 1];
    name = name.replace(/(?:[._\-\s]\d{3})+$/g, '');
    // Three.js sanitizes FBX node punctuation, so Blender's `.002` can arrive
    // as a terminal `002`. Strip it only when the suffix follows a letter;
    // exact names still win before normalized matching.
    name = name.replace(/(\p{L})\d{3}$/u, '$1');
    return name.replace(/[^\p{L}\p{N}]+/gu, '');
}

function geometryVertexCount(mesh) {
    const count = Number(mesh?.geometry?.attributes?.position?.count);
    return Number.isInteger(count) && count >= 0 ? count : null;
}

function collectRenderableMeshes(root) {
    const records = [];
    root?.traverse?.((mesh) => {
        if (!mesh?.isMesh || !mesh.material) return;
        const name = exactMeshName(mesh.name);
        records.push({
            mesh,
            name,
            normalizedName: normalizeAppearanceMeshName(name),
            vertexCount: geometryVertexCount(mesh),
        });
    });
    return records;
}

function uniqueVertexCountWithinTargets(targetRecord, targetRecords) {
    if (targetRecord.vertexCount === null) return false;
    return targetRecords.filter((record) => record.vertexCount === targetRecord.vertexCount).length === 1;
}

function resolveSourceRecord(targetRecord, targetRecords, sourceRecords) {
    const tiers = [];
    if (targetRecord.name && targetRecord.vertexCount !== null) {
        tiers.push({
            method: 'exact-name-vertex-count',
            candidates: sourceRecords.filter((source) => (
                source.name === targetRecord.name && source.vertexCount === targetRecord.vertexCount
            )),
        });
    }
    if (targetRecord.name) {
        tiers.push({
            method: 'exact-name',
            candidates: sourceRecords.filter((source) => source.name === targetRecord.name),
        });
    }
    if (targetRecord.normalizedName && targetRecord.vertexCount !== null) {
        tiers.push({
            method: 'normalized-name-vertex-count',
            candidates: sourceRecords.filter((source) => (
                source.normalizedName === targetRecord.normalizedName &&
                source.vertexCount === targetRecord.vertexCount
            )),
        });
    }
    if (targetRecord.normalizedName) {
        tiers.push({
            method: 'normalized-name',
            candidates: sourceRecords.filter((source) => source.normalizedName === targetRecord.normalizedName),
        });
    }
    if (targetRecord.vertexCount !== null && uniqueVertexCountWithinTargets(targetRecord, targetRecords)) {
        tiers.push({
            method: 'unique-vertex-count',
            candidates: sourceRecords.filter((source) => source.vertexCount === targetRecord.vertexCount),
        });
    }

    for (const tier of tiers) {
        if (tier.candidates.length === 1) {
            return { status: 'matched', method: tier.method, sourceRecord: tier.candidates[0] };
        }
        if (tier.candidates.length > 1) {
            return { status: 'ambiguous', method: tier.method, candidates: tier.candidates };
        }
    }
    return { status: 'unmatched' };
}

function sourceMaterials(record) {
    return (Array.isArray(record?.mesh?.material) ? record.mesh.material : [record?.mesh?.material]).filter(Boolean);
}

function materialSlots(mesh) {
    return Array.isArray(mesh?.material) ? mesh.material : [mesh?.material];
}

function requiredUvAttribute(texture) {
    const channel = Number(texture?.channel);
    if (!Number.isInteger(channel) || channel <= 0) return 'uv';
    return `uv${channel}`;
}

function defaultCompatibility({ targetMesh, sourceRecord }) {
    const attributes = targetMesh?.geometry?.attributes || {};
    const positionCount = Number(attributes.position?.count);
    const materials = sourceMaterials(sourceRecord);
    if (!materials.length) {
        return { compatible: false, reason: 'source mesh has no material' };
    }
    const targetSlots = materialSlots(targetMesh);
    const sourceSlots = materialSlots(sourceRecord?.mesh);
    if (targetSlots.some((material) => !material) || sourceSlots.some((material) => !material)) {
        return { compatible: false, reason: 'material slots must not be empty' };
    }
    if (targetSlots.length !== sourceSlots.length) {
        return {
            compatible: false,
            reason: `material slot count differs (${targetSlots.length} target, ${sourceSlots.length} source)`,
        };
    }
    const groups = Array.isArray(targetMesh?.geometry?.groups) ? targetMesh.geometry.groups : [];
    const invalidGroup = groups.find((group) => (
        !Number.isInteger(Number(group?.materialIndex)) ||
        Number(group.materialIndex) < 0 ||
        Number(group.materialIndex) >= sourceSlots.length
    ));
    if (invalidGroup) {
        return {
            compatible: false,
            reason: `target geometry group has invalid materialIndex ${String(invalidGroup.materialIndex)}`,
        };
    }

    for (const material of materials) {
        if (material.vertexColors && (
            !attributes.color || Number(attributes.color.count) !== positionCount
        )) {
            return { compatible: false, reason: 'source material requires full-length COLOR_0' };
        }
        for (const key of UV_TEXTURE_KEYS) {
            const texture = material[key];
            if (!texture) continue;
            const attributeName = requiredUvAttribute(texture);
            if (
                !attributes[attributeName] ||
                Number(attributes[attributeName].count) !== positionCount
            ) {
                return { compatible: false, reason: `source ${key} requires full-length ${attributeName}` };
            }
        }
    }
    return { compatible: true };
}

function normalizeCompatibility(value) {
    if (value === false) return { compatible: false, reason: 'custom compatibility check rejected match' };
    if (typeof value === 'string') return { compatible: false, reason: value };
    if (value && typeof value === 'object' && value.compatible === false) {
        return { compatible: false, reason: String(value.reason || 'custom compatibility check rejected match') };
    }
    return { compatible: true };
}

export function cloneAppearanceMaterial(material, { textureFlipY = null } = {}) {
    const clonedMaterial = typeof material?.clone === 'function' ? material.clone() : material;
    if (!clonedMaterial || textureFlipY === null) return clonedMaterial;

    const clonedTextures = new Map();
    UV_TEXTURE_KEYS.forEach((key) => {
        const sourceTexture = material?.[key];
        if (!sourceTexture) return;
        let clonedTexture = clonedTextures.get(sourceTexture);
        if (!clonedTexture) {
            clonedTexture = typeof sourceTexture.clone === 'function'
                ? sourceTexture.clone()
                : sourceTexture;
            if (clonedTexture !== sourceTexture) {
                clonedTexture.flipY = !!textureFlipY;
                clonedTexture.needsUpdate = true;
            }
            clonedTextures.set(sourceTexture, clonedTexture);
        }
        clonedMaterial[key] = clonedTexture;
    });
    return clonedMaterial;
}

function cloneMaterialAssignment(material, materialCloner) {
    if (Array.isArray(material)) return material.map((entry) => materialCloner(entry));
    return materialCloner(material);
}

function recordSummary(record) {
    return {
        name: record.name,
        normalizedName: record.normalizedName,
        vertexCount: record.vertexCount,
    };
}

/**
 * Copy only appearance materials from a GLB donor onto an FBX target. Geometry,
 * skinning, bones, morphs and animation clips are never modified.
 *
 * Ambiguous name matches abort the entire operation before the first mutation.
 */
export function transferAppearanceMaterials(targetRoot, sourceRoot, {
    materialCloner = cloneAppearanceMaterial,
    validateMatch = null,
} = {}) {
    const targetRecords = collectRenderableMeshes(targetRoot);
    const sourceRecords = collectRenderableMeshes(sourceRoot);
    const report = {
        targetMeshCount: targetRecords.length,
        sourceMeshCount: sourceRecords.length,
        transferredMeshCount: 0,
        complete: false,
        aborted: false,
        abortedReason: null,
        transferred: [],
        unmatched: [],
        ambiguous: [],
        incompatible: [],
    };

    const decisions = targetRecords.map((targetRecord) => ({
        targetRecord,
        resolution: resolveSourceRecord(targetRecord, targetRecords, sourceRecords),
    }));

    decisions.forEach(({ targetRecord, resolution }) => {
        if (resolution.status === 'unmatched') {
            report.unmatched.push({ target: recordSummary(targetRecord) });
        } else if (resolution.status === 'ambiguous') {
            report.ambiguous.push({
                target: recordSummary(targetRecord),
                method: resolution.method,
                candidates: resolution.candidates.map(recordSummary),
            });
        }
    });

    if (report.ambiguous.length) {
        report.aborted = true;
        report.abortedReason = 'ambiguous-mesh-match';
        return report;
    }

    const compatibleDecisions = [];
    decisions.forEach(({ targetRecord, resolution }) => {
        if (resolution.status !== 'matched') return;
        const context = {
            targetMesh: targetRecord.mesh,
            sourceMesh: resolution.sourceRecord.mesh,
            targetRecord,
            sourceRecord: resolution.sourceRecord,
            method: resolution.method,
        };
        let compatibility = defaultCompatibility(context);
        if (compatibility.compatible && typeof validateMatch === 'function') {
            try {
                compatibility = normalizeCompatibility(validateMatch(context));
            } catch (error) {
                compatibility = {
                    compatible: false,
                    reason: error?.message || String(error),
                };
            }
        }
        if (!compatibility.compatible) {
            report.incompatible.push({
                target: recordSummary(targetRecord),
                source: recordSummary(resolution.sourceRecord),
                method: resolution.method,
                reason: compatibility.reason,
            });
            return;
        }
        compatibleDecisions.push({ targetRecord, resolution });
    });

    compatibleDecisions.forEach(({ targetRecord, resolution }) => {
        targetRecord.mesh.material = cloneMaterialAssignment(
            resolution.sourceRecord.mesh.material,
            materialCloner,
        );
        const assigned = Array.isArray(targetRecord.mesh.material)
            ? targetRecord.mesh.material
            : [targetRecord.mesh.material];
        assigned.filter(Boolean).forEach((material) => {
            material.needsUpdate = true;
        });
        report.transferred.push({
            target: recordSummary(targetRecord),
            source: recordSummary(resolution.sourceRecord),
            method: resolution.method,
        });
    });

    report.transferredMeshCount = report.transferred.length;
    report.complete = (
        report.targetMeshCount > 0 &&
        report.transferredMeshCount === report.targetMeshCount &&
        report.unmatched.length === 0 &&
        report.incompatible.length === 0
    );
    return report;
}
