export const VIEWER_PBR_LIGHTING_LIMITS = Object.freeze({
    environment: Object.freeze({ min: 0, max: 2 }),
    reflection: Object.freeze({ min: 0, max: 4 }),
    sun: Object.freeze({ min: 0, max: 3.5 }),
    materialEnvironment: Object.freeze({ min: 0, max: 4 }),
});

function finiteNumber(value, fallback) {
    const number = Number(value);
    return Number.isFinite(number) ? number : Number(fallback);
}

function clamp(value, limits, fallback) {
    const number = finiteNumber(value, fallback);
    return Math.min(limits.max, Math.max(limits.min, number));
}

export function sanitizeViewerLightingValues(values = {}, fallbacks = {}) {
    return {
        environmentIntensity: clamp(
            values.environmentIntensity,
            VIEWER_PBR_LIGHTING_LIMITS.environment,
            fallbacks.environmentIntensity ?? 1,
        ),
        reflectionIntensity: clamp(
            values.reflectionIntensity,
            VIEWER_PBR_LIGHTING_LIMITS.reflection,
            fallbacks.reflectionIntensity ?? 3,
        ),
        sunIntensity: clamp(
            values.sunIntensity,
            VIEWER_PBR_LIGHTING_LIMITS.sun,
            fallbacks.sunIntensity ?? 2.45,
        ),
    };
}

export function safeViewerMaterialEnvironmentIntensity(
    environmentIntensity,
    reflectionIntensity,
    baseIntensity = 1,
) {
    const safe = sanitizeViewerLightingValues({
        environmentIntensity,
        reflectionIntensity,
    });
    const base = Math.max(0, finiteNumber(baseIntensity, 1));
    return clamp(
        base * safe.environmentIntensity * safe.reflectionIntensity,
        VIEWER_PBR_LIGHTING_LIMITS.materialEnvironment,
        1,
    );
}

export function sanitizeViewerPbrMaterial(material) {
    if (!material || typeof material !== 'object') return 0;
    let changed = 0;
    // Unity-parity runtime policy: exported KHR_materials_specular values above
    // one are legal glTF, but Unity's FBX reference path does not reproduce that
    // overdrive. Clamp only specular strength/color here; texture slots stay intact.
    const specularColor = material.specularColor;
    if (specularColor && ['r', 'g', 'b'].every((key) => Number.isFinite(Number(specularColor[key])))) {
        const r = Math.min(1, Math.max(0, Number(specularColor.r)));
        const g = Math.min(1, Math.max(0, Number(specularColor.g)));
        const b = Math.min(1, Math.max(0, Number(specularColor.b)));
        if (r !== specularColor.r || g !== specularColor.g || b !== specularColor.b) {
            if (typeof specularColor.setRGB === 'function') specularColor.setRGB(r, g, b);
            else Object.assign(specularColor, { r, g, b });
            changed += 1;
        }
    }
    if (Number.isFinite(Number(material.specularIntensity))) {
        const next = Math.min(1, Math.max(0, Number(material.specularIntensity)));
        if (next !== material.specularIntensity) {
            material.specularIntensity = next;
            changed += 1;
        }
    }
    if (changed) material.needsUpdate = true;
    return changed;
}
