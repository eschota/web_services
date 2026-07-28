const SOFTWARE_RENDERER_PATTERNS = Object.freeze([
    /swiftshader/i,
    /llvmpipe/i,
    /softpipe/i,
    /software rasterizer/i,
    /software adapter/i,
    /microsoft basic render/i,
    /\bswrast\b/i,
]);

function finitePositive(value, fallback) {
    const number = Number(value);
    return Number.isFinite(number) && number > 0 ? number : fallback;
}

export function updateBackdropCover(texture, viewAspect) {
    const image = texture?.image;
    const imageWidth = Number(image?.naturalWidth || image?.videoWidth || image?.width || 0);
    const imageHeight = Number(image?.naturalHeight || image?.videoHeight || image?.height || 0);
    if (!texture?.offset?.set || !texture?.repeat?.set || !imageWidth || !imageHeight) return false;

    const safeViewAspect = finitePositive(viewAspect, 1);
    const cacheKey = [
        imageWidth,
        imageHeight,
        Math.round(safeViewAspect * 100000) / 100000,
    ].join(':');
    texture.userData = texture.userData || {};
    if (texture.userData.autorigBackdropCoverKey === cacheKey) return false;

    const imageAspect = imageWidth / imageHeight;
    let offsetX = 0;
    let offsetY = 0;
    let repeatX = 1;
    let repeatY = 1;
    if (imageAspect > safeViewAspect) {
        repeatX = safeViewAspect / imageAspect;
        offsetX = (1 - repeatX) * 0.5;
    } else {
        repeatY = imageAspect / safeViewAspect;
        offsetY = (1 - repeatY) * 0.5;
    }

    texture.offset.set(offsetX, offsetY);
    texture.repeat.set(repeatX, repeatY);
    texture.userData.autorigBackdropCoverKey = cacheKey;
    return true;
}

export function secondaryViewportIntervalMs(profile = {}) {
    const fps = finitePositive(profile.secondaryViewportFps, 30);
    return 1000 / Math.min(60, Math.max(0.5, fps));
}

export function detectSoftwareWebGL(rendererInfo = {}) {
    const renderer = String(rendererInfo.renderer || '');
    const vendor = String(rendererInfo.vendor || '');
    const combined = `${renderer} ${vendor}`.trim();
    return {
        software: SOFTWARE_RENDERER_PATTERNS.some((pattern) => pattern.test(combined)),
        renderer,
        vendor,
    };
}

export function rootMatrixSignature(root) {
    root?.updateMatrixWorld?.(true);
    const elements = root?.matrixWorld?.elements;
    if (!elements || elements.length !== 16) return '';
    return Array.from(elements, (value) => {
        const number = Number(value);
        return Number.isFinite(number) ? Math.round(number * 100000) / 100000 : 'x';
    }).join(',');
}

export function cachedRootBounds(cache, root, compute) {
    if (!(cache instanceof WeakMap) || !root || typeof compute !== 'function') {
        return { value: null, cached: false };
    }
    const signature = rootMatrixSignature(root);
    const existing = cache.get(root);
    if (existing && existing.signature === signature) {
        return { value: existing.value, cached: true };
    }
    const value = compute(root);
    if (value) cache.set(root, { signature, value });
    else cache.delete(root);
    return { value, cached: false };
}
