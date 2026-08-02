(function installAnimationCatalogRetryPolicy(globalScope) {
    'use strict';

    const RETRY_DELAYS_MS = Object.freeze([1500, 4000, 9000, 20000, 40000]);

    function readyEntries(catalog) {
        return Array.isArray(catalog?.animations)
            ? catalog.animations.filter((entry) => Boolean(entry?.available) && Boolean(entry?.ready))
            : [];
    }

    function shouldRetry(taskStatus, catalog) {
        const animations = Array.isArray(catalog?.animations) ? catalog.animations : [];
        return String(taskStatus || '').toLowerCase() === 'done'
            && animations.length > 0
            && readyEntries(catalog).length === 0;
    }

    function delayForAttempt(attempt) {
        const index = Number(attempt);
        return Number.isInteger(index) && index >= 0 && index < RETRY_DELAYS_MS.length
            ? RETRY_DELAYS_MS[index]
            : null;
    }

    globalScope.AnimationCatalogRetryPolicy = Object.freeze({
        RETRY_DELAYS_MS,
        readyEntries,
        shouldRetry,
        delayForAttempt,
    });
})(globalThis);
