export const VIEWER_QUALITY_ORDER = Object.freeze(['high', 'balanced', 'low', 'emergency']);

export function createAdaptiveQualityState(options = {}) {
    const mode = VIEWER_QUALITY_ORDER.includes(options.mode) ? options.mode : 'high';
    return {
        mode,
        lowSeconds: 0,
        highSeconds: 0,
        cooldownUntil: Number(options.cooldownUntil) || 0,
        ignoreUntil: Number(options.ignoreUntil) || 0,
        recoveryProbe: null,
    };
}

export function suppressAdaptiveQuality(state, now, durationMs = 2000) {
    return {
        ...createAdaptiveQualityState(state),
        ...state,
        lowSeconds: 0,
        highSeconds: 0,
        ignoreUntil: Math.max(Number(state?.ignoreUntil) || 0, Number(now) + Math.max(0, Number(durationMs) || 0)),
    };
}

export function sampleAdaptiveQuality(state, sample = {}) {
    const current = { ...createAdaptiveQualityState(state), ...state };
    const now = Number(sample.now) || 0;
    const fps = Number(sample.fps) || 0;
    const p95FrameTime = Number(sample.p95FrameTime);
    const seconds = Math.max(0.1, Number(sample.seconds) || 1);

    if (sample.active === false || now < current.ignoreUntil || now < current.cooldownUntil) {
        return { state: current, change: null };
    }

    const isLow = fps < 29 || (Number.isFinite(p95FrameTime) && p95FrameTime > 42);
    const isSevere = fps < 20;
    const isHealthy = fps > 45 && Number.isFinite(p95FrameTime) && p95FrameTime < 28;

    if (current.recoveryProbe && now < current.recoveryProbe.until && isLow) {
        const next = {
            ...current,
            mode: current.recoveryProbe.from,
            lowSeconds: 0,
            highSeconds: 0,
            cooldownUntil: now + 3000,
            recoveryProbe: null,
        };
        return { state: next, change: { from: current.mode, to: next.mode, reason: `recovery-rollback-${fps}` } };
    }
    if (current.recoveryProbe && now >= current.recoveryProbe.until) current.recoveryProbe = null;

    current.lowSeconds = isLow ? current.lowSeconds + seconds : 0;
    current.highSeconds = isHealthy ? current.highSeconds + seconds : 0;

    const index = VIEWER_QUALITY_ORDER.indexOf(current.mode);
    const lowThreshold = isSevere ? 2 : 3;
    if (current.lowSeconds >= lowThreshold && index < VIEWER_QUALITY_ORDER.length - 1) {
        const from = current.mode;
        current.mode = VIEWER_QUALITY_ORDER[index + 1];
        current.lowSeconds = 0;
        current.highSeconds = 0;
        current.cooldownUntil = now + 2000;
        current.recoveryProbe = null;
        return { state: current, change: { from, to: current.mode, reason: `fps-drop-${fps}` } };
    }

    if (current.highSeconds >= 12 && index > 0) {
        const from = current.mode;
        current.mode = VIEWER_QUALITY_ORDER[index - 1];
        current.lowSeconds = 0;
        current.highSeconds = 0;
        current.cooldownUntil = now + 2000;
        current.recoveryProbe = { from, to: current.mode, until: now + 5000 };
        return { state: current, change: { from, to: current.mode, reason: `fps-recover-${fps}` } };
    }

    return { state: current, change: null };
}
