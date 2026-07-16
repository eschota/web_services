const DEFAULT_FRAME_COUNT = 49;
const DEFAULT_FPS = 30;
const FITTED_ANIMATION_SCHEMA = 'autorig-browser-fitted-animation.v1';

function finiteNumber(value, field) {
    const number = Number(value);
    if (!Number.isFinite(number)) throw new Error(`${field} must be finite`);
    return number;
}

function positiveInteger(value, field) {
    const number = Math.trunc(finiteNumber(value, field));
    if (number <= 0) throw new Error(`${field} must be a positive integer`);
    return number;
}

function clamp(value, minimum, maximum) {
    return Math.min(maximum, Math.max(minimum, value));
}

function cloneColor(value, field) {
    if (!Array.isArray(value) || value.length !== 3) {
        throw new Error(`${field} must be an RGB triplet`);
    }
    return value.map((channel, index) => {
        const number = finiteNumber(channel, `${field}[${index}]`);
        if (number < 0 || number > 1) throw new Error(`${field}[${index}] must be inside [0, 1]`);
        return number;
    });
}

function paletteSource(value) {
    return value?.semantic_profile?.palette_linear
        || value?.palette_linear
        || value?.pixels?.palette_linear
        || value;
}

function assignmentSource(value, options) {
    return options?.nearFarAssignment
        || value?.classification?.near_far_assignment
        || value?.near_far_assignment
        || null;
}

/**
 * Convert the canonical Horse near/far semantic palette to the left/right
 * labels consumed by animation-fitting-semantic-tracker.js.
 */
export function mapHorseSemanticPalette(value, options = {}) {
    const source = paletteSource(value);
    if (!source || typeof source !== 'object' || Array.isArray(source)) {
        throw new Error('Horse semantic palette must be an object');
    }
    const directLabels = ['fore_left', 'fore_right', 'hind_left', 'hind_right'];
    if (directLabels.every((label) => Array.isArray(source[label]))) {
        return Object.fromEntries(directLabels.map((label) => [label, cloneColor(source[label], label)]));
    }

    const assignment = assignmentSource(value, options);
    const result = {};
    for (const family of ['fore', 'hind']) {
        const nearLabel = assignment?.[family]?.near_source_group
            || options?.[`${family}NearSourceGroup`]
            || `${family}_left`;
        const farLabel = assignment?.[family]?.far_source_group
            || options?.[`${family}FarSourceGroup`]
            || `${family}_right`;
        const allowed = new Set([`${family}_left`, `${family}_right`]);
        if (!allowed.has(nearLabel) || !allowed.has(farLabel) || nearLabel === farLabel) {
            throw new Error(`${family} near/far assignment must map once to left and right`);
        }
        result[nearLabel] = cloneColor(source[`${family}_near`], `${family}_near`);
        result[farLabel] = cloneColor(source[`${family}_far`], `${family}_far`);
    }
    return result;
}

function abortError() {
    if (typeof DOMException === 'function') return new DOMException('The operation was aborted', 'AbortError');
    const error = new Error('The operation was aborted');
    error.name = 'AbortError';
    return error;
}

function throwIfAborted(signal) {
    if (signal?.aborted) throw signal.reason || abortError();
}

function waitForMediaEvent(target, eventName, options = {}) {
    const signal = options.signal;
    throwIfAborted(signal);
    return new Promise((resolve, reject) => {
        const cleanup = () => {
            target.removeEventListener?.(eventName, onReady);
            target.removeEventListener?.('error', onError);
            signal?.removeEventListener?.('abort', onAbort);
        };
        const onReady = () => {
            cleanup();
            resolve();
        };
        const onError = () => {
            cleanup();
            reject(target.error || new Error(`Video emitted an error before ${eventName}`));
        };
        const onAbort = () => {
            cleanup();
            reject(signal.reason || abortError());
        };
        target.addEventListener?.(eventName, onReady, { once: true });
        target.addEventListener?.('error', onError, { once: true });
        signal?.addEventListener?.('abort', onAbort, { once: true });
    });
}

async function seekVideo(video, timestampSeconds, signal) {
    throwIfAborted(signal);
    const target = Math.max(0, timestampSeconds);
    if (Math.abs(Number(video.currentTime || 0) - target) <= 1e-7 && Number(video.readyState || 0) >= 2) {
        return;
    }
    const ready = waitForMediaEvent(video, 'seeked', { signal });
    video.currentTime = target;
    await ready;
}

function isBlobLike(source) {
    return typeof Blob === 'function' && source instanceof Blob;
}

function resolveVideoSource(source, options) {
    const urlApi = options.urlApi || globalThis.URL;
    if (isBlobLike(source)) {
        if (typeof urlApi?.createObjectURL !== 'function') throw new Error('Blob video URLs are unavailable');
        const objectUrl = urlApi.createObjectURL(source);
        return { src: objectUrl, revoke: () => urlApi.revokeObjectURL?.(objectUrl) };
    }
    const raw = String(source || '').trim();
    if (!raw) throw new Error('An MP4 file or same-origin URL is required');
    const location = options.location || globalThis.location;
    if (!location?.href || !location?.origin) return { src: raw, revoke: () => {} };
    const resolved = new URL(raw, location.href);
    if (resolved.origin !== location.origin) {
        throw new Error('Animation fitting video URL must be same-origin');
    }
    return { src: resolved.href, revoke: () => {} };
}

/** Decode one immutable CFR MP4 presentation frame per requested timestamp. */
export async function decodeVideoFramesExact(source, options = {}) {
    const frameCount = positiveInteger(options.frameCount ?? DEFAULT_FRAME_COUNT, 'frameCount');
    const fps = finiteNumber(options.fps ?? DEFAULT_FPS, 'fps');
    if (fps <= 0) throw new Error('fps must be positive');
    const documentRef = options.document || globalThis.document;
    const video = options.video || options.videoFactory?.() || documentRef?.createElement?.('video');
    const canvas = options.canvas || options.canvasFactory?.() || documentRef?.createElement?.('canvas');
    if (!video || !canvas) throw new Error('Video and canvas factories are required');
    const context = canvas.getContext?.('2d', { willReadFrequently: true, colorSpace: 'srgb' });
    if (!context) throw new Error('A readable 2D canvas context is required');

    const resolvedSource = resolveVideoSource(source, options);
    try {
        video.muted = true;
        video.defaultMuted = true;
        video.playsInline = true;
        video.preload = 'auto';
        if (typeof source === 'string') video.crossOrigin = 'anonymous';
        video.src = resolvedSource.src;
        video.load?.();
        if (Number(video.readyState || 0) < 1) {
            await waitForMediaEvent(video, 'loadedmetadata', { signal: options.signal });
        }
        if (Number(video.readyState || 0) < 2) {
            await waitForMediaEvent(video, 'loadeddata', { signal: options.signal });
        }
        throwIfAborted(options.signal);

        const duration = finiteNumber(video.duration, 'video duration');
        if (duration <= 0) throw new Error('video duration must be positive');
        const expectedDuration = frameCount / fps;
        const durationTolerance = finiteNumber(
            options.durationToleranceSeconds ?? Math.max(0.004, 0.75 / fps),
            'durationToleranceSeconds',
        );
        if (durationTolerance < 0) throw new Error('durationToleranceSeconds must be non-negative');
        if (options.verifyFrameContract !== false && Math.abs(duration - expectedDuration) > durationTolerance) {
            throw new Error(
                `Video frame contract mismatch: expected ${frameCount} frames at ${fps} fps `
                + `(duration ${expectedDuration.toFixed(6)}s), got ${duration.toFixed(6)}s`,
            );
        }
        const width = positiveInteger(options.width ?? video.videoWidth, 'video width');
        const height = positiveInteger(options.height ?? video.videoHeight, 'video height');
        canvas.width = width;
        canvas.height = height;
        const frames = [];
        for (let index = 0; index < frameCount; index += 1) {
            const timestampSeconds = index / fps;
            await seekVideo(video, timestampSeconds, options.signal);
            context.drawImage(video, 0, 0, width, height);
            const imageData = context.getImageData(0, 0, width, height);
            frames.push({
                index,
                timestampSeconds,
                width,
                height,
                data: new Uint8ClampedArray(imageData.data),
            });
            options.onProgress?.({
                stage: 'decoding',
                frameIndex: index,
                frameCount,
                progress: (index + 1) / frameCount,
            });
        }
        return { frames, durationSeconds: duration, width, height, frameCount, fps };
    } finally {
        try {
            video.pause?.();
            video.removeAttribute?.('src');
            video.load?.();
        } finally {
            resolvedSource.revoke();
        }
    }
}

function finiteArray(value, field) {
    if (!Array.isArray(value) && !ArrayBuffer.isView(value)) {
        throw new Error(`${field} must be an array`);
    }
    return Array.from(value, (item, index) => finiteNumber(item, `${field}[${index}]`));
}

function keyframeTrackName(track, property) {
    const explicit = String(track?.name || '').trim();
    if (explicit.endsWith(`.${property}`)) return explicit;
    const bone = String(track?.bone || track?.target || explicit || '').trim();
    if (!bone) throw new Error(`${property} track is missing a bone/name`);
    return `${bone}.${property}`;
}

function makeThreeTrack(track, property, THREE, field) {
    const times = finiteArray(track?.times, `${field}.times`);
    const values = finiteArray(track?.values, `${field}.values`);
    const stride = property === 'quaternion' ? 4 : 3;
    if (!times.length || values.length !== times.length * stride) {
        throw new Error(`${field} must contain ${stride} values per keyframe`);
    }
    const Constructor = property === 'quaternion'
        ? THREE?.QuaternionKeyframeTrack
        : THREE?.VectorKeyframeTrack;
    if (typeof Constructor !== 'function') throw new Error(`THREE.${property === 'quaternion' ? 'QuaternionKeyframeTrack' : 'VectorKeyframeTrack'} is required`);
    return new Constructor(keyframeTrackName(track, property), times, values);
}

function fittedPayload(result) {
    return result?.clipTrackJson || result?.clip_track_json || result;
}

/** Convert the pure browser solver JSON contract into a Three.js clip. */
export function buildThreeAnimationClip(resultValue, THREE, options = {}) {
    const result = fittedPayload(resultValue);
    if (!result || typeof result !== 'object') throw new Error('fitted animation result is required');
    if (result.schema && result.schema !== FITTED_ANIMATION_SCHEMA) {
        throw new Error(`unsupported fitted animation schema: ${result.schema}`);
    }
    if (typeof THREE?.AnimationClip !== 'function') throw new Error('THREE.AnimationClip is required');
    const sourceTracks = Array.isArray(result.tracks) ? result.tracks : [];
    if (!sourceTracks.length) throw new Error('fitted animation has no quaternion tracks');
    const tracks = sourceTracks.map((track, index) => makeThreeTrack(
        track,
        'quaternion',
        THREE,
        `tracks[${index}]`,
    ));
    const positionTracks = Array.isArray(result.positionTracks) ? result.positionTracks : [];
    positionTracks.forEach((track, index) => {
        tracks.push(makeThreeTrack(track, 'position', THREE, `positionTracks[${index}]`));
    });
    if (result.rootTrack) {
        tracks.push(makeThreeTrack(result.rootTrack, 'position', THREE, 'rootTrack'));
    }
    const name = String(options.clipName || result.name || result.clipName || 'Horse_Fitted').trim() || 'Horse_Fitted';
    const duration = Number.isFinite(Number(result.durationSeconds))
        ? Number(result.durationSeconds)
        : -1;
    const clip = new THREE.AnimationClip(name, duration, tracks);
    clip.userData = {
        ...(clip.userData || {}),
        autorigAnimationFitting: {
            schema: result.schema || FITTED_ANIMATION_SCHEMA,
            frameCount: Number(result.frameCount || 0),
            fps: Number(result.fps || 0),
            loop: result.loop !== false,
            qa: result.qa || {},
        },
    };
    return clip;
}

export function flattenQaMetrics(value, prefix = '', output = []) {
    if (!value || typeof value !== 'object') return output;
    Object.keys(value).sort().forEach((key) => {
        const path = prefix ? `${prefix}.${key}` : key;
        const item = value[key];
        if (item && typeof item === 'object' && !Array.isArray(item)) {
            flattenQaMetrics(item, path, output);
        } else if (['number', 'string', 'boolean'].includes(typeof item)) {
            output.push({ key: path, value: item });
        }
    });
    return output;
}

function injectedFunction(value, names) {
    if (typeof value === 'function') return value;
    for (const name of names) {
        if (typeof value?.[name] === 'function') return value[name].bind(value);
    }
    return null;
}

function ensurePanelStyles(documentRef) {
    if (!documentRef?.head || documentRef.getElementById?.('task-animation-fitting-panel-styles')) return;
    const style = documentRef.createElement('style');
    style.id = 'task-animation-fitting-panel-styles';
    style.textContent = `
        .task-animation-fitting-panel{display:grid;gap:.65rem;padding:.75rem;border:1px solid rgba(96,165,250,.28);border-radius:10px;background:rgba(2,6,23,.62);color:#e2e8f0;font:600 12px/1.35 system-ui,sans-serif}
        .task-animation-fitting-panel__source{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:.4rem}.task-animation-fitting-panel input,.task-animation-fitting-panel button{min-height:32px;border:1px solid rgba(148,163,184,.32);border-radius:7px;background:#0f172a;color:#e2e8f0}.task-animation-fitting-panel input{min-width:0;padding:0 .55rem}.task-animation-fitting-panel button{padding:0 .75rem;cursor:pointer}.task-animation-fitting-panel button:disabled{opacity:.45;cursor:not-allowed}
        .task-animation-fitting-panel__file{grid-column:1/-1}.task-animation-fitting-panel__progress{height:5px;overflow:hidden;border-radius:5px;background:rgba(148,163,184,.18)}.task-animation-fitting-panel__progress>span{display:block;height:100%;width:0;background:linear-gradient(90deg,#06b6d4,#6366f1)}
        .task-animation-fitting-panel__preview{width:100%;max-height:260px;object-fit:contain;background:#020617;border-radius:8px}.task-animation-fitting-panel__scrub{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:.45rem;align-items:center}.task-animation-fitting-panel__qa{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:.2rem .7rem;margin:0}.task-animation-fitting-panel__qa dt{color:#94a3b8;overflow-wrap:anywhere}.task-animation-fitting-panel__qa dd{margin:0;color:#f8fafc;text-align:right}.task-animation-fitting-panel__status[data-error='1']{color:#fca5a5}
    `;
    documentRef.head.appendChild(style);
}

export class TaskAnimationFittingPanel {
    constructor(options = {}) {
        this.THREE = options.THREE || null;
        this.document = options.document || globalThis.document || null;
        this.decoder = options.decoder || decodeVideoFramesExact;
        this.tracker = options.tracker || null;
        this.solver = options.solver || null;
        this.trackerLoader = options.trackerLoader || (() => import('./animation-fitting-semantic-tracker.js?v=2'));
        this.solverLoader = options.solverLoader || (() => import('./animation-fitting-browser-core.js'));
        this.frameCount = positiveInteger(options.frameCount ?? DEFAULT_FRAME_COUNT, 'frameCount');
        this.fps = finiteNumber(options.fps ?? DEFAULT_FPS, 'fps');
        if (this.fps <= 0) throw new Error('fps must be positive');
        this.skeleton = options.skeleton || null;
        this.palette = options.palette || null;
        this.paletteOptions = options.paletteOptions || {};
        this.trackerOptions = options.trackerOptions || {};
        this.solveOptions = options.solveOptions || {};
        this.clipName = String(options.clipName || 'Horse_Walk_Fitted');
        this.onClipReady = typeof options.onClipReady === 'function' ? options.onClipReady : () => {};
        this.onStatus = typeof options.onStatus === 'function' ? options.onStatus : () => {};
        this.onMetrics = typeof options.onMetrics === 'function' ? options.onMetrics : () => {};
        this.onScrub = typeof options.onScrub === 'function' ? options.onScrub : () => {};
        this.frames = [];
        this.qa = {};
        this.busy = false;
        this.abortController = null;
        this.selectedSource = null;
        this.root = null;
        this.elements = {};
        if (options.container) this.mount(options.container);
    }

    configure(options = {}) {
        if ('THREE' in options) this.THREE = options.THREE;
        if ('skeleton' in options) this.skeleton = options.skeleton;
        if ('palette' in options) this.palette = options.palette;
        if ('paletteOptions' in options) this.paletteOptions = options.paletteOptions || {};
        if ('clipName' in options) this.clipName = String(options.clipName || 'Horse_Walk_Fitted');
        if ('solveOptions' in options) this.solveOptions = options.solveOptions || {};
        this._refreshControls();
        return this;
    }

    async _getTracker() {
        let callback = injectedFunction(this.tracker, ['buildSemanticObservations']);
        if (callback) return callback;
        this.tracker = await this.trackerLoader();
        callback = injectedFunction(this.tracker, ['buildSemanticObservations']);
        if (!callback) throw new Error('semantic tracker does not export buildSemanticObservations');
        return callback;
    }

    async _getSolver() {
        let callback = injectedFunction(this.solver, [
            'fitBrowserAnimation',
            'solveBrowserAnimationFitting',
            'fitAnimationFromObservations',
        ]);
        if (callback) return callback;
        this.solver = await this.solverLoader();
        callback = injectedFunction(this.solver, [
            'fitBrowserAnimation',
            'solveBrowserAnimationFitting',
            'fitAnimationFromObservations',
        ]);
        if (!callback) throw new Error('browser fitting core does not export fitBrowserAnimation');
        return callback;
    }

    _setStatus(stage, message, progress = 0, error = false) {
        const snapshot = {
            stage,
            message: String(message || ''),
            progress: clamp(Number(progress) || 0, 0, 1),
            error: Boolean(error),
        };
        this.onStatus(snapshot);
        if (this.elements.status) {
            this.elements.status.textContent = snapshot.message;
            this.elements.status.dataset.error = snapshot.error ? '1' : '0';
        }
        if (this.elements.progress) this.elements.progress.style.width = `${snapshot.progress * 100}%`;
        return snapshot;
    }

    async fitSource(source = this.selectedSource, options = {}) {
        if (this.busy) throw new Error('animation fitting is already running');
        if (!this.skeleton) throw new Error('browser fitting skeleton is not configured');
        if (!this.palette) throw new Error('Horse semantic palette is not configured');
        if (!source) throw new Error('An MP4 file or same-origin URL is required');
        this.busy = true;
        this.abortController = new AbortController();
        this._refreshControls();
        try {
            this._setStatus('decoding', `Decoding exactly ${this.frameCount} frames...`, 0.02);
            const decoded = await this.decoder(source, {
                ...(options.decoderOptions || {}),
                frameCount: this.frameCount,
                fps: this.fps,
                signal: this.abortController.signal,
                onProgress: (event) => {
                    const ratio = Number(event?.progress || 0);
                    this._setStatus('decoding', `Decoding frame ${Number(event?.frameIndex || 0) + 1}/${this.frameCount}...`, 0.05 + ratio * 0.35);
                },
            });
            const frames = Array.isArray(decoded) ? decoded : decoded?.frames;
            if (!Array.isArray(frames) || frames.length !== this.frameCount) {
                throw new Error(`decoder returned ${frames?.length ?? 0} frames; expected exactly ${this.frameCount}`);
            }
            this.frames = frames;
            this._refreshScrubber();
            this.scrub(0);

            this._setStatus('tracking', 'Tracking four persistent Horse limbs...', 0.45);
            const tracker = await this._getTracker();
            const palette = mapHorseSemanticPalette(this.palette, {
                ...this.paletteOptions,
                ...(options.paletteOptions || {}),
            });
            const observations = await tracker(frames, palette, {
                ...this.trackerOptions,
                ...(options.trackerOptions || {}),
                fps: this.fps,
            });
            if (Number(observations?.frame_count) !== this.frameCount) {
                throw new Error('semantic tracker returned an unexpected frame count');
            }

            this._setStatus('solving', 'Solving constrained skeleton motion in the browser...', 0.68);
            const solver = await this._getSolver();
            const result = await solver({
                skeleton: this.skeleton,
                observations,
                options: {
                    ...this.solveOptions,
                    ...(options.solveOptions || {}),
                },
            });
            if (!result || typeof result !== 'object') throw new Error('browser fitting core returned no result');
            const clip = buildThreeAnimationClip(result, options.THREE || this.THREE, {
                clipName: options.clipName || this.clipName,
            });
            this.qa = result.qa || {};
            this._renderQa(this.qa);
            this.onMetrics(this.qa, result);
            const context = { result, observations, frames, qa: this.qa };
            await this.onClipReady(clip, context);
            this._setStatus('ready', `Fitted clip ready: ${clip.name}`, 1);
            return { clip, ...context };
        } catch (error) {
            const cancelled = error?.name === 'AbortError';
            this._setStatus(cancelled ? 'cancelled' : 'failed', cancelled ? 'Animation fitting cancelled.' : String(error?.message || error), 0, !cancelled);
            throw error;
        } finally {
            this.busy = false;
            this.abortController = null;
            this._refreshControls();
        }
    }

    cancel() {
        this.abortController?.abort();
    }

    scrub(frameIndex) {
        if (!this.frames.length) return null;
        const index = clamp(Math.trunc(Number(frameIndex) || 0), 0, this.frames.length - 1);
        const frame = this.frames[index];
        const canvas = this.elements.canvas;
        if (canvas) {
            canvas.width = frame.width;
            canvas.height = frame.height;
            const context = canvas.getContext('2d');
            if (context) {
                const image = context.createImageData(frame.width, frame.height);
                image.data.set(frame.data);
                context.putImageData(image, 0, 0);
            }
        }
        if (this.elements.scrubber) this.elements.scrubber.value = String(index);
        if (this.elements.scrubLabel) {
            const timestamp = Number(frame.timestampSeconds);
            this.elements.scrubLabel.textContent = `${index + 1}/${this.frames.length}${Number.isFinite(timestamp) ? ` · ${timestamp.toFixed(3)}s` : ''}`;
        }
        this.onScrub({ index, frame, frameCount: this.frames.length });
        return frame;
    }

    _renderQa(qa) {
        const target = this.elements.qa;
        if (!target) return;
        target.replaceChildren();
        const rows = flattenQaMetrics(qa).slice(0, 40);
        rows.forEach((row) => {
            const term = this.document.createElement('dt');
            term.textContent = row.key;
            const detail = this.document.createElement('dd');
            detail.textContent = typeof row.value === 'number' ? Number(row.value).toFixed(4) : String(row.value);
            target.append(term, detail);
        });
    }

    _refreshScrubber() {
        if (!this.elements.scrubber) return;
        this.elements.scrubber.min = '0';
        this.elements.scrubber.max = String(Math.max(0, this.frames.length - 1));
        this.elements.scrubber.disabled = !this.frames.length;
    }

    _refreshControls() {
        if (this.elements.start) this.elements.start.disabled = this.busy || !this.selectedSource || !this.skeleton || !this.palette;
        if (this.elements.cancel) this.elements.cancel.disabled = !this.busy;
    }

    mount(container) {
        if (!container?.appendChild || !this.document?.createElement) throw new Error('panel container and document are required');
        this.destroy(false);
        ensurePanelStyles(this.document);
        const root = this.document.createElement('section');
        root.className = 'task-animation-fitting-panel';
        root.setAttribute('aria-label', 'Browser animation fitting');
        root.innerHTML = `
            <div class="task-animation-fitting-panel__source">
                <input type="url" data-source-url placeholder="Same-origin MP4 URL" aria-label="Fitting video URL">
                <button type="button" data-start>Fit animation</button>
                <input class="task-animation-fitting-panel__file" type="file" accept="video/mp4,.mp4" data-source-file aria-label="Choose fitting MP4">
            </div>
            <div class="task-animation-fitting-panel__progress" aria-hidden="true"><span data-progress></span></div>
            <div class="task-animation-fitting-panel__status" data-status aria-live="polite">Choose a semantic Horse MP4.</div>
            <canvas class="task-animation-fitting-panel__preview" data-preview></canvas>
            <div class="task-animation-fitting-panel__scrub">
                <input type="range" min="0" max="0" value="0" disabled data-scrubber aria-label="Decoded frame">
                <span data-scrub-label>0/0</span>
            </div>
            <dl class="task-animation-fitting-panel__qa" data-qa></dl>
            <button type="button" data-cancel disabled>Cancel fitting</button>
        `;
        container.appendChild(root);
        this.root = root;
        this.elements = {
            url: root.querySelector('[data-source-url]'),
            file: root.querySelector('[data-source-file]'),
            start: root.querySelector('[data-start]'),
            cancel: root.querySelector('[data-cancel]'),
            status: root.querySelector('[data-status]'),
            progress: root.querySelector('[data-progress]'),
            canvas: root.querySelector('[data-preview]'),
            scrubber: root.querySelector('[data-scrubber]'),
            scrubLabel: root.querySelector('[data-scrub-label]'),
            qa: root.querySelector('[data-qa]'),
        };
        this.elements.url?.addEventListener('input', () => {
            const value = String(this.elements.url.value || '').trim();
            if (value) {
                this.selectedSource = value;
                if (this.elements.file) this.elements.file.value = '';
            } else if (!this.elements.file?.files?.[0]) {
                this.selectedSource = null;
            }
            this._refreshControls();
        });
        this.elements.file?.addEventListener('change', () => {
            const file = this.elements.file.files?.[0] || null;
            if (file) {
                this.selectedSource = file;
                if (this.elements.url) this.elements.url.value = '';
            } else if (!String(this.elements.url?.value || '').trim()) {
                this.selectedSource = null;
            }
            this._refreshControls();
        });
        this.elements.start?.addEventListener('click', () => {
            void this.fitSource().catch(() => {});
        });
        this.elements.cancel?.addEventListener('click', () => this.cancel());
        this.elements.scrubber?.addEventListener('input', () => this.scrub(this.elements.scrubber.value));
        this._refreshControls();
        return root;
    }

    destroy(cancel = true) {
        if (cancel) this.cancel();
        this.root?.remove?.();
        this.root = null;
        this.elements = {};
    }
}

export const TASK_ANIMATION_FITTING_DEFAULTS = Object.freeze({
    frameCount: DEFAULT_FRAME_COUNT,
    fps: DEFAULT_FPS,
    schema: FITTED_ANIMATION_SCHEMA,
});
