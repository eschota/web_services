/**
 * Production LTX reference flow: one CTA opens a modal, captures a base-pose still,
 * asks Vision for four locked-camera motion prompts, then starts Renderfin jobs
 * that produce video targets for the animation fitting pipeline.
 */

const LS_PREFIX = 'idleLtxGen:';
const LS_VARIANT_PROMPT_PREFIX = 'idleLtxVariantPrompt:';
const LS_TTL_MS = 900000;
const POLL_MS = 5000;
const VARIANT_KEYS = ['idle', 'walk', 'run', 'die'];
const VARIANT_COUNT = VARIANT_KEYS.length;

const STATIC_CAMERA_POSITIVE =
    'Single locked-off tripod shot. Static frame. Fixed viewpoint. The camera is bolted down and never moves. No push-in, no pull-back, no dolly in, no dolly out, no zoom, no pan, no tilt, no orbit, no tracking, no handheld shake, no reframing. The distance between camera and subject never changes. The entire frame, including the selected theme backdrop, stays pixel-locked with zero parallax.';

const VARIANT_DEFAULT_PROMPTS = {
    idle: 'Subtle breathing and small natural idle motion in place. Feet or contact points stay planted. The root stays anchored at the same screen position.',
    walk: 'Walking-in-place motion only, like on an invisible treadmill. The character does not travel across the frame. The root stays anchored and centered.',
    run: 'Running-in-place motion only, like on an invisible treadmill. Stronger energy, but no travel across the frame. The root stays anchored and centered.',
    die: 'Fall or collapse in place into a defeated pose. The camera does not follow, push in, or pull back. No scene change.',
};

function tt(key, fallback, replacements = {}) {
    let text = fallback;
    try {
        if (window.I18n && typeof window.I18n.t === 'function') {
            const translated = window.I18n.t(key, replacements);
            if (translated && translated !== key) text = translated;
        }
    } catch (_) {}
    Object.entries(replacements).forEach(([k, v]) => {
        text = text.replaceAll(`{${k}}`, String(v));
    });
    return text;
}

function formatApiDetail(detail) {
    if (detail == null) return '';
    if (typeof detail === 'string') return detail;
    if (Array.isArray(detail)) {
        return detail
            .map((x) => (x && typeof x === 'object' ? x.msg || x.message || JSON.stringify(x) : String(x)))
            .join('; ');
    }
    return String(detail);
}

function idleLtxPickClips(startJson) {
    if (!startJson || typeof startJson !== 'object') return [];
    const a = startJson.clips_array ?? startJson.clipsArray;
    return Array.isArray(a) ? a : [];
}

function idleLtxPickVision(startJson) {
    if (!startJson || typeof startJson !== 'object') return {};
    const v = startJson.vision_analysis_object ?? startJson.visionAnalysisObject;
    return v && typeof v === 'object' ? v : {};
}

function idleLtxPickMp4Url(...candidates) {
    for (const raw of candidates) {
        const u = String(raw || '').trim();
        if (!/^https?:\/\//i.test(u)) continue;
        const base = u.split('?', 1)[0].toLowerCase();
        if (base.endsWith('.mp4')) return u;
    }
    return '';
}

function idleLtxValidateStrictStartResponse(startJson) {
    if (!startJson || typeof startJson !== 'object') {
        return tt('idle_ltx_error_empty_response', 'Empty generation response.');
    }
    const clips = idleLtxPickClips(startJson);
    if (clips.length !== VARIANT_COUNT) {
        return tt('idle_ltx_error_clip_count', 'Expected {count} reference video jobs, got {actual}.', {
            count: VARIANT_COUNT,
            actual: clips.length,
        });
    }
    if (!startJson.vision_analysis_object || typeof startJson.vision_analysis_object !== 'object') {
        return tt('idle_ltx_error_no_vision', 'Vision analysis is missing from the response.');
    }
    return '';
}

function lsKey(taskId) {
    return `${LS_PREFIX}${taskId}`;
}

function variantPromptLsKey(taskId, key) {
    return `${LS_VARIANT_PROMPT_PREFIX}${taskId}:${key}`;
}

function readLsJob(taskId) {
    try {
        const raw = localStorage.getItem(lsKey(taskId));
        if (!raw) return null;
        const o = JSON.parse(raw);
        if (!o || typeof o !== 'object') return null;
        if (Date.now() - Number(o.savedAt || 0) > LS_TTL_MS) {
            localStorage.removeItem(lsKey(taskId));
            return null;
        }
        return o;
    } catch (_) {
        return null;
    }
}

function writeLsJob(taskId, partial) {
    try {
        const prev = readLsJob(taskId) || {};
        localStorage.setItem(
            lsKey(taskId),
            JSON.stringify({
                ...prev,
                ...partial,
                savedAt: Date.now(),
            }),
        );
    } catch (_) {}
}

function clearLsJob(taskId) {
    try {
        localStorage.removeItem(lsKey(taskId));
    } catch (_) {}
}

function readSavedVariantPrompt(taskId, key) {
    try {
        const s = localStorage.getItem(variantPromptLsKey(taskId, key));
        if (s != null) return String(s);
    } catch (_) {}
    return null;
}

function saveVariantPrompt(taskId, key, text) {
    try {
        localStorage.setItem(variantPromptLsKey(taskId, key), String(text ?? ''));
    } catch (_) {}
}

function waitOneFrame() {
    return new Promise((resolve) => requestAnimationFrame(() => resolve()));
}

function normalizeVariantPrompts(prompts) {
    const out = {};
    for (const key of VARIANT_KEYS) {
        const text = String(prompts?.[key] || VARIANT_DEFAULT_PROMPTS[key] || '').trim();
        out[key] = `${text} ${STATIC_CAMERA_POSITIVE}`.trim();
    }
    return out;
}

function buildVisionUserPrompt(variantPrompts, themeContext = {}) {
    const lines = VARIANT_KEYS.map((key) => `${key}: ${variantPrompts[key]}`);
    const themeLines = [];
    if (themeContext && typeof themeContext === 'object') {
        if (themeContext.theme_name) themeLines.push(`theme: ${themeContext.theme_name}`);
        if (themeContext.theme_short_description) themeLines.push(`theme description: ${themeContext.theme_short_description}`);
        if (Array.isArray(themeContext.semantic_tags) && themeContext.semantic_tags.length) {
            themeLines.push(`theme tags: ${themeContext.semantic_tags.join(', ')}`);
        }
    }
    return [
        'Create four LTX image-to-video prompts from this reference frame.',
        'The generated videos are motion references for browser-side inverse animation fitting of this rigged skeletal mesh.',
        'Use the user variant instructions below as mandatory motion intent.',
        'The reference frame includes the selected 3D viewer theme/background image. Treat the entire frame as a locked static plate, not a camera target around the model.',
        ...themeLines,
        'Keep motion clean, centered, loop/fitting friendly, and suitable for later bone transform optimization.',
        STATIC_CAMERA_POSITIVE,
        'The subject may animate, but the camera must be perfectly stationary. Never move closer or farther from the subject. Never reframe to follow the motion.',
        ...lines,
    ].join('\n');
}

/**
 * @param {object} opts
 * @param {string} opts.taskId
 * @param {() => string | null} opts.captureFrame768
 * @param {() => (void | boolean | Promise<void | boolean>)} [opts.prepareBasePose]
 */
export function createIdleLtxGenerator(opts) {
    const taskId = String(opts.taskId || '').trim();
    const apiOrigin = String(opts.apiOrigin || window.location?.origin || 'https://autorig.online').replace(/\/$/, '');
    const captureFrame768 = opts.captureFrame768;
    const prepareBasePose = opts.prepareBasePose;
    const getThemeContext = typeof opts.getThemeContext === 'function' ? opts.getThemeContext : () => ({});
    const island = document.getElementById('idle-ltx-generator-island');
    if (!taskId || !island || typeof captureFrame768 !== 'function') {
        console.warn('[IdleLTX] init skipped: missing task, island, or capture');
        return;
    }

    const pageBtn = document.getElementById('idle-ltx-generate-btn');
    const modal = document.getElementById('idle-ltx-modal');
    const modalStart = document.getElementById('idle-ltx-modal-start');
    const modalClose = document.getElementById('idle-ltx-modal-close');
    const modalCancel = document.getElementById('idle-ltx-modal-cancel');
    const modalDialog = modal?.querySelector?.('.idle-ltx-modal-dialog');
    const statusEl = document.getElementById('idle-ltx-status-line');
    const placeholder = document.getElementById('idle-ltx-preview-placeholder');
    const snapImg = document.getElementById('idle-ltx-snapshot-img');
    const vidWrap = document.getElementById('idle-ltx-video-below-viewer-wrap');
    const genPreview = document.getElementById('idle-ltx-generate-preview');
    const speciesDisplay = document.getElementById('idle-ltx-species-display');
    const visionFatalAlert = document.getElementById('idle-ltx-vision-fatal-alert');
    const fitPanel = document.getElementById('idle-ltx-fitting-panel');
    const fitVideo = document.getElementById('idle-ltx-fitting-video');
    const fitExit = document.getElementById('idle-ltx-fit-exit');
    const fitStart = document.getElementById('idle-ltx-fit-start');
    const fitSelected = document.getElementById('idle-ltx-fit-selected');
    const fitStatus = document.getElementById('idle-ltx-fitting-status');
    const fitProgressBar = document.getElementById('idle-ltx-fitting-progress-bar');
    const promptEls = new Map();

    for (const key of VARIANT_KEYS) {
        const el = document.querySelector(`textarea[data-variant-key="${key}"]`);
        if (!(el instanceof HTMLTextAreaElement)) continue;
        promptEls.set(key, el);
        const saved = readSavedVariantPrompt(taskId, key);
        el.value = saved && saved.trim() ? saved : VARIANT_DEFAULT_PROMPTS[key];
        let timer = null;
        el.addEventListener('input', () => {
            if (timer) clearTimeout(timer);
            timer = setTimeout(() => saveVariantPrompt(taskId, key, el.value), 350);
        });
        el.addEventListener('change', () => saveVariantPrompt(taskId, key, el.value), { passive: true });
    }

    let lastStartResponse = null;
    let lastStatusResponse = null;
    let pollTimer = null;
    let busy = false;
    let fittingBusy = false;
    let selectedFitClip = null;
    const readyFitClips = new Map();

    const applyDynamicLabels = () => {
        modalClose?.setAttribute('aria-label', tt('idle_ltx_modal_close', 'Close'));
    };

    const setButtonsDisabled = (disabled) => {
        if (pageBtn) pageBtn.disabled = Boolean(disabled);
        if (modalStart) modalStart.disabled = Boolean(disabled);
    };

    const setStatus = (msg, isErr = false) => {
        if (!statusEl) return;
        statusEl.textContent = msg || '';
        statusEl.classList.toggle('idle-ltx-err', Boolean(isErr));
    };

    const setFitStatus = (msg, isErr = false) => {
        if (!fitStatus) return;
        fitStatus.textContent = msg || '';
        fitStatus.classList.toggle('idle-ltx-err', Boolean(isErr));
    };

    const setFitProgress = (value) => {
        if (!fitProgressBar) return;
        const pct = Math.max(0, Math.min(100, Number(value) || 0));
        fitProgressBar.style.width = `${pct.toFixed(0)}%`;
    };

    const hideVisionFatal = () => {
        if (!visionFatalAlert) return;
        visionFatalAlert.textContent = '';
        visionFatalAlert.classList.add('hidden');
    };

    const showVisionFatal = (msg) => {
        const text = String(msg || '').trim();
        if (visionFatalAlert) {
            visionFatalAlert.textContent = text;
            visionFatalAlert.classList.remove('hidden');
        }
        setStatus(text, true);
        openModal();
    };

    const openModal = () => {
        if (!modal) return;
        applyDynamicLabels();
        modal.classList.remove('hidden');
        modal.setAttribute('aria-hidden', 'false');
        document.body.classList.add('idle-ltx-modal-open');
        window.I18n?.applyTranslations?.();
        applyDynamicLabels();
        modal.scrollTop = 0;
        if (modalDialog) modalDialog.scrollTop = 0;
        requestAnimationFrame(() => {
            modal.scrollTop = 0;
            if (modalDialog) {
                modalDialog.scrollTop = 0;
                modalDialog.focus?.({ preventScroll: true });
            } else {
                modalClose?.focus?.({ preventScroll: true });
            }
        });
    };

    /** User-initiated dismiss: always works even while generating/polling. */
    const dismissModal = () => {
        if (!modal || modal.classList.contains('hidden')) return;
        stopPoll();
        busy = false;
        fittingBusy = false;
        closeFittingMode(true);
        clearLsJob(taskId);
        modal.classList.add('hidden');
        modal.setAttribute('aria-hidden', 'true');
        document.body.classList.remove('idle-ltx-modal-open');
        setButtonsDisabled(false);
        setStatus(tt('idle_ltx_generation_dismissed', 'Generation dismissed.'), false);
    };

    function closeFittingMode(force = false) {
        if (fittingBusy && !force) return;
        if (force) fittingBusy = false;
        modal?.classList.remove('is-fitting-mode');
        fitPanel?.classList.add('hidden');
        if (fitVideo) {
            fitVideo.pause();
            fitVideo.removeAttribute('src');
            fitVideo.load();
        }
        selectedFitClip = null;
        setFitStatus('');
        setFitProgress(0);
    }

    const stopPoll = () => {
        if (pollTimer) {
            clearInterval(pollTimer);
            pollTimer = null;
        }
    };

    const showSnapshot = (dataUrl) => {
        if (!snapImg || !placeholder) return;
        snapImg.src = dataUrl;
        snapImg.classList.remove('hidden');
        placeholder.classList.add('hidden');
    };

    const readVariantPromptsFromUi = () => {
        const prompts = {};
        for (const key of VARIANT_KEYS) {
            const el = promptEls.get(key);
            const value = String(el?.value || '').trim() || VARIANT_DEFAULT_PROMPTS[key];
            prompts[key] = value;
            saveVariantPrompt(taskId, key, value);
        }
        return normalizeVariantPrompts(prompts);
    };

    const updateVariantRowMeta = (vision, clipsFromServer) => {
        const species = String(vision?.detected_species_string || 'model');
        const conf = Number(vision?.species_confidence_float);
        const confStr = Number.isFinite(conf) ? conf.toFixed(2) : '—';
        const hasClips = Array.isArray(clipsFromServer) && clipsFromServer.length > 0;
        for (let i = 0; i < VARIANT_COUNT; i++) {
            const clip = hasClips ? clipsFromServer[i] : null;
            const nameEl = document.getElementById(`idle-ltx-v-name-${i}`);
            const metaEl = document.getElementById(`idle-ltx-v-meta-${i}`);
            const key = VARIANT_KEYS[i] || `variant_${i}`;
            const vname = clip?.variant_name_string ? String(clip.variant_name_string) : key;
            if (nameEl) nameEl.textContent = tt(`idle_ltx_variant_${key}`, vname);
            if (metaEl) {
                metaEl.textContent = tt('idle_ltx_variant_meta', 'Vision: {species} · confidence {confidence}', {
                    species,
                    confidence: confStr,
                });
            }
        }
    };

    const restoreVisionPreviewFromLs = (resume) => {
        const v = resume?.visionSnapshot;
        if (!v || typeof v !== 'object') return;
        if (speciesDisplay) {
            const c = Number(v.species_confidence_float);
            speciesDisplay.textContent = `${String(v.detected_species_string || '—')} (${Number.isFinite(c) ? c.toFixed(2) : '—'})`;
        }
        genPreview?.classList.remove('hidden');
    };

    const setVariantStatus = (idx, text) => {
        const el = document.getElementById(`idle-ltx-v-status-${idx}`);
        if (el) el.textContent = text;
    };

    const setVariantErr = (idx, text, show) => {
        const el = document.getElementById(`idle-ltx-v-err-${idx}`);
        if (!el) return;
        el.textContent = text || '';
        el.classList.toggle('hidden', !show);
    };

    const setVariantLoading = (idx, text, hide) => {
        const el = document.getElementById(`idle-ltx-v-loading-${idx}`);
        if (!el) return;
        el.textContent = text || '';
        el.classList.toggle('hidden', Boolean(hide));
    };

    const resetVideos = () => {
        readyFitClips.clear();
        closeFittingMode(true);
        for (let i = 0; i < VARIANT_COUNT; i++) {
            const vid = document.getElementById(`idle-ltx-result-video-below-${i}`);
            const shell = document.getElementById(`idle-ltx-v-shell-${i}`);
            if (vid) {
                vid.pause();
                vid.onerror = null;
                vid.onloadeddata = null;
                vid.removeAttribute('src');
                vid.load();
            }
            shell?.classList.add('hidden');
            setVariantErr(i, '', false);
            setVariantLoading(i, tt('idle_ltx_waiting', 'Waiting...'), false);
            setVariantStatus(i, '');
            const nameEl = document.getElementById(`idle-ltx-v-name-${i}`);
            const metaEl = document.getElementById(`idle-ltx-v-meta-${i}`);
            if (nameEl) nameEl.textContent = tt(`idle_ltx_variant_${VARIANT_KEYS[i]}`, VARIANT_KEYS[i]);
            if (metaEl) metaEl.textContent = '';
            document.getElementById(`idle-ltx-fit-btn-${i}`)?.classList.add('hidden');
        }
        vidWrap?.classList.add('hidden');
        vidWrap?.setAttribute('aria-hidden', 'true');
    };

    const registerFitClip = (idx, videoUrl, clipMeta = {}) => {
        const index = Number(idx);
        const key = VARIANT_KEYS[index] || `variant_${index}`;
        const nameEl = document.getElementById(`idle-ltx-v-name-${index}`);
        const variantName = String(clipMeta.variantName || nameEl?.textContent || key).trim();
        const payload = { index, videoUrl: String(videoUrl || ''), variantName };
        readyFitClips.set(index, payload);
        const btn = document.getElementById(`idle-ltx-fit-btn-${index}`);
        if (btn) {
            btn.classList.remove('hidden');
            btn.dataset.videoUrl = payload.videoUrl;
            btn.dataset.variantName = payload.variantName;
        }
    };

    const openFittingMode = (clip) => {
        if (!clip?.videoUrl || !fitPanel || !fitVideo) return;
        selectedFitClip = clip;
        openModal();
        modal?.classList.add('is-fitting-mode');
        fitPanel.classList.remove('hidden');
        if (fitSelected) fitSelected.textContent = clip.variantName || `variant ${clip.index + 1}`;
        fitVideo.src = clip.videoUrl;
        fitVideo.muted = true;
        fitVideo.defaultMuted = true;
        fitVideo.loop = true;
        fitVideo.load();
        void fitVideo.play().catch(() => {});
        setFitStatus(tt('idle_ltx_fit_ready', 'Ready to fit a bone animation from this reference.'));
        setFitProgress(0);
        if (modalDialog) {
            const top = Math.max(0, fitPanel.offsetTop - 12);
            modalDialog.scrollTo({ top, behavior: 'smooth' });
        } else {
            fitPanel.scrollIntoView({ block: 'center', behavior: 'smooth' });
        }
    };

    const updateGeneratePreview = (startJson) => {
        const v = idleLtxPickVision(startJson);
        if (!v || Object.keys(v).length === 0) return;
        if (speciesDisplay) {
            const c = Number(v.species_confidence_float);
            speciesDisplay.textContent = `${String(v.detected_species_string || '—')} (${Number.isFinite(c) ? c.toFixed(2) : '—'})`;
        }
        genPreview?.classList.remove('hidden');
    };

    async function fetchClipStatus(clipMeta) {
        const params = new URLSearchParams();
        if (clipMeta.outUrl) params.set('output_url_string', clipMeta.outUrl);
        if (clipMeta.taskId) params.set('renderfin_task_id', clipMeta.taskId);
        const r = await fetch(
            `${apiOrigin}/api/task/${encodeURIComponent(taskId)}/idle-ltx/clip-status?${params.toString()}`,
            { cache: 'no-store', credentials: 'same-origin' },
        );
        const j = await r.json().catch(() => ({}));
        lastStatusResponse = { httpOk: r.ok, httpStatus: r.status, body: j };
        if (!r.ok) {
            throw new Error(formatApiDetail(j.detail) || j.error_string || j.message || `clip-status HTTP ${r.status}`);
        }
        return j;
    }

    async function verifyVideoReachable(url) {
        const u = String(url || '').trim();
        if (!/^https?:\/\//i.test(u)) return { ok: false, detail: 'Full http(s) URL required' };

        let sameOrigin = false;
        try {
            sameOrigin = new URL(u, window.location.href).origin === window.location.origin;
        } catch {
            sameOrigin = false;
        }

        if (sameOrigin) {
            try {
                const head = await fetch(u, { method: 'HEAD', cache: 'no-store', credentials: 'same-origin' });
                if (head.ok) return { ok: true, via: 'head' };
                const rg = await fetch(u, {
                    method: 'GET',
                    cache: 'no-store',
                    credentials: 'same-origin',
                    headers: { Range: 'bytes=0-0' },
                });
                if (rg.ok || rg.status === 206) return { ok: true, via: 'range' };
                return { ok: false, detail: `HTTP ${rg.status}` };
            } catch {
                // Try backend verification below.
            }
        }

        const r = await fetch(
            `${apiOrigin}/api/task/${encodeURIComponent(taskId)}/idle-ltx/verify-mp4?video_url_string=${encodeURIComponent(u)}`,
            { cache: 'no-store', credentials: 'same-origin' },
        );
        let j = {};
        try {
            j = await r.json();
        } catch (_) {}
        if (!r.ok) return { ok: false, detail: formatApiDetail(j.detail) || `API ${r.status}` };
        if (j.ok_bool === true) return { ok: true, via: 'api' };
        return { ok: false, detail: `HTTP ${j.http_status_int ?? '?'}` };
    }

    async function attachVideoIfReady(idx, videoUrl, clipMeta = {}) {
        const u = String(videoUrl || '').trim();
        if (!/^https?:\/\//i.test(u)) return 'retry';

        const shell = document.getElementById(`idle-ltx-v-shell-${idx}`);
        const vid = document.getElementById(`idle-ltx-result-video-below-${idx}`);
        if (!shell || !vid) return 'retry';

        setVariantLoading(idx, tt('idle_ltx_checking_video', 'Checking video...'), false);
        const reach = await verifyVideoReachable(u);
        if (!reach.ok) {
            setVariantStatus(idx, tt('idle_ltx_video_not_ready', 'Reference video file is not ready yet. Retrying...'));
            return 'retry';
        }

        shell.classList.remove('hidden');
        setVariantLoading(idx, tt('idle_ltx_loading_player', 'Loading video player...'), false);
        vid.onerror = () => {
            const err = vid.error;
            const code = err && typeof err.code === 'number' ? err.code : '?';
            setVariantErr(idx, tt('idle_ltx_player_error', 'Player error ({code}).', { code }), true);
        };
        vid.onloadeddata = () => setVariantErr(idx, '', false);
        vid.muted = true;
        vid.defaultMuted = true;
        try {
            vid.playsInline = true;
        } catch (_) {}
        vid.pause();
        vid.src = u;
        vid.load();
        const tryPlay = () => vid.play().catch(() => {});
        void tryPlay();
        vid.addEventListener('canplay', () => void tryPlay(), { once: true });
        setVariantStatus(idx, tt('idle_ltx_ready', 'Ready.'));
        registerFitClip(idx, u, clipMeta);
        return 'ok';
    }

    function startPollingClipStates(clipStates) {
        const prevSnap = readLsJob(taskId) || {};
        writeLsJob(taskId, {
            ...prevSnap,
            idleClips: clipStates.map((c) => ({
                index_int: c.index,
                renderfin_task_id_string: c.taskId,
                output_url_string: c.outUrl,
                variant_name_string: c.variantName,
            })),
        });

        let alive = true;
        const tick = async () => {
            if (!alive) return;
            try {
                const results = await Promise.all(
                    clipStates.map(async (c) => {
                        if (c.finalized) return c;
                        try {
                            const j = await fetchClipStatus(c);
                            const stNum = Number(j.status_int);
                            const st = Number.isFinite(stNum) ? stNum : NaN;
                            const phase = String(j.phase_string || '');
                            const phaseLower = phase.toLowerCase();
                            const looksComplete =
                                st === 3 ||
                                phaseLower === 'completed' ||
                                phaseLower === 'complete' ||
                                String(j.status_string || '').toLowerCase().includes('complete');
                            const vidUrl = String(j.video_url_string || '').trim();
                            const oUrl = String(j.output_url_string || '').trim();
                            if (oUrl && oUrl !== c.outUrl) c.outUrl = oUrl;
                            const mp4Url = idleLtxPickMp4Url(vidUrl, oUrl, c.outUrl);
                            if (Number.isFinite(st)) {
                                setVariantStatus(c.index, phase || renderfinStatusLabel(st));
                            } else if (phase) {
                                setVariantStatus(c.index, phase);
                            }
                            if (st === 4) {
                                c.failed = true;
                                c.finalized = true;
                                setVariantErr(c.index, String(j.error_string || 'Render failed'), true);
                                setVariantLoading(c.index, '', true);
                            } else if (looksComplete && mp4Url) {
                                c.verifyTries = (c.verifyTries || 0) + 1;
                                const att = await attachVideoIfReady(c.index, mp4Url, c);
                                if (att === 'ok') {
                                    c.finalized = true;
                                    c.videoUrl = mp4Url;
                                    setVariantLoading(c.index, '', true);
                                } else if (c.verifyTries > 120) {
                                    c.failed = true;
                                    c.finalized = true;
                                    setVariantErr(c.index, tt('idle_ltx_video_timeout', 'Reference video URL did not become reachable.'), true);
                                    setVariantLoading(c.index, '', true);
                                }
                            } else if (st !== 4 && mp4Url) {
                                c.earlyMp4Tries = (c.earlyMp4Tries || 0) + 1;
                                if (c.earlyMp4Tries >= 2 && c.earlyMp4Tries % 2 === 0) {
                                    if ((await attachVideoIfReady(c.index, mp4Url, c)) === 'ok') {
                                        c.finalized = true;
                                        c.videoUrl = mp4Url;
                                        setVariantLoading(c.index, '', true);
                                    }
                                }
                            }
                        } catch (e) {
                            c.statusErrorTries = (c.statusErrorTries || 0) + 1;
                            const msg = String(e.message || e);
                            setVariantStatus(c.index, tt('idle_ltx_status_retry', 'Status temporarily unavailable. Retrying...'));
                            setVariantErr(c.index, msg, c.statusErrorTries >= 6);
                            if (c.statusErrorTries >= 20) {
                                c.failed = true;
                                c.finalized = true;
                                setVariantLoading(c.index, '', true);
                            }
                        }
                        return c;
                    }),
                );

                const doneN = results.filter((c) => c.finalized).length;
                const snap = readLsJob(taskId) || {};
                writeLsJob(taskId, {
                    ...snap,
                    idleClips: results.map((c) => ({
                        index_int: c.index,
                        renderfin_task_id_string: c.taskId,
                        output_url_string: c.outUrl,
                        variant_name_string: c.variantName,
                    })),
                });

                if (doneN >= results.length) {
                    alive = false;
                    stopPoll();
                    clearLsJob(taskId);
                    busy = false;
                    setButtonsDisabled(false);
                    const anyFailed = results.some((c) => c.failed);
                    setStatus(anyFailed ? tt('idle_ltx_done_with_errors', 'Some reference videos failed.') : tt('idle_ltx_all_ready', 'All reference videos are ready.'));
                    return;
                }
                setStatus(tt('idle_ltx_polling', 'Generating fitting references: {done}/{total} ready.', { done: doneN, total: results.length }));
            } catch (e) {
                alive = false;
                stopPoll();
                clearLsJob(taskId);
                busy = false;
                setButtonsDisabled(false);
                setStatus(String(e.message || e), true);
            }
        };

        void tick();
        pollTimer = setInterval(tick, POLL_MS);
    }

    async function runGenerate() {
        if (busy) return;
        busy = true;
        setButtonsDisabled(true);
        stopPoll();
        hideVisionFatal();
        resetVideos();
        openModal();

        setStatus(tt('idle_ltx_status_base_pose', 'Preparing base pose...'));
        try {
            if (typeof prepareBasePose === 'function') {
                await prepareBasePose();
            } else if (typeof window.setCurrentModelBasePose === 'function') {
                window.setCurrentModelBasePose();
            }
            await waitOneFrame();
        } catch (e) {
            console.warn('[IdleLTX] base-pose preparation failed; continuing with current pose', e);
        }

        setStatus(tt('idle_ltx_status_capture', 'Capturing start frame...'));
        const frame = captureFrame768();
        if (!frame) {
            setStatus(tt('idle_ltx_error_no_viewer', 'The 3D preview is not ready yet.'), true);
            busy = false;
            setButtonsDisabled(false);
            return;
        }
        showSnapshot(frame);

        const variantPrompts = readVariantPromptsFromUi();
        const themeContext = (() => {
            try {
                const ctx = getThemeContext();
                return ctx && typeof ctx === 'object' ? ctx : {};
            } catch (_) {
                return {};
            }
        })();
        setStatus(tt('idle_ltx_status_analyzing', 'Analyzing model...'));
        let startJson;
        try {
            const baseBody = {
                frame_jpeg_base64_string: frame,
                user_prompt_string: buildVisionUserPrompt(variantPrompts, themeContext),
                variant_prompts_object: variantPrompts,
                variant_prompts_array: VARIANT_KEYS.map((key) => ({
                    variant_name_string: key,
                    user_prompt_string: variantPrompts[key],
                })),
                theme_context_object: themeContext,
                frame_count_int: 129,
            };
            const vResp = await fetch(`${apiOrigin}/api/task/${encodeURIComponent(taskId)}/idle-ltx/vision-start`, {
                method: 'POST',
                credentials: 'same-origin',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(baseBody),
            });
            const phase = await vResp.json().catch(() => ({}));
            if (!vResp.ok) {
                lastStartResponse = { httpOk: vResp.ok, httpStatus: vResp.status, body: phase };
                throw new Error(formatApiDetail(phase.detail) || phase.message || `vision-start HTTP ${vResp.status}`);
            }
            if (!phase.success_bool) {
                lastStartResponse = { httpOk: vResp.ok, httpStatus: vResp.status, body: phase };
                throw new Error('vision-start: success_bool false');
            }
            const visionObj = phase.vision_analysis_object;
            const variants = Array.isArray(visionObj?.ltx_variants_array) ? visionObj.ltx_variants_array : [];
            if (variants.length < VARIANT_COUNT) {
                lastStartResponse = { httpOk: true, httpStatus: vResp.status, body: phase };
                throw new Error(`Vision returned ${variants.length} variants, expected ${VARIANT_COUNT}.`);
            }

            const clips = [];
            for (let i = 0; i < VARIANT_COUNT; i++) {
                const key = VARIANT_KEYS[i];
                setStatus(tt('idle_ltx_status_generating_variant', 'Generating {variant}...', {
                    variant: tt(`idle_ltx_variant_${key}`, key),
                }));
                const row = variants[i] && typeof variants[i] === 'object' ? variants[i] : {};
                const promptClip = String(row.prompt_string || visionObj.ltx_base_prompt_string || '').trim();
                if (!promptClip) throw new Error(`Empty prompt for ${key}.`);
                const rVar = await fetch(
                    `${apiOrigin}/api/task/${encodeURIComponent(taskId)}/idle-ltx/render-variant`,
                    {
                        method: 'POST',
                        credentials: 'same-origin',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({
                            index_int: i,
                            image_url_string: phase.image_url_string,
                            user_name_string: phase.user_name_string,
                            frame_count_int: phase.frame_count_int || 129,
                            prompt_string: promptClip,
                            user_variant_prompt_string: variantPrompts[key],
                            negative_prompt_string: phase.negative_prompt_string,
                            variant_name_string: key,
                            detected_species_string: visionObj.detected_species_string,
                            species_confidence_float: visionObj.species_confidence_float,
                        }),
                    },
                );
                const rj = await rVar.json().catch(() => ({}));
                if (!rVar.ok) {
                    lastStartResponse = {
                        httpOk: rVar.ok,
                        httpStatus: rVar.status,
                        body: rj,
                        vision_phase: phase,
                        clips_so_far: clips,
                    };
                    throw new Error(formatApiDetail(rj.detail) || rj.message || `render-variant ${i} HTTP ${rVar.status}`);
                }
                if (!rj.success_bool || !rj.clip_object) {
                    lastStartResponse = { httpOk: rVar.ok, httpStatus: rVar.status, body: rj };
                    throw new Error(`render-variant ${i}: missing clip object`);
                }
                clips.push(rj.clip_object);
            }

            startJson = {
                success_bool: true,
                vision_analysis_object: visionObj,
                vision_provider_string: phase.vision_provider_string,
                clips_array: clips,
                user_prompt_string: phase.user_prompt_string,
                variant_prompts_object: variantPrompts,
                image_url_string: phase.image_url_string,
                user_name_string: phase.user_name_string,
                upload_response_object: phase.upload_response_object,
                pipeline_string: 'production-modal-vision-start+4x-render-variant',
                clip_count_int: VARIANT_COUNT,
                renderfin_task_ids_array: clips.map((c) => String(c.renderfin_task_id_string || '')),
            };
            if (clips[0]) {
                startJson.generate_video_request_object = clips[0].generate_video_request_object;
                startJson.generate_video_response_object = clips[0].generate_video_response_object;
                startJson.generate_video_http_status_int = clips[0].generate_video_http_status_int;
            }

            const shapeErr = idleLtxValidateStrictStartResponse(startJson);
            if (shapeErr) {
                lastStartResponse = { httpOk: true, httpStatus: 200, body: startJson };
                throw new Error(shapeErr);
            }
            lastStartResponse = { httpOk: true, httpStatus: 200, body: startJson };
            console.debug('[IdleLTX] start response', lastStartResponse);
        } catch (e) {
            console.error('[IdleLTX] generation failed', e, lastStartResponse, lastStatusResponse);
            showVisionFatal(
                `${tt('idle_ltx_generation_failed', 'Video generation stopped before completion.')}\n\n${String(e.message || e)}`,
            );
            busy = false;
            setButtonsDisabled(false);
            return;
        }

        updateGeneratePreview(startJson);
        const clips = idleLtxPickClips(startJson);
        const vision = idleLtxPickVision(startJson);
        updateVariantRowMeta(vision, clips);
        vidWrap?.classList.remove('hidden');
        vidWrap?.setAttribute('aria-hidden', 'false');

        const clipStates = clips.slice(0, VARIANT_COUNT).map((row, i) => ({
            index: Number(row.index_int ?? i),
            taskId: String(row.renderfin_task_id_string || '').trim(),
            outUrl: String(row.output_url_string || '').trim(),
            variantName: String(row.variant_name_string || VARIANT_KEYS[i] || `clip_${i}`),
            finalized: false,
            videoUrl: '',
        }));

        writeLsJob(taskId, {
            idleClips: clipStates.map((c) => ({
                index_int: c.index,
                renderfin_task_id_string: c.taskId,
                output_url_string: c.outUrl,
                variant_name_string: c.variantName,
            })),
            visionSnapshot: vision,
            clipsMeta: clips,
            vision_provider_string: String(startJson.vision_provider_string || ''),
        });

        clipStates.forEach((c) => {
            setVariantLoading(c.index, tt('idle_ltx_renderfin_waiting', 'Generating...'), false);
            setVariantErr(c.index, '', false);
        });

        setStatus(tt('idle_ltx_status_polling_started', 'Generating fitting references...'));
        startPollingClipStates(clipStates);
    }

    pageBtn?.addEventListener('click', () => openModal());
    modalStart?.addEventListener('click', () => void runGenerate());
    modalClose?.addEventListener('click', dismissModal);
    modalCancel?.addEventListener('click', dismissModal);
    fitExit?.addEventListener('click', closeFittingMode);
    fitStart?.addEventListener('click', async () => {
        if (fittingBusy || !selectedFitClip) return;
        fittingBusy = true;
        if (fitStart) fitStart.disabled = true;
        setFitProgress(2);
        setFitStatus(tt('idle_ltx_fit_notifying', 'Starting fitting mode...'));
        try {
            await fetch(`${apiOrigin}/api/task/${encodeURIComponent(taskId)}/idle-ltx/fitting-started`, {
                method: 'POST',
                credentials: 'same-origin',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    variant_name_string: selectedFitClip.variantName,
                    video_url_string: selectedFitClip.videoUrl,
                }),
            }).catch(() => null);
            if (typeof window.startAnimalAnimationFittingFromVideo !== 'function') {
                throw new Error(tt('idle_ltx_fit_no_viewer', 'The 3D animation fitter is not ready yet.'));
            }
            const result = await window.startAnimalAnimationFittingFromVideo({
                videoUrl: selectedFitClip.videoUrl,
                variantName: selectedFitClip.variantName,
                index: selectedFitClip.index,
                onProgress: (pct, message) => {
                    setFitProgress(pct);
                    if (message) setFitStatus(message);
                },
            });
            setFitProgress(100);
            const clipName = result?.clip_name_string || selectedFitClip.variantName || 'fitted animation';
            setFitStatus(tt('idle_ltx_fit_done', 'Fitted bone animation applied: {clip}', { clip: clipName }));
        } catch (e) {
            setFitStatus(String(e.message || e), true);
        } finally {
            fittingBusy = false;
            if (fitStart) fitStart.disabled = false;
        }
    });
    for (let i = 0; i < VARIANT_COUNT; i++) {
        document.getElementById(`idle-ltx-fit-btn-${i}`)?.addEventListener('click', (ev) => {
            const btn = ev.currentTarget;
            const clip = readyFitClips.get(i) || {
                index: i,
                videoUrl: String(btn?.dataset?.videoUrl || ''),
                variantName: String(btn?.dataset?.variantName || VARIANT_KEYS[i] || `variant_${i}`),
            };
            openFittingMode(clip);
        });
    }
    modal?.addEventListener('click', (ev) => {
        if (ev.target && ev.target instanceof Element && ev.target.getAttribute('data-idle-ltx-close') === '1') {
            dismissModal();
        }
    });
    document.addEventListener('keydown', (ev) => {
        if (ev.key !== 'Escape') return;
        if (!modal || modal.classList.contains('hidden')) return;
        dismissModal();
    });
    window.addEventListener('languageChanged', () => {
        applyDynamicLabels();
        updateVariantRowMeta(readLsJob(taskId)?.visionSnapshot || {}, readLsJob(taskId)?.clipsMeta || []);
    });

    applyDynamicLabels();
    resetVideos();
    const resume = readLsJob(taskId);
    const resumeClips = Array.isArray(resume?.idleClips) ? resume.idleClips : null;
    const resumeHasIds = Boolean(
        resumeClips?.some((r) => String(r?.renderfin_task_id_string || '').trim() || String(r?.output_url_string || '').trim()),
    );
    if (resumeClips && resumeClips.length > 0 && !resumeHasIds) {
        clearLsJob(taskId);
    } else if (resumeClips && resumeClips.length > 0 && resumeHasIds) {
        restoreVisionPreviewFromLs(resume);
        if (resume.visionSnapshot && Array.isArray(resume.clipsMeta)) {
            updateVariantRowMeta(resume.visionSnapshot, resume.clipsMeta);
        }
        setStatus(tt('idle_ltx_resume_polling', 'Restoring video generation status...'));
        busy = true;
        setButtonsDisabled(true);
        const clipStates = resumeClips.slice(0, VARIANT_COUNT).map((row, i) => ({
            index: Number(row.index_int ?? i),
            taskId: String(row.renderfin_task_id_string || '').trim(),
            outUrl: String(row.output_url_string || '').trim(),
            variantName: String(row.variant_name_string || VARIANT_KEYS[i] || `clip_${i}`),
            finalized: false,
            videoUrl: '',
        }));
        vidWrap?.classList.remove('hidden');
        vidWrap?.setAttribute('aria-hidden', 'false');
        setStatus(tt('idle_ltx_resume_background', 'Resuming video generation in the background…'), false);
        clipStates.forEach((c) => {
            setVariantLoading(c.index, tt('idle_ltx_restoring', 'Restoring...'), false);
            const nameEl = document.getElementById(`idle-ltx-v-name-${c.index}`);
            if (nameEl && !resume.clipsMeta) nameEl.textContent = tt(`idle_ltx_variant_${VARIANT_KEYS[c.index]}`, c.variantName);
        });
        startPollingClipStates(clipStates);
    }

    console.log('[IdleLTX] production module ready for task', taskId);
}

function renderfinStatusLabel(st) {
    const n = Number(st);
    if (n === 0) return 'Accepting';
    if (n === 1) return 'Pending';
    if (n === 2) return 'In progress';
    if (n === 3) return 'Completed';
    if (n === 4) return 'Failed';
    if (!Number.isFinite(n)) return 'unknown';
    return String(n);
}
