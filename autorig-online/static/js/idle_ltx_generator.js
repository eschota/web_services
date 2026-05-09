/**
 * Idle LTX — Vision → species-specific LTX prompts → 4× Renderfin generate_video.
 * @see https://free3d.online/renderfin-skill.md
 */

const LS_PREFIX = 'idleLtxGen:';
const LS_USER_PROMPT_PREFIX = 'idleLtxUserPrompt:';
const LS_TTL_MS = 900000;
const POLL_MS = 5000;
const VARIANT_COUNT = 4;

/** Sync with idle_ltx_vision.IDLE_LTX_USER_PROMPT_DEFAULT */
const IDLE_LTX_USER_PROMPT_DEFAULT = `Create locked-camera idle loop prompts for skeletal animation fitting. Minimal in-place motion only; feet planted; no locomotion.`;

function renderfinStatusLabel(st) {
    const n = Number(st);
    if (n === 0) return 'Accepting';
    if (n === 1) return 'Pending';
    if (n === 2) return 'InProgress';
    if (n === 3) return 'Completed';
    if (n === 4) return 'Failed';
    if (!Number.isFinite(n)) return 'unknown';
    return String(n);
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

/** Normalize start JSON keys (snake_case server vs possible camelCase proxies). */
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

/** Prefer first http(s) URL whose path ends in .mp4 (output / playback). */
function idleLtxPickMp4Url(...candidates) {
    for (const raw of candidates) {
        const u = String(raw || '').trim();
        if (!/^https?:\/\//i.test(u)) continue;
        const base = u.split('?', 1)[0].toLowerCase();
        if (base.endsWith('.mp4')) return u;
    }
    return '';
}

/**
 * Require full Idle LTX start shape (Vision JSON + N variants). No client-side «fake» vision.
 * @returns {string} empty if ok, else user-facing error
 */
function idleLtxValidateStrictStartResponse(startJson) {
    if (!startJson || typeof startJson !== 'object') {
        return 'Пустой ответ start.';
    }
    const clips = idleLtxPickClips(startJson);
    if (clips.length === 0) {
        return 'Нет clips_array. Сервер отдал не формат /idle-ltx/start (часто это другой маршрут или старая сборка без Vision).';
    }
    if (clips.length !== VARIANT_COUNT) {
        return `Ожидалось clips_array из ${VARIANT_COUNT} элементов, пришло ${clips.length}. Обновите backend или проверьте, что вызывается POST /api/task/…/idle-ltx/start.`;
    }
    if (!('vision_analysis_object' in startJson) || startJson.vision_analysis_object == null) {
        return 'В ответе нет vision_analysis_object — шаг Vision на сервере не отражён в JSON.';
    }
    if (typeof startJson.vision_analysis_object !== 'object') {
        return 'Поле vision_analysis_object имеет неверный тип.';
    }
    const prov = String(startJson.vision_provider_string || '').trim();
    if (!prov) {
        return 'Пустой vision_provider_string — неизвестно, какой Vision отработал.';
    }
    if (prov === 'single_clip') {
        return 'Подмена Vision: vision_provider_string = «single_clip». Это не полный Idle LTX.';
    }
    return '';
}

async function copyToClipboard(text) {
    const s = String(text ?? '');
    try {
        await navigator.clipboard.writeText(s);
        return true;
    } catch (_) {
        try {
            const ta = document.createElement('textarea');
            ta.value = s;
            ta.style.position = 'fixed';
            ta.style.left = '-9999px';
            document.body.appendChild(ta);
            ta.select();
            document.execCommand('copy');
            document.body.removeChild(ta);
            return true;
        } catch (__) {
            return false;
        }
    }
}

function lsKey(taskId) {
    return `${LS_PREFIX}${taskId}`;
}

function userPromptLsKey(taskId) {
    return `${LS_USER_PROMPT_PREFIX}${taskId}`;
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

function readSavedUserPrompt(taskId) {
    try {
        const s = localStorage.getItem(userPromptLsKey(taskId));
        if (s != null) return String(s);
    } catch (_) {}
    return null;
}

function saveUserPrompt(taskId, text) {
    try {
        localStorage.setItem(userPromptLsKey(taskId), String(text ?? ''));
    } catch (_) {}
}

/**
 * @param {object} opts
 * @param {string} opts.taskId
 * @param {() => string | null} opts.captureFrame768
 */
export function createIdleLtxGenerator(opts) {
    const taskId = String(opts.taskId || '').trim();
    const apiOrigin = String(opts.apiOrigin || window.location?.origin || 'https://autorig.online').replace(/\/$/, '');
    const captureFrame768 = opts.captureFrame768;
    const island = document.getElementById('idle-ltx-generator-island');
    if (!taskId || !island || typeof captureFrame768 !== 'function') {
        console.warn('[IdleLTX] init skipped: missing task, island, or capture');
        return;
    }

    const btn = document.getElementById('idle-ltx-generate-btn');
    const statusEl = document.getElementById('idle-ltx-status-line');
    const placeholder = document.getElementById('idle-ltx-preview-placeholder');
    const snapImg = document.getElementById('idle-ltx-snapshot-img');
    const vidWrap = document.getElementById('idle-ltx-video-below-viewer-wrap');
    const userPromptEl = document.getElementById('idle-ltx-user-prompt');
    const genPreview = document.getElementById('idle-ltx-generate-preview');
    const visionProvDisplay = document.getElementById('idle-ltx-vision-provider-display');
    const speciesDisplay = document.getElementById('idle-ltx-species-display');
    const payloadPre = document.getElementById('idle-ltx-payload-json-pre');
    const copyPayload = document.getElementById('idle-ltx-copy-payload-btn');
    const copyStart = document.getElementById('idle-ltx-copy-start-response-btn');
    const copyStatus = document.getElementById('idle-ltx-copy-status-btn');
    const visionFatalAlert = document.getElementById('idle-ltx-vision-fatal-alert');

    /** @type {Map<number, object>} */
    const lastPayloadByIndex = new Map();

    const savedUp = readSavedUserPrompt(taskId);
    if (userPromptEl) {
        userPromptEl.value =
            savedUp != null && savedUp.length > 0 ? savedUp : IDLE_LTX_USER_PROMPT_DEFAULT;
    }
    userPromptEl?.addEventListener(
        'change',
        () => {
            saveUserPrompt(taskId, userPromptEl.value);
        },
        { passive: true },
    );
    let _upT = null;
    userPromptEl?.addEventListener('input', () => {
        if (_upT) clearTimeout(_upT);
        _upT = setTimeout(() => saveUserPrompt(taskId, userPromptEl.value), 400);
    });

    let lastStartResponse = null;
    let lastStatusResponse = null;
    let pollTimer = null;
    let busy = false;

    const setStatus = (msg, isErr = false) => {
        if (!statusEl) return;
        statusEl.textContent = msg || '';
        statusEl.classList.toggle('idle-ltx-err', Boolean(isErr));
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
            try {
                visionFatalAlert.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
            } catch (_) {}
        }
        setStatus(text, true);
    };

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

    const updateVariantRowMeta = (vision, clipsFromServer) => {
        const species = String(vision?.detected_species_string || '—');
        const conf = Number(vision?.species_confidence_float);
        const confStr = Number.isFinite(conf) ? conf.toFixed(2) : '—';
        const hasClips = Array.isArray(clipsFromServer) && clipsFromServer.length > 0;
        for (let i = 0; i < VARIANT_COUNT; i++) {
            const clip = hasClips ? clipsFromServer[i] : null;
            const nameEl = document.getElementById(`idle-ltx-v-name-${i}`);
            const metaEl = document.getElementById(`idle-ltx-v-meta-${i}`);
            const vname = clip?.variant_name_string ? String(clip.variant_name_string) : '—';
            if (nameEl) nameEl.textContent = vname;
            if (metaEl) {
                const pr = String(clip?.prompt_string || '');
                const neg = String(clip?.negative_prompt_string || '');
                const prTrunc = pr.length > 280 ? `${pr.slice(0, 280)}…` : pr || '—';
                const negDisp = neg.length > 200 ? `${neg.slice(0, 200)}…` : neg || '—';
                metaEl.innerHTML = `
                  <div><strong>Вид (Vision):</strong> ${species} &nbsp;·&nbsp; <strong>confidence:</strong> ${confStr}</div>
                  <div><strong>task_id:</strong> <code>${String(clip?.renderfin_task_id_string || '—')}</code></div>
                  <div><strong>output_url:</strong> <code style="word-break:break-all">${String(clip?.output_url_string || '—')}</code></div>
                  <div><strong>prompt:</strong> ${prTrunc}</div>
                  <div><strong>negative:</strong> ${negDisp}</div>
                `;
            }
        }
    };

    const restoreVisionPreviewFromLs = (resume) => {
        const v = resume?.visionSnapshot;
        if (!v || typeof v !== 'object') return;
        if (visionProvDisplay) visionProvDisplay.textContent = String(resume.vision_provider_string || '—');
        if (speciesDisplay) {
            const c = Number(v.species_confidence_float);
            speciesDisplay.textContent = `${String(v.detected_species_string || '—')} (confidence ${Number.isFinite(c) ? c.toFixed(2) : '—'})`;
        }
        if (payloadPre) payloadPre.textContent = JSON.stringify(v, null, 2);
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
            setVariantLoading(i, 'Ожидание…', false);
            setVariantStatus(i, '');
            const nameEl = document.getElementById(`idle-ltx-v-name-${i}`);
            const metaEl = document.getElementById(`idle-ltx-v-meta-${i}`);
            if (nameEl) nameEl.textContent = '—';
            if (metaEl) metaEl.innerHTML = '';
        }
        vidWrap?.classList.add('hidden');
        vidWrap?.setAttribute('aria-hidden', 'true');
    };

    const updateGeneratePreview = (startJson) => {
        const v = idleLtxPickVision(startJson);
        if (!v || Object.keys(v).length === 0) return;
        if (visionProvDisplay) {
            visionProvDisplay.textContent = String(startJson.vision_provider_string || '—');
        }
        if (speciesDisplay) {
            const c = Number(v.species_confidence_float);
            speciesDisplay.textContent = `${String(v.detected_species_string || '—')} (confidence ${Number.isFinite(c) ? c.toFixed(2) : '—'})`;
        }
        if (payloadPre) {
            payloadPre.textContent = JSON.stringify(v, null, 2);
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
            throw new Error(
                formatApiDetail(j.detail) ||
                    j.error_string ||
                    j.message ||
                    `clip-status HTTP ${r.status} (${params.toString().slice(0, 120)}…)`,
            );
        }
        return j;
    }

    /**
     * True only if the browser can reach the file (HEAD or tiny Range GET).
     * Same-origin: check from the page (matches «открывается в новой вкладке»).
     * Other origins: fallback to backend verify-mp4.
     */
    async function verifyVideoReachable(url) {
        const u = String(url || '').trim();
        if (!/^https?:\/\//i.test(u)) return { ok: false, detail: 'Нужен полный http(s) URL' };

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
                // Same host but fetch failed (сеть): ниже — проверка через API.
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
        if (!r.ok) {
            const d = formatApiDetail(j.detail);
            return { ok: false, detail: d || `API ${r.status}` };
        }
        if (j.ok_bool === true) return { ok: true, via: 'api' };
        return { ok: false, detail: `ответ на URL: HTTP ${j.http_status_int ?? '?'}` };
    }

    /**
     * Плеер не показываем, пока URL не прошёл проверку (HEAD/Range с этой страницы или verify API).
     * @returns {'ok'|'retry'}
     */
    async function attachVideoIfReady(idx, videoUrl) {
        const u = String(videoUrl || '').trim();
        if (!/^https?:\/\//i.test(u)) return 'retry';

        const shell = document.getElementById(`idle-ltx-v-shell-${idx}`);
        const vid = document.getElementById(`idle-ltx-result-video-below-${idx}`);
        if (!shell || !vid) return 'retry';

        setVariantLoading(idx, 'Проверка доступности видео (HEAD)…', false);

        const reach = await verifyVideoReachable(u);
        if (!reach.ok) {
            setVariantStatus(
                idx,
                reach.detail ? `Файл ещё не готов или недоступен: ${reach.detail} — повтор…` : 'Ожидание ответа по URL видео…',
            );
            return 'retry';
        }

        shell.classList.remove('hidden');
        setVariantLoading(idx, 'Загрузка в плеер…', false);

        const clearPlayerErr = () => setVariantErr(idx, '', false);
        vid.onerror = () => {
            const err = vid.error;
            const code = err && typeof err.code === 'number' ? err.code : '?';
            setVariantErr(
                idx,
                `Плеер: ошибка (${code}). Откройте output_url выше в новой вкладке.`,
                true,
            );
        };
        vid.onloadeddata = clearPlayerErr;

        try {
            vid.referrerPolicy = '';
        } catch (_) {}

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

        setVariantStatus(idx, 'Готово.');
        return 'ok';
    }

    /**
     * @param {Array<object>} clipStates — mutable row state
     */
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
                                String(j.status_string || '')
                                    .toLowerCase()
                                    .includes('complete');
                            const vidUrl = String(j.video_url_string || '').trim();
                            const oUrl = String(j.output_url_string || '').trim();
                            if (oUrl && oUrl !== c.outUrl) c.outUrl = oUrl;
                            const mp4Url = idleLtxPickMp4Url(vidUrl, oUrl, c.outUrl);
                            if (Number.isFinite(st)) {
                                setVariantStatus(
                                    c.index,
                                    `${phase || '—'} · status_int=${st} · source=${j.source_string || '?'}`,
                                );
                            } else if (phase) {
                                setVariantStatus(c.index, `${phase} · source=${j.source_string || '?'}`);
                            }
                            if (st === 4) {
                                c.failed = true;
                                c.finalized = true;
                                const err = String(j.error_string || 'Render failed');
                                const rsv = j.render_server_name_string ? ` · server=${j.render_server_name_string}` : '';
                                const pid = j.prompt_id_string ? ` · prompt_id=${j.prompt_id_string}` : '';
                                setVariantErr(c.index, `${err}${rsv}${pid}`, true);
                                setVariantLoading(c.index, '', true);
                            } else if (looksComplete && mp4Url) {
                                c.verifyTries = (c.verifyTries || 0) + 1;
                                const att = await attachVideoIfReady(c.index, mp4Url);
                                if (att === 'ok') {
                                    c.finalized = true;
                                    c.videoUrl = mp4Url;
                                    setVariantLoading(c.index, '', true);
                                } else if (c.verifyTries > 120) {
                                    c.failed = true;
                                    c.finalized = true;
                                    setVariantErr(
                                        c.index,
                                        'Видео долго не проходит проверку доступности по URL. Откройте output_url выше.',
                                        true,
                                    );
                                    setVariantLoading(c.index, '', true);
                                }
                            } else if (st !== 4 && mp4Url) {
                                c.earlyMp4Tries = (c.earlyMp4Tries || 0) + 1;
                                if (c.earlyMp4Tries >= 2 && c.earlyMp4Tries % 2 === 0) {
                                    if ((await attachVideoIfReady(c.index, mp4Url)) === 'ok') {
                                        c.finalized = true;
                                        c.videoUrl = mp4Url;
                                        setVariantLoading(c.index, '', true);
                                        setVariantStatus(
                                            c.index,
                                            `${phase || '—'} · воспроизведение по output URL`,
                                        );
                                    }
                                }
                            }
                        } catch (e) {
                            c.failed = true;
                            setVariantErr(c.index, String(e.message || e), true);
                            c.finalized = true;
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
                    if (btn) btn.disabled = false;
                    const anyFailed = results.some((c) => c.failed);
                    setStatus(
                        anyFailed
                            ? 'Есть клипы с ошибкой — см. красный текст в карточках.'
                            : 'Все клипы завершены — проверьте плееры ниже.',
                    );
                    return;
                }
                setStatus(`Опрос клипов: завершено ${doneN}/${results.length} (каждые ${POLL_MS / 1000} с)`);
            } catch (e) {
                alive = false;
                stopPoll();
                clearLsJob(taskId);
                busy = false;
                if (btn) btn.disabled = false;
                setStatus(String(e.message || e), true);
            }
        };

        void tick();
        pollTimer = setInterval(tick, POLL_MS);
    }

    const runGenerate = async () => {
        if (busy) return;
        busy = true;
        if (btn) btn.disabled = true;
        stopPoll();
        hideVisionFatal();
        resetVideos();
        setStatus('Снимок 768×448…');
        const frame = captureFrame768();
        if (!frame) {
            setStatus('Нет готового animal viewer — дождитесь загрузки 3D превью.', true);
            busy = false;
            if (btn) btn.disabled = false;
            return;
        }
        showSnapshot(frame);

        setStatus('Загрузка кадра → Vision (отдельный запрос)…');
        let startJson;
        try {
            const up = String(userPromptEl?.value || '').trim();
            const baseBody = {
                frame_jpeg_base64_string: frame,
                user_prompt_string: up.length > 0 ? up : undefined,
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
                throw new Error(
                    formatApiDetail(phase.detail) || phase.message || `vision-start HTTP ${vResp.status}`,
                );
            }
            if (!phase.success_bool) {
                lastStartResponse = { httpOk: vResp.ok, httpStatus: vResp.status, body: phase };
                throw new Error('vision-start: success_bool false');
            }
            const visionObj = phase.vision_analysis_object;
            const variants = Array.isArray(visionObj?.ltx_variants_array) ? visionObj.ltx_variants_array : [];
            if (variants.length < VARIANT_COUNT) {
                lastStartResponse = { httpOk: true, httpStatus: vResp.status, body: phase };
                throw new Error(
                    `Vision: в ltx_variants_array нужно минимум ${VARIANT_COUNT} записей, пришло ${variants.length}.`,
                );
            }

            const clips = [];
            for (let i = 0; i < VARIANT_COUNT; i++) {
                setStatus(`Vision готово. Renderfin: вариант ${i + 1}/${VARIANT_COUNT} (отдельный POST)…`);
                const row = variants[i] && typeof variants[i] === 'object' ? variants[i] : {};
                const promptClip = String(row.prompt_string || visionObj.ltx_base_prompt_string || '').trim();
                if (!promptClip) {
                    throw new Error(`Пустой prompt_string для варианта ${i}`);
                }
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
                            negative_prompt_string: phase.negative_prompt_string,
                            variant_name_string: String(row.variant_name_string || `variant_${i}`),
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
                    throw new Error(
                        formatApiDetail(rj.detail) || rj.message || `render-variant ${i} HTTP ${rVar.status}`,
                    );
                }
                if (!rj.success_bool || !rj.clip_object) {
                    lastStartResponse = { httpOk: rVar.ok, httpStatus: rVar.status, body: rj };
                    throw new Error(`render-variant ${i}: нет success / clip_object`);
                }
                clips.push(rj.clip_object);
            }

            startJson = {
                success_bool: true,
                vision_analysis_object: visionObj,
                vision_provider_string: phase.vision_provider_string,
                clips_array: clips,
                user_prompt_string: phase.user_prompt_string,
                image_url_string: phase.image_url_string,
                user_name_string: phase.user_name_string,
                upload_response_object: phase.upload_response_object,
                pipeline_string: 'vision-start+4x-render-variant',
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
            hideVisionFatal();
        } catch (e) {
            console.error('[IdleLTX] start failed', e);
            showVisionFatal(
                `Idle LTX: этап остановлен (часто это Vision или конфиг API). Видео на Renderfin не отправлялось.\n\n${String(e.message || e)}`,
            );
            busy = false;
            if (btn) btn.disabled = false;
            return;
        }

        updateGeneratePreview(startJson);
        const clips = idleLtxPickClips(startJson);
        const vision = idleLtxPickVision(startJson);
        updateVariantRowMeta(vision, clips);

        lastPayloadByIndex.clear();
        clips.forEach((row, i) => {
            if (row?.generate_video_request_object) lastPayloadByIndex.set(Number(row.index_int ?? i), row.generate_video_request_object);
        });

        vidWrap?.classList.remove('hidden');
        vidWrap?.setAttribute('aria-hidden', 'false');

        const clipStates = clips.slice(0, VARIANT_COUNT).map((row, i) => ({
            index: Number(row.index_int ?? i),
            taskId: String(row.renderfin_task_id_string || '').trim(),
            outUrl: String(row.output_url_string || '').trim(),
            variantName: String(row.variant_name_string || `clip_${i}`),
            finalized: false,
            videoUrl: '',
        }));

        if (clipStates.length === 0) {
            const keys =
                startJson && typeof startJson === 'object' ? Object.keys(startJson).sort().join(', ') : '';
            console.error('[IdleLTX] empty clips_array; response keys:', keys, startJson);
            showVisionFatal(
                `Ответ start без clips_array (ключи: ${keys || '—'}). Проверьте деплой backend и «Копировать ответ start».`,
            );
            busy = false;
            if (btn) btn.disabled = false;
            return;
        }

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
            setVariantLoading(c.index, 'Renderfin: ожидание (опрос статуса)…', false);
            setVariantErr(c.index, '', false);
        });

        setStatus('Renderfin: опрос статусов по каждому клипу…');
        startPollingClipStates(clipStates);
    };

    btn?.addEventListener('click', () => void runGenerate());

    for (let i = 0; i < VARIANT_COUNT; i++) {
        document.getElementById(`idle-ltx-v-copy-${i}`)?.addEventListener('click', async () => {
            const p = lastPayloadByIndex.get(i);
            if (!p) {
                setStatus(`Нет payload для варианта ${i} — сначала «Генерировать».`, true);
                return;
            }
            const ok = await copyToClipboard(JSON.stringify(p, null, 2));
            setStatus(ok ? `Payload варианта ${i} скопирован.` : 'Не удалось скопировать.', !ok);
        });
    }

    copyPayload?.addEventListener('click', async () => {
        const all = Array.from(lastPayloadByIndex.entries())
            .sort((a, b) => a[0] - b[0])
            .map(([idx, obj]) => ({ index_int: idx, generate_video_request_object: obj }));
        if (all.length === 0) {
            setStatus('Нет payload — сначала «Генерировать».', true);
            return;
        }
        const ok = await copyToClipboard(JSON.stringify({ clips: all }, null, 2));
        setStatus(ok ? 'Все payload скопированы.' : 'Не удалось скопировать.', !ok);
    });

    copyStart?.addEventListener('click', async () => {
        const p = lastStartResponse;
        if (!p) {
            setStatus('Нет ответа start — сначала «Генерировать».', true);
            return;
        }
        const ok = await copyToClipboard(JSON.stringify(p.body != null ? p.body : p, null, 2));
        setStatus(ok ? 'Ответ /idle-ltx/start скопирован.' : 'Не удалось скопировать.', !ok);
    });

    copyStatus?.addEventListener('click', async () => {
        const p = lastStatusResponse;
        if (!p) {
            setStatus('Нет ответа clip-status — дождитесь polling.', true);
            return;
        }
        const ok = await copyToClipboard(JSON.stringify(p, null, 2));
        setStatus(ok ? 'Последний clip-status скопирован.' : 'Не удалось скопировать.', !ok);
    });

    const resume = readLsJob(taskId);
    const resumeClips = Array.isArray(resume?.idleClips) ? resume.idleClips : null;
    const resumeHasIds = Boolean(
        resumeClips?.some(
            (r) =>
                String(r?.renderfin_task_id_string || '').trim() ||
                String(r?.output_url_string || '').trim(),
        ),
    );
    if (resumeClips && resumeClips.length > 0 && !resumeHasIds) {
        console.warn('[IdleLTX] dropping stale idleClips (no task_id / output_url)');
        try {
            clearLsJob(taskId);
        } catch (_) {}
    } else if (resumeClips && resumeClips.length > 0 && resumeHasIds) {
        restoreVisionPreviewFromLs(resume);
        if (resume.visionSnapshot && Array.isArray(resume.clipsMeta)) {
            updateVariantRowMeta(resume.visionSnapshot, resume.clipsMeta);
        }
        lastPayloadByIndex.clear();
        (resume.clipsMeta || []).forEach((row, i) => {
            if (row?.generate_video_request_object)
                lastPayloadByIndex.set(Number(row.index_int ?? i), row.generate_video_request_object);
        });
        setStatus('Восстановление опроса клипов из localStorage…');
        busy = true;
        if (btn) btn.disabled = true;
        const clipStates = resumeClips.slice(0, VARIANT_COUNT).map((row, i) => ({
            index: Number(row.index_int ?? i),
            taskId: String(row.renderfin_task_id_string || '').trim(),
            outUrl: String(row.output_url_string || '').trim(),
            variantName: String(row.variant_name_string || `clip_${i}`),
            finalized: false,
            videoUrl: '',
        }));
        vidWrap?.classList.remove('hidden');
        vidWrap?.setAttribute('aria-hidden', 'false');
        clipStates.forEach((c) => {
            setVariantLoading(c.index, 'Восстановление…', false);
            const nameEl = document.getElementById(`idle-ltx-v-name-${c.index}`);
            if (nameEl && !resume.clipsMeta) nameEl.textContent = c.variantName;
        });
        startPollingClipStates(clipStates);
    }

    console.log('[IdleLTX] module ready for task', taskId);
}
