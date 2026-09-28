/**
 * Upload check, second half: hair, loose clothing, tail and pose.
 *
 * The rig-type check already renders the model from every side. Four of those
 * renders are tiled into one sheet and judged once; the answer fills a row of
 * switches the user can change, and picks the pipeline to recommend:
 * Simple Rig, or the AI pipeline that animates hair and cloth.
 *
 * The switches are the point, not decoration: the check is right about ten
 * times in twelve, and a model the user knows better than the check must be
 * one click away from the other pipeline.
 */
(function () {
    'use strict';

    const PARTS = [
        { id: 'long_hair', ru: 'Длинные волосы', en: 'Long hair', icon: '💇' },
        { id: 'skirt', ru: 'Юбка', en: 'Skirt', icon: '👗' },
        { id: 'dress', ru: 'Платье', en: 'Dress', icon: '👘' },
        { id: 'cape', ru: 'Плащ', en: 'Cape', icon: '🦸' },
        { id: 'coat_tail', ru: 'Полы пальто', en: 'Coat tails', icon: '🧥' },
        { id: 'robe', ru: 'Мантия', en: 'Robe', icon: '🧙' },
        { id: 'scarf', ru: 'Шарф', en: 'Scarf', icon: '🧣' },
        { id: 'tail', ru: 'Хвост', en: 'Tail', icon: '🐾' },
    ];

    const TEXT = {
        ru: {
            title: 'Что будет двигаться',
            checking: 'Смотрю на волосы, одежду и позу…',
            failed: 'Автоматически определить не вышло — отметьте вручную.',
            untextured: 'У модели нет текстур — оценка по форме, проверьте отметки.',
            refining: 'Нет текстур — дорисовываю персонажа по Z-depth, ~1 мин…',
            refined: 'Уточнено по Z-depth',
            paintedTitle: 'Так ИИ дорисовал модель по её глубине',
            hint: 'Отметки можно поменять — выбор пайплайна пересчитается.',
            hair: { none: 'волос не видно', short: 'короткие волосы', long: 'длинные волосы', unknown: 'волосы не определены' },
            poseOk: 'T-pose',
            poseA: 'A-pose — не T-pose',
            poseOther: 'Не T-pose',
            poseUnknown: 'Поза не определена',
            pipeline: 'Пайплайн',
            simple: 'Simple Rig',
            simpleSub: 'Стандартный риг',
            pro: 'AI pro hair and clothes animation',
            proSub: 'Best Results — волосы и одежда анимируются, поза перегенерируется при необходимости',
            recommended: 'рекомендуем',
            early: 'ранний доступ',
        },
        en: {
            title: 'What will move',
            checking: 'Checking hair, clothing and pose…',
            failed: 'Could not tell automatically — mark it yourself.',
            untextured: 'The model has no textures — judged by shape, check the marks.',
            refining: 'No textures — repainting the character from Z-depth, ~1 min…',
            refined: 'Refined from Z-depth',
            paintedTitle: 'How the AI repainted the model from its depth',
            hint: 'Change any mark — the pipeline choice follows.',
            hair: { none: 'no hair visible', short: 'short hair', long: 'long hair', unknown: 'hair not determined' },
            poseOk: 'T-pose',
            poseA: 'A-pose — not a T-pose',
            poseOther: 'Not a T-pose',
            poseUnknown: 'Pose not determined',
            pipeline: 'Pipeline',
            simple: 'Simple Rig',
            simpleSub: 'Standard rig',
            pro: 'AI pro hair and clothes animation',
            proSub: 'Best Results — hair and clothes are animated, the pose is regenerated if needed',
            recommended: 'recommended',
            early: 'early access',
        },
    };

    const lang = () => (window.I18n && String(window.I18n.currentLang || '').startsWith('ru') ? 'ru' : 'en');
    const tx = () => TEXT[lang()];

    const CSS = [
        '.rap{margin:.35rem 0 .5rem;padding:.5rem .65rem;border:1px solid var(--border-color,#2a3140);',
        'border-radius:12px;background:var(--bg-secondary,rgba(255,255,255,.03));text-align:left}',
        '.rap-head{display:flex;align-items:center;gap:.5rem;flex-wrap:wrap;margin-bottom:.35rem}',
        '.rap-title{font-weight:700;font-size:.9rem}',
        '.rap-status{font-size:.78rem;color:var(--text-secondary,#8b99ad)}',
        '.rap-status.rap-warn{color:#d29922}',
        '.rap-spin{width:12px;height:12px;border-radius:50%;border:2px solid currentColor;border-right-color:transparent;',
        'display:inline-block;animation:rapspin .8s linear infinite;vertical-align:-2px;margin-right:.3rem}',
        '@keyframes rapspin{to{transform:rotate(360deg)}}',
        '.rap-chips{display:flex;flex-wrap:wrap;gap:.25rem}',
        '.rap-chip{border:1px solid var(--border-color,#2a3140);background:transparent;color:inherit;border-radius:999px;',
        'padding:.2rem .45rem;font-size:.74rem;cursor:pointer;line-height:1.2;transition:background .12s,border-color .12s}',
        '.rap-chip[aria-pressed="true"]{background:rgba(63,185,80,.16);border-color:#3fb950;font-weight:600}',
        '.rap-chip .rap-ai{font-size:.62rem;margin-left:.3rem;padding:0 .3rem;border-radius:5px;background:#2f6feb;color:#fff;vertical-align:1px}',
        '.rap-marks{display:flex;flex-wrap:wrap;gap:.3rem;margin:.35rem 0 .4rem}',
        '.rap-mark{font-size:.76rem;padding:.2rem .55rem;border-radius:7px;background:rgba(139,153,173,.14)}',
        '.rap-mark.rap-pose-bad{background:rgba(210,153,34,.18);color:#e3b341;font-weight:600}',
        '.rap-mark.rap-pose-ok{background:rgba(63,185,80,.14);color:#3fb950}',
        '.rap-hint{font-size:.72rem;color:var(--text-secondary,#8b99ad);margin:.35rem 0 .5rem}',
        '.rap-pipes{display:grid;grid-template-columns:1fr 1fr;gap:.4rem}',
        '@media(max-width:560px){.rap-pipes{grid-template-columns:1fr}}',
        '.rap-pipe{display:block;text-align:left;border:1.5px solid var(--border-color,#2a3140);border-radius:10px;',
        'padding:.35rem .55rem;background:transparent;color:inherit;cursor:pointer}',
        '.rap-pipe[aria-checked="true"]{border-color:var(--accent,#2f6feb);background:rgba(47,111,235,.12)}',
        '.rap-pipe b{font-size:.84rem;display:block}',
        '.rap-pipe small{font-size:.68rem;line-height:1.25;color:var(--text-secondary,#8b99ad);display:block;margin-top:.15rem}',
        '.rap-tag{display:inline-block;font-size:.62rem;margin-left:.35rem;padding:.02rem .35rem;border-radius:5px;',
        'background:#3fb950;color:#0d1117;font-weight:700;vertical-align:1px}',
        '.rap-tag.rap-early{background:#d29922}',
        '.rap-thumb{float:right;margin:0 0 .3rem .5rem}',
        '.rap-thumb img{width:54px;height:96px;object-fit:cover;border-radius:6px;border:1px solid var(--border-color,#2a3140);display:block}',
    ].join('\n');

    function injectCss() {
        if (document.getElementById('rap-css')) return;
        const el = document.createElement('style');
        el.id = 'rap-css';
        el.textContent = CSS;
        document.head.appendChild(el);
    }

    /** Four renders, one sheet: front, back, left, right. */
    function buildSheet(dataUrls) {
        return Promise.all(dataUrls.map((src) => new Promise((resolve, reject) => {
            const img = new Image();
            img.onload = () => resolve(img);
            img.onerror = reject;
            img.src = src;
        }))).then((imgs) => {
            const side = 512;
            const canvas = document.createElement('canvas');
            canvas.width = side * 2;
            canvas.height = side * 2;
            const ctx = canvas.getContext('2d');
            imgs.forEach((img, i) => {
                const s = Math.min(side / img.width, side / img.height);
                const w = img.width * s;
                const h = img.height * s;
                ctx.drawImage(img, (i % 2) * side + (side - w) / 2, Math.floor(i / 2) * side + (side - h) / 2, w, h);
            });
            return canvas.toDataURL('image/jpeg', 0.88);
        });
    }

    /** Partial answers are normal: every field falls back to "unknown". */
    function start(renders, options) {
        const untextured = !!(options && options.untextured);
        const depth = options && options.depth;
        // An untextured model is judged twice: at once on the grey renders, and
        // again, about a minute later, on a picture the farm paints over its
        // Z-depth. The second answer arrives as `refine` on the first.
        const refine = untextured && depth
            ? fetch('/api/rig-v2/vision/appearance-depth', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ depth_png_base64_string: depth, lang_string: lang() }),
            }).then((r) => r.json()).catch((err) => ({ success_bool: false, error_string: String(err) }))
            : null;
        return buildSheet(renders)
            .then((sheet) => fetch('/api/rig-v2/vision/appearance', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ image_jpg_base64_string: sheet, lang_string: lang() }),
            }))
            .then((r) => r.json())
            .then((data) => Object.assign({ untextured, refine }, data))
            .catch((err) => ({ success_bool: false, status_string: 'network', error_string: String(err), untextured, refine }));
    }

    function partsFrom(result) {
        const on = new Set();
        if (!result || !result.success_bool) return on;
        if (result.hair === 'long') on.add('long_hair');
        (result.loose_clothing || []).forEach((c) => { if (PARTS.some((p) => p.id === c)) on.add(c); });
        if (result.tail) on.add('tail');
        return on;
    }

    function escapeHtml(s) {
        const map = { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' };
        return String(s).replace(/[&<>"']/g, (c) => map[c]);
    }

    /**
     * Draw the panel into `host` and wire it. `onTouch` is called the first time
     * the user changes anything, so the page can stop its auto-start timer: a
     * countdown that fires while someone is still ticking boxes starts the
     * wrong job.
     */
    function mount(host, promise, onTouch, onHold) {
        injectCss();
        const state = {
            ai: new Set(), on: new Set(), result: null,
            pipeline: 'simple', pipelineByUser: false, touched: false,
            refining: false, painted: '',
        };
        const root = document.createElement('div');
        root.className = 'rap';
        root.id = 'rig-appearance-panel';
        host.appendChild(root);

        const touch = () => {
            if (!state.touched) {
                state.touched = true;
                if (typeof onTouch === 'function') onTouch();
            }
        };
        const recommended = () => (state.on.size ? 'ai_pro' : 'simple');

        function poseMark(T, r) {
            if (r.pose === 't_pose') return '<span class="rap-mark rap-pose-ok">✓ ' + T.poseOk + '</span>';
            if (r.pose === 'a_pose') return '<span class="rap-mark rap-pose-bad">⚠ ' + T.poseA + '</span>';
            if (r.pose === 'other') {
                const note = r.pose_note ? ': ' + escapeHtml(r.pose_note) : '';
                return '<span class="rap-mark rap-pose-bad">⚠ ' + T.poseOther + note + '</span>';
            }
            return '<span class="rap-mark">' + T.poseUnknown + '</span>';
        }

        function render(phase) {
            const T = tx();
            const r = state.result || {};
            let status = '';
            if (phase === 'loading') status = '<span class="rap-status"><span class="rap-spin"></span>' + T.checking + '</span>';
            else if (state.refining) status = '<span class="rap-status"><span class="rap-spin"></span>' + T.refining + '</span>';
            else if (state.painted) status = '<span class="rap-status">' + T.refined + '</span>';
            else if (!r.success_bool) status = '<span class="rap-status rap-warn">' + T.failed + '</span>';
            else if (r.untextured) status = '<span class="rap-status rap-warn">' + T.untextured + '</span>';

            const chips = PARTS.map((p) => {
                const ai = state.ai.has(p.id) ? '<span class="rap-ai">AI</span>' : '';
                return '<button type="button" class="rap-chip" data-part="' + p.id + '" aria-pressed="'
                    + state.on.has(p.id) + '">' + p.icon + ' ' + p[lang()] + ai + '</button>';
            }).join('');

            let marks = '';
            if (phase !== 'loading' && r.success_bool) {
                const hair = T.hair[r.hair] || T.hair.unknown;
                const note = r.hair_note ? ' — ' + escapeHtml(r.hair_note) : '';
                marks = '<div class="rap-marks"><span class="rap-mark">💇 ' + hair + note + '</span>' + poseMark(T, r) + '</div>';
            } else {
                marks = '<div class="rap-marks"></div>';
            }

            const rec = recommended();
            const pipe = (id, title, sub, extra) => {
                const tag = rec === id && phase !== 'loading' ? '<span class="rap-tag">' + T.recommended + '</span>' : '';
                return '<button type="button" class="rap-pipe" role="radio" data-pipe="' + id + '" aria-checked="'
                    + (state.pipeline === id) + '"><b>' + title + tag + extra + '</b><small>' + sub + '</small></button>';
            };

            const thumb = state.painted
                ? '<a class="rap-thumb" href="' + escapeHtml(state.painted) + '" target="_blank" rel="noopener" title="'
                  + T.paintedTitle + '"><img src="' + escapeHtml(state.painted) + '" alt=""></a>'
                : '';
            root.innerHTML = ''
                + thumb
                + '<div class="rap-head"><span class="rap-title" title="' + T.hint + '">' + T.title + '</span>' + status + '</div>'
                + '<div class="rap-chips" role="group">' + chips + '</div>'
                + marks
                + '<div class="rap-pipes" role="radiogroup">'
                + pipe('simple', T.simple, T.simpleSub, '')
                + pipe('ai_pro', T.pro, T.proSub, '<span class="rap-tag rap-early">' + T.early + '</span>')
                + '</div>';
        }

        root.addEventListener('click', (ev) => {
            const chip = ev.target.closest('.rap-chip');
            if (chip) {
                const id = chip.dataset.part;
                if (state.on.has(id)) state.on.delete(id); else state.on.add(id);
                // The pipeline follows the marks until the user picks one outright.
                if (!state.pipelineByUser) state.pipeline = recommended();
                touch();
                render('ready');
                return;
            }
            const pipe = ev.target.closest('.rap-pipe');
            if (pipe) {
                state.pipeline = pipe.dataset.pipe;
                state.pipelineByUser = true;
                touch();
                render('ready');
            }
        });

        render('loading');
        const adopt = (result) => {
            state.ai = partsFrom(result);
            // A user who already started ticking boxes keeps their marks.
            if (!state.touched) {
                state.on = new Set(state.ai);
                if (!state.pipelineByUser) state.pipeline = recommended();
            }
        };
        Promise.resolve(promise).then((result) => {
            state.result = result || { success_bool: false };
            adopt(state.result);
            if (state.result.refine) {
                state.refining = true;
                // The repaint takes about a minute, longer than the auto-start
                // countdown; the page is asked to hold so the job does not
                // start on the grey guess while the better answer is on its way.
                if (typeof onHold === 'function') onHold();
                state.result.refine.then((better) => {
                    state.refining = false;
                    if (better && better.success_bool) {
                        state.painted = better.painted_url_string || '';
                        // The repaint is judged from the front only, so it misses
                        // what hangs behind — the first pass found a cape the
                        // repaint never saw. Parts are the union of both passes;
                        // hair and pose come from the repaint, which sees them in
                        // colour.
                        const first = state.result || {};
                        const union = [...new Set([...(first.loose_clothing || []), ...(better.loose_clothing || [])])];
                        state.result = Object.assign({}, better, {
                            untextured: true,
                            loose_clothing: union,
                            tail: !!(first.tail || better.tail),
                            hair: first.hair === 'long' || better.hair === 'long' ? 'long' : better.hair,
                        });
                        adopt(state.result);
                    }
                    render('ready');
                });
            }
            render('ready');
        });

        window.addEventListener('languageChanged', () => render(state.result ? 'ready' : 'loading'));

        return {
            /** What goes into the task: the check's answer and the user's final word. */
            choice() {
                const r = state.result || {};
                return {
                    pipeline_string: state.pipeline,
                    recommended_pipeline_string: recommended(),
                    parts_array: [...state.on],
                    detected_parts_array: [...state.ai],
                    user_changed_bool: state.touched,
                    hair_string: r.hair || 'unknown',
                    hair_note_string: r.hair_note || '',
                    headwear_bool: !!r.headwear,
                    pose_string: r.pose || 'unknown',
                    pose_note_string: r.pose_note || '',
                    untextured_bool: !!r.untextured,
                    depth_painted_url_string: state.painted || '',
                    check_ok_bool: !!r.success_bool,
                    model_used_string: r.model_used_string || '',
                };
            },
        };
    }

    window.RigAppearance = { start, mount, buildSheet, PARTS };
})();
