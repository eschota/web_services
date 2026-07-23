(function () {
    'use strict';

    const params = new URLSearchParams(window.location.search);
    const taskId = params.get('id');
    if (!taskId) return;

    let state = null;
    let selectedAmount = 20;
    const language = ['en', 'ru', 'zh', 'hi'].includes(localStorage.getItem('autorig_lang'))
        ? localStorage.getItem('autorig_lang')
        : (['en', 'ru', 'zh', 'hi'].includes(navigator.language.split('-')[0]) ? navigator.language.split('-')[0] : 'en');
    const messages = {
        en: {
            title: 'Offer to buy this 3D model', button: 'Offer to buy this model',
            copy: 'Choose an offer amount. The author and administrator will be emailed. Payment and downloads are not enabled automatically.',
            custom: 'Custom amount, USD ($1 minimum)', submit: 'Send offer', sent: 'The offer was sent to the author.',
            login: 'Sign in to send an offer.', unavailable: 'The author of this task cannot receive offers.',
            cancel: 'Cancel', latest: 'Latest offer'
        },
        ru: {
            title: 'Предложить автору купить 3D-модель', button: 'Предложить автору купить модель',
            copy: 'Выберите сумму предложения. Автор и администратор получат письмо. Оплата и скачивание не открываются автоматически.',
            custom: 'Своя сумма, USD (минимум $1)', submit: 'Отправить предложение', sent: 'Предложение отправлено автору.',
            login: 'Войдите, чтобы отправить предложение.', unavailable: 'Автор этой задачи не может получать предложения.',
            cancel: 'Отмена', latest: 'Последнее предложение'
        },
        zh: {
            title: '向作者提出购买此 3D 模型', button: '向作者提出购买模型',
            copy: '请选择报价金额。作者和管理员将收到电子邮件。付款和下载权限不会自动开启。',
            custom: '自定义金额，美元（最低 $1）', submit: '发送报价', sent: '报价已发送给作者。',
            login: '请登录后发送报价。', unavailable: '此任务的作者无法接收报价。',
            cancel: '取消', latest: '最新报价'
        },
        hi: {
            title: 'लेखक को यह 3D मॉडल खरीदने का प्रस्ताव दें', button: 'मॉडल खरीदने का प्रस्ताव दें',
            copy: 'प्रस्ताव राशि चुनें। लेखक और व्यवस्थापक को ईमेल मिलेगा। भुगतान और डाउनलोड अपने-आप चालू नहीं होंगे।',
            custom: 'अपनी राशि, USD (न्यूनतम $1)', submit: 'प्रस्ताव भेजें', sent: 'प्रस्ताव लेखक को भेज दिया गया है।',
            login: 'प्रस्ताव भेजने के लिए साइन इन करें।', unavailable: 'इस कार्य का लेखक प्रस्ताव प्राप्त नहीं कर सकता।',
            cancel: 'रद्द करें', latest: 'नवीनतम प्रस्ताव'
        }
    };
    const text = messages[language];

    function css() {
        const style = document.createElement('style');
        style.textContent = `
          #model-sale-card{padding:20px;margin-top:16px;border:1px solid rgba(99,102,241,.4);border-radius:14px;background:rgba(99,102,241,.08);text-align:center}
          #model-sale-card h3{margin:0 0 10px}
          #model-sale-dialog{max-width:520px;width:calc(100% - 32px);border:1px solid rgba(255,255,255,.14);border-radius:16px;background:#171722;color:#f5f5fa;padding:0}
          #model-sale-dialog::backdrop{background:rgba(0,0,0,.72)}
          .model-sale-body{padding:24px}.model-sale-presets{display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin:20px 0}
          .model-sale-preset{padding:12px;border:1px solid #6366f1;border-radius:9px;background:transparent;color:inherit;font-weight:700;cursor:pointer}
          .model-sale-preset.selected{background:#6366f1;color:#fff}
          #model-sale-custom{width:100%;box-sizing:border-box;padding:12px;border-radius:8px;border:1px solid #555;background:#0f0f16;color:#fff}
          .model-sale-actions{display:flex;justify-content:flex-end;gap:10px;margin-top:20px}
          #model-sale-status{min-height:22px;margin-top:12px;color:#b7c0ff}
        `;
        document.head.appendChild(style);
    }

    function hideForeignDownloads() {
        document.getElementById('downloads-card')?.classList.add('hidden');
        [
            '#custom-anim-download-btn',
            '#custom-anim-download-pack-btn',
            '#download-all-btn',
            '[onclick*="downloadAnimalVariant"]',
            '[onclick*="downloadSelectedAnimation"]'
        ].forEach((selector) => {
            document.querySelectorAll(selector).forEach((el) => el.classList.add('hidden'));
        });
    }

    function createUi() {
        if (document.getElementById('model-sale-card')) return;
        const host = document.getElementById('task-sidebar-bundle') || document.querySelector('main') || document.body;
        const card = document.createElement('section');
        card.id = 'model-sale-card';
        card.innerHTML = `<h3>${text.title}</h3><p>${text.copy}</p><button type="button" class="btn btn-primary" id="model-sale-open">${text.button}</button><div id="model-sale-card-status"></div>`;
        host.appendChild(card);

        const dialog = document.createElement('dialog');
        dialog.id = 'model-sale-dialog';
        dialog.innerHTML = `
          <form class="model-sale-body" id="model-sale-form">
            <h2>${text.title}</h2><p>${text.copy}</p>
            <div class="model-sale-presets">
              ${(state.presets_usd || [20, 50, 100]).map((v) => `<button type="button" class="model-sale-preset${v === 20 ? ' selected' : ''}" data-amount="${v}">$${v}</button>`).join('')}
            </div>
            <label>${text.custom}<input id="model-sale-custom" type="number" min="1" max="1000000" step="0.01" placeholder="1.00"></label>
            <div id="model-sale-status"></div>
            <div class="model-sale-actions"><button type="button" class="btn btn-secondary" id="model-sale-close">${text.cancel}</button><button type="submit" class="btn btn-primary">${text.submit}</button></div>
          </form>`;
        document.body.appendChild(dialog);

        card.querySelector('#model-sale-open').addEventListener('click', () => {
            if (!state.authenticated) {
                const next = window.location.pathname + window.location.search + (window.location.search ? '&' : '?') + 'offer=1';
                window.location.href = '/auth/login?next=' + encodeURIComponent(next);
                return;
            }
            dialog.showModal();
        });
        dialog.querySelector('#model-sale-close').addEventListener('click', () => dialog.close());
        dialog.addEventListener('click', (event) => { if (event.target === dialog) dialog.close(); });
        dialog.querySelectorAll('.model-sale-preset').forEach((button) => {
            button.addEventListener('click', () => {
                selectedAmount = Number(button.dataset.amount);
                dialog.querySelectorAll('.model-sale-preset').forEach((item) => item.classList.toggle('selected', item === button));
                dialog.querySelector('#model-sale-custom').value = '';
            });
        });
        dialog.querySelector('#model-sale-custom').addEventListener('input', (event) => {
            if (event.target.value) {
                selectedAmount = Number(event.target.value);
                dialog.querySelectorAll('.model-sale-preset').forEach((item) => item.classList.remove('selected'));
            }
        });
        dialog.querySelector('#model-sale-form').addEventListener('submit', submitOffer);
        if (params.get('offer') === '1' && state.authenticated) dialog.showModal();
        if (state.active_offer) {
            card.querySelector('#model-sale-card-status').textContent =
                `${text.latest}: $${state.active_offer.amount_usd} (${state.active_offer.status})`;
        }
    }

    async function submitOffer(event) {
        event.preventDefault();
        const dialog = document.getElementById('model-sale-dialog');
        const status = dialog.querySelector('#model-sale-status');
        const submit = dialog.querySelector('[type="submit"]');
        status.textContent = '';
        submit.disabled = true;
        try {
            const response = await fetch(`/api/task/${encodeURIComponent(taskId)}/sale-offers`, {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({amount_usd: selectedAmount})
            });
            const payload = await response.json().catch(() => ({}));
            if (!response.ok) {
                const detail = typeof payload.detail === 'string' ? payload.detail : payload.detail?.message;
                throw new Error(detail || `HTTP ${response.status}`);
            }
            status.textContent = text.sent;
            document.getElementById('model-sale-card-status').textContent = text.sent;
            setTimeout(() => dialog.close(), 1200);
        } catch (error) {
            status.textContent = error.message || String(error);
        } finally {
            submit.disabled = false;
        }
    }

    async function init() {
        css();
        try {
            const response = await fetch(`/api/task/${encodeURIComponent(taskId)}/sale-offer-state`, {credentials: 'same-origin'});
            if (!response.ok) return;
            state = await response.json();
            window.ModelSaleOfferState = state;
            if (state.can_download) return;
            hideForeignDownloads();
            new MutationObserver(hideForeignDownloads).observe(document.body, {
                childList: true,
                subtree: true,
                attributes: true,
                attributeFilter: ['class']
            });
            if (state.offer_available) createUi();
            else {
                const card = document.createElement('section');
                card.id = 'model-sale-card';
                card.textContent = text.unavailable;
                (document.getElementById('task-sidebar-bundle') || document.body).appendChild(card);
            }
        } catch (error) {
            console.warn('[ModelSale] state unavailable', error);
        }
    }

    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
    else init();
})();
