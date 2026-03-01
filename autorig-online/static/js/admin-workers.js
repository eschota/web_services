/**
 * AutoRig Online - Workers Admin Page
 */

const WorkersAdmin = {
    workers: [],

    async init() {
        await I18n.init();
        this.setupTheme();
        this.setupEventListeners();
        await this.loadWorkers();
    },

    setupTheme() {
        const toggle = document.getElementById('theme-toggle');
        const savedTheme = localStorage.getItem('autorig_theme') || 'dark';
        document.documentElement.setAttribute('data-theme', savedTheme);
        if (toggle) toggle.textContent = savedTheme === 'dark' ? '☀️' : '🌙';

        toggle?.addEventListener('click', () => {
            const current = document.documentElement.getAttribute('data-theme');
            const newTheme = current === 'dark' ? 'light' : 'dark';
            document.documentElement.setAttribute('data-theme', newTheme);
            localStorage.setItem('autorig_theme', newTheme);
            toggle.textContent = newTheme === 'dark' ? '☀️' : '🌙';
        });
    },

    setupEventListeners() {
        const addWorkerForm = document.getElementById('add-worker-form');
        addWorkerForm?.addEventListener('submit', async (e) => {
            e.preventDefault();
            const urlEl = document.getElementById('new-worker-url');
            const weightEl = document.getElementById('new-worker-weight');
            const enabledEl = document.getElementById('new-worker-enabled');

            const url = (urlEl?.value || '').trim();
            const weight = parseInt(weightEl?.value || '0', 10) || 0;
            const enabled = !!enabledEl?.checked;

            if (!url) {
                alert('Worker URL is required');
                return;
            }

            await this.createWorker({ url, weight, enabled });
            if (urlEl) urlEl.value = '';
        });
    },

    async loadWorkers() {
        const tbody = document.getElementById('workers-table');
        if (tbody) {
            tbody.innerHTML = `
                <tr>
                    <td colspan="9" style="text-align: center; color: var(--text-muted);">Loading...</td>
                </tr>
            `;
        }

        try {
            const resp = await fetch('/api/admin/workers');
            if (resp.status === 403) {
                window.location.href = '/';
                return;
            }
            const data = await resp.json();
            if (!resp.ok) {
                alert(data.detail || 'Failed to load workers');
                return;
            }
            this.workers = data.workers || [];
            this.renderWorkers(this.workers);
        } catch (e) {
            console.error('Error loading workers:', e);
            alert('Failed to load workers');
        }
    },

    renderWorkers(workers) {
        const tbody = document.getElementById('workers-table');
        if (!tbody) return;

        const total = workers.length;
        const enabledCount = workers.filter(w => w.enabled).length;
        const totalEl = document.getElementById('workers-total');
        const enabledEl = document.getElementById('workers-enabled');
        if (totalEl) totalEl.textContent = String(total);
        if (enabledEl) enabledEl.textContent = String(enabledCount);

        if (workers.length === 0) {
            tbody.innerHTML = `
                <tr>
                    <td colspan="9" style="text-align: center; color: var(--text-muted);">No workers configured</td>
                </tr>
            `;
            return;
        }

        tbody.innerHTML = workers.map(w => `
            <tr data-worker-id="${w.id}">
                <td>${w.id}</td>
                <td>
                    <input class="form-input" id="worker-url-${w.id}" value="${this.escapeHtml(w.url)}" style="width: 100%;">
                </td>
                <td>
                    <input class="form-input" type="number" id="worker-weight-${w.id}" value="${w.weight}" style="width: 100px;">
                </td>
                <td style="text-align: center;">
                    <input type="checkbox" id="worker-enabled-${w.id}" ${w.enabled ? 'checked' : ''} style="width: 18px; height: 18px;">
                </td>
                <td style="text-align: right; font-variant-numeric: tabular-nums;">${w.done_tasks ?? 0}</td>
                <td style="text-align: right; font-variant-numeric: tabular-nums;">${w.total_tasks ?? 0}</td>
                <td style="text-align: right; font-variant-numeric: tabular-nums;">${this.formatPct(w.done_share_pct)}</td>
                <td>${this.formatDate(w.updated_at)}</td>
                <td style="white-space: nowrap;">
                    <button class="btn btn-secondary" data-action="save-worker" data-worker-id="${w.id}" style="padding: 0.25rem 0.5rem; font-size: 0.75rem;">
                        Save
                    </button>
                    <button class="btn btn-ghost" data-action="delete-worker" data-worker-id="${w.id}" style="padding: 0.25rem 0.5rem; font-size: 0.75rem; color: var(--error);">
                        Delete
                    </button>
                </td>
            </tr>
        `).join('');

        tbody.querySelectorAll('button[data-action="save-worker"]').forEach(btn => {
            btn.addEventListener('click', async () => {
                const workerId = parseInt(btn.getAttribute('data-worker-id'), 10);
                const urlEl = document.getElementById(`worker-url-${workerId}`);
                const weightEl = document.getElementById(`worker-weight-${workerId}`);
                const enabledEl = document.getElementById(`worker-enabled-${workerId}`);

                const url = (urlEl?.value || '').trim();
                const weight = parseInt(weightEl?.value || '0', 10) || 0;
                const enabled = !!enabledEl?.checked;

                btn.disabled = true;
                try {
                    await this.updateWorker(workerId, { url, weight, enabled });
                } finally {
                    btn.disabled = false;
                }
            });
        });

        tbody.querySelectorAll('button[data-action="delete-worker"]').forEach(btn => {
            btn.addEventListener('click', async () => {
                const workerId = parseInt(btn.getAttribute('data-worker-id'), 10);
                const ok = confirm(`Delete worker #${workerId}?`);
                if (!ok) return;
                btn.disabled = true;
                try {
                    await this.deleteWorker(workerId);
                } finally {
                    btn.disabled = false;
                }
            });
        });
    },

    async createWorker(payload) {
        const btn = document.getElementById('add-worker-btn');
        if (btn) btn.disabled = true;
        try {
            const resp = await fetch('/api/admin/workers', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
            const data = await resp.json().catch(() => ({}));
            if (!resp.ok) {
                alert(data.detail || 'Failed to create worker');
                return;
            }
            await this.loadWorkers();
        } catch (e) {
            console.error('Create worker error:', e);
            alert('Failed to create worker');
        } finally {
            if (btn) btn.disabled = false;
        }
    },

    async updateWorker(workerId, payload) {
        try {
            const resp = await fetch(`/api/admin/workers/${workerId}`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
            const data = await resp.json().catch(() => ({}));
            if (!resp.ok) {
                alert(data.detail || 'Failed to update worker');
                return;
            }
            await this.loadWorkers();
        } catch (e) {
            console.error('Update worker error:', e);
            alert('Failed to update worker');
        }
    },

    async deleteWorker(workerId) {
        try {
            const resp = await fetch(`/api/admin/workers/${workerId}`, { method: 'DELETE' });
            const data = await resp.json().catch(() => ({}));
            if (!resp.ok) {
                alert(data.detail || 'Failed to delete worker');
                return;
            }
            await this.loadWorkers();
        } catch (e) {
            console.error('Delete worker error:', e);
            alert('Failed to delete worker');
        }
    },

    formatDate(isoString) {
        if (!isoString) return '-';
        try {
            const date = new Date(isoString);
            return date.toLocaleString();
        } catch {
            return String(isoString);
        }
    },

    formatPct(value) {
        const n = Number(value);
        if (!Number.isFinite(n)) return '0%';
        return `${n.toFixed(1)}%`;
    },

    escapeHtml(str) {
        return String(str || '')
            .replaceAll('&', '&amp;')
            .replaceAll('<', '&lt;')
            .replaceAll('>', '&gt;')
            .replaceAll('"', '&quot;')
            .replaceAll("'", '&#039;');
    }
};

document.addEventListener('DOMContentLoaded', () => WorkersAdmin.init());

