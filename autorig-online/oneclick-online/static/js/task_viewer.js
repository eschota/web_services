export class task_viewer {
    constructor(options = {}) {
        this.loadPreparedModel = options.loadPreparedModel;
        this.loadFbxModel = options.loadFbxModel;
        this.setStatus = options.setStatus || (() => {});
        this.postLog = options.postLog || null; // (level, message, extra?) => void
        this.logPrefix = options.logPrefix || '[task_viewer]';
    }

    getMeshFbxUrls(taskData) {
        const ready = Array.isArray(taskData?.ready_urls) ? taskData.ready_urls : [];
        const urls = ready.filter((url) => {
            if (typeof url !== 'string') return false;
            const lower = url.toLowerCase();
            return lower.includes('/meshes/') && lower.includes('.fbx');
        });
        return [...new Set(urls)];
    }

    getMeshFbxFilenames(taskData) {
        const urls = this.getMeshFbxUrls(taskData);
        const files = [];
        for (const url of urls) {
            try {
                const clean = String(url).split('?')[0];
                const name = clean.split('/').pop();
                if (name && name.toLowerCase().endsWith('.fbx')) files.push(name);
            } catch {
                // ignore
            }
        }
        return [...new Set(files)];
    }

    getMeshFbxProxyUrls(taskId, taskData) {
        const names = this.getMeshFbxFilenames(taskData);
        return names.map((name) => `/api/task/${taskId}/meshes/${encodeURIComponent(name)}`);
    }

    hasMeshFbx(taskData) {
        return this.getMeshFbxUrls(taskData).length > 0;
    }

    async loadPreparedModelIfReady(taskId, taskData) {
        if (!taskData?.prepared_glb_ready || !this.loadPreparedModel) {
            return false;
        }
        const ok = await this.loadPreparedModel(`/api/task/${taskId}/prepared.glb`, 'Prepared Model');
        return !!ok;
    }

    async loadMeshesFallback(taskId, taskData) {
        const urls = this.getMeshFbxProxyUrls(taskId, taskData);
        if (!urls.length || !this.loadFbxModel) {
            return false;
        }

        this.setStatus(`Loading FBX fallback (${urls.length})...`);
        console.log(`${this.logPrefix} loading FBX fallback`, urls);

        let loadedCount = 0;
        for (let i = 0; i < urls.length; i += 1) {
            const url = urls[i];
            const label = `FBX ${i + 1}/${urls.length}`;
            try {
                const ok = await this.loadFbxModel(url, label, i, urls.length);
                if (ok) loadedCount += 1;
            } catch (err) {
                console.warn(`${this.logPrefix} failed to load ${label}:`, err);
                try {
                    if (this.postLog) {
                        this.postLog('error', `${this.logPrefix} failed to load ${label}`, {
                            url,
                            error: String(err && (err.message || err)),
                            stack: err && err.stack ? String(err.stack) : null,
                        });
                    }
                } catch {
                    // ignore log transport failures
                }
            }
        }

        const success = loadedCount > 0;
        if (success) {
            this.setStatus(`Ready (FBX fallback: ${loadedCount}/${urls.length})`);
            console.log(`${this.logPrefix} FBX fallback loaded ${loadedCount}/${urls.length}`);
        } else {
            this.setStatus('FBX fallback failed');
        }
        return success;
    }
}

