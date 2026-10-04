(() => {
    const $ = (id) => document.getElementById(id);
    const revealNodes = Array.from(document.querySelectorAll(".reveal"));

    const input = $("scene-file-input");
    const pickBtn = $("upload-pick-btn");
    const heroPickBtn = $("hero-upload-trigger");
    const fileMeta = $("upload-file-meta");
    const statusEl = $("upload-status");
    const fillEl = $("upload-progress-fill");
    const percentEl = $("upload-progress-percent");
    const speedEl = $("upload-progress-speed");
    const etaEl = $("upload-progress-eta");
    const chunksEl = $("upload-progress-chunks");

    let selectedFile = null;
    let isUploading = false;
    let uploadStartedAt = 0;
    let completedBytes = 0;
    let activeChunkBytes = {};

    const CHUNK_SIZE = 12 * 1024 * 1024;
    const MAX_PARALLEL = 3;
    const MAX_RETRIES = 5;

    const formatBytes = (bytes) => {
        if (bytes >= 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`;
        if (bytes >= 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
        if (bytes >= 1024) return `${(bytes / 1024).toFixed(1)} KB`;
        return `${bytes} B`;
    };

    const formatEta = (seconds) => {
        if (!Number.isFinite(seconds) || seconds < 0) return "Calculating";
        if (seconds < 60) return `${Math.round(seconds)}s left`;
        const m = Math.floor(seconds / 60);
        const s = Math.round(seconds % 60);
        return `${m}m ${s}s left`;
    };

    const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

    function setStatus(text, isError = false) {
        statusEl.textContent = text;
        statusEl.style.color = isError ? "var(--danger)" : "var(--primary-strong)";
    }

    function updateProgress(totalBytes, totalChunks, doneChunks) {
        const activeBytes = Object.values(activeChunkBytes).reduce((sum, value) => sum + (Number(value) || 0), 0);
        const uploadedBytes = Math.min(totalBytes, completedBytes + activeBytes);
        const pct = Math.min(100, Math.round((uploadedBytes / totalBytes) * 100));
        fillEl.style.width = `${pct}%`;
        percentEl.textContent = `${pct}%`;

        const elapsedSec = Math.max(1, (Date.now() - uploadStartedAt) / 1000);
        const speedBps = uploadedBytes / elapsedSec;
        speedEl.textContent = `${(speedBps / (1024 * 1024)).toFixed(2)} MB/s`;
        etaEl.textContent = formatEta((totalBytes - uploadedBytes) / Math.max(speedBps, 1));
        chunksEl.textContent = `${doneChunks} / ${totalChunks} chunks • ${formatBytes(uploadedBytes)} / ${formatBytes(totalBytes)}`;
    }

    async function fetchJson(url, options, label) {
        const response = await fetch(url, options);
        let payload = null;
        try {
            payload = await response.json();
        } catch (_) {
            payload = null;
        }
        if (!response.ok) {
            const detail = payload && payload.detail ? payload.detail : `${label} failed (${response.status})`;
            throw new Error(detail);
        }
        return payload;
    }

    function uploadChunk(uploadId, index, blob, doneChunks, totalBytes, totalChunks) {
        return new Promise((resolve, reject) => {
            const xhr = new XMLHttpRequest();
            xhr.open("POST", "/api/upload/chunk");
            xhr.timeout = 300000;

            const formData = new FormData();
            formData.append("upload_id", uploadId);
            formData.append("chunk_index", String(index));
            formData.append("file", blob, `part_${index}`);

            xhr.onload = () => {
                if (xhr.status >= 200 && xhr.status < 300) {
                    resolve();
                } else {
                    reject(new Error(`Chunk ${index + 1} failed (${xhr.status})`));
                }
            };

            xhr.onerror = () => reject(new Error(`Chunk ${index + 1} network error`));
            xhr.ontimeout = () => reject(new Error(`Chunk ${index + 1} timeout`));

            xhr.upload.onprogress = (event) => {
                if (!event.lengthComputable) return;
                activeChunkBytes[index] = Math.max(0, Math.min(event.loaded, blob.size));
                updateProgress(totalBytes, totalChunks, doneChunks);
            };

            xhr.send(formData);
        });
    }

    async function uploadChunkWithRetry(uploadId, index, chunkBlob, totalBytes, totalChunks, doneChunksRef) {
        let attempt = 0;
        while (attempt < MAX_RETRIES) {
            try {
                await uploadChunk(uploadId, index, chunkBlob, doneChunksRef.value, totalBytes, totalChunks);
                return;
            } catch (error) {
                attempt += 1;
                delete activeChunkBytes[index];
                updateProgress(totalBytes, totalChunks, doneChunksRef.value);
                if (attempt >= MAX_RETRIES) throw error;
                const backoff = Math.min(1000 * Math.pow(2, attempt - 1), 12000);
                setStatus(`Retry chunk ${index + 1}/${totalChunks} (attempt ${attempt + 1})`);
                await sleep(backoff);
            }
        }
    }

    async function runUploadFlow(file) {
        const totalBytes = file.size;
        const totalChunks = Math.ceil(totalBytes / CHUNK_SIZE);
        const doneChunksRef = { value: 0 };
        completedBytes = 0;
        activeChunkBytes = {};
        uploadStartedAt = Date.now();

        setStatus("Initializing upload...");
        updateProgress(totalBytes, totalChunks, 0);

        const initData = new FormData();
        initData.append("filename", file.name);
        initData.append("total_size", String(totalBytes));
        initData.append("chunk_size", String(CHUNK_SIZE));
        initData.append("total_chunks", String(totalChunks));
        const initPayload = await fetchJson("/api/upload/init", { method: "POST", body: initData }, "Upload init");
        const uploadId = initPayload.upload_id;
        if (!uploadId) throw new Error("Upload ID was not returned by backend");

        const pending = Array.from({ length: totalChunks }, (_, i) => i);
        const pool = new Set();

        const schedule = async (chunkIndex) => {
            const start = chunkIndex * CHUNK_SIZE;
            const end = Math.min(start + CHUNK_SIZE, totalBytes);
            const blob = file.slice(start, end);

            await uploadChunkWithRetry(uploadId, chunkIndex, blob, totalBytes, totalChunks, doneChunksRef);
            delete activeChunkBytes[chunkIndex];
            completedBytes += blob.size;
            doneChunksRef.value += 1;
            updateProgress(totalBytes, totalChunks, doneChunksRef.value);
            setStatus(`Uploading chunk ${doneChunksRef.value}/${totalChunks}...`);
        };

        for (const idx of pending) {
            while (pool.size >= MAX_PARALLEL) {
                await Promise.race(pool);
            }
            const task = schedule(idx).finally(() => pool.delete(task));
            pool.add(task);
        }
        await Promise.all(pool);

        setStatus("Finalizing and creating task...");
        const completeData = new FormData();
        completeData.append("upload_id", uploadId);
        completeData.append("type", "t_pose");
        const completePayload = await fetchJson("/api/upload/complete", { method: "POST", body: completeData }, "Upload complete");
        if (!completePayload || !completePayload.task_id) {
            throw new Error("Task ID is missing after upload completion");
        }
        window.location.href = `/task?id=${completePayload.task_id}`;
    }

    function onPickFile() {
        input.click();
    }

    function onFileSelected(event) {
        const file = event.target.files && event.target.files[0];
        if (!file) return;
        const fileNameLower = file.name.toLowerCase();
        if (!fileNameLower.endsWith(".zip")) {
            selectedFile = null;
            fileMeta.textContent = "Please choose a .zip package";
            setStatus("Invalid file type", true);
            return;
        }
        selectedFile = file;
        fileMeta.textContent = `${file.name} • ${formatBytes(file.size)}`;
        onStartUpload();
    }

    async function onStartUpload() {
        if (!selectedFile || isUploading) return;
        isUploading = true;
        pickBtn.disabled = true;
        heroPickBtn.disabled = true;
        setStatus("Starting upload...");
        try {
            await runUploadFlow(selectedFile);
        } catch (error) {
            setStatus(error.message || "Upload failed", true);
            isUploading = false;
            pickBtn.disabled = false;
            heroPickBtn.disabled = false;
        }
    }

    function setupReveal() {
        const observer = new IntersectionObserver((entries) => {
            entries.forEach((entry) => {
                if (entry.isIntersecting) {
                    entry.target.classList.add("is-visible");
                    observer.unobserve(entry.target);
                }
            });
        }, { threshold: 0.2 });

        revealNodes.forEach((node) => observer.observe(node));
    }

    pickBtn.addEventListener("click", onPickFile);
    heroPickBtn.addEventListener("click", () => {
        document.getElementById("upload-block").scrollIntoView({ behavior: "smooth", block: "center" });
        input.click();
    });
    input.addEventListener("change", onFileSelected);

    setupReveal();
})();
