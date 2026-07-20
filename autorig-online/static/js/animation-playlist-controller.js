const DEFAULT_SHORT_CLIP_SECONDS = 0.1;

function normalizeKey(value) {
    return String(value || '')
        .trim()
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, '_')
        .replace(/_+/g, '_')
        .replace(/^_+|_+$/g, '');
}

function manifestClips(manifest) {
    return Array.isArray(manifest?.clips) ? manifest.clips : [];
}

function explicitClipKeys(definition) {
    return [
        definition?.id,
        definition?.name,
        definition?.clip_name,
        ...(Array.isArray(definition?.legacy_aliases) ? definition.legacy_aliases : []),
    ].map(normalizeKey).filter(Boolean);
}

export function buildAnimationPlaylist(clips, manifest = null, options = {}) {
    const embedded = Array.isArray(clips) ? clips.filter(Boolean) : [];
    const shortClipSeconds = Number.isFinite(options.shortClipSeconds)
        ? Math.max(0, options.shortClipSeconds)
        : DEFAULT_SHORT_CLIP_SECONDS;
    const definitions = manifestClips(manifest);
    const definitionByKey = new Map();

    definitions.forEach((definition, definitionIndex) => {
        explicitClipKeys(definition).forEach((key) => {
            if (!definitionByKey.has(key)) {
                definitionByKey.set(key, { definition, definitionIndex });
            }
        });
    });

    const entries = embedded.map((clip, embeddedIndex) => {
        const match = definitionByKey.get(normalizeKey(clip?.name));
        const definition = match?.definition || null;
        const duration = Number.isFinite(Number(clip?.duration))
            ? Math.max(0, Number(clip.duration))
            : Math.max(0, Number(definition?.duration) || 0);
        const order = Number.isFinite(Number(definition?.order))
            ? Number(definition.order)
            : (match ? match.definitionIndex : definitions.length + embeddedIndex);
        const endPoseId = String(definition?.end_pose_id || '');

        return {
            id: String(definition?.id || clip?.name || `clip_${embeddedIndex}`),
            name: String(clip?.name || definition?.id || `clip_${embeddedIndex}`),
            clip,
            definition,
            embeddedIndex,
            order,
            duration,
            loop: definition ? definition.loop === true : true,
            terminal: definition?.terminal === true || endPoseId === 'death_end',
            autoplay: definition?.autoplay !== false
                && definition?.pose !== true
                && duration > shortClipSeconds,
        };
    });

    return entries.sort((left, right) => (
        left.order - right.order || left.embeddedIndex - right.embeddedIndex
    ));
}

export function captureRootTransform(root) {
    if (!root) return null;
    return {
        position: root.position?.clone?.() || null,
        quaternion: root.quaternion?.clone?.() || null,
        scale: root.scale?.clone?.() || null,
    };
}

export function restoreRootTransform(root, snapshot) {
    if (!root || !snapshot) return false;
    if (snapshot.position && root.position?.copy) root.position.copy(snapshot.position);
    if (snapshot.quaternion && root.quaternion?.copy) root.quaternion.copy(snapshot.quaternion);
    if (snapshot.scale && root.scale?.copy) root.scale.copy(snapshot.scale);
    root.updateMatrixWorld?.(true);
    return true;
}

export class AnimationPlaylistController {
    constructor(options = {}) {
        this.playClip = options.playClip || (() => null);
        this.restoreRoot = options.restoreRoot || (() => {});
        this.onClipChange = options.onClipChange || (() => {});
        this.onStateChange = options.onStateChange || (() => {});
        this.shortClipSeconds = Number.isFinite(options.shortClipSeconds)
            ? options.shortClipSeconds
            : DEFAULT_SHORT_CLIP_SECONDS;

        this.mixer = null;
        this.entries = [];
        this.autoplayEntries = [];
        this.currentEntry = null;
        this.currentAction = null;
        this.currentAutoplayIndex = -1;
        this.autoplay = false;
        this.manualMode = false;
        this.suspended = false;

        this._handleMixerFinished = this._handleMixerFinished.bind(this);
    }

    configure({ mixer = null, clips = [], manifest = null } = {}) {
        if (this.mixer?.removeEventListener) {
            this.mixer.removeEventListener('finished', this._handleMixerFinished);
        }

        this.mixer = mixer;
        this.entries = buildAnimationPlaylist(clips, manifest, {
            shortClipSeconds: this.shortClipSeconds,
        });
        this.autoplayEntries = this.entries.filter((entry) => entry.autoplay);
        this.currentEntry = null;
        this.currentAction = null;
        this.currentAutoplayIndex = -1;
        this.autoplay = false;
        this.suspended = false;

        if (this.mixer?.addEventListener) {
            this.mixer.addEventListener('finished', this._handleMixerFinished);
        }
        this._emitState();
        return this.entries;
    }

    destroy() {
        if (this.mixer?.removeEventListener) {
            this.mixer.removeEventListener('finished', this._handleMixerFinished);
        }
        this.mixer = null;
        this.entries = [];
        this.autoplayEntries = [];
        this.currentEntry = null;
        this.currentAction = null;
        this.autoplay = false;
        this.suspended = false;
    }

    startAutoplay(startName = null) {
        if (this.manualMode || !this.autoplayEntries.length) return false;
        const requestedIndex = startName
            ? this.autoplayEntries.findIndex((entry) => entry.name === startName || entry.id === startName)
            : -1;
        this.autoplay = true;
        this.suspended = false;
        this.currentAutoplayIndex = requestedIndex >= 0 ? requestedIndex : 0;
        this._playEntry(this.autoplayEntries[this.currentAutoplayIndex], 'autoplay');
        return true;
    }

    markInteraction(reason = 'user') {
        if (!this.manualMode) {
            this.manualMode = true;
            this.autoplay = false;
            this._emitState(reason);
        }
    }

    manualPlay(name) {
        this.markInteraction('manual-play');
        const entry = this.entries.find((candidate) => candidate.name === name || candidate.id === name);
        if (!entry) return false;
        this._playEntry(entry, 'manual');
        return true;
    }

    manualPause() {
        this.markInteraction('manual-pause');
        if (this.mixer) this.mixer.timeScale = 0;
        this._emitState('manual-pause');
    }

    suspend(reason = 'visibility-hidden') {
        if (this.suspended) return false;
        this.suspended = true;
        if (this.mixer) this.mixer.timeScale = 0;
        this._emitState(reason);
        return true;
    }

    resume(reason = 'visibility-visible') {
        if (!this.suspended) return false;
        this.suspended = false;
        if (this.mixer && this.autoplay && !this.manualMode) {
            this.mixer.timeScale = 1;
        }
        this._emitState(reason);
        return true;
    }

    _playEntry(entry, mode) {
        if (!entry) return false;
        const previous = this.currentEntry;
        const isAutoplay = mode === 'autoplay';
        const loopOnce = isAutoplay || !entry.loop;
        const terminalTransition = previous?.terminal || entry.terminal;

        this.restoreRoot({ previous, next: entry, mode });
        this.currentEntry = entry;
        this.currentAction = this.playClip(entry.name, {
            fade: terminalTransition ? 0 : 0.2,
            restart: true,
            loopOnce,
            clampWhenFinished: !isAutoplay && loopOnce && entry.terminal,
            source: mode,
        }) || null;
        if (this.mixer) this.mixer.timeScale = 1;
        this.onClipChange(entry, {
            mode,
            index: isAutoplay ? this.currentAutoplayIndex : this.entries.indexOf(entry),
            total: isAutoplay ? this.autoplayEntries.length : this.entries.length,
        });
        this._emitState(mode);
        return true;
    }

    _handleMixerFinished(event = {}) {
        if (!this.autoplay || this.manualMode || this.suspended || !this.autoplayEntries.length) return;
        if (this.currentAction && event.action && event.action !== this.currentAction) return;
        this.currentAutoplayIndex = (this.currentAutoplayIndex + 1) % this.autoplayEntries.length;
        this._playEntry(this.autoplayEntries[this.currentAutoplayIndex], 'autoplay');
    }

    _emitState(reason = 'configure') {
        this.onStateChange({
            autoplay: this.autoplay,
            manualMode: this.manualMode,
            suspended: this.suspended,
            currentId: this.currentEntry?.id || null,
            currentName: this.currentEntry?.name || null,
            currentAutoplayIndex: this.currentAutoplayIndex,
            total: this.entries.length,
            autoplayTotal: this.autoplayEntries.length,
            reason,
        });
    }
}
