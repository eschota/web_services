// Blender/glTF can represent a one-frame rest action as one frame of duration.
// Treat anything shorter than a tenth of a second as a pose, not a preview clip.
export const MOVING_CLIP_MIN_DURATION_SECONDS = 0.1;

export function animationPreviewCandidates(selected = {}) {
    const values = [
        selected.action_name,
        selected.name,
        selected.id,
        selected.file_name,
    ];
    const seen = new Set();
    return values
        .map((value) => String(value || '').trim())
        .filter((value) => {
            if (!value) return false;
            const key = value.toLowerCase();
            if (seen.has(key)) return false;
            seen.add(key);
            return true;
        });
}

export function preferredMovingClip(clips = []) {
    if (!Array.isArray(clips) || !clips.length) return null;
    return clips.find((clip) => Number(clip?.duration) > MOVING_CLIP_MIN_DURATION_SECONDS) || clips[0] || null;
}

export function shouldApplyCatalogPreview({ automatic = false } = {}) {
    return !automatic;
}

export function shouldLoadExternalFbxPreview({
    automatic = false,
    embeddedMatched = false,
    isAnimalTask = false,
    previewUrl = '',
} = {}) {
    return Boolean(
        String(previewUrl || '').trim()
        && !automatic
        && !embeddedMatched
        && !isAnimalTask
    );
}
