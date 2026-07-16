const ACTION_CONTRACTS = Object.freeze({
    idle_neutral: Object.freeze({ generationMode: 'loop', frameCount: 97 }),
    idle_alert: Object.freeze({ generationMode: 'loop', frameCount: 97 }),
    idle_relaxed: Object.freeze({ generationMode: 'loop', frameCount: 97 }),
    idle_look_around: Object.freeze({ generationMode: 'loop', frameCount: 97 }),
    idle_fidget: Object.freeze({ generationMode: 'loop', frameCount: 97 }),
    walk_forward: Object.freeze({ generationMode: 'loop', frameCount: 49 }),
    walk_backward: Object.freeze({ generationMode: 'loop', frameCount: 49 }),
    trot_jog: Object.freeze({ generationMode: 'loop', frameCount: 49 }),
    run: Object.freeze({ generationMode: 'loop', frameCount: 49 }),
    sprint: Object.freeze({ generationMode: 'loop', frameCount: 49 }),
    turn_left_90: Object.freeze({ generationMode: 'one_shot', frameCount: 33 }),
    turn_right_90: Object.freeze({ generationMode: 'one_shot', frameCount: 33 }),
    turn_around_180: Object.freeze({ generationMode: 'one_shot', frameCount: 33 }),
    stop_brake: Object.freeze({ generationMode: 'one_shot', frameCount: 33 }),
    jump_air: Object.freeze({ generationMode: 'loop', frameCount: 49 }),
    fall: Object.freeze({ generationMode: 'one_shot', frameCount: 49 }),
    jump_start: Object.freeze({ generationMode: 'one_shot', frameCount: 33 }),
    jump_land: Object.freeze({ generationMode: 'one_shot', frameCount: 33 }),
    jump_full: Object.freeze({ generationMode: 'one_shot', frameCount: 49 }),
    attack_primary: Object.freeze({ generationMode: 'one_shot', frameCount: 49 }),
    attack_secondary: Object.freeze({ generationMode: 'one_shot', frameCount: 49 }),
    attack_heavy: Object.freeze({ generationMode: 'one_shot', frameCount: 49 }),
    hit_front: Object.freeze({ generationMode: 'one_shot', frameCount: 33 }),
    hit_left: Object.freeze({ generationMode: 'one_shot', frameCount: 33 }),
    hit_right: Object.freeze({ generationMode: 'one_shot', frameCount: 33 }),
    death: Object.freeze({ generationMode: 'one_shot', frameCount: 65 }),
    get_up: Object.freeze({ generationMode: 'one_shot', frameCount: 65 }),
    eat_interact: Object.freeze({ generationMode: 'loop', frameCount: 97 }),
    sleep_rest: Object.freeze({ generationMode: 'loop', frameCount: 97 }),
    vocalize_emote: Object.freeze({ generationMode: 'one_shot', frameCount: 33 }),
});

const ACTION_ALIASES = Object.freeze({
    idle: 'idle_neutral',
    walk: 'walk_forward',
    jog: 'trot_jog',
    trot: 'trot_jog',
    die: 'death',
});

function normalizedToken(value) {
    return String(value || '')
        .trim()
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, '_')
        .replace(/^_+|_+$/g, '');
}

function optionalPositiveInteger(value, field) {
    if (value == null || value === '') return null;
    const number = Number(value);
    if (!Number.isInteger(number) || number <= 0 || (number - 1) % 8 !== 0) {
        throw new Error(`${field} must be a positive 8n+1 frame count`);
    }
    return number;
}

function optionalGenerationMode(value) {
    if (value == null || value === '') return '';
    const mode = normalizedToken(value);
    if (mode !== 'loop' && mode !== 'one_shot') {
        throw new Error('generationMode must be loop or one_shot');
    }
    return mode;
}

/** Resolve the immutable browser copy of animation-fitting action contracts. */
export function resolveAnimationFittingAction(value, overrides = {}) {
    const source = value && typeof value === 'object' ? value : { actionId: value };
    const requested = normalizedToken(
        source.actionId
        || source.action_id_string
        || source.variantName
        || source.variant_name_string,
    );
    const actionId = ACTION_ALIASES[requested] || requested;
    const contract = ACTION_CONTRACTS[actionId];
    if (!contract) throw new Error(`Unknown animation-fitting action: ${requested || '(empty)'}`);

    const suppliedFrameCount = optionalPositiveInteger(
        overrides.frameCount
        ?? overrides.frame_count_int
        ?? source.frameCount
        ?? source.frame_count_int,
        'frameCount',
    );
    if (suppliedFrameCount != null && suppliedFrameCount !== contract.frameCount) {
        throw new Error(
            `Animation-fitting action ${actionId} requires ${contract.frameCount} frames, got ${suppliedFrameCount}`,
        );
    }

    let suppliedMode = optionalGenerationMode(
        overrides.generationMode
        ?? overrides.generation_mode_string
        ?? source.generationMode
        ?? source.generation_mode_string,
    );
    const suppliedLoop = overrides.loop ?? source.loop;
    if (suppliedLoop != null) {
        const loopMode = suppliedLoop === false ? 'one_shot' : 'loop';
        if (suppliedMode && suppliedMode !== loopMode) {
            throw new Error('loop and generationMode disagree');
        }
        suppliedMode = loopMode;
    }
    if (suppliedMode && suppliedMode !== contract.generationMode) {
        throw new Error(
            `Animation-fitting action ${actionId} requires ${contract.generationMode}, got ${suppliedMode}`,
        );
    }

    return Object.freeze({
        actionId,
        generationMode: contract.generationMode,
        frameCount: contract.frameCount,
        isLoop: contract.generationMode === 'loop',
        isOneShot: contract.generationMode === 'one_shot',
        requestedId: requested,
        aliasApplied: requested !== actionId,
    });
}

export const ANIMATION_FITTING_ACTION_CONTRACTS = ACTION_CONTRACTS;
export const ANIMATION_FITTING_ACTION_ALIASES = ACTION_ALIASES;
