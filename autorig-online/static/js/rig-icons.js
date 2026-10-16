/**
 * Rig type → static Icons_png asset (RGBA PNGs).
 * Humanoid uses Human.png; animals use TitleCase filenames (Dog.png, Bear.png, …).
 */
(function () {
    const ICONS_BASE = '/static/Icons_png';
    /** @type {readonly string[]} */
    const RIG_ICON_ANIMAL_KEYS = [
        'dog', 'bear', 'cat', 'cow', 'deer', 'elephant', 'giraffe',
        'horse', 'mouse', 'pig', 'rabbit', 'turtle',
        // V3 rig categories (2026-10-16): Bird.png, Serpent.png, Monster.png, Multilimb.png, Hands.png, Vehicle.png, Other.png
        'bird', 'serpent', 'monster', 'multilimb', 'hands', 'vehicle', 'other',
    ];
    const animalSet = new Set(RIG_ICON_ANIMAL_KEYS);

    /**
     * @param {string} [rigKey]
     * @returns {string}
     */
    function resolveRigIconUrl(rigKey) {
        const k = String(rigKey || 'humanoid').trim().toLowerCase();
        if (k === 'humanoid') {
            return `${ICONS_BASE}/Human.png?v=rigicons1`;
        }
        if (animalSet.has(k)) {
            const file = k.charAt(0).toUpperCase() + k.slice(1);
            return `${ICONS_BASE}/${file}.png?v=rigicons1`;
        }
        return `${ICONS_BASE}/Human.png?v=rigicons1`;
    }

    window.RIG_ICON_ANIMAL_KEYS = RIG_ICON_ANIMAL_KEYS;
    window.resolveRigIconUrl = resolveRigIconUrl;
})();
