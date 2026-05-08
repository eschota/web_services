/**
 * MVP similarity: downscaled grayscale MAE + simple silhouette overlap proxy.
 */

export function downsampleGray(imageData, outW, outH) {
    const { data, width, height } = imageData;
    const out = new Uint8Array(outW * outH);
    const xStep = width / outW;
    const yStep = height / outH;
    let k = 0;
    for (let y = 0; y < outH; y++) {
        for (let x = 0; x < outW; x++) {
            const sx = Math.min(width - 1, (x + 0.5) * xStep) | 0;
            const sy = Math.min(height - 1, (y + 0.5) * yStep) | 0;
            const i = (sy * width + sx) * 4;
            const r = data[i];
            const g = data[i + 1];
            const b = data[i + 2];
            out[k++] = ((r * 0.299 + g * 0.587 + b * 0.114) | 0) & 255;
        }
    }
    return out;
}

export function silhouetteMask(gray, threshold = 40) {
    const n = gray.length;
    const mask = new Uint8Array(n);
    for (let i = 0; i < n; i++) {
        mask[i] = gray[i] > threshold ? 1 : 0;
    }
    return mask;
}

export function maskIoU(a, b) {
    let inter = 0;
    let union = 0;
    for (let i = 0; i < a.length; i++) {
        const ai = a[i];
        const bi = b[i];
        if (ai && bi) inter++;
        if (ai || bi) union++;
    }
    return union > 0 ? inter / union : 0;
}

export function maeGray(a, b) {
    let s = 0;
    const n = Math.min(a.length, b.length);
    for (let i = 0; i < n; i++) s += Math.abs(a[i] - b[i]);
    return n ? s / n / 255 : 0;
}

export class SimilarityScorer {
    constructor(options = {}) {
        this.compare_width_int = options.compare_width_int || 128;
        this.compare_height_int = options.compare_height_int || 128;
    }

    scoreFrame(renderedImageData, targetImageData) {
        const w = this.compare_width_int;
        const h = this.compare_height_int;
        const gR = downsampleGray(renderedImageData, w, h);
        const gT = downsampleGray(targetImageData, w, h);
        const mae = 1 - Math.min(1, maeGray(gR, gT) * 2);
        const mA = silhouetteMask(gR, 35);
        const mB = silhouetteMask(gT, 35);
        const iou = maskIoU(mA, mB);
        const silhouette_score = iou;
        const pixel_score = mae;
        const score = silhouette_score * 0.55 + pixel_score * 0.45;
        return {
            score_float: Math.max(0, Math.min(1, score)),
            silhouette_score_float: silhouette_score,
            pixel_score_float: pixel_score,
            convergence_percent_float: Math.max(0, Math.min(100, score * 100)),
        };
    }

    scoreAnimation(renderedFrames, targetFrames) {
        if (!renderedFrames?.length || renderedFrames.length !== targetFrames.length) {
            return { score_float: 0, convergence_percent_float: 0 };
        }
        let sum = 0;
        for (let i = 0; i < renderedFrames.length; i++) {
            sum += this.scoreFrame(renderedFrames[i], targetFrames[i]).score_float;
        }
        const avg = sum / renderedFrames.length;
        return {
            score_float: avg,
            convergence_percent_float: Math.max(0, Math.min(100, avg * 100)),
        };
    }
}
