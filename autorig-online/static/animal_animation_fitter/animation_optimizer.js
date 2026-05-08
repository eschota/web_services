import { SimilarityScorer } from './similarity_scorer.js';

function clonePoseStates(ps) {
    return JSON.parse(JSON.stringify(ps));
}

/**
 * Stochastic coordinate descent (MVP) — no full brute force.
 */
export class AnimationOptimizer {
    constructor({
        boneParameterModel,
        modelFrameRenderer,
        similarityScorer,
        targetFrames,
        quality_level_string = 'fast',
        max_compute_time_seconds_float = 30,
        target_convergence_percent_float = 90,
        output_fps_int = 24,
        onProgress = null,
    }) {
        this.boneModel = boneParameterModel;
        this.renderer = modelFrameRenderer;
        this.scorer = similarityScorer;
        this.targetFrames = targetFrames;
        this.quality = quality_level_string;
        this.maxTime = max_compute_time_seconds_float * 1000;
        this.targetConv = target_convergence_percent_float;
        this.fps = output_fps_int;
        this.onProgress = onProgress;
        this._stop = false;
    }

    stop() {
        this._stop = true;
    }

    _qualityConfig() {
        if (this.quality === 'high') {
            return { compare: 256, mut: 0.04, batchIters: 8 };
        }
        if (this.quality === 'balanced') {
            return { compare: 192, mut: 0.06, batchIters: 12 };
        }
        return { compare: 128, mut: 0.08, batchIters: 16 };
    }

    _emptyPoseState(frameIndex, time) {
        const bones = {};
        for (const p of this.boneModel.getActiveBoneParameters()) {
            bones[p.bone_name_string] = {
                rotation_x_float: 0,
                rotation_y_float: 0,
                rotation_z_float: 0,
            };
        }
        return { frame_index_int: frameIndex, time_seconds_float: time, bones };
    }

    _imageDataFromTarget(t) {
        return new ImageData(
            new Uint8ClampedArray(t.image_data_uint8),
            t.width,
            t.height
        );
    }

    _scorePoses(poseStates) {
        const { compare: cw } = this._qualityConfig();
        const ch = cw;
        const rendered = [];
        for (let i = 0; i < poseStates.length; i++) {
            const img = this.renderer.renderPoseToImageData(poseStates[i], cw, ch);
            rendered.push(img);
        }
        const targets = this.targetFrames.map((t) => this._imageDataFromTarget(t));
        return this.scorer.scoreAnimation(rendered, targets);
    }

    async runFitting() {
        const t0 = performance.now();
        const n = this.targetFrames.length;
        const dt = 1 / this.fps;
        const { mut, batchIters } = this._qualityConfig();

        const poseStates = [];
        for (let i = 0; i < n; i++) {
            const t = this.targetFrames[i].time_seconds_float;
            poseStates.push(this._emptyPoseState(i, t));
        }

        // Procedural seed: tiny breathing on first spine-like bone
        const active = this.boneModel.getActiveBoneParameters();
        const spine = active.find((b) => /spine|chest|body/i.test(b.bone_name_string)) || active[0];
        if (spine) {
            for (let i = 0; i < n; i++) {
                const w = Math.sin((i / n) * Math.PI * 2) * 0.04;
                const b = poseStates[i].bones[spine.bone_name_string];
                if (b) b.rotation_x_float = w;
            }
        }

        this.scorer.compare_width_int = this._qualityConfig().compare;
        this.scorer.compare_height_int = this._qualityConfig().compare;

        let best = { pose_states_array: clonePoseStates(poseStates), score_float: -1, convergence_percent_float: 0 };
        let currentScore = this._scorePoses(poseStates);
        best.score_float = currentScore.score_float;
        best.convergence_percent_float = currentScore.convergence_percent_float;
        best.pose_states_array = clonePoseStates(poseStates);

        let iter = 0;
        while (!this._stop && performance.now() - t0 < this.maxTime) {
            if (currentScore.convergence_percent_float >= this.targetConv) break;

            const bonePick = active[Math.floor(Math.random() * active.length)];
            const fi = Math.floor(Math.random() * n);
            const axis = ['rotation_x_float', 'rotation_y_float', 'rotation_z_float'][Math.floor(Math.random() * 3)];
            const delta = (Math.random() - 0.5) * 2 * mut;

            const trial = clonePoseStates(poseStates);
            const entry = trial[fi].bones[bonePick.bone_name_string];
            if (!entry) continue;
            entry[axis] = (entry[axis] || 0) + delta;

            const sc = this._scorePoses(trial);
            iter++;
            if (sc.score_float >= currentScore.score_float) {
                for (let k = 0; k < poseStates.length; k++) {
                    poseStates[k] = trial[k];
                }
                currentScore = sc;
                if (sc.score_float > best.score_float) {
                    best = {
                        pose_states_array: clonePoseStates(poseStates),
                        score_float: sc.score_float,
                        convergence_percent_float: sc.convergence_percent_float,
                    };
                }
            }

            if (iter % batchIters === 0) {
                await new Promise((r) => requestAnimationFrame(() => r()));
                this.onProgress?.({
                    iterations_int: iter,
                    convergence_percent_float: currentScore.convergence_percent_float,
                    elapsed_ms_float: performance.now() - t0,
                    stage_string: 'coordinate_descent',
                });
            }
        }

        return {
            pose_states_array: best.pose_states_array,
            score_float: best.score_float,
            convergence_percent_float: best.convergence_percent_float,
            metadata: {
                stopped_early_bool: this._stop,
                time_exceeded_bool: performance.now() - t0 >= this.maxTime,
                iterations_int: iter,
            },
        };
    }
}
