/**
 * Browser-side idle fitting orchestrator (MVP).
 */

import { BoneParameterModel } from './bone_parameter_model.js';
import { ModelFrameRenderer } from './model_frame_renderer.js';
import { TargetVideoFrameExtractor } from './target_video_frame_extractor.js';
import { SimilarityScorer } from './similarity_scorer.js';
import { AnimationOptimizer } from './animation_optimizer.js';
import { exportAnimationJson, buildThreeAnimationClip } from './animation_clip_exporter.js';

function findSkinnedMesh(root) {
    let sm = null;
    root?.traverse?.((o) => {
        if (o.isSkinnedMesh && !sm) sm = o;
    });
    return sm;
}

export class AnimalAnimationFitter {
    constructor(viewerCtx, skinnedMesh, options = {}) {
        this.viewerCtx = viewerCtx;
        this.skinnedMesh = skinnedMesh;
        this.options = {
            target_convergence_percent_float: 90,
            max_compute_time_seconds_float: 120,
            quality_level_string: 'balanced',
            frame_sample_count_int: 32,
            output_fps_int: 24,
            enable_webgpu_bool: false,
            enable_debug_overlay_bool: true,
            ...options,
        };
        this._videoUrl = '';
        this._targetFrames = [];
        this._boneModel = null;
        this._frameRenderer = null;
        this._scorer = null;
        this._optimizer = null;
        this._best = null;
        this._clip = null;
        this._mixer = null;
        this._action = null;
    }

    async loadTargetVideo(video_url_string) {
        this._videoUrl = video_url_string;
        console.log('[AnimalAnimationFitter] loadTargetVideo', video_url_string);
    }

    async prepare() {
        const sm = this.skinnedMesh || findSkinnedMesh(this.viewerCtx.model);
        if (!sm?.isSkinnedMesh) {
            throw new Error('No SkinnedMesh found for fitting');
        }
        this.skinnedMesh = sm;
        const q = this.options.quality_level_string;
        this._boneModel = new BoneParameterModel(sm, { quality_level_string: q });
        this._frameRenderer = new ModelFrameRenderer(this.viewerCtx.THREE, {
            scene: this.viewerCtx.scene,
            camera: this.viewerCtx.camera,
            renderer: this.viewerCtx.renderer,
            skinnedMesh: sm,
        });
        const cq = q === 'high' ? 384 : q === 'balanced' ? 256 : 192;
        this._scorer = new SimilarityScorer({ compare_width_int: cq, compare_height_int: cq });
        console.log('[AnimalAnimationFitter] prepare ok; bones:', sm.skeleton.bones.length);
    }

    async runFitting(onProgress) {
        if (!this._videoUrl) throw new Error('No video URL');
        await this.prepare();
        const n = Math.min(
            64,
            Math.max(8, this.options.frame_sample_count_int | 0)
        );
        const cw = this._scorer.compare_width_int || 192;
        this._targetFrames = await TargetVideoFrameExtractor.extractFrames(
            this._videoUrl,
            n,
            cw,
            cw,
            (p) => onProgress?.({ stage_string: 'extract', ...p })
        );

        this._optimizer = new AnimationOptimizer({
            boneParameterModel: this._boneModel,
            modelFrameRenderer: this._frameRenderer,
            similarityScorer: this._scorer,
            targetFrames: this._targetFrames,
            quality_level_string: this.options.quality_level_string,
            max_compute_time_seconds_float: this.options.max_compute_time_seconds_float,
            target_convergence_percent_float: this.options.target_convergence_percent_float,
            output_fps_int: this.options.output_fps_int,
            onProgress,
        });

        const result = await this._optimizer.runFitting();
        this._best = result;
        console.log('[AnimalAnimationFitter] fitting done', result.metadata);
        return result;
    }

    stop() {
        this._optimizer?.stop();
    }

    getProgress() {
        return this._best;
    }

    exportAnimationClip() {
        if (!this._best?.pose_states_array) return null;
        return buildThreeAnimationClip(
            this._best.pose_states_array,
            this.skinnedMesh,
            'auto_fitted_idle'
        );
    }

    exportAnimationJson(extra = {}) {
        if (!this._best?.pose_states_array) return null;
        return exportAnimationJson(this._best.pose_states_array, this.skinnedMesh, {
            fps_int: this.options.output_fps_int,
            source_video_url_string: this._videoUrl,
            convergence_percent_float: this._best.convergence_percent_float,
            ...extra,
        });
    }

    applyBestAnimationToViewer() {
        const THREE = this.viewerCtx.THREE;
        const clip = this.exportAnimationClip();
        if (!clip) {
            console.warn('[AnimalAnimationFitter] no clip to apply');
            return;
        }
        const sm = this.skinnedMesh;
        if (this._mixer) {
            this._mixer.stopAllAction();
        }
        this._mixer = new THREE.AnimationMixer(sm);
        this._action = this._mixer.clipAction(clip);
        this._action.setLoop(THREE.LoopRepeat, Infinity);
        this._action.play();

        const asv = this.viewerCtx.animalStandaloneViewer;
        if (asv) {
            asv.fitterMixer = this._mixer;
        }
        console.log('[AnimalAnimationFitter] AnimationMixer playing (hooked to viewer loop)');
    }
}
