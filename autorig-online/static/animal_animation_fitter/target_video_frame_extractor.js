/**
 * Extract evenly spaced frames using HTMLVideoElement (WebCodecs optional later).
 */

export class TargetVideoFrameExtractor {
    /**
     * @param {string} video_url_string
     * @param {number} frame_sample_count_int
     * @param {number} target_width_int
     * @param {number} target_height_int
     * @returns {Promise<Array<{ frame_index_int:number, time_seconds_float:number, image_data_uint8:Uint8ClampedArray, width:number, height:number }>>}
     */
    static async extractFrames(
        video_url_string,
        frame_sample_count_int,
        target_width_int,
        target_height_int,
        progressCb = null
    ) {
        const video = document.createElement('video');
        video.crossOrigin = 'anonymous';
        video.muted = true;
        video.playsInline = true;
        video.src = video_url_string;

        await new Promise((resolve, reject) => {
            video.onloadedmetadata = () => resolve(null);
            video.onerror = () => reject(new Error('Video load failed (CORS or invalid URL). Use same-origin proxy.'));
        });

        const duration = Number(video.duration) || 0;
        const n = Math.max(2, Math.min(128, frame_sample_count_int | 0));
        const canvas = document.createElement('canvas');
        canvas.width = target_width_int;
        canvas.height = target_height_int;
        const ctx = canvas.getContext('2d', { willReadFrequently: true });

        const frames = [];
        for (let i = 0; i < n; i++) {
            const t = duration > 0 ? (i / n) * duration : 0;
            video.currentTime = t;
            await new Promise((resolve) => {
                const onSeek = () => {
                    video.removeEventListener('seeked', onSeek);
                    resolve(null);
                };
                video.addEventListener('seeked', onSeek);
            });
            ctx.drawImage(video, 0, 0, target_width_int, target_height_int);
            const img = ctx.getImageData(0, 0, target_width_int, target_height_int);
            frames.push({
                frame_index_int: i,
                time_seconds_float: t,
                image_data_uint8: img.data,
                width: target_width_int,
                height: target_height_int,
            });
            progressCb?.({ frame: i + 1, total: n });
        }
        video.removeAttribute('src');
        video.load();
        return frames;
    }
}
