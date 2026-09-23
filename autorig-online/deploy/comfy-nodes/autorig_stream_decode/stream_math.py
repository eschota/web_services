"""Pure temporal-window math of AutorigStreamVideoSave (no ComfyUI imports,
so renderfin's unit tests can pin it against comfy.utils.tiled_scale_multidim)."""
import torch


def temporal_plan(latent_frames, tile_t, overlap_t, upscale, index):
    """Temporal windows exactly as comfy.utils.tiled_scale_multidim lays them out.

    Returns [(latent_start, latent_len, out_start, out_len)] and the feather
    width in output frames. `upscale(n)` maps a latent length to a frame count
    and `index * latent_start` is the window's first output frame.
    """
    if latent_frames <= tile_t:
        starts = [0]
    else:
        starts = list(range(0, latent_frames - overlap_t, tile_t - overlap_t))
    plan = []
    for pos in starts:
        pos = max(0, min(latent_frames - overlap_t, pos))
        length = min(tile_t, latent_frames - pos)
        plan.append((pos, length, round(index * pos), round(upscale(length))))
    feather = round(upscale(overlap_t))
    return plan, feather


def temporal_mask(frames, feather):
    """tiled_scale_multidim's linear feather along one axis."""
    mask = torch.ones(frames)
    if feather < frames:
        for t in range(feather):
            a = (t + 1) / feather
            mask[t] *= a
            mask[frames - 1 - t] *= a
    return mask


class TemporalBlender:
    """Accumulate weighted windows; release frames no later window can touch."""

    def __init__(self):
        self.start = 0
        self.acc = None
        self.weight = None

    def add(self, frames, out_start, mask):
        end = out_start + frames.shape[0]
        if self.acc is None:
            self.start = out_start
            self.acc = torch.zeros((end - out_start,) + tuple(frames.shape[1:]), dtype=torch.float32)
            self.weight = torch.zeros(end - out_start, dtype=torch.float32)
        elif end > self.start + self.acc.shape[0]:
            grow = end - (self.start + self.acc.shape[0])
            self.acc = torch.cat([self.acc, torch.zeros((grow,) + tuple(self.acc.shape[1:]), dtype=torch.float32)])
            self.weight = torch.cat([self.weight, torch.zeros(grow, dtype=torch.float32)])
        lo = out_start - self.start
        m = mask.to(torch.float32)
        self.acc[lo:lo + frames.shape[0]] += frames.to(torch.float32) * m.view(-1, 1, 1, 1)
        self.weight[lo:lo + frames.shape[0]] += m

    def release(self, upto=None):
        """Normalised frames before output index `upto` (all when None)."""
        if self.acc is None:
            return None
        n = self.acc.shape[0] if upto is None else max(0, min(self.acc.shape[0], upto - self.start))
        if n == 0:
            return None
        out = self.acc[:n] / (self.weight[:n].view(-1, 1, 1, 1) + 1e-8)
        self.acc = self.acc[n:].clone()
        self.weight = self.weight[n:].clone()
        self.start += n
        return out
