"""Preserve and reuse pinned raw model measurements before calibration QA.

Raw outputs are never labelled calibrated/accepted observations. A failed depth
calibration must not erase completed tracking and require another GPU replay.
"""
from __future__ import annotations
import hashlib, json, os
from pathlib import Path
import numpy as np

def sha(p):
    h=hashlib.sha256()
    with Path(p).open('rb') as f:
        for b in iter(lambda:f.read(1048576),b''): h.update(b)
    return h.hexdigest()

def array_sha(a):
    a=np.ascontiguousarray(a)
    h=hashlib.sha256(); h.update(str(a.dtype).encode()); h.update(str(a.shape).encode()); h.update(a.tobytes()); return h.hexdigest()

def store(root,stage,binding,arrays,provenance):
    root=Path(root); root.mkdir(parents=True,exist_ok=True)
    data=root/(stage+'.npz'); meta=root/(stage+'.json')
    if data.exists() or meta.exists(): raise ValueError('raw capture already exists; validated reuse is required')
    temp=root/('.'+stage+'.'+str(os.getpid())+'.npz')
    np.savez_compressed(temp,**arrays); os.replace(temp,data)
    report={'schema':'autorig.equine-raw-measurements/1','stage':stage,'binding':binding,
        'data_sha256':sha(data),'provenance':provenance,'calibration_accepted':False,'animation_quality_approved':False}
    temp=meta.with_suffix('.tmp'); temp.write_text(json.dumps(report,indent=2,allow_nan=False)+'\n'); os.replace(temp,meta)

def load(root,stage,binding):
    root=Path(root); meta=root/(stage+'.json'); data=root/(stage+'.npz')
    if not meta.exists() and not data.exists(): return None
    if not meta.is_file() or not data.is_file(): raise ValueError('partial raw capture requires inspection')
    report=json.loads(meta.read_text())
    if report.get('schema')!='autorig.equine-raw-measurements/1' or report.get('stage')!=stage or report.get('binding')!=binding:
        raise ValueError('raw capture identity changed')
    if report.get('calibration_accepted') is not False or report.get('animation_quality_approved') is not False or sha(data)!=report.get('data_sha256'):
        raise ValueError('raw capture integrity/approval mismatch')
    with np.load(data,allow_pickle=False) as z: arrays={k:z[k] for k in z.files}
    print(json.dumps({'raw_stage_reused':stage,'raw_sha256':report['data_sha256'],'calibration_accepted':False}),flush=True)
    return arrays,report['provenance']

def install(cli,root,scope_sha,reuse_only=False,priority_anchor_ids=()):
    """Process-local delegates; original installed runtime files stay unchanged."""
    from animation_fitting.tracking_runtime.models import TrackResult,MaskResult,DepthResult
    from animation_fitting.tracking_runtime.core import _contact_sheet
    originals={name:getattr(cli,name) for name in ('TapNextPPBackend','Sam2VideoMaskBackend','VideoDepthAnythingBackend')}
    root=Path(root); tracked={}
    def binding(video,**extra): return {'scope_sha256':scope_sha,'video_sha256':video.source_sha256,'frame_count':video.frame_count,'width':video.width,'height':video.height,**extra}
    class Tracker:
        def __init__(self,*args,**kwargs): self.impl=originals['TapNextPPBackend'](*args,**kwargs)
        def track(self,video,seeds):
            b=binding(video,anchor_ids=list(seeds.anchor_ids),seed_points_sha256=array_sha(seeds.points_xy))
            cached=load(root,'tracks',b)
            if cached:
                a,p=cached; result=TrackResult(a['points_xy'],a['visible'],a['confidence'],p)
            else:
                if reuse_only: raise ValueError('tracks raw cache is absent; GPU replay refused')
                result=self.impl.track(video,seeds)
                store(root,'tracks',b,{'points_xy':result.points_xy,'visible':result.visible,'confidence':result.confidence,
                    'anchor_ids':np.asarray(seeds.anchor_ids),'seed_points_xy':seeds.points_xy},result.provenance)
            tracked.update(result=result,ids=seeds.anchor_ids)
            return result
    class Segmenter:
        def __init__(self,*args,**kwargs): self.impl=originals['Sam2VideoMaskBackend'](*args,**kwargs)
        def segment(self,video,initial_mask):
            b=binding(video,initial_mask_sha256=array_sha(initial_mask)); cached=load(root,'masks',b)
            if cached:
                a,p=cached; result=MaskResult(a['masks'],a.get('confidence'),p)
            else:
                if reuse_only: raise ValueError('mask raw cache is absent; GPU replay refused')
                result=self.impl.segment(video,initial_mask); arrays={'masks':result.masks}
                if result.confidence is not None: arrays['confidence']=result.confidence
                store(root,'masks',b,arrays,result.provenance)
            tr=tracked.get('result')
            if tr is not None:
                ids=list(tracked['ids']); chosen=[ids.index(x) for x in priority_anchor_ids if x in ids] or list(range(len(ids)))
                _contact_sheet(root/'raw-tracks-contact-sheet.jpg',video.frames_bgr,result.masks,tr.points_xy[:,chosen],tr.visible[:,chosen],[ids[i] for i in chosen])
            return result
    class Depth:
        def __init__(self,*args,**kwargs): self.impl=originals['VideoDepthAnythingBackend'](*args,**kwargs)
        def infer(self,video):
            b=binding(video,encoder=self.impl.encoder); cached=load(root,'relative-depth',b)
            if cached:
                a,p=cached; return DepthResult(a['relative_depth'],p)
            if reuse_only: raise ValueError('depth raw cache is absent; GPU replay refused')
            result=self.impl.infer(video); store(root,'relative-depth',b,{'relative_depth':result.relative_depth},result.provenance); return result
    cli.TapNextPPBackend=Tracker; cli.Sam2VideoMaskBackend=Segmenter; cli.VideoDepthAnythingBackend=Depth
    return root
