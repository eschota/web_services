"""Build a nominal contact-IK pose envelope; not a 30 Hz animation clip."""
import argparse
import hashlib
import json
from pathlib import Path
import numpy as np

from .author_quadruped_motion import AuthoringRig
from .quadruped_clip_semantics import validate_v2_clip, verify_profile_sources
from .quadruped_contact_ik import sample_local_pose, correct_contact_pose


def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--report',type=Path,required=True);p.add_argument('--output',type=Path,required=True)
    p.add_argument('--action',default='jump_full');a=p.parse_args()
    report_path=a.report.resolve();report=json.loads(report_path.read_text())
    if report.get('schema')!='autorig-quadruped-export-candidate.v2':raise ValueError('V2 report required')
    rig_pin=report['rig_blueprint_pin'];rig_path=Path(rig_pin['path'])
    if sha(rig_path)!=rig_pin['sha256']:raise ValueError('Blueprint changed')
    blueprint=json.loads(rig_path.read_text())
    clip=next(c for c in report['clips'] if c['action']==a.action)
    verify_profile_sources(clip);context=validate_v2_clip(clip,blueprint)
    gameplay=json.loads(Path(clip['profile_sources']['gameplay_profile']).read_text())
    rig=AuthoringRig(blueprint,gameplay)
    samples=[];failures=[];maximum=0.
    for step in range((context.sample_count-1)*4+1):
        frame=step/4;i=int(frame);f=frame-i;j=min(i+1,context.sample_count-1)
        actor=context.actor_translation[i]*(1-f)+context.actor_translation[j]*f
        targets={n:context.targets[n][i]*(1-f)+context.targets[n][j]*f+actor for n in context.contacts}
        # Runtime state persists until the next sampled contact event. The
        # older both-endpoint policy remains separately recorded as a QA mask.
        active={n:bool(context.contacts[n][i]) for n in context.contacts}
        conservative={n:active[n] and (f==0 or context.contacts[n][j]) for n in active}
        if any(active[n] and abs(targets[n][2]-context.ground_height)>context.ground_tolerance for n in active):
            raise ValueError('Event contact window has an off-plane target')
        original=sample_local_pose(clip,frame)
        try:
            _,corrected,metrics=correct_contact_pose(rig,original,actor,targets,active)
            ok=True
            maximum=max(maximum,max(metrics.get('per_bone_correction_degrees',{}).values(),default=0.))
        except ValueError as exc:
            corrected=original;metrics={'error':str(exc)};ok=False;failures.append({'frame':frame,'error':str(exc)})
        samples.append({'frame':frame,'actor_translation':actor.tolist(),
            'world_targets':{n:v.tolist() for n,v in targets.items()},'active_contacts':active,
            'conservative_qa_contacts':conservative,
            'original_local':{n:m.ravel().tolist() for n,m in original.items()},
            'corrected_local':{n:m.ravel().tolist() for n,m in corrected.items()},
            'correction_passed':ok,'correction_metrics':metrics})
    result={'schema':'autorig-quadruped-contact-ik-envelope.v1','case':'nominal_flat_reference',
        'source_report_pin':{'path':str(report_path),'sha256':sha(report_path)},'rig_blueprint_pin':rig_pin,
        'action':a.action,'sample_rate_hz':120,'samples':samples,'failures':failures,
        'maximum_local_correction_degrees':maximum,
        'contact_policy':'latest event state; targets must remain on reference ground',
        'scope':'nominal translation-only replay; not collision/engine integration or an exported animation',
        'passed':not failures,'quality_approved':False}
    output=a.output.resolve();output.parent.mkdir(parents=True,exist_ok=True)
    with output.open('x',encoding='utf-8') as stream:json.dump(result,stream,separators=(',',':'),allow_nan=False)
    print(json.dumps({'samples':len(samples),'failures':failures,'maximum_correction_degrees':maximum,'path':str(output)}))
    if failures:raise SystemExit(1)


if __name__=='__main__':main()
