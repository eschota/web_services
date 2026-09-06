"""Author the first bounded, vertical standing-jump v2 candidate."""
from __future__ import annotations
import argparse
import copy
import hashlib
import json
import math
import numbers
import re
from pathlib import Path

import numpy as np
from scipy.spatial.transform import Rotation

from .author_quadruped_motion import AuthoringRig, LEGS, ReachError, body_basis, rx, solve_leg, transform
from .quadruped_clip_semantics import validate_v2_clip

ACTIONS = ("jump_start", "jump_air", "jump_land", "jump_full")

def _smooth(u):
    u=float(np.clip(u,0,1));return u*u*u*(10+u*(-15+6*u))

def _hash_bytes(data):return hashlib.sha256(data).hexdigest()

def _read_json(path):
    data=Path(path).read_bytes();return json.loads(data),_hash_bytes(data)

def _validated_profile(profile):
    if not isinstance(profile, dict):raise ValueError("experimental jump profile must be an object")
    profile.setdefault("landing_preload_height_fraction",0.)
    required={"sample_count":65,"fps":30,"flight_start_sample":16,"flight_end_sample":40,
              "fore_liftoff_sample":12,"hind_liftoff_sample":16,"fore_touchdown_sample":40,"hind_touchdown_sample":42}
    if profile.get("schema")!="autorig-quadruped-jump-profile.experimental.v1" or profile.get("quality_approved") is not False:
        raise ValueError("unsupported experimental jump profile")
    for key,value in required.items():
        if isinstance(profile.get(key),bool) or not isinstance(profile.get(key),numbers.Integral) or profile.get(key)!=value:
            raise ValueError(f"fixed first-milestone contract mismatch: {key}")
    numeric=("gravity_height_per_second_squared","crouch_height_fraction","fore_lift_height_fraction",
             "fore_lift_y_fraction","hind_lift_height_fraction","hind_lift_y_fraction","tuck_pitch_radians",
             "landing_preload_height_fraction")
    if any(isinstance(profile.get(k),bool) or not isinstance(profile.get(k),numbers.Real) or not math.isfinite(profile[k]) for k in numeric):
        raise ValueError("jump dimensions must be finite numbers")
    if not .04<=profile["crouch_height_fraction"]<=.14 or not 4<=profile["gravity_height_per_second_squared"]<=9:
        raise ValueError("jump dimensions exceed experimental caps")
    preload=profile["landing_preload_height_fraction"]
    if not 0<=preload<=.04 or profile["crouch_height_fraction"]+preload>.14:
        raise ValueError("landing preload exceeds bounded crouch budget")
    if not .05<=profile["fore_lift_height_fraction"]<=.35 or not .05<=profile["hind_lift_height_fraction"]<=.30 or abs(profile["fore_lift_y_fraction"])>.12 or abs(profile["hind_lift_y_fraction"])>.12 or not -.5<=profile["tuck_pitch_radians"]<=0:
        raise ValueError("limb jump dimensions exceed experimental caps")
    bones=profile.get("spine_bones");amps=profile.get("spine_amplitudes_degrees")
    if (not isinstance(bones,list) or len(bones)!=4 or any(not isinstance(x,str) for x in bones) or len(set(bones))!=4
            or not isinstance(amps,list) or len(amps)!=4 or any(isinstance(x,bool) or not isinstance(x,numbers.Real) or not math.isfinite(x) or abs(x)>5 for x in amps)):
        raise ValueError("invalid bounded spine declaration")
    return profile

def _actor_z(profile,height):
    g=profile["gravity_height_per_second_squared"]*height;duration=(40-16)/30
    v0=.5*g*duration
    z=[]
    for sample in range(65):
        t=(sample-16)/30
        z.append(.5*g*t*(duration-t) if 16<=sample<=40 else 0.)
    return np.asarray(z),g,duration,v0

def _root_offset(sample,crouch,v0,preload=0.):
    exponent=v0*(4/30)/crouch
    if sample<=8:return -crouch*_smooth(sample/8)
    if sample<=12:return -crouch
    if sample<=16:
        u=(sample-12)/4;return -crouch*(1-u**exponent)
    if sample<32:return 0.
    if sample<40:return -preload*_smooth((sample-32)/8)
    if sample<=44:
        u=(sample-40)/4;return -preload-crouch*(1-(1-u)**exponent)
    return -(crouch+preload)*(1-_smooth((sample-44)/20))

def _tuck(sample):
    if sample<=12:return 0.
    if sample<24:return _smooth((sample-12)/12)
    if sample<=32:return 1.
    if sample<44:return 1-_smooth((sample-32)/12)
    return 0.

def _lift_amount(name,sample):
    fore=name.startswith("fore");lift=12 if fore else 16;touch=40 if fore else 42
    if sample<lift or sample>=touch:return 0.
    if sample<24:amount=_smooth((sample-lift)/(24-lift))
    elif sample<=32:amount=1.
    else:amount=1-_smooth((sample-32)/(touch-32))
    return amount

def _foot_pitch(name,sample,profile):
    return profile["tuck_pitch_radians"]*_lift_amount(name,sample)

def _target(rig,name,sample,profile):
    fore=name.startswith("fore");center=rig.limbs[name]["stance_center"].copy()
    delta=np.array([0.,profile["fore_lift_y_fraction" if fore else "hind_lift_y_fraction"]*rig.height,
                    profile["fore_lift_height_fraction" if fore else "hind_lift_height_fraction"]*rig.height])
    amount=_lift_amount(name,sample)
    return center+delta*amount

def _contacts(name,sample):
    return not ((12 if name.startswith("fore") else 16)<=sample<(40 if name.startswith("fore") else 42))

def _rotate_basis(rig,basis,bone,angle):
    axis=rig.rest[bone][:3,:3].T@np.array([1.,0,0])
    basis[bone]=transform(Rotation.from_rotvec(axis*angle).as_matrix())

def author_jump_clips(rig,profile,*,source_sha256,rig_blueprint_sha256,gameplay_profile_sha256,jump_profile_sha256):
    for pin in (source_sha256,rig_blueprint_sha256,gameplay_profile_sha256,jump_profile_sha256):
        if not isinstance(pin,str) or re.fullmatch(r'[0-9a-f]{64}',pin) is None:
            raise ValueError('Authoring provenance pins must be SHA-256 hex digests')
    profile=_validated_profile(copy.deepcopy(profile))
    if source_sha256!=rig.payload.get("source_sha256"):
        raise ValueError("source_sha256 must exactly match the rig blueprint source")
    if any(name not in rig.rows for name in profile["spine_bones"]):raise ValueError("jump spine bone missing")
    pelvis,spine1,spine2,spine3=profile["spine_bones"]
    if rig.rows[pelvis]["parent"]!=rig.root or rig.rows[spine1]["parent"]!=rig.root or rig.rows[spine2]["parent"]!=spine1 or rig.rows[spine3]["parent"]!=spine2:
        raise ValueError("jump spine declaration does not match pelvis/spine hierarchy")
    actor_z,g,duration,v0=_actor_z(profile,rig.height);crouch=profile["crouch_height_fraction"]*rig.height
    preload=profile["landing_preload_height_fraction"]*rig.height
    frames=[];skins=[];contacts={n:[] for n in LEGS};targets={n:[] for n in LEGS};joint={n:[] for n in LEGS};errors=[]
    for sample in range(65):
        tuck=_tuck(sample);root_z=_root_offset(sample,crouch,v0,preload)
        basis=body_basis(rig,"idle_neutral",0,np.array([0.,0.,root_z]))
        for bone,degrees in zip(profile["spine_bones"],profile["spine_amplitudes_degrees"]):
            _rotate_basis(rig,basis,bone,math.radians(degrees)*tuck)
        torso,_=rig.fk(basis);overrides={}
        for name in LEGS:
            limb=rig.limbs[name];names=limb["bones"];limb["fetlock_rest"]=rig.rest[names[3]][:3,3]
            target=_target(rig,name,sample,profile);pitch=_foot_pitch(name,sample,profile)
            parent=rig.rows[names[0]]["parent"];base=torso[parent][:3,:3]@rig.rest[parent][:3,:3].T
            try:angles,q,error=solve_leg(limb,torso[names[0]][:3,3],base,target,pitch,rig.height)
            except ReachError as exc:raise ReachError(f"jump frame {sample} {name}: {exc}") from exc
            errors.append(error);joint[name].append(q)
            for bone,angle in zip(names[:3],angles):overrides[bone]=base@rx(angle)@rig.rest[bone][:3,:3]
            for row in limb["extra"]:overrides[row["bone"]]=base@rx(angles[row["segment"]])@rig.rest[row["bone"]][:3,:3]
            for bone in names[3:]:overrides[bone]=rx(pitch)@rig.rest[bone][:3,:3]
            contacts[name].append(_contacts(name,sample));targets[name].append(target.tolist())
        world,local=rig.fk(basis,overrides);skins.append(rig.skin(world)+np.array([0,0,actor_z[sample]]))
        frames.append({"time":sample/30,"bones":{name:{"translation":local[name][:3,3].tolist(),
            "rotation":Rotation.from_matrix(local[name][:3,:3]).as_quat().tolist(),"scale":[1.,1.,1.]} for name in rig.order}})
    for name in rig.order:
        for left,right in zip(frames,frames[1:]):
            q=np.asarray(right["bones"][name]["rotation"])
            if np.dot(left["bones"][name]["rotation"],q)<0:right["bones"][name]["rotation"]=(-q).tolist()
    skins=np.asarray(skins);phase_rows=[{"kind":"support","start":0,"end":16},{"kind":"flight","start":16,"end":40},{"kind":"support","start":40,"end":65}]
    events=[]
    for sample in range(65):
        for name in LEGS:
            previous=True if sample==0 else contacts[name][sample-1];current=contacts[name][sample]
            if current!=previous:events.append({"foot":name,"kind":"touchdown" if current else "liftoff","sample":sample})
    anchors={name:{"sole_vertices":rig.limbs[name]["sole_indices"],"foot_vertices":rig.limbs[name]["foot_indices"]} for name in LEGS}
    offsets=np.array([f["bones"][rig.root]["translation"] for f in frames])-rig.local[rig.root][:3,3]
    actor_translation=np.c_[np.zeros((65,2)),actor_z]
    canonical_physics={"scope":"canonical jump_full reference; slices retain absolute samples",
        "gravity_m_per_second_squared":g,"flight_duration_seconds":duration,"takeoff_velocity_m_per_second":v0,
        "apex_height_m":float(actor_z.max()),"apex_equation_error_m":abs(float(actor_z.max())-v0*v0/(2*g))}
    errors=np.asarray(errors).reshape(65,len(LEGS))

    def slice_qa(start,end,clip_frames,clip_contacts,clip_targets):
        sliced_skins=skins[start:end];sliced_actor=actor_translation[start:end];foot_qa={}
        for name in LEGS:
            soles=sliced_skins[:,rig.limbs[name]["sole_indices"]].mean(axis=1)
            world_targets=np.asarray(clip_targets[name])+sliced_actor
            planted=np.asarray(clip_contacts[name],bool);both=planted[:-1]&planted[1:]
            foot_heights=sliced_skins[:,rig.limbs[name]["foot_indices"],2];air=~planted
            foot_qa[name]={"max_target_error_m":float(np.linalg.norm(soles-world_targets,axis=1).max()),
                "max_planted_height_m":float(np.abs(soles[planted,2]).max()) if planted.any() else None,
                "max_planted_slide_per_frame_m":float(np.linalg.norm(np.diff(soles,axis=0),axis=1)[both].max()) if both.any() else None,
                "stance_samples_evaluated":int(planted.sum()),"min_foot_surface_height_m":float(foot_heights.min()),
                "min_air_foot_surface_height_m":float(foot_heights[air].min()) if air.any() else None,
                "air_clearance_nonnegative":bool(foot_heights[air].min()>=0) if air.any() else None,
                "air_samples_evaluated":int(air.sum())}
        spine_ranges={}
        for name in profile["spine_bones"]:
            rest_rotation=rig.local[name][:3,:3];angles=[]
            for frame in clip_frames:
                current=Rotation.from_quat(frame["bones"][name]["rotation"]).as_matrix()
                angles.append(np.degrees(np.linalg.norm(Rotation.from_matrix(rest_rotation.T@current).as_rotvec())))
            spine_ranges[name]=float(max(angles)-min(angles))
        mesh_min=float(sliced_skins[:,:,2].min());qa={"quality_approved":False,
            "candidate_kind":"engineered standing vertical jump reference","canonical_sample_range":[start,end],
            "solver_max_error_m":float(errors[start:end].max()),"mesh_min_reference_world_height_m":mesh_min,
            "mesh_clearance_nonnegative":bool(mesh_min>=0),"surface_clearance_gate_m":.006,
            "surface_clearance_within_gate":bool(mesh_min>=-.006),"feet":foot_qa,
            "canonical_jump_physics":dict(canonical_physics),
            "joint_bounds":{name:{"min":np.min(joint[name][start:end],axis=0).tolist(),
                "max":np.max(joint[name][start:end],axis=0).tolist(),"bounds":[x.tolist() for x in rig.limbs[name]["bounds"]]} for name in LEGS},
            "spinal_articulation":{"model":"jump_tuck_sagittal","bones":list(profile["spine_bones"]),
                "local_rotation_ranges_degrees":spine_ranges,"rest_geometry_changed":False,"anatomical_approval":False}}
        if start==0 and end==65:qa["physics"]=dict(canonical_physics)
        return qa
    base={"schema":"autorig-authored-quadruped-clip.v2","action":"jump_full","timing":{"fps":30,"sample_count":65,"interval_count":64},
          "playback":{"mode":"one_shot","seam_policy":"end_pose"},"motion":{"world_owner":"controller","pose_root":rig.root,
          "pose_space":"actor_local","baked_actor_translation":False,"pose_root_offsets":offsets.tolist()},
          "reference_actor_motion":{"mode":"one_shot","translations":actor_translation.tolist()},
          "ground":{"space":"reference_world","height":0.,"tolerance":min(.001*rig.height,.006)},"frames":frames,
          "contacts":contacts,"hoof_targets":targets,"surface_anchors":anchors,"entry_contacts":{name:True for name in LEGS},
          "phases":phase_rows,"events":events,"root_motion":False,"root_delta":[0.,0.,0.],"reference_speed":0.,
          "rig_source_sha256":rig.payload["source_sha256"],"rig_blueprint_sha256":rig_blueprint_sha256,
          "source_sha256":source_sha256,"gameplay_profile_sha256":gameplay_profile_sha256,"jump_profile_sha256":jump_profile_sha256,
          "gameplay_profile_contract_sha256":_hash_bytes(json.dumps(rig.profile,sort_keys=True,separators=(",",":"),allow_nan=False).encode()),
          "jump_profile_contract_sha256":_hash_bytes(json.dumps(profile,sort_keys=True,separators=(",",":"),allow_nan=False).encode())}
    ranges={"jump_start":(0,25),"jump_air":(24,33),"jump_land":(32,65),"jump_full":(0,65)};result={}
    for action,(start,end) in ranges.items():
        clip=copy.deepcopy(base);clip["action"]=action
        if action=="jump_air":clip["playback"]={"mode":"hold","seam_policy":"end_pose"}
        for key in ("frames",):clip[key]=clip[key][start:end]
        clip["timing"]={"fps":30,"sample_count":end-start,"interval_count":end-start-1}
        for frame in clip["frames"]:frame["time"]-=start/30
        for key in ("pose_root_offsets",):clip["motion"][key]=clip["motion"][key][start:end]
        clip["reference_actor_motion"]["translations"]=clip["reference_actor_motion"]["translations"][start:end]
        for key in ("contacts","hoof_targets"):
            for name in LEGS:clip[key][name]=clip[key][name][start:end]
        clip["entry_contacts"]={name:(base["entry_contacts"][name] if start==0 else base["contacts"][name][start-1]) for name in LEGS}
        clip["events"]=[]
        for i in range(end-start):
            for name in LEGS:
                prev=clip["entry_contacts"][name] if i==0 else clip["contacts"][name][i-1];cur=clip["contacts"][name][i]
                if cur!=prev:clip["events"].append({"foot":name,"kind":"touchdown" if cur else "liftoff","sample":i})
        clip["phases"]=[];cursor=0
        while cursor<end-start:
            kind="flight" if not any(clip["contacts"][n][cursor] for n in LEGS) else "support";stop=cursor+1
            while stop<end-start and (not any(clip["contacts"][n][stop] for n in LEGS))==(kind=="flight"):stop+=1
            clip["phases"].append({"kind":kind,"start":cursor,"end":stop});cursor=stop
        clip["qa"]=slice_qa(start,end,clip["frames"],clip["contacts"],clip["hoof_targets"])
        validate_v2_clip(clip,rig.payload);result[action]=clip
    return result

def main():
    p=argparse.ArgumentParser();p.add_argument("--source",type=Path,required=True);p.add_argument("--rig",type=Path,required=True)
    p.add_argument("--gameplay-profile",type=Path,required=True);p.add_argument("--profile",type=Path,required=True);p.add_argument("--output-dir",type=Path,required=True);a=p.parse_args()
    source=a.source.read_bytes();rig_payload,rig_sha=_read_json(a.rig);gameplay,gp_sha=_read_json(a.gameplay_profile);profile,jp_sha=_read_json(a.profile)
    clips=author_jump_clips(AuthoringRig(rig_payload,gameplay),profile,source_sha256=_hash_bytes(source),rig_blueprint_sha256=rig_sha,gameplay_profile_sha256=gp_sha,jump_profile_sha256=jp_sha)
    a.output_dir.mkdir(parents=True,exist_ok=False)
    recipe_dir=a.output_dir/'_recipe_inputs';recipe_dir.mkdir()
    recipe_sources={}
    for name,path,pin in (('gameplay_profile',a.gameplay_profile,gp_sha),('jump_profile',a.profile,jp_sha)):
        data=path.read_bytes()
        if _hash_bytes(data)!=pin:raise ValueError('Recipe changed during authoring: '+name)
        snapshot=recipe_dir/(name+'.json');snapshot.write_bytes(data)
        recipe_sources[name]=str(snapshot.resolve())
    for name,clip in clips.items():
        clip['profile_sources']=dict(recipe_sources)
        (a.output_dir/f"{name}.json").write_text(json.dumps(clip,separators=(",",":"),allow_nan=False)+"\n",encoding="utf-8")
    print(json.dumps({"clips":list(clips),"qa":clips["jump_full"]["qa"]},separators=(",",":")))

if __name__=="__main__":main()
