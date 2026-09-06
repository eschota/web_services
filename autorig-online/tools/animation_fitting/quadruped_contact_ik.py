"""Bounded contact-IK kernel for already sampled quadruped animation poses."""
from __future__ import annotations
import copy
import math
import numbers

import numpy as np
from scipy.spatial.transform import Rotation

from .author_quadruped_motion import LEGS, ReachError, rx, solve_leg, wrap_angle


def _finite_array(value,shape,label):
    def contains_bool(item):
        if isinstance(item,(bool,np.bool_)):return True
        return isinstance(item,(list,tuple,np.ndarray)) and any(contains_bool(x) for x in item)
    if contains_bool(value):raise ValueError(f"{label} must not contain booleans")
    raw=np.asarray(value)
    if raw.dtype.kind not in "iuf" or raw.dtype.kind=="b":raise ValueError(f"{label} must be numeric")
    result=np.array(raw,dtype=float,copy=True)
    if result.shape!=shape or not np.isfinite(result).all():raise ValueError(f"{label} must be finite shape {shape}")
    return result

def _rigid_matrix(value,label):
    matrix=_finite_array(value,(4,4),label);rotation=matrix[:3,:3]
    if (not np.allclose(matrix[3],[0,0,0,1],rtol=0,atol=1e-5)
            or not np.allclose(rotation.T@rotation,np.eye(3),rtol=0,atol=1e-5)
            or abs(np.linalg.det(rotation)-1)>1e-5):
        raise ValueError(f"{label} must be a unit rigid affine matrix")
    return matrix


def sample_local_pose(clip,frame):
    """Sample local TRS with linear T/S and shortest-hemisphere quaternion nlerp."""
    if isinstance(frame,bool) or not isinstance(frame,numbers.Real) or not math.isfinite(frame):raise ValueError("frame must be finite")
    frames=clip.get("frames") if isinstance(clip,dict) else None
    if not isinstance(frames,list) or not frames:raise ValueError("clip frames required")
    if frame<0 or frame>len(frames)-1:raise ValueError("frame outside clip")
    lower=int(math.floor(frame));upper=min(lower+1,len(frames)-1);u=float(frame-lower)
    a,b=frames[lower].get("bones"),frames[upper].get("bones")
    if not isinstance(a,dict) or not isinstance(b,dict) or set(a)!=set(b):raise ValueError("frame bone coverage mismatch")
    result={}
    for name in a:
        if set(a[name])!={"translation","rotation","scale"} or set(b[name])!={"translation","rotation","scale"}:raise ValueError("exact TRS required")
        ta=_finite_array(a[name]["translation"],(3,),"translation");tb=_finite_array(b[name]["translation"],(3,),"translation")
        sa=_finite_array(a[name]["scale"],(3,),"scale");sb=_finite_array(b[name]["scale"],(3,),"scale")
        if not np.allclose(sa,1,rtol=0,atol=1e-5) or not np.allclose(sb,1,rtol=0,atol=1e-5):raise ValueError("sample scale must be unit")
        qa=_finite_array(a[name]["rotation"],(4,),"rotation");qb=_finite_array(b[name]["rotation"],(4,),"rotation")
        if abs(np.linalg.norm(qa)-1)>1e-6 or abs(np.linalg.norm(qb)-1)>1e-6:raise ValueError("quaternion must be normalized")
        if qa@qb<0:qb=-qb
        q=(1-u)*qa+u*qb;norm=np.linalg.norm(q)
        if norm<1e-12:raise ValueError("quaternion interpolation degenerate")
        q/=norm;m=np.eye(4);m[:3,:3]=Rotation.from_quat(q).as_matrix()@np.diag((1-u)*sa+u*sb);m[:3,3]=(1-u)*ta+u*tb
        result[name]=m
    return result


def _world_from_local(rig,local):
    world={}
    for name in rig.order:
        parent=rig.rows[name]["parent"];world[name]=(world[parent] if parent else np.eye(4))@local[name]
    return world


def _current_q(rig,limb,world,base_rotation):
    heads=np.array([world[name][:3,3] for name in limb["bones"][:4]])
    vectors=np.diff(heads,axis=0)@base_rotation
    if np.any(np.linalg.norm(vectors,axis=1)<1e-10):raise ValueError("posed limb has degenerate primary segments")
    theta=np.arctan2(vectors[:,1],-vectors[:,2])
    rest_theta=math.atan2(limb["vectors"][0,1],-limb["vectors"][0,2])
    return np.array([wrap_angle(theta[0]-rest_theta),wrap_angle(theta[1]-theta[0]),wrap_angle(theta[2]-theta[1])])


def correct_contact_pose(rig,local_pose,actor_translation,world_targets,active_contacts,max_pose_correction_degrees=8):
    """Correct active flat-ground contacts without changing torso or inactive legs."""
    if not isinstance(local_pose,dict) or set(local_pose)!=set(rig.order):raise ValueError("complete local pose required")
    original={name:_rigid_matrix(matrix,f"{name} local matrix") for name,matrix in local_pose.items()}
    actor=_finite_array(actor_translation,(3,),"actor translation")
    if not isinstance(world_targets,dict) or set(world_targets)!=set(LEGS):raise ValueError("all four world targets required")
    targets={name:_finite_array(world_targets[name],(3,),f"{name} world target")-actor for name in LEGS}
    if not isinstance(active_contacts,dict) or set(active_contacts)!=set(LEGS) or any(type(active_contacts[name]) is not bool for name in LEGS):
        raise ValueError("all four active-contact booleans required")
    if isinstance(max_pose_correction_degrees,bool) or not isinstance(max_pose_correction_degrees,numbers.Real) or not math.isfinite(max_pose_correction_degrees) or not 0<max_pose_correction_degrees<=8:
        raise ValueError("invalid pose correction cap")
    original_world=_world_from_local(rig,original)
    if not any(active_contacts.values()):return original_world,{k:v.copy() for k,v in original.items()},{"legs":{}}
    basis={name:np.linalg.inv(rig.local[name])@original[name] for name in rig.order};overrides={};metrics={}
    for name in LEGS:
        if not active_contacts[name]:continue
        source=rig.limbs[name];limb=copy.deepcopy(source);names=limb["bones"]
        parent=rig.rows[names[0]]["parent"]
        base=original_world[parent][:3,:3]@rig.rest[parent][:3,:3].T
        current_q=_current_q(rig,limb,original_world,base)
        limb["posture_prior"]=current_q.copy();limb["fetlock_rest"]=rig.rest[names[3]][:3,3].copy()
        angles,q,error=solve_leg(limb,original_world[names[0]][:3,3],base,targets[name],0.,rig.height)
        if error>rig.height*1e-4:raise ReachError(f"{name} endpoint error {error:.9f} exceeds contact cap")
        correction=np.degrees(np.abs([wrap_angle(a-b) for a,b in zip(q,current_q)]))
        if correction.max()>max_pose_correction_degrees+1e-9:
            raise ReachError(f"{name} correction {correction.max():.6f} exceeds cap")
        for bone,angle in zip(names[:3],angles):overrides[bone]=base@rx(angle)@rig.rest[bone][:3,:3]
        for row in limb["extra"]:overrides[row["bone"]]=base@rx(angles[row["segment"]])@rig.rest[row["bone"]][:3,:3]
        for bone in names[3:]:overrides[bone]=rig.rest[bone][:3,:3]
        metrics[name]={"current_q":current_q.tolist(),"solved_q":q.tolist(),"joint_bounds":[x.tolist() for x in limb["bounds"]],
            "max_correction_degrees":float(correction.max()),"endpoint_error_m":float(error)}
    candidate_world,candidate_local=rig.fk(basis,overrides)
    changed=set()
    for name in LEGS:
        if active_contacts[name]:changed.update(rig.limbs[name]["bones"]);changed.update(row["bone"] for row in rig.limbs[name]["extra"])
    final_local={}
    for name in rig.order:
        if name in changed:
            matrix=candidate_local[name].copy()
            matrix[:3,:3]=Rotation.from_matrix(matrix[:3,:3]).as_matrix()
            final_local[name]=matrix
        else:
            final_local[name]=original[name].copy()
    per_bone={}
    for name in sorted(changed):
        old_rotation=Rotation.from_matrix(original[name][:3,:3]).as_matrix()
        new_rotation=Rotation.from_matrix(final_local[name][:3,:3]).as_matrix()
        delta=old_rotation.T@new_rotation
        cosine=np.clip((np.trace(delta)-1)/2,-1,1);per_bone[name]=float(np.degrees(np.arccos(cosine)))
    worst=max(per_bone.values(),default=0.)
    if worst>max_pose_correction_degrees+1e-7:raise ReachError(f"local bone correction {worst:.6f} exceeds cap")
    final_world=_world_from_local(rig,final_local)
    return final_world,final_local,{"legs":metrics,"per_bone_correction_degrees":per_bone,
        "worst_bone_correction_degrees":worst,"max_pose_correction_degrees":float(max_pose_correction_degrees)}
