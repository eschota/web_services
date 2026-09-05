"""Author contact-controlled quadruped candidates on an explicit skeletal profile.

The solver works in the anatomical sagittal plane with finite hip, middle
and distal-joint bounds. These bounds are engineering authoring limits, not
claims about veterinary ranges. Video fitting can refine this motion later;
no generated clip is automatically declared production approved.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import math
from pathlib import Path

import numpy as np
from scipy.optimize import minimize_scalar
from scipy.spatial.transform import Rotation

from .game_timing import timing

LEGS = ('hind_left', 'fore_left', 'hind_right', 'fore_right')
DEFAULT_PROFILE=Path(__file__).parent/'profiles/horse_gameplay_grounded.v1.json'
IDLES = ('idle_neutral','idle_alert','idle_relaxed','idle_look_around','idle_fidget')
SUPPORTED_ACTIONS = (*IDLES,'walk_forward','walk_backward','trot_jog')


class ReachError(ValueError):
    pass


def rx(a):
    c,s=math.cos(a),math.sin(a)
    return np.array([[1,0,0],[0,c,-s],[0,s,c]],dtype=float)


def transform(r=np.eye(3), p=(0,0,0)):
    m=np.eye(4);m[:3,:3]=r;m[:3,3]=p
    return m


def wrap_angle(a):
    return (a+math.pi)%(2*math.pi)-math.pi


def swing_bump(t):
    return 64*t**3*(1-t)**3


def hoof_trajectory(phase, duty, stride, lift, center, direction=1):
    """C2 horizontal path and C1 lift; stance velocity matches both joins."""
    u=phase%1.0
    if u<duty:
        return np.array([center[0],center[1]+direction*stride*(u/duty-.5),0.]), True, 0.
    t=(u-duty)/(1-duty)
    # Quintic Hermite with endpoint tangent matching the linear stance path.
    tangent=(1-duty)/duty
    y=.5+tangent*t + (-10-10*tangent)*t**3 + (15+15*tangent)*t**4 + (-6-6*tangent)*t**5
    bump=swing_bump(t)
    # Earlier clearance prevents the interpolated skeletal arc from clipping
    # the floor immediately after takeoff. Vertical speed is still zero at
    # both contact boundaries; foot pitch retains the gentler C2 envelope.
    lift_bump=16*t*t*(1-t)*(1-t)
    return np.array([center[0],center[1]+direction*stride*y,lift*lift_bump]), False, bump


class AuthoringRig:
    def __init__(self,payload,profile=None):
        if payload.get('schema')!='autorig-quadruped-authoring-rig.v1':
            raise ValueError('Unsupported authoring rig')
        self.payload=payload
        self.profile=profile if profile is not None else json.loads(DEFAULT_PROFILE.read_text())
        if self.profile.get('schema')!='autorig-quadruped-gameplay-profile.v1':raise ValueError('Unsupported gameplay profile')
        if set(self.profile['limbs'])!=set(LEGS):raise ValueError('Profile must explicitly identify all four legs')
        self.gaits=self.profile['gaits']
        rest_policy=self.profile.get('rest_pose_policy','require_within_limits')
        if rest_policy not in ('require_within_limits','project_within_limits'):
            raise ValueError('Invalid rest pose policy')
        projection_cap=self.profile.get('max_rest_projection_degrees',0.)
        if not math.isfinite(projection_cap) or not 0<=projection_cap<=15:
            raise ValueError('Invalid bounded rest projection cap')
        for name,gait in self.gaits.items():
            if len(gait['phases'])!=4 or any(not 0<=p<1 for p in gait['phases']):raise ValueError('Invalid gait phases')
            if not 0<gait['duty']<1 or gait['direction'] not in (-1,1):raise ValueError('Invalid stance policy')
            if any(not math.isfinite(gait[k]) or gait[k]<0 for k in ('stride_height','lift_height','body_drop','bob')):
                raise ValueError('Invalid gait dimensions')
        self.rows={b['name']:b for b in payload['bones']}
        if len(self.rows)!=len(payload['bones']):raise ValueError('Duplicate bones')
        self.rest={n:np.array(b['rest_world'],float).reshape(4,4) for n,b in self.rows.items()}
        self.local={n:np.array(b['rest_local'],float).reshape(4,4) for n,b in self.rows.items()}
        self.root=self.profile['root']
        if self.root not in self.rows:raise ValueError('Missing motion root')
        self.order=[];pending=set(self.rows)
        while pending:
            ready=sorted(n for n in pending if self.rows[n]['parent'] is None or self.rows[n]['parent'] in self.order)
            if not ready:raise ValueError('Skeleton hierarchy is cyclic or incomplete')
            self.order.extend(ready);pending.difference_update(ready)
        self.vertices=[v for m in payload['meshes'] for v in m['vertices']]
        self.points=np.array([v['point'] for v in self.vertices],float)
        self.inverse={n:np.linalg.inv(m) for n,m in self.rest.items()}
        self.joint_index={n:i for i,n in enumerate(self.order)}
        self.skin_joints=np.zeros((len(self.vertices),4),dtype=np.int32)
        self.skin_weights=np.zeros((len(self.vertices),4))
        for i,v in enumerate(self.vertices):
            if not 1<=len(v['weights'])<=4:raise ValueError('Expected one to four skin influences')
            for k,w in enumerate(v['weights']):
                if w['bone'] not in self.joint_index or not math.isfinite(w['weight']) or w['weight']<=0:
                    raise ValueError('Invalid skin influence')
                self.skin_joints[i,k]=self.joint_index[w['bone']];self.skin_weights[i,k]=w['weight']
            if abs(self.skin_weights[i].sum()-1)>1e-5:raise ValueError('Skin weights must sum to one')
        inverse=np.array([self.inverse[n] for n in self.order])
        homogeneous=np.c_[self.points,np.ones(len(self.points))]
        self.skin_bind=np.einsum('nvij,nj->nvi',inverse[self.skin_joints],homogeneous)
        self.limbs={}
        for name in LEGS:
            declaration=self.profile['limbs'][name]
            names=declaration['chain'];extra=declaration['intermediate']
            if len(names)!=5 or any(n not in self.rows for n in names+[e['bone'] for e in extra]):
                raise ValueError(f'Missing explicit anatomical chain {name}')
            if any(e['segment'] not in (0,1,2) for e in extra):raise ValueError('Invalid intermediate segment')
            heads=np.array([self.rest[n][:3,3] for n in names[:4]])
            vectors=np.diff(heads,axis=0)
            theta=np.arctan2(vectors[:,1],-vectors[:,2])
            neutral=np.array([0,wrap_angle(theta[1]-theta[0]),wrap_angle(theta[2]-theta[1])])
            lower=np.radians(declaration['joint_lower_degrees']);upper=np.radians(declaration['joint_upper_degrees'])
            if lower.shape!=(3,) or upper.shape!=(3,) or not np.isfinite(lower).all() or not np.isfinite(upper).all() or np.any(lower>=upper):
                raise ValueError('Invalid finite joint bounds')
            posture_prior=np.clip(neutral,lower,upper)
            projection=np.abs(np.degrees(posture_prior-neutral))
            if np.any(projection>1e-9) and (rest_policy!='project_within_limits' or projection.max()>projection_cap):
                raise ValueError(f'Rest chain outside authoring limits: {name}; projection={projection.tolist()}')
            foot_indices=[i for i,v in enumerate(self.vertices) if sum(w['weight'] for w in v['weights'] if w['bone'] in names[3:])>.9]
            if not foot_indices:raise ValueError(f'Missing foot surface: {name}')
            sole_z=float(self.points[foot_indices,2].min())
            sole_indices=[i for i in foot_indices if self.points[i,2]<=sole_z+1e-5]
            sole=self.points[sole_indices].mean(axis=0)
            self.limbs[name]={'bones':names,'extra':extra,'vectors':vectors,'neutral':neutral,
                'bounds':(lower,upper),'sole':sole,'sole_indices':sole_indices,'foot_indices':foot_indices,
                'bend_sign':-1 if name in ('fore_left','fore_right') else 1,
                'posture_prior':posture_prior,'rest_projection_degrees':projection.tolist()}
        self.height=float(np.mean([self.rest[l['bones'][0]][2,3] for l in self.limbs.values()]))
        stance_cap=self.profile.get('max_stance_center_adjustment_height_fraction',.35)
        if not math.isfinite(stance_cap) or not 0<=stance_cap<=.5:
            raise ValueError('Invalid stance center adjustment cap')
        for name,limb in self.limbs.items():
            anchor=self.profile['limbs'][name].get('stance_center_joint')
            if anchor is not None and (type(anchor) is not int or anchor not in (0,1)):
                raise ValueError('Stance center joint must be hip (0) or elbow (1)')
            center=limb['sole'].copy()
            if anchor is not None:
                foot_offset=limb['sole']-self.rest[limb['bones'][3]][:3,3]
                center[1]=self.rest[limb['bones'][anchor]][1,3]+foot_offset[1]
                center[2]=0
            offset=center-limb['sole']
            if np.linalg.norm(offset)>self.height*stance_cap:
                raise ValueError(f'Stance center exceeds bounded adjustment: {name}')
            limb['stance_center']=center
            limb['stance_center_offset']=offset.tolist()

    def fk(self,basis=None,world_rotations=None):
        basis=basis or {};world_rotations=world_rotations or {}
        world={};local={}
        for n in self.order:
            parent=self.rows[n]['parent'];pm=world[parent] if parent else np.eye(4)
            lm=self.local[n] @ basis.get(n,np.eye(4))
            if n in world_rotations:
                lm[:3,:3]=pm[:3,:3].T @ world_rotations[n]
            local[n]=lm;world[n]=pm @ lm
        return world,local

    def skin(self,world):
        matrices=np.array([world[n] for n in self.order])
        posed=np.einsum('nvij,nvj->nvi',matrices[self.skin_joints],self.skin_bind)
        return np.sum(posed[:,:,:3]*self.skin_weights[:,:,None],axis=1)


def solve_leg(limb,hip,base_rotation,target,pitch,height):
    foot_offset=limb['sole']-limb['fetlock_rest']
    fetlock_target=target-rx(pitch) @ foot_offset
    neutral=limb['neutral'];vectors=limb['vectors']
    def deltas(q):
        return np.array([q[0],q[0]+q[1]-neutral[1],q[0]+q[1]-neutral[1]+q[2]-neutral[2]])
    def endpoint(q):
        return hip+sum((base_rotation @ rx(a) @ v for a,v in zip(deltas(q),vectors)),np.zeros(3))
    # Fix the distal angle, collapse links 2+3, then solve the two-link
    # triangle exactly. Only the one-dimensional posture choice remains.
    # This avoids a badly conditioned position-vs-style least-squares fit.
    d=base_rotation.T @ (fetlock_target-hip)
    length=np.linalg.norm(vectors[:,1:],axis=1);distance=float(np.linalg.norm(d[1:]))
    theta0=math.atan2(vectors[0,1],-vectors[0,2])
    lower,upper=limb['bounds'];is_fore=limb['bend_sign']<0
    prior=limb['posture_prior'].copy();bump=min(1,abs(pitch)/.22)
    prior[1]+=(-.2 if is_fore else .3)*bump
    prior[2]+=(.8 if is_fore else -.5)*bump
    def configuration(distal):
        l1,l2,l3=length
        combined=math.sqrt(l2*l2+l3*l3+2*l2*l3*math.cos(distal))
        cosine=(distance*distance-l1*l1-combined*combined)/(2*l1*combined)
        if abs(cosine)>1+1e-10:return None
        elbow=(-1 if is_fore else 1)*math.acos(np.clip(cosine,-1,1))
        beta=math.atan2(l3*math.sin(distal),l2+l3*math.cos(distal))
        theta=math.atan2(d[1],-d[2])-math.atan2(combined*math.sin(elbow),l1+combined*math.cos(elbow))
        q=np.array([wrap_angle(theta-theta0),elbow-beta,distal])
        ankle=pitch-math.atan2(base_rotation[2,1],base_rotation[1,1])-deltas(q)[2]
        if np.any(q<lower-1e-9) or np.any(q>upper+1e-9) or abs(ankle)>1.6:return None
        return q
    def cost(distal):
        q=configuration(distal)
        # The distal bend follows one smooth gait profile. Giving all three
        # angles equal style weight creates competing S-shaped postures and
        # can jump 60 degrees between otherwise identical reachable footholds.
        return float(100*(q[2]-prior[2])**2+.1*np.sum((q[:2]-prior[:2])**2)) if q is not None else 1e10
    grid=np.linspace(lower[2],upper[2],129)
    costs=[cost(v) for v in grid];idx=int(np.argmin(costs))
    if costs[idx]>=1e10:raise ReachError(f'No feasible bounded leg configuration: d={d}, lengths={length}, neutral={neutral}, target={target}, pitch={pitch}')
    result=minimize_scalar(cost,bounds=(grid[max(0,idx-1)],grid[min(128,idx+1)]),
                           method='bounded',options={'xatol':1e-10,'maxiter':100})
    if not result.success:raise ValueError('Posture minimization did not converge')
    q=configuration(result.x) if result.fun<costs[idx] else configuration(grid[idx])
    error=float(np.linalg.norm(endpoint(q)-fetlock_target))
    if error>height*.002:raise ReachError(f'Unreachable hoof target: error={error:.6f}')
    return deltas(q),q,error


def body_basis(rig,action,phase,root_position):
    root_rest=rig.local[rig.root][:3,:3]
    world_rotation=rx(.008*math.sin(2*math.pi*phase) if action in rig.gaits else 0)
    basis={rig.root:transform(root_rest.T @ world_rotation @ root_rest,root_rest.T @ root_position)}
    def rotate(name,axis,angle):
        local_axis=rig.rest[name][:3,:3].T @ np.array(axis,float)
        basis[name]=transform(Rotation.from_rotvec(local_axis*angle).as_matrix())
    body=rig.profile['body']
    nod=.025*math.sin(4*math.pi*phase) if action in rig.gaits else .012*math.sin(2*math.pi*phase)
    nod+= -.10 if action=='idle_alert' else .10 if action=='idle_relaxed' else 0
    rotate(body['neck'],(1,0,0),nod)
    rotate(body['head'],(1,0,0),-nod*.3)
    if action=='idle_look_around':rotate(body['head'],(0,0,1),.3*math.sin(2*math.pi*phase))
    if action=='idle_fidget':rotate(body['head'],(0,0,1),.12*math.sin(8*math.pi*phase)*math.sin(math.pi*phase)**2)
    for i,bone in enumerate(body['tail']):rotate(bone,(0,0,1),(.065 if action=='idle_fidget' else .035)*math.sin(2*math.pi*phase-i*.45))
    for i,bone in enumerate(body['ears']):
        rotate(bone,(0,1,0),(.18 if action=='idle_fidget' else .03)*math.sin(4*math.pi*phase+i*.8))
    if action in rig.gaits:
        gait=rig.gaits[action];stride=gait['stride_height']*rig.height
        for li,name in enumerate(LEGS):
            declaration=rig.profile['limbs'][name]
            if declaration.get('scapula'):
                center=rig.limbs[name]['stance_center']
                target,_,_=hoof_trajectory(phase-gait['phases'][li],gait['duty'],stride,
                    gait['lift_height']*rig.height,center,gait['direction'])
                angle=declaration['scapula_radians_per_height']*(target[1]-center[1])/rig.height
                rotate(declaration['scapula'],(1,0,0),angle)
    return basis


def author_clip(rig,action,root_motion=False):
    # A bounded whole-body height calibration accommodates different segment
    # proportions. It never changes the requested footholds or joint limits.
    cap=rig.profile.get('max_body_drop_adjustment_height_fraction',.03)
    if not math.isfinite(cap) or not 0<=cap<=.08:raise ValueError('Invalid body-height calibration cap')
    adjustments=np.linspace(0,cap,7) if action in rig.gaits else [0]
    error=None
    for extra in adjustments:
        try:return _author_clip(rig,action,root_motion,float(extra))
        except ReachError as exc:error=exc
    raise ReachError(f'No feasible motion within bounded body-height calibration: {error}')


def _author_clip(rig,action,root_motion=False,body_drop_adjustment=0):
    if action not in SUPPORTED_ACTIONS:raise ValueError(f'Unsupported authored action: {action}')
    contract=timing(action);count=contract['sample_count'];duration=contract['duration_seconds']
    gait=rig.gaits.get(action)
    stride=gait['stride_height']*rig.height if gait else 0.
    speed=(gait['direction']*stride/(gait['duty']*duration)) if gait else 0.
    frames=[];skins=[];contacts={name:[] for name in LEGS};targets={name:[] for name in LEGS}
    joint_samples={name:[] for name in LEGS};max_solve_error=0.
    for i in range(count):
        phase=i/(count-1);time=i/30
        bob=(gait['bob']*math.cos(4*math.pi*phase)-gait['body_drop']-body_drop_adjustment) if gait else .0015*math.sin(2*math.pi*phase)
        root_position=np.array([0,-speed*time if root_motion else 0,bob*rig.height])
        basis=body_basis(rig,action,phase,root_position)
        torso,_=rig.fk(basis)
        overrides={}
        for li,name in enumerate(LEGS):
            limb=rig.limbs[name];names=limb['bones'];extra=limb['extra']
            hip=torso[names[0]][:3,3]
            parent=rig.rows[names[0]]['parent']
            base_rotation=torso[parent][:3,:3] @ rig.rest[parent][:3,:3].T
            limb['fetlock_rest']=rig.rest[names[3]][:3,3]
            center=limb['stance_center'].copy()
            if gait:
                # Use the declared support stance; source-pose foot offsets are
                # retained unless a bounded anatomical anchor is explicit.
                target,stance,bump=hoof_trajectory(phase-gait['phases'][li],gait['duty'],stride,
                    gait['lift_height']*rig.height,center,gait['direction'])
            else:target,stance,bump=center.copy(),True,0.
            target[1]+=-speed*time if root_motion else 0
            pitch=-.22*bump*(gait['direction'] if gait else 1)
            try:
                angles,q,error=solve_leg(limb,hip,base_rotation,target,pitch,rig.height)
            except ReachError as exc:
                raise ReachError(f'{action} frame {i} {name}: {exc}') from exc
            max_solve_error=max(max_solve_error,error);joint_samples[name].append(q.tolist())
            for bone,angle in zip(names[:3],angles):
                overrides[bone]=base_rotation @ rx(angle) @ rig.rest[bone][:3,:3]
            for row in extra:
                bone=row['bone'];overrides[bone]=base_rotation @ rx(angles[row['segment']]) @ rig.rest[bone][:3,:3]
            for bone in names[3:]:overrides[bone]=rx(pitch) @ rig.rest[bone][:3,:3]
            contacts[name].append(stance);targets[name].append(target.tolist())
        world,local=rig.fk(basis,overrides)
        skin=rig.skin(world);skins.append(skin)
        frame={'time':time,'bones':{}}
        for n in rig.order:
            matrix=local[n]
            frame['bones'][n]={'translation':matrix[:3,3].tolist(),
                               'rotation':Rotation.from_matrix(matrix[:3,:3]).as_quat().tolist()}
        frames.append(frame)
    # Quaternion hemisphere continuity preserves shortest-path interpolation.
    for n in rig.order:
        for a,b in zip(frames,frames[1:]):
            qa=np.array(a['bones'][n]['rotation']);qb=np.array(b['bones'][n]['rotation'])
            if qa@qb<0:b['bones'][n]['rotation']=(-qb).tolist()
    skins=np.asarray(skins);report={}
    for name in LEGS:
        ids=rig.limbs[name]['sole_indices'];foot_ids=rig.limbs[name]['foot_indices']
        soles=skins[:,ids].mean(axis=1)
        virtual=soles.copy()
        if not root_motion:virtual[:,1]-=speed*np.arange(count)/30
        stance=np.array(contacts[name],bool)
        deltas=np.linalg.norm(np.diff(virtual,axis=0),axis=1)
        both=stance[:-1]&stance[1:]
        report[name]={'stance_frames':np.flatnonzero(stance).tolist(),
            'stance_center':rig.limbs[name]['stance_center'].tolist(),
            'stance_center_offset':rig.limbs[name]['stance_center_offset'],
            'rest_projection_degrees':rig.limbs[name]['rest_projection_degrees'],
            'max_stance_slide_per_frame':float(deltas[both].max()) if both.any() else 0,
            'max_stance_height':float(np.abs(soles[stance,2]).max()),
            'min_foot_surface_height':float(skins[:,foot_ids,2].min()),
            'max_hoof_target_error':float(np.linalg.norm(soles-np.array(targets[name]),axis=1).max()),
            'joint_min':np.min(joint_samples[name],axis=0).tolist(),
            'joint_max':np.max(joint_samples[name],axis=0).tolist(),
            'joint_bounds':[v.tolist() for v in rig.limbs[name]['bounds']]}
    seam=skins[-1]-skins[0]
    root_delta=np.array([0,-speed*duration if root_motion else 0,0])
    seam-=root_delta
    max_seam=float(np.linalg.norm(seam,axis=1).max())
    qa={'solver_max_error':max_solve_error,'mesh_pose_seam':max_seam,
        'body_drop_adjustment_height_fraction':body_drop_adjustment,
        'mesh_min_height':float(skins[:,:,2].min()),'feet':report,
        'quality_approved':False,'measurement_space':'deformed_mesh_armature_space',
        'contact_policy':'in-place contacts measured after virtual actor translation' if not root_motion else 'world planted contacts'}
    if max_seam>rig.height*.001 or any(r['max_stance_slide_per_frame']>rig.height*.001 for r in report.values()):
        raise ValueError('Authored clip failed deformed-mesh contact or seam check: '+json.dumps(qa))
    return {'schema':'autorig-authored-quadruped-clip.v1','action':action,'timing':contract,
        'root_motion':root_motion,'root_delta':root_delta.tolist(),'reference_speed':speed,
        'rig_source_sha256':rig.payload['source_sha256'],'authoring_limits_status':'engineering_candidate',
        'gameplay_profile_id':rig.profile['profile_id'],
        'gameplay_profile_contract_sha256':hashlib.sha256(json.dumps(rig.profile,sort_keys=True,separators=(',',':')).encode()).hexdigest(),
        'frames':frames,'contacts':contacts,'hoof_targets':targets,
        'surface_anchors':{name:{'sole_vertices':rig.limbs[name]['sole_indices'],
                                'foot_vertices':rig.limbs[name]['foot_indices']} for name in LEGS},'qa':qa}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rig',type=Path,required=True)
    parser.add_argument('--actions',nargs='+',choices=SUPPORTED_ACTIONS,default=['idle_neutral','walk_forward','trot_jog'])
    parser.add_argument('--output-dir',type=Path,required=True)
    parser.add_argument('--profile',type=Path,default=DEFAULT_PROFILE)
    parser.add_argument('--root-motion',action='store_true')
    args=parser.parse_args()
    data=args.rig.read_bytes();rig=AuthoringRig(json.loads(data),json.loads(args.profile.read_text()))
    args.output_dir.mkdir(parents=True,exist_ok=False)
    for action in args.actions:
        clip=author_clip(rig,action,args.root_motion)
        clip['rig_blueprint_sha256']=hashlib.sha256(data).hexdigest()
        (args.output_dir/(action+'.json')).write_text(json.dumps(clip,separators=(',',':'),allow_nan=False)+'\n',encoding='utf-8')
        print(json.dumps({'action':action,'qa':clip['qa']}))


if __name__=='__main__':main()
