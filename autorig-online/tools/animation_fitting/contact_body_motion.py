"""Periodic body-root authoring from explicit stance impulses.

This is an engineering motion model, not a measured horse centre of mass.
Each foot supplies a smooth nonnegative vertical pulse only during stance.
Analytic integration closes height and velocity without a loop seam and leaves
ballistic vertical acceleration during flight. Pitch is a separate style rule.
"""
from __future__ import annotations

import math
import numpy as np


class ContactBodyMotion:
    def __init__(self, gait, duration):
        config=gait['body_dynamics']
        if config.get('model')!='contact_impulses':raise ValueError('Unsupported body dynamics model')
        self.duration=float(duration)
        self.gravity=float(config['gravity_height_per_second_squared'])
        self.pitch_gain=float(config.get('pitch_load_gain_radians',0.))
        self.duty=float(gait['duty'])
        self.phases=np.asarray(gait['phases'],dtype=float)
        self.loads=np.asarray(config['vertical_impulse_fractions'],dtype=float)
        if not math.isfinite(self.duration) or self.duration<=0:raise ValueError('Invalid body cycle duration')
        if not math.isfinite(self.gravity) or self.gravity<=0:raise ValueError('Invalid normalized body gravity')
        if not math.isfinite(self.pitch_gain):raise ValueError('Invalid body pitch gain')
        if not math.isfinite(self.duty) or not 0<self.duty<1:raise ValueError('Invalid body stance duty')
        if self.phases.shape!=(4,) or not np.isfinite(self.phases).all() or np.any((self.phases<0)|(self.phases>=1)):
            raise ValueError('Body phases must identify four limbs')
        if self.loads.shape!=(4,) or not np.isfinite(self.loads).all() or np.any(self.loads<0) or abs(self.loads.sum()-1)>1e-10:
            raise ValueError('Vertical impulse fractions must be a four-limb simplex')
        self._cycle_displacement=self._unclosed_height(np.asarray(1.))

    def _integrals(self, phase):
        p=np.asarray(phase,dtype=float)
        forces=np.zeros(p.shape+(4,));first=np.zeros_like(p);second=np.zeros_like(p)
        tau=2*math.pi
        for leg,(offset,weight) in enumerate(zip(self.phases,self.loads)):
            for shift in (-1.,0.):
                start=offset+shift
                u=(p-start)/self.duty;u0=-start/self.duty
                q=np.clip(u,0,1);q0=float(np.clip(u0,0,1))
                f=weight/self.duty*(1-np.cos(tau*q))
                forces[...,leg]+=np.where((u>0)&(u<1),f,0.)
                primitive=weight*(q-np.sin(tau*q)/tau)
                primitive0=weight*(q0-math.sin(tau*q0)/tau)
                integral=weight*self.duty*(q*q/2+(np.cos(tau*q)-1)/tau**2+np.maximum(u-1,0))
                integral0=weight*self.duty*(q0*q0/2+(math.cos(tau*q0)-1)/tau**2+max(u0-1,0))
                first+=primitive-primitive0
                second+=integral-integral0-p*primitive0
        return forces,first,second

    def _unclosed_height(self, phase):
        _,_,integral=self._integrals(phase)
        return self.gravity*self.duration**2*(integral-phase*phase/2)

    def sample(self, phase):
        p=np.asarray(phase,dtype=float)
        if not np.isfinite(p).all():raise ValueError('Body phase must be finite')
        p=p%1.
        forces,first,second=self._integrals(p)
        height=self.gravity*self.duration**2*(second-p*p/2)-self._cycle_displacement*p
        velocity=self.gravity*self.duration*(first-p)-self._cycle_displacement/self.duration
        acceleration=self.gravity*(forces.sum(axis=-1)-1)
        # Limb order is hind L, fore L, hind R, fore R. This is a bounded
        # authoring response to load transfer, not a solved pitching torque.
        pitch=self.pitch_gain*(forces[...,1]+forces[...,3]-forces[...,0]-forces[...,2])
        if not all(np.isfinite(x).all() for x in (height,velocity,acceleration,pitch)):
            raise ValueError('Body dynamics produced non-finite motion')
        return {'height_fraction':height,'velocity_height_per_second':velocity,
                'acceleration_height_per_second_squared':acceleration,
                'vertical_load_body_weights':forces,'pitch_radians':pitch}
