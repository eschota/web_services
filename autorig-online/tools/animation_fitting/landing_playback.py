"""Collision-gated pose playback for an eventual quadruped controller.

This clock emits pose/blend instructions only. It does not move the actor,
detect collision, or solve feet. A consumer must snapshot the evaluated pose
when requested and apply a contact-constrained recovery blend. It is not an
implemented engine controller or evidence that a landing mesh is correct.
"""
from dataclasses import dataclass
import math


@dataclass(frozen=True)
class LandingStep:
    state: str
    pose_frame: float
    capture_current_pose: bool = False
    recovery_blend_weight: float | None = None
    contact_solver_required: bool = False
    air_blend_weight: float | None = None


class LandingPlayback:
    """One landing attempt, with reference touchdown gated by real grounding.

    `grounded` is the controller's current collision result, not an authored
    animation event. `ground_near` requests an approach while still airborne.
    Supply a stable, controller-filtered grounding result, rather than a raw
    single-ray flicker. Returning to air ends this attempt; create a new
    attempt on re-entry. When capture is requested, snapshot the evaluated
    pose BEFORE applying the returned pose frame or blend target.
    """

    def __init__(self, sample_count, precontact_frame, touchdown_frame,
                 recovery_blend_seconds=.15, air_blend_seconds=.12):
        if (type(sample_count) is not int or not 3 <= sample_count <= 3601 or
                type(precontact_frame) is not int or type(touchdown_frame) is not int or
                not 0 <= precontact_frame < touchdown_frame < sample_count):
            raise ValueError('Ordered precontact/touchdown frames within a landing clip required')
        for duration in (recovery_blend_seconds, air_blend_seconds):
            if (isinstance(duration, bool) or not isinstance(duration, (int, float)) or
                    not math.isfinite(duration) or duration <= 0):
                raise ValueError('Finite positive blend durations required')
        self.last_frame = sample_count - 1
        self.precontact_frame = precontact_frame
        self.touchdown_frame = touchdown_frame
        self.blend_seconds = float(recovery_blend_seconds)
        self.air_blend_seconds = float(air_blend_seconds)
        self.state = 'approach'
        self.frame = 0.
        self.recovery_elapsed = 0.
        self.air_blend_elapsed = 0.

    def _return_to_air(self):
        self.state = 'air_blend'
        self.air_blend_elapsed = 0.
        return LandingStep('air_blend', self.frame, capture_current_pose=True,
                           air_blend_weight=0.)

    def advance(self, seconds, *, grounded, ground_near):
        if (isinstance(seconds, bool) or not isinstance(seconds, (int, float)) or
                not math.isfinite(seconds) or seconds < 0):
            raise ValueError('Elapsed time must be finite and nonnegative')
        if type(grounded) is not bool or type(ground_near) is not bool:
            raise ValueError('Controller collision and proximity states must be booleans')
        if self.state == 'air':
            return LandingStep('air', self.frame, air_blend_weight=1.)
        if self.state == 'air_blend':
            self.air_blend_elapsed += seconds
            fraction = min(1., self.air_blend_elapsed / self.air_blend_seconds)
            weight = fraction * fraction * (3 - 2 * fraction)
            if fraction == 1:
                self.state = 'air'
            return LandingStep(self.state, self.frame, air_blend_weight=weight)
        if self.state in ('recovery', 'complete'):
            if not grounded:
                return self._return_to_air()
            self.recovery_elapsed += seconds
            self.frame = min(self.last_frame, self.touchdown_frame + self.recovery_elapsed * 30)
            fraction = min(1., self.recovery_elapsed / self.blend_seconds)
            # Zero endpoint derivatives avoid a new discontinuity when the
            # consumer leaves the captured pose or finishes the blend.
            weight = fraction * fraction * (3 - 2 * fraction)
            if self.frame == self.last_frame and fraction == 1:
                self.state = 'complete'
            return LandingStep(self.state, self.frame, recovery_blend_weight=weight,
                               contact_solver_required=True)
        if grounded:
            self.state = 'recovery'
            self.recovery_elapsed = 0.
            self.frame = float(self.touchdown_frame)
            # Even an early collision starts at weight zero from the current
            # evaluated pose. Never cut straight to the canonical target.
            return LandingStep('recovery', self.frame, capture_current_pose=True,
                               recovery_blend_weight=0., contact_solver_required=True)
        if not ground_near:
            return self._return_to_air()
        self.frame = min(self.precontact_frame, self.frame + seconds * 30)
        self.state = 'precontact_hold' if self.frame == self.precontact_frame else 'approach'
        return LandingStep(self.state, self.frame)
