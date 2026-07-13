"""Offline video-to-skeleton fitting tools for AutoRig animal rigs."""

from .errors import ContractError, DependencyUnavailableError, OptimizationError
from .observations import ObservationSet, load_observations
from .optimizer import FittingConfig, FittingResult, fit_sequence
from .rig import RigBundle, load_rig_bundle

__all__ = [
    "ContractError",
    "DependencyUnavailableError",
    "OptimizationError",
    "FittingConfig",
    "FittingResult",
    "ObservationSet",
    "RigBundle",
    "fit_sequence",
    "load_observations",
    "load_rig_bundle",
]
