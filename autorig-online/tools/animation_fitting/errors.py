class FittingError(RuntimeError):
    """Base class for deterministic fitting failures."""


class ContractError(FittingError):
    """Input data violates a versioned schema or an explicit fitting contract."""


class DependencyUnavailableError(FittingError):
    """An executable or Python dependency is unavailable."""


class OptimizationError(FittingError):
    """The numerical optimizer cannot produce a valid result."""
