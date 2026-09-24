"""Exceptions raised by AutoRig Regen.

Every failure the orchestrator is expected to survive is a
:class:`DecompositionError`: when it sees one it falls back to plain rigging
without cloth. The message is meant to be read by a person and says what was
wrong with the input ("no T-pose arm band found", "alignment residual too
high", ...), not where the code was.
"""

from __future__ import annotations


class DecompositionError(RuntimeError):
    """The inputs cannot be decomposed into body / hair / cloth parts."""


class MeshLoadError(DecompositionError):
    """A mesh file is missing, unreadable, or holds no usable triangles."""
