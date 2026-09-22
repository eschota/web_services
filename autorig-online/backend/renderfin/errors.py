"""Fault taxonomy shared by the render submit path.

A submit failure is one of two completely different things, and the dispatcher
used to treat them identically:

* a failure that describes **the request** - a source image or control video
  that cannot be downloaded, a template the prompt asked for that does not
  exist, a checkpoint the prompt named that no worker validates.  Handing the
  same task to the next box reproduces it byte for byte, so the task is what
  has to stop.  The box is healthy and must stay in the rotation.
* a failure that describes **the box** - connection refused, a timeout, a 5xx
  out of ComfyUI, an upload endpoint that will not take bytes.  That one earns
  a cooldown: another box can very likely do this work right now.

On 2026-09-22 one video request carried an unreachable ``image_url``.  The
generic handler quarantined every box the request touched, so four of the four
workers that can run ``gen_animation_by_url.json`` were put in ``render_error``
for ten minutes by a single bad URL, and every later video sat Pending while
all four machines were idle and answering ``/queue`` with 200.

Both :mod:`renderfin.comfy_adapter` and :mod:`renderfin.queue` import from
here so the classification is carried by the exception *type* and never by
matching on the text of a message.
"""
from __future__ import annotations

__all__ = [
    "RenderDispatchError",
    "RequestFaultError",
    "is_request_fault",
]


class RenderDispatchError(RuntimeError):
    """Base for failures raised while dispatching one render task."""


class RequestFaultError(RenderDispatchError):
    """The failure describes the request, not the worker that reported it.

    Every other worker would fail this task in exactly the same way, so the
    dispatcher must spend the task's own attempts and must NOT touch
    ``_server_submit_cooldowns`` or ``server.status``.
    """


def is_request_fault(exc: BaseException) -> bool:
    """Is this submit failure the request's fault rather than the box's?

    Anything not explicitly marked is treated as a fault of the box, which is
    the safe default: quarantining a healthy box costs one cooldown, while
    keeping a genuinely broken box in rotation costs every task that lands on
    it.
    """
    return isinstance(exc, RequestFaultError)
