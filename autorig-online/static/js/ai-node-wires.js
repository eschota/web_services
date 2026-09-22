/**
 * Colour the wires that feed a node while it renders.
 *
 * Drawflow paints every connection the same, so on a large composition there
 * is no way to see where the run currently is. This module watches the node
 * state labels the runner writes (`.nstate.running`) and marks the SVG
 * connections leading into a running node, so they can be drawn green and
 * animated by CSS. It reads the DOM only; nothing here edits the graph.
 */
(function (global) {
  'use strict';

  const RUNNING_CLASS = 'wire-running';
  const DONE_CLASS = 'wire-done';

  /** Target node id of a Drawflow connection element from its class list. */
  function targetNodeId(classList) {
    for (const name of classList || []) {
      const match = /^node_in_node-(.+)$/.exec(String(name));
      if (match) return match[1];
    }
    return null;
  }

  /** Source node id of a Drawflow connection element from its class list. */
  function sourceNodeId(classList) {
    for (const name of classList || []) {
      const match = /^node_out_node-(.+)$/.exec(String(name));
      if (match) return match[1];
    }
    return null;
  }

  /**
   * Decide the wire state from the two endpoint states: a wire into a running
   * node is "running"; a wire between two finished nodes is "done"; anything
   * else keeps the default look.
   */
  function wireState(sourceState, targetState) {
    if (targetState === 'running') return RUNNING_CLASS;
    if (sourceState === 'done' && targetState === 'done') return DONE_CLASS;
    return '';
  }

  function install(options) {
    const canvas = options && options.canvas;
    if (!canvas || typeof MutationObserver === 'undefined') return null;

    function stateOf(id) {
      const node = document.getElementById('node-' + id);
      const label = node && node.querySelector('.nstate');
      if (!label) return '';
      if (label.classList.contains('running')) return 'running';
      if (label.classList.contains('done')) return 'done';
      if (label.classList.contains('failed')) return 'failed';
      return '';
    }

    let scheduled = false;
    function repaint() {
      scheduled = false;
      canvas.querySelectorAll('svg.connection').forEach(wire => {
        const wanted = wireState(stateOf(sourceNodeId(wire.classList)), stateOf(targetNodeId(wire.classList)));
        wire.classList.toggle(RUNNING_CLASS, wanted === RUNNING_CLASS);
        wire.classList.toggle(DONE_CLASS, wanted === DONE_CLASS);
      });
    }
    function schedule() {
      if (scheduled) return;
      scheduled = true;
      setTimeout(repaint, 60);
    }

    const observer = new MutationObserver(records => {
      for (const record of records) {
        if (record.type === 'childList') { schedule(); return; }
        const target = record.target;
        if (target && target.classList && target.classList.contains('nstate')) { schedule(); return; }
      }
    });
    observer.observe(canvas, {subtree:true, childList:true, attributes:true, attributeFilter:['class']});
    schedule();
    return {repaint, destroy() { observer.disconnect(); }};
  }

  global.AINodeWires = Object.freeze({install, wireState, targetNodeId, sourceNodeId,
    RUNNING_CLASS, DONE_CLASS});

  // Self-installing: the editor page has a static #canvas, and this module
  // must not depend on edits to the editor script.
  if (typeof document !== 'undefined' && typeof document.addEventListener === 'function') {
    document.addEventListener('DOMContentLoaded', () => {
      const canvas = document.getElementById('canvas');
      if (canvas) install({canvas});
    });
  }
})(typeof window !== 'undefined' ? window : globalThis);
