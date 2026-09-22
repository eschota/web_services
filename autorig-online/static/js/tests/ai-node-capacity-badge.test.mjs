/**
 * The number in a node header: how many computers can run this job now.
 *
 * The badge is read at a glance and acted on — somebody picks a video model
 * and wants to know whether that leaves one box or four — so what it says has
 * to follow the fleet's own capacity answer exactly, including the case where
 * a chosen checkpoint narrows it.
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';
import vm from 'node:vm';
import {fileURLToPath} from 'node:url';
import path from 'node:path';

const here = path.dirname(fileURLToPath(import.meta.url));
const sourcePath = path.resolve(here, '..', 'ai-nodes.js');

/* A DOM small enough to be obvious and real enough to hold a header. */
function element(tag, className) {
  return {
    tagName: tag, className: className || '', textContent: '', title: '',
    id: '', dataset: {}, value: '', children: [], parent: null,
    classList: {
      set: new Set(),
      toggle(name, on) { if (on) this.set.add(name); else this.set.delete(name); },
      contains(name) { return this.set.has(name); }
    },
    appendChild(child) { child.parent = this; this.children.push(child); return child; },
    remove() {
      if (this.parent) {
        this.parent.children = this.parent.children.filter(item => item !== this);
      }
    },
    querySelector(selector) {
      const matches = item => {
        if (selector.startsWith('.')) return item.className.split(' ').includes(selector.slice(1));
        if (selector === '[data-param="checkpoint"]') return item.dataset.param === 'checkpoint';
        return false;
      };
      for (const child of this.children) {
        if (matches(child)) return child;
        const deeper = child.querySelector(selector);
        if (deeper) return deeper;
      }
      return null;
    }
  };
}

function load(nodes) {
  const source = fs.readFileSync(sourcePath, 'utf8');
  const start = source.indexOf('  let fleetCapacity = null;');
  const end = source.indexOf('  function startCapacityBadges() {');
  assert.ok(start > 0 && end > start, 'the capacity badge block is no longer there');
  const context = {
    KIND_SERVICE: 'service',
    nodeMeta: new Map(Object.entries(nodes).map(([id, node]) => [id, node.meta])),
    meta: id => (nodes[id] || {}).meta,
    nodeElement: id => (nodes[id] || {}).element,
    document: {createElement: tag => element(tag)},
    window: {},
    setTimeout() {}, clearTimeout() {}, setInterval() {}
  };
  const api = vm.runInNewContext(
    source.slice(start, end) +
    ';({capacityFor, capacityTitle, applyCapacityBadge, paintCapacityBadges,' +
    ' load: value => { fleetCapacity = value; }})', context);
  return api;
}

function serviceNode(serviceId, checkpointValue) {
  const node = element('div', 'ainode svc-' + serviceId);
  const head = node.appendChild(element('div', 'nhead'));
  head.appendChild(element('b'));
  if (checkpointValue !== undefined) {
    const picker = node.appendChild(element('input'));
    picker.dataset.param = 'checkpoint';
    picker.value = checkpointValue;
  }
  return node;
}

const CAPACITY = {
  video: {
    kind_string: 'comfy', total_int: 4, idle_int: 3,
    computers_array: ['f12', 'f15', 'f5', 'Raptor'],
    idle_array: ['f12', 'f5', 'Raptor'],
    checkpoints_object: {
      'ltx-2.3-22b.safetensors': {
        total_int: 1, idle_int: 0, computers_array: ['worker-4090'], idle_array: []
      }
    }
  },
  vision: {kind_string: 'ai', total_int: 2, idle_int: 2, computers_array: ['f13', 'f2'],
           checkpoints_object: {}},
  video_frame: {kind_string: 'local', total_int: 1, idle_int: 1,
                computers_array: ['this server'], checkpoints_object: {}},
  upscale_video: {kind_string: 'comfy', total_int: 0, idle_int: 0,
                  computers_array: [], checkpoints_object: {}}
};


test('the badge counts the computers that can run the service', () => {
  const node = serviceNode('vision');
  const api = load({'7': {meta: {kind: 'service', service: 'vision'}, element: node}});
  api.load(CAPACITY);
  assert.equal(api.applyCapacityBadge('7'), true);
  const badge = node.querySelector('.ncap');
  assert.equal(badge.textContent, '2');
  assert.equal(badge.title, '2 computers can run this now: f13, f2; 2 idle');
  assert.ok(badge.classList.contains('free'));
  assert.ok(!badge.classList.contains('none'));
});


test('choosing a checkpoint narrows the number to the boxes holding it', () => {
  const node = serviceNode('video', '');
  const api = load({'3': {meta: {kind: 'service', service: 'video'}, element: node}});
  api.load(CAPACITY);
  api.applyCapacityBadge('3');
  assert.equal(node.querySelector('.ncap').textContent, '4');

  // The same node, one model later: four computers become one.
  node.querySelector('[data-param="checkpoint"]').value = 'ltx-2.3-22b.safetensors';
  api.applyCapacityBadge('3');
  const badge = node.querySelector('.ncap');
  assert.equal(badge.textContent, '1');
  assert.equal(badge.title,
    '1 computer can run this now with ltx-2.3-22b.safetensors: worker-4090; 0 idle');
  // Capable but every one of them busy: the number is not a free count.
  assert.ok(!badge.classList.contains('free'));
});


test('nothing to run it on is said in words, not left as a zero', () => {
  const node = serviceNode('upscale_video');
  const api = load({'9': {meta: {kind: 'service', service: 'upscale_video'}, element: node}});
  api.load(CAPACITY);
  api.applyCapacityBadge('9');
  const badge = node.querySelector('.ncap');
  assert.equal(badge.textContent, '0');
  assert.ok(badge.classList.contains('none'));
  assert.equal(badge.title, 'No computer can run this right now.');
});


test('a service that runs on this host carries no farm badge at all', () => {
  const node = serviceNode('video_frame');
  const api = load({'1': {meta: {kind: 'service', service: 'video_frame'}, element: node}});
  api.load(CAPACITY);
  assert.equal(api.applyCapacityBadge('1'), false);
  assert.equal(node.querySelector('.ncap'), null);
});


test('an unknown service and a fleet that has not answered leave the header alone', () => {
  const node = serviceNode('qwen_image');
  const api = load({'2': {meta: {kind: 'service', service: 'qwen_image'}, element: node}});
  assert.equal(api.applyCapacityBadge('2'), false);
  api.load(CAPACITY);
  assert.equal(api.applyCapacityBadge('2'), false);
  assert.equal(node.querySelector('.ncap'), null);
});


test('every service node on the canvas is painted in one pass', () => {
  const vision = serviceNode('vision');
  const video = serviceNode('video', '');
  const input = serviceNode('text');
  const api = load({
    a: {meta: {kind: 'service', service: 'vision'}, element: vision},
    b: {meta: {kind: 'service', service: 'video'}, element: video},
    c: {meta: {kind: 'input', entityType: 'text'}, element: input}
  });
  api.load(CAPACITY);
  api.paintCapacityBadges();
  assert.equal(vision.querySelector('.ncap').textContent, '2');
  assert.equal(video.querySelector('.ncap').textContent, '4');
  // An input node has no service and therefore no number.
  assert.equal(input.querySelector('.ncap'), null);
});
