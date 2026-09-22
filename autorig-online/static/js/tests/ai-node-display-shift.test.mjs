import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import vm from 'node:vm';
import {fileURLToPath} from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const sourcePath = path.join(here, '..', 'ai-node-display.js');
const source = fs.readFileSync(sourcePath, 'utf8');

/**
 * Just enough DOM for the display module: class and id matching, direct-child
 * selectors and the handful of element properties it writes. A real browser is
 * not needed to prove that one Shift-click reaches every node.
 */
function matches(element, token) {
  let rest = String(token).trim();
  const attribute = /\[id\^="([^"]+)"\]/.exec(rest);
  if (attribute) {
    if (!String(element.id).startsWith(attribute[1])) return false;
    rest = rest.replace(attribute[0], '');
  }
  if (rest.startsWith('#')) return element.id === rest.slice(1);
  return rest.split('.').filter(Boolean)
    .every(name => element.classes.includes(name));
}

class Element {
  constructor(tag, options) {
    options = options || {};
    this.nodeType = 1;
    this.tagName = String(tag).toUpperCase();
    this.id = options.id || '';
    this.classes = (options.className || '').split(' ').filter(Boolean);
    this.children = [];
    this.dataset = {};
    this.attributes = {};
    this.textContent = '';
    this.title = '';
    this.type = '';
    this.open = false;
    this.handlers = new Map();
    this.style = {
      values: {},
      setProperty: (name, value) => { this.style.values[name] = value; }
    };
  }
  get className() { return this.classes.join(' '); }
  set className(value) { this.classes = String(value).split(' ').filter(Boolean); }
  appendChild(child) { this.children.push(child); child.parent = this; return child; }
  setAttribute(name, value) { this.attributes[name] = String(value); }
  getAttribute(name) { return name in this.attributes ? this.attributes[name] : null; }
  addEventListener(type, handler) {
    if (!this.handlers.has(type)) this.handlers.set(type, []);
    this.handlers.get(type).push(handler);
  }
  removeEventListener() {}
  dispatch(type, event) {
    (this.handlers.get(type) || []).forEach(handler => handler(event));
  }
  matches(selector) { return matches(this, selector); }
  contains(other) {
    if (other === this) return true;
    return this.children.some(child => child.contains(other));
  }
  querySelectorAll(selector) {
    const scoped = selector.startsWith(':scope');
    const steps = selector.replace(/^:scope\s*>\s*/, '').split('>').map(step => step.trim());
    if (scoped) {
      let level = [this];
      steps.forEach(step => {
        level = level.reduce((found, node) =>
          found.concat(node.children.filter(child => matches(child, step))), []);
      });
      return level;
    }
    const found = [];
    const walk = node => node.children.forEach(child => {
      if (matches(child, steps[steps.length - 1])) found.push(child);
      walk(child);
    });
    walk(this);
    return found;
  }
  querySelector(selector) { return this.querySelectorAll(selector)[0] || null; }
}

function buildSandbox(nodeCount) {
  const canvas = new Element('div', {id: 'canvas'});
  const nodes = [];
  for (let index = 1; index <= nodeCount; index++) {
    const node = new Element('div', {id: 'node-' + index, className: 'drawflow-node ainode'});
    node.appendChild(new Element('div', {className: 'nhead'}));
    canvas.appendChild(node);
    nodes.push(node);
  }
  const head = new Element('head');
  const document = {
    head,
    getElementById: id => (id === 'canvas' ? canvas : null),
    createElement: tag => new Element(tag)
  };
  const sandbox = {
    window: {}, document, console,
    CSS: {escape: value => value},
    requestAnimationFrame: callback => { callback(); return 1; },
    setTimeout: callback => { callback(); return 1; },
    MutationObserver: class { observe() {} disconnect() {} },
    Object, Array, String, Number, Math, Set, Map, JSON
  };
  vm.runInNewContext(source, sandbox, {filename: sourcePath});
  return {sandbox, canvas, nodes};
}

function install(nodeCount, defaultMode) {
  const {sandbox, canvas, nodes} = buildSandbox(nodeCount);
  const changes = [];
  const editor = {
    zoom: 1,
    on() {},
    updateConnectionNodes() {}
  };
  const meta = new Map(nodes.map(node => [node.id.slice(5), {kind: 'service'}]));
  const display = sandbox.window.AINodeDisplay.install({
    editor, canvas,
    defaultMode: defaultMode || 'medium',
    getMeta: id => meta.get(String(id)) || null,
    onModeChange: (id, mode) => {
      changes.push([String(id), mode]);
      const item = meta.get(String(id));
      if (item) item.displayMode = mode;
    }
  });
  return {display, canvas, nodes, meta, changes};
}

function modes(nodes) {
  return nodes.map(node => node.dataset.displayMode);
}

function clickModeButton(node, shiftKey) {
  const button = node.querySelector('.node-display-mode');
  assert.ok(button, 'every node header must carry a display-mode button');
  button.dispatch('click', {
    shiftKey: !!shiftKey,
    preventDefault() {}, stopPropagation() {}
  });
  return button;
}

test('a plain click changes only the node that was clicked', () => {
  const {nodes, changes} = install(4);
  assert.deepEqual(modes(nodes), ['medium', 'medium', 'medium', 'medium']);
  clickModeButton(nodes[1], false);
  assert.deepEqual(modes(nodes), ['medium', 'all', 'medium', 'medium']);
  assert.deepEqual(changes, [['2', 'all']]);
});

test('Shift-click applies the clicked node\'s next mode to every node in the graph', () => {
  const {nodes, changes} = install(5);
  clickModeButton(nodes[2], true);
  assert.deepEqual(modes(nodes), ['all', 'all', 'all', 'all', 'all']);
  assert.deepEqual(changes.map(entry => entry[0]), ['1', '2', '3', '4', '5']);
  assert.ok(changes.every(entry => entry[1] === 'all'));
});

test('Shift-click reports the new mode for every node so metadata stays in step', () => {
  const {nodes, meta, changes} = install(3);
  clickModeButton(nodes[0], true);          // medium -> all
  changes.length = 0;
  clickModeButton(nodes[0], true);          // all -> small
  assert.deepEqual(modes(nodes), ['small', 'small', 'small']);
  assert.deepEqual(Array.from(meta.values(), item => item.displayMode),
    ['small', 'small', 'small']);
  assert.equal(changes.length, 3);
});

test('a graph whose nodes disagree is brought to one mode by a single Shift-click', () => {
  const {display, nodes} = install(4);
  display.setMode(nodes[0], 'small');
  display.setMode(nodes[3], 'all');
  assert.deepEqual(modes(nodes), ['small', 'medium', 'medium', 'all']);
  clickModeButton(nodes[0], true);          // the clicked node is small -> medium
  assert.deepEqual(modes(nodes), ['medium', 'medium', 'medium', 'medium']);
});

test('the mode wraps around and the host API can set every node directly', () => {
  const {display, nodes} = install(2, 'all');
  clickModeButton(nodes[0], true);
  assert.deepEqual(modes(nodes), ['small', 'small']);
  clickModeButton(nodes[0], true);
  assert.deepEqual(modes(nodes), ['medium', 'medium']);
  display.setModeAll('all');
  assert.deepEqual(modes(nodes), ['all', 'all']);
  assert.equal(display.getMode(nodes[1]), 'all');
});

test('the button says that Shift reaches the whole graph', () => {
  const {nodes} = install(1);
  const button = nodes[0].querySelector('.node-display-mode');
  assert.match(button.title, /Shift/);
  assert.match(button.getAttribute('aria-label'), /Shift/);
});
