/**
 * What the fleet strip says about the queue, and the one control in it.
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';
import vm from 'node:vm';
import {fileURLToPath} from 'node:url';
import path from 'node:path';

const here = path.dirname(fileURLToPath(import.meta.url));
const sourcePath = path.join(here, '..', 'ai-entities.js');

function load(pathname = '/nodes') {
  const source = fs.readFileSync(sourcePath, 'utf8');
  const style = {textContent: ''};
  const context = {
    window: {location: {pathname}},
    document: {
      getElementById: () => ({}),          // styles are considered injected
      createElement: () => style,
      head: {appendChild() {}}
    },
    location: {pathname},
    fetch: () => Promise.reject(new Error('no network in a test')),
    setInterval: () => 0, clearInterval: () => {}, setTimeout: () => 0
  };
  context.window.document = context.document;
  context.globalThis = context;
  vm.runInNewContext(source, context, {filename: sourcePath});
  return context.window.AIEntities;
}

test('a place in the queue is a rank out of a total', () => {
  const api = load();
  assert.equal(api.queuePlaceLabel(7, 21), '#7 of 21');
  assert.equal(api.queuePlaceLabel(1, 1), '#1 of 1');
});

test('nothing is claimed about a job that is not waiting', () => {
  const api = load();
  assert.equal(api.queuePlaceLabel(0, 21), '');
  assert.equal(api.queuePlaceLabel(undefined, undefined), '');
  assert.equal(api.queuePlaceLabel(null, 4), '');
});

test('a total that cannot be right is left off rather than shown', () => {
  const api = load();
  // The length is measured a moment apart from the rank, so it can lag.
  assert.equal(api.queuePlaceLabel(9, 4), '#9');
  assert.equal(api.queuePlaceLabel(3, 0), '#3');
});

test('only an admin is offered the button, and it says how much it would stop', () => {
  const api = load();
  const markup = api.queueAdminMarkup({admin_bool: true, queue_object: {queued_int: 12}});
  assert.match(markup, /class="fleet-clear"/);
  assert.match(markup, /Clear queue \(12\)/);
  assert.doesNotMatch(markup, /disabled/);
});

test('with an empty queue the button is there but has nothing to do', () => {
  const api = load();
  const markup = api.queueAdminMarkup({admin_bool: true, queue_object: {queued_int: 0}});
  assert.match(markup, /disabled/);
});

test('a signed-out owner on the editor page is told where the button went', () => {
  const api = load('/nodes');
  const markup = api.queueAdminMarkup({admin_bool: false, queue_object: {queued_int: 12}});
  assert.doesNotMatch(markup, /fleet-clear/);
  assert.match(markup, /\/auth\/login/);
});

test('everywhere else, a visitor is shown nothing about clearing queues', () => {
  const api = load('/image');
  assert.equal(api.queueAdminMarkup({admin_bool: false, queue_object: {queued_int: 12}}), '');
  // An admin still gets it: the control is not specific to the editor.
  assert.match(api.queueAdminMarkup({admin_bool: true, queue_object: {queued_int: 2}}),
    /fleet-clear/);
});
