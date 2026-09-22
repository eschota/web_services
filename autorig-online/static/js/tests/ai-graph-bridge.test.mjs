import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import vm from 'node:vm';
import {fileURLToPath} from 'node:url';


const sourcePath = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', 'ai-graph-bridge.js');

function loadBridge() {
  const context = {window:{}, console, setTimeout, clearTimeout};
  vm.runInNewContext(fs.readFileSync(sourcePath, 'utf8'), context, {filename:sourcePath});
  return context.window.AIGraphBridge;
}

function fixture() {
  let name = 'Graph';
  const nodes = [{
    id:'1', kind:'input', entity_type:'text', value:'hello',
    x:0, y:0, params:{_display_mode:'medium'}
  }];
  const links = [];
  const results = {'1':{status:'done', type:'text', value:'hello'}};
  const data = {'1':{pos_x:0, pos_y:0}};

  const getGraph = () => ({
    name,
    nodes:nodes.map(node => ({...node, x:data[node.id].pos_x, y:data[node.id].pos_y,
                              params:{...(node.params || {})}})),
    links:links.map(link => ({...link})),
    results:{...results}
  });
  const editor = {
    module:'Home', drawflow:{drawflow:{Home:{data}}},
    updateConnectionNodes() {},
    removeNodeId(value) {
      const id = String(value).replace(/^node-/, '');
      const index = nodes.findIndex(node => node.id === id);
      if (index >= 0) nodes.splice(index, 1);
      delete data[id];
    },
    removeSingleConnection() {}, addConnection() {}
  };
  const addServiceNode = (service, x, y, params) => {
    const id = '2';
    // This mirrors Drawflow controls: a new node serialises every UI default,
    // and number/select controls are read back as strings in this fixture.
    nodes.push({id, kind:'service', service, x, y, params:{
      width:'960', height:'540', steps:'0', cfg:'0', checkpoint:'', ...(params || {})
    }});
    data[id] = {pos_x:x, pos_y:y};
    return id;
  };
  const applyParams = (id, params) => {
    const node = nodes.find(item => item.id === String(id));
    Object.entries(params || {}).forEach(([key, value]) => {
      node.params[key] = ['width','height','steps','cfg','seed'].includes(key)
        ? String(value) : value;
    });
  };
  const bridge = loadBridge().create({
    editor, getGraph, addServiceNode,
    addInputNode() { throw new Error('not used'); },
    applyParams, getMeta() { return {inFields:[], outFields:['image_url_string']}; },
    setGraphName(value) { name = value; },
    invalidateNodeAndDownstream() {}
  });
  return {bridge, getGraph, nodes};
}

test('new node accepts materialised defaults and numeric DOM strings', async () => {
  const {bridge, getGraph} = fixture();
  const base = getGraph();
  const validated = {
    name:'Graph',
    nodes:[...base.nodes, {
      id:'temporary-image', kind:'service', service:'image',
      entity_type:null, value:null, x:50, y:60, params:{width:1030, height:1527}
    }],
    links:[], results:{old:{status:'done'}}
  };
  const result = await bridge.applyGraph(validated, {
    baseGraph:base,
    operations:[{op:'add_node', node:validated.nodes[1]}],
    invalidatedIds:['temporary-image']
  });
  assert.equal(result.idMapping['temporary-image'], '2');
  const added = result.graph.nodes.find(node => node.id === '2');
  assert.equal(added.params.width, '1030');
  assert.equal(added.params.height, '1527');
  assert.equal(added.params.steps, '0');
  assert.deepEqual(result.graph.results, base.results, 'validated old results were not imported');
});

test('existing numeric parameter compares equal across number and DOM string forms', async () => {
  const {bridge, getGraph, nodes} = fixture();
  nodes.push({id:'2', kind:'service', service:'image', x:50, y:60,
              params:{width:'960', height:'540', steps:'0', cfg:'0', checkpoint:''}});
  // The fake Drawflow store belongs to the fixture; creating through the
  // helper ensures it receives a matching position row.
  nodes.pop();
  const validatedAdd = {
    name:'Graph', nodes:[...getGraph().nodes, {id:'tmp', kind:'service', service:'image',
      x:50, y:60, params:{width:960}}], links:[]
  };
  const first = await bridge.applyGraph(validatedAdd, {
    baseGraph:getGraph(), operations:[{op:'add_node', node:validatedAdd.nodes[1]}], invalidatedIds:[]
  });
  const baseWithNumericSnapshot = JSON.parse(JSON.stringify(first.graph));
  baseWithNumericSnapshot.nodes.find(node => node.id === '2').params.width = 960;
  const validatedUpdate = JSON.parse(JSON.stringify(baseWithNumericSnapshot));
  validatedUpdate.nodes.find(node => node.id === '2').params.width = 1030;
  const second = await bridge.applyGraph(validatedUpdate, {
    baseGraph:baseWithNumericSnapshot,
    operations:[{op:'update_params', id:'2', values:{width:1030}}], invalidatedIds:['2']
  });
  assert.equal(second.graph.nodes.find(node => node.id === '2').params.width, '1030');
});
