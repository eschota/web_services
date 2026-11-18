const EPS = 1e-4;
const cross = (a, b) => a[0] * b[1] - a[1] * b[0];
const between = (from, to) => [to[0] - from[0], to[1] - from[1]];
const length = (a, b) => Math.hypot(...between(a, b));
const clamp = (value, low, high) => Math.max(low, Math.min(high, value));
const pointAt = (edge, t) => [edge.a[0] + (edge.b[0] - edge.a[0]) * t, edge.a[1] + (edge.b[1] - edge.a[1]) * t];
const verticesOf = ([left, top, right, bottom]) => [[left,top], [right,top], [right,bottom], [left,bottom]];
const outline = room => room.polygon_px?.length >= 3 ? room.polygon_px : verticesOf(room.bbox_px);
const keyOf = point => point.map(value => Math.round(value / EPS)).join(':');
const span = (a, b) => Math.abs(a - b) < EPS;
const boundsOf = points => {
  const x = points.map(point => point[0]);
  const y = points.map(point => point[1]);
  return [Math.min(...x), Math.min(...y), Math.max(...x), Math.max(...y)];
};

export function inPolygon([x, y], points) {
  let inside = false;
  for (let i = 0, j = points.length - 1; i < points.length; j = i++) {
    const [ax, ay] = points[i], [bx, by] = points[j];
    if ((ay > y) !== (by > y) && x < (bx - ax) * (y - ay) / (by - ay) + ax) inside = !inside;
  }
  return inside;
}

function segment(wall) {
  return { a: [wall.x1, wall.y1], b: [wall.x2, wall.y2], wall };
}

function sideEdges(room) {
  const points = outline(room);
  const sides = ['top', 'right', 'bottom', 'left'];
  return points.map((a, i) => ({
    a, b: points[(i + 1) % points.length], room,
    type: room.polygon_px ? room.edge_wall_types?.[i] : room.wall_types?.[sides[i]],
    thickness_cm: room.polygon_px ? room.edge_wall_thickness_cm?.[i] : room.wall_thickness_cm?.[sides[i]],
  }));
}

function parameter(point, edge) {
  const [dx, dy] = between(edge.a, edge.b);
  return ((point[0] - edge.a[0]) * dx + (point[1] - edge.a[1]) * dy) / (dx * dx + dy * dy);
}

export function snapToWalls(point, walls, tolerance) {
  let best = point;
  let distance = tolerance;
  for (const wall of walls) {
    const edge = segment(wall);
    if (length(edge.a, edge.b) < EPS) continue;
    const t = clamp(parameter(point, edge), 0, 1);
    const projection = pointAt(edge, t);
    const gap = length(point, projection);
    if (gap < distance) {
      best = projection;
      distance = gap;
    }
  }
  return best;
}

function isOnLine(point, edge) {
  return Math.abs(cross(between(edge.a, edge.b), between(edge.a, point))) <= EPS * length(edge.a, edge.b);
}

function splitCandidates(room, wall, tolerance) {
  if (room.polygon_px || room.schematic || room.is_heated === false) return null;
  const [l,t,r,b] = room.bbox_px;
  const horizontal = span(wall.y1, wall.y2);
  const vertical = span(wall.x1, wall.x2);
  const requireMargin = (value, min, max) => value > min + tolerance * 2 && value < max - tolerance * 2;
  const matches = (first, last, min, max) =>
    Math.abs(Math.min(first,last) - min) <= tolerance && Math.abs(Math.max(first,last) - max) <= tolerance;
  if (horizontal && requireMargin(wall.y1,t,b) && matches(wall.x1,wall.x2,l,r)) {
    return { ratio:(wall.y1-t)/(b-t), childBoxes:[[l,t,r,wall.y1],[l,wall.y1,r,b]],
      retained:['top','bottom'], divided:['left','right'], dimension:'v' };
  }
  if (vertical && requireMargin(wall.x1,l,r) && matches(wall.y1,wall.y2,t,b)) {
    return { ratio:(wall.x1-l)/(r-l), childBoxes:[[l,t,wall.x1,b],[wall.x1,t,r,b]],
      retained:['left','right'], divided:['top','bottom'], dimension:'u' };
  }
  return null;
}

function remap(value, ratio, child) {
  return clamp(child ? (value - ratio) / (1 - ratio) : value / ratio, 0, 1);
}

function divideRoom(room, wall, split, id) {
  const { ratio, childBoxes, retained, divided, dimension } = split;
  const answer = childBoxes.map((bbox_px, child) => {
    const outer = retained[child];
    const inner = retained[1 - child];
    const keep = [outer, ...divided];
    const wall_types = Object.fromEntries(keep.filter(side => room.wall_types?.[side]).map(side => [side, room.wall_types[side]]));
    wall_types[inner] = wall.type;
    const wall_thickness_cm = Object.fromEntries(keep.filter(side => room.wall_thickness_cm?.[side] != null)
      .map(side => [side, room.wall_thickness_cm[side]]));
    wall_thickness_cm[inner] = wall.thickness_cm;
    const raw = key => room[key]?.[outer] != null ? { [outer]: room[key][outer] } : {};
    return {
      ...room, id: child ? id('room') : room.id, bbox_px,
      verified: false, raw_measurements_cm: raw('raw_measurements_cm'),
      raw_measurement_boxes_px: raw('raw_measurement_boxes_px'),
      measurement_bboxes_px: raw('measurement_bboxes_px'),
      measurement_confidences: raw('measurement_confidences'),
      wall_types, wall_thickness_cm, openings: [], symbols: [],
    };
  });
  let ambiguous = false;
  for (const opening of room.openings || []) {
    if (retained.includes(opening.wall)) {
      answer[retained.indexOf(opening.wall)].openings.push({ ...opening });
      continue;
    }
    const oldMeasurement = room.raw_measurements_cm?.[opening.wall];
    const width = opening.width_cm || 0;
    const halfWidth = oldMeasurement > 0 ? width / oldMeasurement / 2 : 0.12;
    let position = opening.t ?? 0.5;
    if (oldMeasurement > 0 && opening.offset_cm != null) position = opening.offset_cm / oldMeasurement + halfWidth;
    else if (oldMeasurement > 0 && opening.offset_right_cm != null) position = 1 - opening.offset_right_cm / oldMeasurement - halfWidth;
    const child = +(position > ratio);
    const next = { ...opening, t: remap(position, ratio, child) };
    if (child) delete next.offset_cm;
    else delete next.offset_right_cm;
    if (Math.abs(position - ratio) < halfWidth || oldMeasurement == null) {
      next.confidence = Math.min(next.confidence ?? 1, 0.65);
      next.verified = false;
      ambiguous = true;
    }
    answer[child].openings.push(next);
  }
  for (const symbol of room.symbols || []) {
    const position = symbol[dimension] ?? 0.5;
    const child = +(position > ratio);
    answer[child].symbols.push({ ...symbol, [dimension]: remap(position, ratio, child) });
  }
  return { answer, ambiguous };
}

function connection(a, b) {
  const vector = between(a.a, a.b);
  const opposite = between(b.a, b.b);
  const offset = between(a.a, b.a);
  const denom = cross(vector, opposite);
  if (Math.abs(denom) > EPS) {
    const t = cross(offset, opposite) / denom, u = cross(offset, vector) / denom;
    if (t >= -EPS && u >= -EPS && t <= 1 + EPS && u <= 1 + EPS) {
      a.cuts.push(clamp(t, 0, 1));
      b.cuts.push(clamp(u, 0, 1));
    }
    return;
  }
  if (!isOnLine(b.a, a)) return;
  for (const p of [a.a, a.b]) {
    const t = parameter(p, b);
    if (t > 0 && t < 1) b.cuts.push(t);
  }
  for (const p of [b.a, b.b]) {
    const t = parameter(p, a);
    if (t > 0 && t < 1) a.cuts.push(t);
  }
}

function possibleIntersection(a, b) {
  if (a.right + EPS < b.left || b.right + EPS < a.left) return false;
  return a.bottom + EPS >= b.top && b.bottom + EPS >= a.top;
}

function graphFor(segments) {
  const edges = segments.filter(edge => length(edge.a, edge.b) > EPS).map(edge => {
    const [left, top, right, bottom] = boundsOf([edge.a, edge.b]);
    return { ...edge, left, top, right, bottom, cuts: [0, 1] };
  });
  for (let i = 0; i < edges.length; i++) {
    for (let j = i + 1; j < edges.length; j++) if (possibleIntersection(edges[i], edges[j])) connection(edges[i], edges[j]);
  }
  const vertices = new Map();
  const vertex = point => {
    const key = keyOf(point);
    if (!vertices.has(key)) vertices.set(key, { key, point, arcs: new Map() });
    return vertices.get(key);
  };
  const connect = (start, end) => {
    if (!start.arcs.has(end.key)) start.arcs.set(end.key, {
      start, end, angle: Math.atan2(end.point[1] - start.point[1], end.point[0] - start.point[0]), visited: false,
    });
  };
  for (const edge of edges) {
    edge.cuts.sort((a,b) => a-b);
    for (let i = 0; i < edge.cuts.length - 1; i++) {
      const start = vertex(pointAt(edge, edge.cuts[i]));
      const end = vertex(pointAt(edge, edge.cuts[i+1]));
      if (start !== end) {
        connect(start, end);
        connect(end, start);
      }
    }
  }
  for (const v of vertices.values()) v.order = [...v.arcs.values()].sort((a,b) => a.angle - b.angle);
  return { edges, vertices };
}

function simplePolygon(points) {
  let output = points;
  // Cut dead-end trips when a wall ends inside a closed room.
  while (output.length > 3) {
    const spike = output.findIndex((point, i) => keyOf(point) === keyOf(output[(i+2)%output.length]));
    if (spike < 0) break;
    const rotate = output.slice(spike).concat(output.slice(0,spike));
    output = [rotate[0], ...rotate.slice(3)];
  }
  return output.filter((point, i, nodes) => {
    const previous = nodes[(i+nodes.length-1)%nodes.length], next = nodes[(i+1)%nodes.length];
    return Math.abs(cross(between(previous, point), between(point, next))) > EPS;
  });
}

function boundedFaces(vertices, minArea) {
  const faces = [];
  for (const start of vertices.values()) {
    for (const arc of start.order) {
      if (arc.visited) continue;
      const points = [];
      let item = arc;
      while (!item.visited) {
        item.visited = true;
        points.push(item.start.point);
        const { order } = item.end;
        const back = order.findIndex(candidate => candidate.end === item.start);
        item = order[(back + order.length - 1) % order.length];
      }
      if (item !== arc || points.length < 3) continue;
      const polygon = simplePolygon(points);
      const area = polygon.reduce((sum, point, i) => sum + cross(point, polygon[(i+1)%polygon.length]), 0) / 2;
      if (area > minArea) faces.push(polygon);
    }
  }
  return faces;
}

function innerPoint(points) {
  // Follow an interior edge normal so concave polygons have an interior witness.
  for (let i = 0; i < points.length; i++) {
    const a = points[i], b = points[(i+1)%points.length];
    const [dx,dy] = between(a,b), divisor = Math.hypot(dx,dy);
    const point = [(a[0]+b[0])/2 - dy*.01/divisor, (a[1]+b[1])/2 + dx*.01/divisor];
    if (inPolygon(point, points)) return point;
  }
  return points[0];
}

function segmentMatch(edge, boundary) {
  return isOnLine(edge.a, boundary) && isOnLine(edge.b, boundary) &&
    Math.min(parameter(edge.a,boundary),parameter(edge.b,boundary)) < 1 - EPS &&
    Math.max(parameter(edge.a,boundary),parameter(edge.b,boundary)) > EPS;
}

function faceRoom(polygon, allEdges, id) {
  const bbox_px = boundsOf(polygon);
  const rectangle = verticesOf(bbox_px);
  const isRect = polygon.length === 4 && rectangle.every(corner => polygon.some(point => length(point,corner) < EPS));
  const room = { id:id('room'), bbox_px, raw_measurements_cm:{}, is_heated:true,
    confidence:1, verified:false, openings:[], symbols:[], wall_types:{}, wall_thickness_cm:{} };
  if (!isRect) {
    room.polygon_px = polygon;
    room.edge_wall_types = [];
    room.edge_wall_thickness_cm = [];
    for (const [index,a] of polygon.entries()) {
      const side = {a,b:polygon[(index + 1) % polygon.length]};
      const owner = allEdges.find(edge => segmentMatch(edge,side));
      room.edge_wall_types[index] = owner?.wall?.type ?? owner?.type ?? null;
      room.edge_wall_thickness_cm[index] = owner?.wall?.thickness_cm ?? owner?.thickness_cm ?? null;
    }
    return room;
  }
  const sides = ['top','right','bottom','left'];
  for (let i = 0; i < 4; i++) {
    const faceEdge = { a:rectangle[i], b:rectangle[(i+1)%4] };
    const owner = allEdges.find(edge => segmentMatch(edge, faceEdge));
    const type = owner?.wall?.type ?? owner?.type;
    const thickness = owner?.wall?.thickness_cm ?? owner?.thickness_cm;
    if (type) room.wall_types[sides[i]] = type;
    if (thickness != null) room.wall_thickness_cm[sides[i]] = thickness;
  }
  return room;
}

function removeCoveredWalls(edges, roomEdges, id) {
  const wallEdges = edges.filter(edge => edge.wall);
  const walls = [];
  for (const edge of wallEdges) {
    const intervals = [];
    for (const target of roomEdges) {
      if (!segmentMatch(edge, target)) continue;
      const a = clamp(parameter(target.a,edge), 0, 1), b = clamp(parameter(target.b,edge), 0, 1);
      intervals.push([Math.min(a,b), Math.max(a,b)]);
    }
    if (!intervals.length) {
      walls.push(edge.wall);
      continue;
    }
    intervals.sort((a,b) => a[0]-b[0]);
    let cursor = 0;
    let reused = false;
    const retain = (start, end) => {
      const [x1,y1] = pointAt(edge,start), [x2,y2] = pointAt(edge,end);
      walls.push({ ...edge.wall, id:reused ? id('wall') : edge.wall.id, x1,y1,x2,y2 });
      reused = true;
    };
    for (const [a,b] of intervals) {
      if (a > cursor + EPS) retain(cursor,a);
      cursor = Math.max(cursor,b);
    }
    if (cursor < 1 - EPS) retain(cursor,1);
  }
  return walls;
}

/** Add a wall, materializing affected bounded rooms without inventing survey measurements. */
export function insertWallTopology(floor, wall, id, tolerance) {
  const rooms = floor.rooms || [];
  const free = floor.walls || [];
  for (const room of rooms) {
    const split = splitCandidates(room, wall, tolerance);
    if (!split) continue;
    const { answer, ambiguous } = divideRoom(room, wall, split, id);
    return { rooms:rooms.flatMap(item => item === room ? answer : [item]), walls:free,
      created:answer, replaced:[room], affectedWalls:[], ambiguous };
  }
  const walls = [...free, wall];
  if (wall.type === 'AUX' || walls.length < 3 || walls.length > 350) {
    return { rooms, walls, created:[], replaced:[], affectedWalls:[] };
  }
  const existing = rooms.map(outline);
  const wallsForGraph = walls.filter(item => item.type !== 'AUX');
  const allEdges = wallsForGraph.map(segment).concat(rooms.flatMap(sideEdges));
  const { edges, vertices } = graphFor(allEdges);
  const faces = boundedFaces(vertices, tolerance*tolerance*4).filter(points => {
    const inside = innerPoint(points);
    return !existing.some(polygon => inPolygon(inside, polygon));
  });
  if (!faces.length) return { rooms, walls, created:[], replaced:[], affectedWalls:[] };
  const created = faces.map(points => faceRoom(points, edges, id));
  const retained = removeCoveredWalls(edges, created.flatMap(sideEdges), id);
  return { rooms:[...rooms,...created], walls:[...retained,...walls.filter(segment => segment.type === 'AUX')],
    created, replaced:[], affectedWalls:wallsForGraph.filter(item => !retained.includes(item)) };
}
