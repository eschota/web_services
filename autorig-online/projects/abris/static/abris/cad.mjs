import {
  boundsOfPoints, mapPolygonPoint, pointInPolygon, polygonIntersectionArea, polygonLabelPoint,
  signedArea, solveMeasuredPolygon, usesMeasuredPolygon,
} from "./polygon.mjs";

const SIDES = ["top", "right", "bottom", "left"];
const MIN_MEASUREMENT_CM = 15;
const MAX_MEASUREMENT_CM = 10000;
const MAX_ROOMS = 120;
const EPSILON = 0.000001;
const LAYERS = {
  WALL_MAIN: "Капитальная стена",
  WALL_PART: "Перегородка",
  VERANDA: "Открытая терраса",
  WINDOWS: "Окно",
  DOORS: "Дверной проём",
  DOOR_GLAZED: "Остеклённая дверь",
  STAIRS: "Лестница",
  EQUIPMENT: "Оборудование",
  COLUMNS: "Колонна",
  TEXT_DIM: "Линейный размер",
  TEXT_ROOM: "Номер помещения",
  LEGEND: "Условные обозначения",
};
const OPENING_TYPES = new Set(["WINDOW_STD", "DOOR_SINGLE", "DOOR_DOUBLE", "DOOR_GLAZED", "ARCH"]);
const WALL_TYPES = new Set(["WALL_CAPITAL", "WALL_PARTITION", "VERANDA", "AUX"]);
const A4 = [210, 297];
const FRAME = { x: 10, y: 10, width: 190, height: 277 };
const PLAN = { x: 19, y: 48, width: 172, height: 182 };
const PDF_DPI = 300;

const clamp = (value, minimum, maximum) => Math.max(minimum, Math.min(maximum, value));
const finite = (value) => (
  typeof value === "number" && Number.isFinite(value) ? value : null
);
const validRange = (value, minimum, maximum) => (
  finite(value) !== null && minimum <= value && value <= maximum
);
const coordinate = (value) => Math.round((value + EPSILON) * 100) / 100;
const display = (value) => value.toFixed(2);
const mm = (value) => Math.round(value * 1000000) / 1000;
const average = (values) => values.reduce((total, value) => total + value, 0) / values.length;
const lowConfidence = (value) => finite(value) !== null && value >= 0 && value < 0.85;
const safeText = (value) => String(value ?? "").replace(/[&<>"']/gu, (character) => (
  { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&apos;" }[character]
));

function median(values) {
  if (!values.length) return null;
  const sorted = [...values].sort((first, second) => first - second);
  const middle = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[middle] : average(sorted.slice(middle - 1, middle + 1));
}

function checkBBox(bounds) {
  return Array.isArray(bounds) && bounds.length === 4 && bounds.every((value) => finite(value) !== null)
    && bounds[2] > bounds[0] && bounds[3] > bounds[1];
}

function recordIssue(issues, severity, code, floor, room, detail, opening, side, objectId, location = {}) {
  const place = room ? `${floor.title}, ${room.number ? `пом. ${room.number}` : "открытый контур"}` : floor.title;
  issues.push({
    severity,
    code,
    floor_id: floor.floor_id,
    room_id: room?.id,
    opening_id: opening?.id,
    side,
    object_id: objectId,
    message: `${place}. ${detail}`,
    ...location,
  });
}

function readMeasurementPair(room, sides, floor, issues) {
  const values = [];
  const weights = [];
  for (const side of sides) {
    const value = room.raw_measurements_cm?.[side];
    if (value === null || value === undefined) continue;
    if (!validRange(value, MIN_MEASUREMENT_CM, MAX_MEASUREMENT_CM)) {
      recordIssue(issues, "error", "measurement-range", floor, room,
        `Промер должен быть от ${MIN_MEASUREMENT_CM} до ${MAX_MEASUREMENT_CM} см.`);
      continue;
    }
    values.push(value);
    const weight = room.measurement_weights?.[side];
    weights.push(validRange(weight, 0.01, 100) ? weight : 1);
  }
  const spread = values.length === 2 ? Math.abs(values[0] - values[1]) : 0;
  const weightedSum = values.reduce((total, value, index) => total + value * weights[index], 0);
  const totalWeight = weights.reduce((total, value) => total + value, 0);
  return { value: values.length ? coordinate(weightedSum / totalWeight / 100) : null, weight: totalWeight, spread };
}

function prepareRooms(floor, nextNumber, issues) {
  const inputs = Array.isArray(floor.rooms) ? floor.rooms.slice(0, MAX_ROOMS) : [];
  const measuredCalibration = validRange(floor.cm_per_px, 0.01, 30) ? floor.cm_per_px / 100 : null;
  const validBounds = inputs.map((room) => room?.bbox_px).filter(checkBBox);
  const outside = Math.max(0, ...validBounds.map((bounds) => bounds[2]));
  let cursor = outside + 50;
  let syntheticCount = 0;
  const rooms = [];
  for (const input of inputs.filter((value) => value && typeof value === "object")) {
    const room = {
      ...input,
      id: input.id ?? `room_${rooms.length + 1}`,
      number: input.is_heated === false || input.is_open === true ? null : nextNumber.value++,
      effective_thickness_m: {},
      wall_types: { ...input.wall_types },
    };
    if (lowConfidence(room.confidence) && room.verified !== true) {
      recordIssue(issues, "error", "room-confidence", floor, room,
        "Сверьте контур с абрисом и подтвердите помещение.");
    }
    if (usesMeasuredPolygon(room)) {
      const geometry = solveMeasuredPolygon(room);
      for (const { severity, code, message, edge_index, diagonal_index } of geometry.issues) {
        recordIssue(issues, severity, code, floor, room, message, null, undefined, undefined, { edge_index, diagonal_index });
      }
      if (room.polygon_px.every((point) => Array.isArray(point) && point.length === 2 && point.every(Number.isFinite))) {
        const bounds = boundsOfPoints(room.polygon_px);
        if (checkBBox(bounds)) room.bbox_px = bounds;
      }
      if (checkBBox(room.bbox_px)) {
        room.polygon_fit = geometry;
        room.discrepancy_cm = geometry.points ? Math.max(...geometry.residuals_cm.map(Math.abs)) : 0;
        room.measurements = [geometry.width, geometry.length].map((value) => ({ value: value ?? null, weight: 1, spread: 0 }));
        if (room.schematic) syntheticCount += 1;
        rooms.push(room);
        continue;
      }
    }
    const sideNames = { top: "сверху", right: "справа", bottom: "снизу", left: "слева" };
    for (const side of SIDES) {
      if (finite(room.raw_measurements_cm?.[side]) === null
          || !lowConfidence(room.measurement_confidences?.[side]) || room.verified === true) continue;
      recordIssue(issues, "error", "measurement-confidence", floor, room,
        `Сверьте распознанный промер ${sideNames[side]} с абрисом.`, null, side);
    }
    const horizontal = readMeasurementPair(room, ["top", "bottom"], floor, issues);
    const vertical = readMeasurementPair(room, ["left", "right"], floor, issues);
    room.discrepancy_cm = Math.max(horizontal.spread, vertical.spread);
    if (room.discrepancy_cm > 10) {
      recordIssue(issues, "error", "opposing-spread", floor, room,
        `Расхождение противоположных промеров ${room.discrepancy_cm.toFixed(1)} см. Проверьте исходные размеры.`);
    } else if (room.discrepancy_cm > 3) {
      recordIssue(issues, "warning", "opposing-spread", floor, room,
        `Расхождение противоположных промеров ${room.discrepancy_cm.toFixed(1)} см.`);
    }
    room.measurements = [horizontal, vertical];
    if (room.schematic && checkBBox(room.bbox_px)) syntheticCount += 1;
    if (!checkBBox(room.bbox_px)) {
      const width = (horizontal.value ?? 3) * 60;
      const height = (vertical.value ?? 3) * 60;
      room.bbox_px = [cursor, 35, cursor + width, 35 + height];
      cursor += width + 50;
      syntheticCount += 1;
    }
    for (const [index, measurement] of room.measurements.entries()) {
      if (measurement.value !== null) continue;
      if (measuredCalibration && checkBBox(input.bbox_px)) {
        measurement.value = coordinate((input.bbox_px[index + 2] - input.bbox_px[index]) * measuredCalibration);
        measurement.weight = 0.15;
        recordIssue(issues, "warning", "calibrated-dimension", floor, room,
          `Размер по ${index ? "вертикали" : "горизонтали"} взят из заданной калибровки.`);
      } else {
        recordIssue(issues, "error", "missing-measurement", floor, room,
          `Введите промер по ${index ? "вертикали" : "горизонтали"}.`);
      }
    }
    rooms.push(room);
  }
  if (syntheticCount) {
    recordIssue(issues, "error", "missing-layout", floor, null,
      "Нарисуйте расположение помещений. Для комнат без координат показана условная раскладка.");
  }
  if (floor.rooms?.length > MAX_ROOMS) {
    recordIssue(issues, "error", "room-limit", floor, null, `Поддерживается до ${MAX_ROOMS} помещений на этаж.`);
  }
  return rooms;
}

function roomScale(rooms, index) {
  return median(rooms.flatMap((room) => {
    const measured = room.measurements[index].value;
    const span = room.bbox_px[index + 2] - room.bbox_px[index];
    return measured !== null ? [measured / span] : [];
  }));
}

function solveLinear(matrix, vector) {
  // Symmetric positive definite normal equations. A small position prior anchors each component.
  for (let column = 0; column < vector.length; column += 1) {
    const diagonal = matrix[column][column];
    for (let row = column + 1; row < vector.length; row += 1) {
      if (!matrix[row][column]) continue;
      const factor = matrix[row][column] / diagonal;
      for (let index = column; index < vector.length; index += 1) {
        matrix[row][index] -= factor * matrix[column][index];
      }
      vector[row] -= factor * vector[column];
    }
  }
  for (let row = vector.length - 1; row >= 0; row -= 1) {
    for (let column = row + 1; column < vector.length; column += 1) {
      vector[row] -= matrix[row][column] * vector[column];
    }
    vector[row] /= matrix[row][row];
  }
  return vector;
}

function wallThickness(room, side, fallback) {
  const value = room.wall_thickness_cm?.[side];
  return validRange(value, 1, 150) ? value / 100 : fallback;
}

function sharedWall(first, second) {
  const types = [first.type, second.type];
  const main = types.includes("WALL_CAPITAL");
  const open = types.every((type) => type === "VERANDA" || type === "AUX");
  const explicit = [first, second].flatMap(({ thickness }) => {
    const value = thickness;
    return validRange(value, 1, 150) ? [value / 100] : [];
  });
  const thickness = open ? 0 : coordinate(explicit.length ? average(explicit) : main ? 0.28 : 0.12);
  return { type: open ? "VERANDA" : main ? "WALL_CAPITAL" : "WALL_PARTITION", thickness };
}

function polygonSideRange(room, axis, side) {
  if (!room.polygon_fit) return undefined;
  const points = room.polygon_fit.points;
  if (!points) return null;
  const coordinate = side ? (axis ? room.polygon_fit.length : room.polygon_fit.width) : 0;
  let longest = null;
  for (const [index, start] of points.entries()) {
    const next = (index + 1) % points.length;
    const end = points[next];
    if (Math.abs(start[axis] - coordinate) > EPSILON || Math.abs(end[axis] - coordinate) > EPSILON) continue;
    const [first, second] = [room.polygon_px[index][1 - axis], room.polygon_px[next][1 - axis]];
    if (!longest || Math.abs(second - first) > longest.range[1] - longest.range[0]) {
      longest = { range: [Math.min(first, second), Math.max(first, second)], index };
    }
  }
  return longest;
}

function solveAxis(rooms, axis, factor, tolerance, floor, issues) {
  const edges = rooms.flatMap((room) => {
    const perpendicular = 1 - axis;
    const names = axis ? ["top", "bottom"] : ["left", "right"];
    return names.map((side, localIndex) => {
      const outer = polygonSideRange(room, axis, localIndex);
      return {
        room,
        side,
        end: Boolean(localIndex),
        pixel: room.bbox_px[axis + localIndex * 2],
        perpendicular: outer?.range ?? [room.bbox_px[perpendicular], room.bbox_px[perpendicular + 2]],
        linkable: outer !== null,
        polygonIndex: outer?.index,
        type: outer ? room.edge_wall_types?.[outer.index] : room.wall_types[side],
        thickness: outer ? room.edge_wall_thickness_cm?.[outer.index] : room.wall_thickness_cm?.[side],
      };
    });
  });
  const parents = edges.map((_, index) => index);
  const delta = new Float64Array(edges.length);
  const anchors = (index) => {
    if (parents[index] === index) return index;
    const parent = parents[index];
    parents[index] = anchors(parent);
    delta[index] += delta[parent];
    return parents[index];
  };
  const constrain = (first, second, difference, sameRoom = false) => {
    const firstParent = anchors(first);
    const secondParent = anchors(second);
    if (firstParent === secondParent) return Math.abs(delta[second] - delta[first] - difference) < 0.011;
    if (!sameRoom && (anchors(first ^ 1) === secondParent || anchors(second ^ 1) === firstParent)) return false;
    parents[secondParent] = firstParent;
    delta[secondParent] = difference + delta[first] - delta[second];
    return true;
  };
  for (const [index, room] of rooms.entries()) {
    if (room.polygon_fit?.points) constrain(index * 2, index * 2 + 1, room.measurements[axis].value, true);
  }

  const alignments = [];
  const partitions = [];
  for (const [firstIndex, first] of edges.entries()) {
    for (let secondIndex = firstIndex + 1; secondIndex < edges.length; secondIndex += 1) {
      const second = edges[secondIndex];
      if (first.room === second.room || !first.linkable || !second.linkable) continue;
      const distance = Math.abs(first.pixel - second.pixel);
      const [firstStart, firstEnd] = first.perpendicular;
      const [secondStart, secondEnd] = second.perpendicular;
      const overlap = Math.min(firstEnd, secondEnd) - Math.max(firstStart, secondStart);
      if (first.end === second.end) {
        if (distance <= tolerance && overlap >= -tolerance * 3) {
          alignments.push({ firstIndex, secondIndex, distance });
        }
        continue;
      }
      const earlier = first.end ? first : second;
      const later = first.end ? second : first;
      const gap = later.pixel - earlier.pixel;
      const wall = sharedWall(first, second);
      const bothMain = first.type === "WALL_CAPITAL" && second.type === "WALL_CAPITAL";
      const measuredLimit = bothMain ? Math.min(tolerance * 4, 1.5 * wall.thickness / factor) : 0;
      const gapLimit = Math.max(10, tolerance * 1.9, measuredLimit);
      if (gap >= -0.65 * tolerance && gap <= gapLimit
          && overlap >= Math.min(firstEnd - firstStart, secondEnd - secondStart) * 0.32) {
        partitions.push({ firstIndex, secondIndex, distance, ...wall });
      }
    }
  }
  alignments.sort((first, second) => first.distance - second.distance);
  partitions.sort((first, second) => first.distance - second.distance);
  const fixedPairs = new Set();
  for (const exterior of floor.exterior_alignments ?? []) {
    const firstIndex = edges.findIndex((entry) => entry.side === exterior.side && entry.room.id === exterior.room_a);
    const secondIndex = edges.findIndex((entry) => entry.side === exterior.side && entry.room.id === exterior.room_b);
    if (firstIndex < 0 || secondIndex < 0 || firstIndex === secondIndex) continue;
    const first = edges[firstIndex];
    const second = edges[secondIndex];
    const difference = wallThickness(first.room, first.side, 0.28) - wallThickness(second.room, second.side, 0.28);
    if (!constrain(firstIndex, secondIndex, difference * (first.end ? 1 : -1))) {
      recordIssue(issues, "error", "exterior-alignment", floor, first.room,
        "Заданное выравнивание наружных граней противоречит другим стенам.");
    }
    fixedPairs.add(Math.min(firstIndex, secondIndex) + ":" + Math.max(firstIndex, secondIndex));
  }
  for (const { firstIndex, secondIndex } of alignments) {
    if (!fixedPairs.has(firstIndex + ":" + secondIndex)) constrain(firstIndex, secondIndex, 0);
  }
  for (const { firstIndex, secondIndex, type, thickness } of partitions) {
    const first = edges[firstIndex];
    const second = edges[secondIndex];
    if (!constrain(firstIndex, secondIndex, first.end ? thickness : -thickness)) {
      recordIssue(issues, "error", "partition-conflict", floor, first.room,
        "Смежные стены заданы с несовместимыми толщинами или положением.");
      continue;
    }
    for (const edge of [first, second]) {
      if (edge.polygonIndex === undefined) {
        edge.room.effective_thickness_m[edge.side] = thickness;
        edge.room.wall_types[edge.side] ??= type;
      } else {
        (edge.room.effective_edge_thickness_m ??= {})[edge.polygonIndex] = thickness;
        (edge.room.inferred_edge_wall_types ??= {})[edge.polygonIndex] = type;
      }
    }
  }

  const roots = new Map();
  const variables = edges.map((_, index) => {
    const root = anchors(index);
    if (!roots.has(root)) roots.set(root, roots.size);
    return roots.get(root);
  });
  const matrix = Array.from({ length: roots.size }, () => new Float64Array(roots.size));
  const vector = new Float64Array(roots.size);
  const origin = Math.min(...edges.map((edge) => edge.pixel));
  const weakPositionWeight = 0.00001;
  for (const [index, edge] of edges.entries()) {
    const variable = variables[index];
    matrix[variable][variable] += weakPositionWeight;
    vector[variable] += weakPositionWeight * ((edge.pixel - origin) * factor - delta[index]);
  }
  for (const [index, room] of rooms.entries()) {
    const start = index * 2;
    const end = start + 1;
    const first = variables[start];
    const second = variables[end];
    if (first === second) continue;
    const measurement = room.measurements[axis];
    const length = measurement.value ?? Math.max(0.15, (edges[end].pixel - edges[start].pixel) * factor);
    const weight = measurement.weight || 0.03;
    const target = length - delta[end] + delta[start];
    matrix[first][first] += weight;
    matrix[second][second] += weight;
    matrix[first][second] -= weight;
    matrix[second][first] -= weight;
    vector[first] -= weight * target;
    vector[second] += weight * target;
  }
  const values = solveLinear(matrix, vector);
  const solvedEdges = edges.map((_, index) => values[variables[index]] + delta[index]);
  const minimum = Math.min(...solvedEdges);
  const positions = solvedEdges.map((value) => coordinate(value - minimum));
  const points = edges.map((edge, index) => [edge.pixel, positions[index]]);
  return { positions, project: piecewiseMap(points, factor) };
}

function piecewiseMap(points, factor) {
  const byPixel = new Map();
  for (const [pixel, value] of points) {
    if (!byPixel.has(pixel)) byPixel.set(pixel, []);
    byPixel.get(pixel).push(value);
  }
  const samples = [...byPixel].map(([pixel, values]) => [pixel, average(values)])
    .sort(([first], [second]) => first - second);
  if (!samples.length) return (pixel) => pixel * factor;
  return (pixel) => {
    const index = samples.findIndex(([source]) => source >= pixel);
    if (!index) return samples[0][1] + (pixel - samples[0][0]) * factor;
    if (index === -1) {
      const [source, target] = samples.at(-1);
      return target + (pixel - source) * factor;
    }
    const [sourceA, targetA] = samples[index - 1];
    const [sourceB, targetB] = samples[index];
    return targetA + (targetB - targetA) * (pixel - sourceA) / (sourceB - sourceA);
  };
}

function overlap(a, b, c, d) {
  return Math.min(b, d) - Math.max(a, c);
}

function roomOutline(room) {
  return room.polygon_fit?.points ? room.polygon : [
    [room.x, room.y], [room.x + room.width, room.y],
    [room.x + room.width, room.y + room.length], [room.x, room.y + room.length],
  ];
}

const RECTANGLE_TRIANGLES = [[0, 1, 2], [0, 2, 3]];

function checkRoomGeometry(rooms, floor, issues) {
  const validRooms = rooms.filter((room) => !room.polygon_fit || room.polygon_fit.points);
  for (const [index, first] of validRooms.entries()) {
    for (const second of validRooms.slice(index + 1)) {
      if (overlap(first.x, first.x + first.width, second.x, second.x + second.width) <= 0.015
          || overlap(first.y, first.y + first.length, second.y, second.y + second.length) <= 0.015) continue;
      if ((first.polygon_fit || second.polygon_fit) && polygonIntersectionArea(
        roomOutline(first), first.polygon_fit?.triangles ?? RECTANGLE_TRIANGLES,
        roomOutline(second), second.polygon_fit?.triangles ?? RECTANGLE_TRIANGLES, 0.000225,
      ) <= 0.000225) continue;
      recordIssue(issues, "error", "room-overlap", floor, first,
        `Площадь пересекается с ${second.number ? `помещением ${second.number}` : "открытым контуром"}. Проверьте контуры и промеры.`);
    }
  }
}

function placeOpenings(room, floor, issues) {
  const placed = [];
  for (const opening of room.openings ?? []) {
    const { wall } = opening;
    const polygon = room.polygon_fit;
    const edgeIndex = opening.edge_index;
    const useEdge = Boolean(polygon);
    const correctSide = useEdge ? Number.isInteger(edgeIndex) && edgeIndex >= 0 && edgeIndex < room.polygon_px.length
      : SIDES.includes(wall);
    const span = useEdge ? polygon.edgeLengths?.[edgeIndex] : wall === "top" || wall === "bottom" ? room.width : room.length;
    const width = finite(opening.width_cm) / 100;
    const type = opening.type === "DOOR_SIMPLE" ? "DOOR_SINGLE" : opening.type;
    if (!correctSide || !OPENING_TYPES.has(type)) {
      recordIssue(issues, "error", "opening-type", floor, room, "Укажите стену и тип проёма.", opening);
      continue;
    }
    if (span === undefined) continue;
    if (width < 0.05 || width > span + EPSILON) {
      recordIssue(issues, "error", "opening-width", floor, room, "Проверьте ширину проёма.", opening);
      continue;
    }
    const left = finite(opening.offset_cm);
    const right = finite(opening.offset_right_cm);
    const centre = validRange(opening.t, 0, 1) ? opening.t : 0.5;
    const offset = left !== null ? left / 100 : right !== null ? span - width - right / 100 :
      clamp(span * centre - width / 2, 0, span - width);
    if (offset < -EPSILON || offset + width > span + EPSILON
        || (left !== null && right !== null && Math.abs(span - width - (left + right) / 100) > 0.03)) {
      recordIssue(issues, "error", "opening-offset", floor, room, "Привязка выводит проём за стену или противоречит второму отступу.", opening);
      continue;
    }
    if (placed.some((previous) => (useEdge ? previous.edge_index === edgeIndex : previous.wall === wall)
        && overlap(previous.offset_m, previous.offset_m + previous.width_m, offset, offset + width) > 0.005)) {
      recordIssue(issues, "error", "opening-overlap", floor, room, "Проёмы перекрываются. Проверьте ширины и отступы.", opening);
      continue;
    }
    if (lowConfidence(opening.confidence) && opening.verified !== true) {
      recordIssue(issues, "error", "opening-confidence", floor, room, "Сверьте с абрисом и подтвердите проём.", opening);
    }
    placed.push({ ...opening, type, offset_m: Math.max(0, offset), width_m: width });
  }
  return placed;
}

function solveRoomSymbols(room, floor, issues) {
  return (room.symbols ?? []).map((symbol) => {
    const issue = (message) => recordIssue(issues, "error", "symbol-measurements", floor, room,
      message, null, undefined, symbol.id);
    if (!validRange(symbol.width_cm, 1, 1500) || !validRange(symbol.depth_cm, 1, 1500)
        || symbol.width_cm > room.width * 100 + EPSILON || symbol.depth_cm > room.length * 100 + EPSILON) {
      issue("Проверьте габариты условного знака.");
      return symbol;
    }
    if (!room.polygon_fit?.points) return symbol;
    const [left, top, right, bottom] = room.bbox_px;
    const u = validRange(symbol.u, 0, 1) ? symbol.u : 0.5;
    const v = validRange(symbol.v, 0, 1) ? symbol.v : 0.5;
    const position = mapPolygonPoint([left + u * (right - left), top + v * (bottom - top)], room.polygon_px, room.polygon);
    if (!position) {
      issue("Переместите условный знак внутрь контура.");
      return symbol;
    }
    const fitted = { ...symbol, position_m: position };
    const { x, y, width, depth } = symbolBounds(fitted, room);
    const footprint = [[x, y], [x + width, y], [x + width, y + depth], [x, y + depth]];
    const inside = polygonIntersectionArea(room.polygon, room.polygon_fit.triangles, footprint, RECTANGLE_TRIANGLES);
    if (inside + EPSILON < width * depth) {
      issue("Условный знак пересекает контур комнаты. Проверьте положение и габариты.");
      return symbol;
    }
    return fitted;
  });
}

function solveFloor(input, floorIndex, nextNumber, issues) {
  const floor = {
    ...input,
    floor_id: input.floor_id ?? `floor_${floorIndex + 1}`,
    title: input.title || `Этаж ${floorIndex + 1}`,
  };
  const offset = issues.length;
  const rooms = prepareRooms(floor, nextNumber, issues);
  if (!rooms.length) {
    recordIssue(issues, "error", "no-rooms", floor, null, "Добавьте хотя бы одно помещение.");
  }
  const scaleX = roomScale(rooms, 0);
  const scaleY = roomScale(rooms, 1);
  const scale = scaleX ?? scaleY ?? 0.017;
  const tolerance = clamp((median(rooms.map((room) => Math.min(
    room.bbox_px[2] - room.bbox_px[0], room.bbox_px[3] - room.bbox_px[1],
  ))) ?? 120) * 0.035, 3, 12);
  const xAxis = solveAxis(rooms, 0, scaleX ?? scale, tolerance, floor, issues);
  const yAxis = solveAxis(rooms, 1, scaleY ?? scale, tolerance, floor, issues);
  for (const [index, room] of rooms.entries()) {
    room.x = xAxis.positions[index * 2];
    room.y = yAxis.positions[index * 2];
    const axes = [xAxis.positions, yAxis.positions];
    const solvedSizes = axes.map((positions, axis) => {
      const expected = room.measurements[axis].value;
      const measured = coordinate(positions[index * 2 + 1] - positions[index * 2]);
      if (measured <= 0.05) {
        recordIssue(issues, "error", "collapsed-room", floor, room, "Помещение не замыкается. Проверьте контуры.");
        return expected ?? 0.15;
      }
      const correction = expected === null ? 0 : Math.abs(measured - expected) * 100;
      if (correction > 3.01) {
        recordIssue(issues, correction > 10.01 ? "error" : "warning", "chain-discrepancy", floor, room,
          `Согласование смежных стен меняет размер по ${axis ? "вертикали" : "горизонтали"} на ${Math.round(correction)} см.`);
      }
      room.discrepancy_cm = Math.max(room.discrepancy_cm, Math.round(correction));
      return measured;
    });
    const polygon = room.polygon_fit;
    [room.width, room.length] = polygon?.points ? [polygon.width, polygon.length] : solvedSizes;
    room.area_m2 = polygon ? (polygon.points ? coordinate(polygon.area) : null) : coordinate(room.width * room.length);
    room.solved_dimensions_m = { width: room.width, length: room.length, discrepancy_cm: room.discrepancy_cm };
    if (polygon?.points) {
      room.polygon = polygon.points.map(([x, y]) => [room.x + x, room.y + y]);
      room.edge_lengths_m = polygon.edgeLengths;
      room.solved_dimensions_m.edge_lengths = polygon.edgeLengths;
      room.solved_dimensions_m.area_m2 = room.area_m2;
    } else if (polygon && room.polygon_px.every(
      (point) => Array.isArray(point) && point.length === 2 && point.every((value) => finite(value) !== null)
    )) {
      const [left, top, right, bottom] = room.bbox_px;
      room.polygon = room.polygon_px.map(([x, y]) => [
        room.x + (x - left) * room.width / (right - left),
        room.y + (y - top) * room.length / (bottom - top),
      ]);
    }
    room.symbols = solveRoomSymbols(room, floor, issues);
    room.openings = placeOpenings(room, floor, issues);
    for (const side of SIDES) {
      const type = room.wall_types[side];
      if (!WALL_TYPES.has(type)) room.wall_types[side] = room.number === null ? "VERANDA" : "WALL_CAPITAL";
      if (room.effective_thickness_m[side] !== undefined) continue;
      const base = room.wall_types[side] === "WALL_PARTITION" ? 0.12 : 0.28;
      room.effective_thickness_m[side] = wallThickness(room, side, base);
    }
  }
  checkRoomGeometry(rooms, floor, issues);
  const walls = (Array.isArray(floor.walls) ? floor.walls : []).flatMap((wall) => {
    if (wall.room_boundary || ![wall.x1, wall.y1, wall.x2, wall.y2].every((value) => finite(value) !== null)) return [];
    return [{
      ...wall,
      x1: xAxis.project(wall.x1), y1: yAxis.project(wall.y1),
      x2: xAxis.project(wall.x2), y2: yAxis.project(wall.y2),
    }];
  });
  const floorIssues = issues.slice(offset);
  return { ...floor, rooms, walls, issues: floorIssues, valid: !floorIssues.some((issue) => issue.severity === "error") };
}

export function solve(project) {
  const issues = [];
  const nextNumber = { value: 1 };
  const inputFloors = Array.isArray(project?.floors) ? project.floors : [];
  const floors = inputFloors.filter(Boolean).map((floor, index) => solveFloor(floor, index, nextNumber, issues));
  return {
    floors,
    issues,
    warnings: issues.map((issue) => issue.message),
    valid: Boolean(floors.length) && floors.every((floor) => floor.valid),
  };
}

function drawing() {
  const items = [];
  return {
    items,
    line(layer, x1, y1, x2, y2) {
      if (Math.hypot(x2 - x1, y2 - y1) < EPSILON) return;
      items.push({ kind: "line", layer, x1, y1, x2, y2 });
    },
    circle(layer, x, y, radius) {
      items.push({ kind: "circle", layer, x, y, radius });
    },
    text(layer, x, y, value, size, rotation = 0, anchor = "middle") {
      items.push({ kind: "text", layer, x, y, value, size, rotation, anchor });
    },
    rect(layer, x, y, width, height) {
      this.line(layer, x, y, x + width, y);
      this.line(layer, x + width, y, x + width, y + height);
      this.line(layer, x + width, y + height, x, y + height);
      this.line(layer, x, y + height, x, y);
    },
  };
}

function makeWallBand(room, side) {
  const horizontal = side === "top" || side === "bottom";
  const reversed = side === "top" || side === "left";
  const thickness = room.effective_thickness_m[side];
  const start = horizontal ? room.x : room.y;
  const span = horizontal ? room.width : room.length;
  const face = (horizontal ? room.y : room.x) + (reversed ? 0 : horizontal ? room.length : room.width);
  return {
    room,
    side,
    type: room.wall_types[side],
    start,
    end: start + span,
    face,
    outer: face + (reversed ? -thickness : thickness),
    horizontal,
    thickness,
    openings: room.openings.filter((opening) => opening.wall === side).map((opening) => ({
      ...opening, from: start + opening.offset_m, to: start + opening.offset_m + opening.width_m,
    })),
  };
}

function opposingBands(first, second) {
  return first.room !== second.room && first.horizontal === second.horizontal
    && Math.abs(first.face - second.outer) < 0.015 && Math.abs(first.outer - second.face) < 0.015
    && overlap(first.start, first.end, second.start, second.end) > 0.015;
}

function wallLayer(type) {
  return type === "VERANDA" ? "VERANDA" : type === "WALL_PARTITION" ? "WALL_PART" : "WALL_MAIN";
}

function drawOpening(draw, field, opening, frameA, frameB) {
  const layer = opening.type === "WINDOW_STD" ? "WINDOWS" :
    opening.type === "DOOR_GLAZED" ? "DOOR_GLAZED" : "DOORS";
  const { from, to } = opening;
  const width = to - from;
  const centre = (from + to) / 2;
  const depth = frameB - frameA;
  draw.line(layer, ...field(from, frameA), ...field(from, frameB));
  draw.line(layer, ...field(to, frameA), ...field(to, frameB));
  if (layer === "WINDOWS" || layer === "DOOR_GLAZED") {
    for (const offset of [0.33, 0.67]) {
      draw.line(layer, ...field(from, frameA + depth * offset), ...field(to, frameA + depth * offset));
    }
    if (layer === "DOOR_GLAZED") {
      for (const separator of [from + width / 3, from + width * 2 / 3]) {
        draw.line(layer, ...field(separator, frameA + depth * 0.19), ...field(separator, frameA + depth * 0.81));
      }
    }
  } else if (opening.type !== "ARCH") {
    const extension = Math.sign(depth || 1) * Math.min(0.08, width / 5);
    const marks = opening.type === "DOOR_DOUBLE" ? [from + width * 0.4, from + width * 0.6] : [centre];
    for (const mark of marks) {
      draw.line(layer, ...field(mark, frameA - extension), ...field(mark, frameB + extension));
    }
  }
}

function drawWallBand(draw, band, bands) {
  if (band.type === "AUX") return;
  const field = (along, across) => band.horizontal ? [along, across] : [across, along];
  const { start, end, face, outer } = band;
  const layer = wallLayer(band.type);
  const neighbours = bands.filter((other) => opposingBands(band, other));
  const openings = [...band.openings, ...neighbours.flatMap((other) => other.openings)]
    .filter((opening) => overlap(opening.from, opening.to, start, end) > EPSILON)
    .sort((first, second) => first.from - second.from);
  let current = start;
  let outerStart = start;
  let outerEnd = end;
  if (band.type === "WALL_CAPITAL" && !neighbours.length) {
    const sides = band.horizontal ? ["left", "right"] : ["top", "bottom"];
    const adjoining = sides.map((side) => bands.find((item) => item.room === band.room && item.side === side));
    const external = (item) => item?.type === "WALL_CAPITAL" && !bands.some((other) => opposingBands(item, other));
    if (external(adjoining[0])) {
      outerStart -= band.room.effective_thickness_m[sides[0]];
    }
    if (external(adjoining[1])) {
      outerEnd += band.room.effective_thickness_m[sides[1]];
    }
  }
  const bandLine = (from, to) => {
    if (to <= from + EPSILON) return;
    draw.line(layer, ...field(from, face), ...field(to, face));
    if (band.type !== "VERANDA") draw.line(layer, ...field(from, outer), ...field(to, outer));
  };
  if (band.type !== "VERANDA") {
    draw.line(layer, ...field(outerStart, outer), ...field(start, outer));
    draw.line(layer, ...field(end, outer), ...field(outerEnd, outer));
  }
  for (const opening of openings) {
    const from = Math.max(start, opening.from);
    const to = Math.min(end, opening.to);
    bandLine(current, from);
    if (to > current + EPSILON && opening.from >= start - EPSILON && opening.to <= end + EPSILON) {
      drawOpening(draw, field, opening, face, band.type === "VERANDA" ? face : outer);
    }
    current = Math.max(current, to);
  }
  bandLine(current, end);
}

function drawExteriorJoints(draw, bands) {
  const outside = bands.filter((band) => band.type === "WALL_CAPITAL"
    && !bands.some((other) => opposingBands(band, other)));
  for (const band of outside) {
    const side = band.horizontal ? "right" : "bottom";
    for (const next of outside) {
      if (next.room === band.room || next.side !== band.side) continue;
      const gap = next.start - band.end;
      const thickness = band.room.effective_thickness_m[side];
      if (gap <= EPSILON || Math.abs(gap - thickness) >= 0.015 || Math.abs(next.outer - band.outer) > 0.5) continue;
      const end = (band.end + next.start) / 2;
      const point = (along, across) => band.horizontal ? [along, across] : [across, along];
      draw.line("WALL_MAIN", ...point(band.end, band.outer), ...point(end, band.outer));
      draw.line("WALL_MAIN", ...point(end, band.outer), ...point(end, next.outer));
      draw.line("WALL_MAIN", ...point(end, next.outer), ...point(next.start, next.outer));
    }
  }
}

function polygonWallBands(room) {
  const winding = Math.sign(signedArea(room.polygon));
  return room.polygon.map((start, edge) => {
    const end = room.polygon[(edge + 1) % room.polygon.length];
    const span = room.edge_lengths_m[edge];
    const along = [(end[0] - start[0]) / span, (end[1] - start[1]) / span];
    const outward = [along[1] * winding, -along[0] * winding];
    const rawType = room.edge_wall_types?.[edge];
    const type = WALL_TYPES.has(rawType) ? rawType
      : room.inferred_edge_wall_types?.[edge] ?? (room.number === null ? "VERANDA" : "WALL_CAPITAL");
    const explicit = room.edge_wall_thickness_cm?.[edge];
    const fallback = type === "WALL_PARTITION" ? 0.12 : 0.28;
    const thickness = type === "VERANDA" || type === "AUX" ? 0 : room.effective_edge_thickness_m?.[edge]
      ?? (validRange(explicit, 1, 150) ? explicit / 100 : fallback);
    const field = (distance, width) => [
      start[0] + distance * along[0] + width * outward[0],
      start[1] + distance * along[1] + width * outward[1],
    ];
    const openings = room.openings.filter((item) => item.edge_index === edge)
      .map((item) => ({ ...item, from: item.offset_m, to: item.offset_m + item.width_m }));
    return { room, start, span, along, outward, type, thickness, field, openings };
  });
}

function polygonAxisBand(band) {
  const horizontal = Math.abs(band.along[1]) < EPSILON;
  if (!horizontal && Math.abs(band.along[0]) >= EPSILON) return null;
  const axis = horizontal ? 0 : 1;
  const face = band.start[1 - axis];
  const outer = band.field(0, band.thickness)[1 - axis];
  const left = band.start[axis];
  const right = band.field(band.span, 0)[axis];
  const openings = band.openings.map((item) => {
    const [first, second] = [band.field(item.from, 0)[axis], band.field(item.to, 0)[axis]];
    return { ...item, from: Math.min(first, second), to: Math.max(first, second) };
  });
  return { room: band.room, horizontal, face, outer, start: Math.min(left, right), end: Math.max(left, right), openings, type: band.type };
}

function rectangleBandAsEdge(band) {
  const start = band.horizontal ? [band.start, band.face] : [band.face, band.start];
  const direction = Math.sign(band.outer - band.face);
  const field = (distance, width) => band.horizontal
    ? [band.start + distance, band.face + width * direction] : [band.face + width * direction, band.start + distance];
  return {
    ...band, start, span: band.end - band.start, field,
    outward: band.horizontal ? [0, direction] : [direction, 0],
    openings: band.openings.map((opening) => ({ ...opening, from: opening.from - band.start, to: opening.to - band.start })),
  };
}

function sharedPolygonOpenings(band, other) {
  if (band.room === other.room || band.thickness <= 0 || other.thickness <= 0) return [];
  const dot = ([x, y], [u, v]) => x * u + y * v;
  if (dot(band.outward, other.outward) > -1 + EPSILON) return [];
  const local = ([x, y]) => [x - band.start[0], y - band.start[1]];
  const offset = dot(local(other.start), band.outward);
  if (Math.abs(offset - band.thickness) >= 0.015 || Math.abs(offset - other.thickness) >= 0.015) return [];
  return other.openings.flatMap((opening) => {
    const first = dot(local(other.field(opening.from, 0)), band.along);
    const second = dot(local(other.field(opening.to, 0)), band.along);
    const from = Math.min(first, second);
    const to = Math.max(first, second);
    return overlap(from, to, 0, band.span) > EPSILON ? [{ ...opening, from, to }] : [];
  });
}

function polygonCorner(first, next) {
  const firstEnd = first.field(first.span, first.thickness);
  const nextStart = next.field(0, next.thickness);
  if (!first.thickness || !next.thickness) return [firstEnd, nextStart];
  const cross = ([x, y], [u, v]) => x * v - y * u;
  const denominator = cross(first.along, next.along);
  if (Math.abs(denominator) <= EPSILON) return [firstEnd, nextStart];
  const delta = [nextStart[0] - firstEnd[0], nextStart[1] - firstEnd[1]];
  const distance = cross(delta, next.along) / denominator;
  const limit = Math.max(first.thickness, next.thickness) * 4;
  if (Math.abs(distance) > limit) return [firstEnd, nextStart];
  const corner = first.field(first.span + distance, first.thickness);
  return [corner, corner];
}

function drawPolygonWall(draw, band, corners, otherBands) {
  if (band.type === "AUX") return;
  const { field, span, thickness } = band;
  const layer = wallLayer(band.type);
  const openings = [...band.openings, ...otherBands.flatMap((other) => sharedPolygonOpenings(band, other))]
    .sort((first, second) => first.from - second.from);
  const projectedDistance = ([x, y]) => (x - band.start[0]) * band.along[0] + (y - band.start[1]) * band.along[1];
  const first = projectedDistance(corners.start);
  const last = projectedDistance(corners.end);
  const wallLine = (from, to, depth) => {
    if (to > from + EPSILON) draw.line(layer, ...field(from, depth), ...field(to, depth));
  };
  let inner = 0;
  let outer = first;
  for (const opening of openings) {
    const from = clamp(opening.from, 0, span);
    const to = clamp(opening.to, 0, span);
    wallLine(inner, from, 0);
    if (thickness) wallLine(outer, Math.min(from, last), thickness);
    if (to > inner + EPSILON && opening.from >= -EPSILON && opening.to <= span + EPSILON) {
      drawOpening(draw, field, opening, 0, thickness);
    }
    inner = Math.max(inner, to);
    outer = Math.max(outer, to);
  }
  wallLine(inner, span, 0);
  if (thickness) wallLine(outer, last, thickness);
}

function drawPolygonRoom(draw, bands, otherBands) {
  const corners = bands.map((band, index) => polygonCorner(band, bands[(index + 1) % bands.length]));
  for (const [index, band] of bands.entries()) {
    const previousCorner = corners[(index + bands.length - 1) % bands.length];
    const nextCorner = corners[index];
    drawPolygonWall(draw, band, { start: previousCorner[1], end: nextCorner[0] }, otherBands);
    draw.line(wallLayer(band.type), ...nextCorner[0], ...nextCorner[1]);
  }
}

function drawFreeWall(draw, wall) {
  if (wall.type === "AUX") return;
  const { x1, y1, x2, y2 } = wall;
  const span = Math.hypot(x2 - x1, y2 - y1);
  if (span <= EPSILON) return;
  const layer = wallLayer(wall.type);
  if (wall.type === "VERANDA") {
    draw.line(layer, x1, y1, x2, y2);
    return;
  }
  const thickness = validRange(wall.thickness_cm, 1, 150)
    ? wall.thickness_cm / 100 : wall.type === "WALL_PARTITION" ? 0.12 : 0.28;
  const xOffset = (y2 - y1) * thickness / span / 2;
  const yOffset = (x1 - x2) * thickness / span / 2;
  draw.line(layer, x1 - xOffset, y1 - yOffset, x2 - xOffset, y2 - yOffset);
  draw.line(layer, x1 + xOffset, y1 + yOffset, x2 + xOffset, y2 + yOffset);
  draw.line(layer, x1 - xOffset, y1 - yOffset, x1 + xOffset, y1 + yOffset);
  draw.line(layer, x2 - xOffset, y2 - yOffset, x2 + xOffset, y2 + yOffset);
}

function symbolBounds(symbol, room) {
  if (!validRange(symbol.width_cm, 1, 1500) || !validRange(symbol.depth_cm, 1, 1500)) return null;
  const width = Math.min(room.width, symbol.width_cm / 100);
  const depth = Math.min(room.length, symbol.depth_cm / 100);
  const u = validRange(symbol.u, 0, 1) ? symbol.u : 0.5;
  const v = validRange(symbol.v, 0, 1) ? symbol.v : 0.5;
  if (symbol.position_m) {
    return { x: symbol.position_m[0] - width / 2, y: symbol.position_m[1] - depth / 2, width, depth };
  }
  const x = room.x + clamp(room.width * u - width / 2, 0, room.width - width);
  const y = room.y + clamp(room.length * v - depth / 2, 0, room.length - depth);
  return { x, y, width, depth };
}

function drawSymbol(draw, symbol, room) {
  const aliases = { STAIRCASE: "STAIRS", STOVE_CHIMNEY: "STOVE", KITCHEN_UNIT: "KITCHEN" };
  const type = aliases[symbol.type] ?? symbol.type;
  const box = symbolBounds(symbol, room);
  if (!box || !["STAIRS", "STOVE", "KITCHEN", "COLUMN", "SHAFT"].includes(type)) return;
  const layer = type === "STAIRS" ? "STAIRS" : type === "COLUMN" ? "COLUMNS" : "EQUIPMENT";
  const { x, y, width, depth } = box;
  draw.rect(layer, x, y, width, depth);
  if (type === "STAIRS") {
    const steps = clamp(Math.round(depth / 0.22), 3, 22);
    for (let step = 1; step < steps; step += 1) {
      const stepY = y + depth * step / steps;
      draw.line(layer, x, stepY, x + width, stepY);
    }
    draw.line(layer, x + width * 0.5, y + depth * 0.85, x + width * 0.5, y + depth * 0.12);
    draw.line(layer, x + width * 0.5, y + depth * 0.12, x + width * 0.32, y + depth * 0.30);
    draw.line(layer, x + width * 0.5, y + depth * 0.12, x + width * 0.68, y + depth * 0.30);
  }
  if (type === "STOVE" || type === "SHAFT") {
    draw.line(layer, x, y, x + width, y + depth);
    if (type === "SHAFT") draw.line(layer, x + width, y, x, y + depth);
  }
  if (type === "KITCHEN") {
    const radius = Math.min(width * 0.09, depth * 0.26);
    draw.circle(layer, x + width * 0.15, y + depth * 0.5, radius);
    draw.circle(layer, x + width * 0.37, y + depth * 0.5, radius);
    draw.line(layer, x + width * 0.55, y, x + width * 0.55, y + depth);
    draw.line(layer, x + width * 0.55, y + depth * 0.5, x + width, y + depth * 0.5);
  }
}

function dimensionText(draw, room, size, technicalPassport) {
  if (room.polygon) return;
  const x = room.x + room.width / 2;
  const y = room.y + room.length / 2;
  const offset = size * 0.82;
  const visible = (side) => room.wall_types[side] !== "AUX";
  const furniture = (room.symbols ?? []).map((symbol) => symbolBounds(symbol, room)).filter(Boolean);
  const addDimension = (choices, value, vertical) => {
    const extent = size * (value.length * 0.32 + 0.14);
    const halfWidth = vertical ? size * 0.55 : extent;
    const halfHeight = vertical ? extent : size * 0.55;
    const clear = ([px, py]) => px - halfWidth >= room.x && px + halfWidth <= room.x + room.width
      && py - halfHeight >= room.y && py + halfHeight <= room.y + room.length
      && !furniture.some((box) => overlap(px - halfWidth, px + halfWidth, box.x, box.x + box.width) > EPSILON
        && overlap(py - halfHeight, py + halfHeight, box.y, box.y + box.depth) > EPSILON);
    const [px, py] = choices.find(clear) ?? choices[0];
    draw.text("TEXT_DIM", px, py, value, size, vertical ? -90 : 0);
  };
  if (room.width > size * 2.3 && room.length > size * 2.3) {
    if (visible("top")) addDimension([
      [x, room.y + offset], [x + room.width * 0.22, room.y + offset], [x - room.width * 0.22, room.y + offset],
      [x, room.y + room.length - offset],
    ], display(room.width), false);
    if (visible("left")) addDimension([
      [room.x + offset, y], [room.x + offset, y - room.length * 0.2], [room.x + offset, y + room.length * 0.2],
      [room.x + room.width - offset, y], [room.x + room.width - offset, y - room.length * 0.2],
    ], display(room.length), true);
  }
  if (room.number === null) return;
  draw.text("TEXT_ROOM", x, y, String(room.number), size * 1.22);
  if (technicalPassport) {
    draw.text("TEXT_ROOM", x, y + size * 1.27, `${display(room.area_m2)} м²`, size);
    if (room.width > size * 4 && room.length > size * 4) {
      const thickness = room.effective_thickness_m.bottom;
      if (visible("bottom")) {
        draw.text("TEXT_DIM", x, room.y + room.length - offset, `t=${display(thickness)}`, size * 0.82);
      }
    }
  }
}

function dimensionTextPolygon(draw, room, bands, size, technicalPassport) {
  const [x, y] = polygonLabelPoint(room.polygon, room.polygon_fit.triangles);
  if (room.number !== null) {
    draw.text("TEXT_ROOM", x, y, String(room.number), size * 1.22);
    if (technicalPassport) draw.text("TEXT_ROOM", x, y + size * 1.27, `${display(room.area_m2)} м²`, size);
  }
  for (const band of bands) {
    if (band.type === "AUX" || band.span <= size * 2.6) continue;
    const midpoint = band.field(band.span / 2, -size * 0.85);
    if (!pointInPolygon(midpoint, room.polygon)) continue;
    const angle = Math.atan2(band.along[1], band.along[0]) * 180 / Math.PI;
    const rotation = angle <= -90 ? angle + 180 : angle > 90 ? angle - 180 : angle;
    draw.text("TEXT_DIM", ...midpoint, display(band.span), size, rotation);
    if (technicalPassport && band.thickness > 0 && band.span > size * 4) {
      const inward = band.field(band.span / 2, -size * 1.92);
      if (pointInPolygon(inward, room.polygon)) draw.text("TEXT_DIM", ...inward, `t=${display(band.thickness)}`, size * 0.82, rotation);
    }
  }
}

function modelGeometry(floor, size, technicalPassport) {
  const draw = drawing();
  const bands = floor.rooms.flatMap((room) => room.polygon
    ? [] : SIDES.map((side) => makeWallBand(room, side)));
  const polygons = new Map(floor.rooms.filter((room) => room.polygon_fit?.points)
    .map((room) => [room.id, polygonWallBands(room)]));
  const polygonEdges = [...polygons.values()].flat();
  const aligned = polygonEdges.map(polygonAxisBand).filter(Boolean);
  const allBands = [...bands, ...aligned];
  const edgeBands = [...bands.map(rectangleBandAsEdge), ...polygonEdges];
  for (const band of bands) drawWallBand(draw, band, allBands);
  drawExteriorJoints(draw, bands);
  for (const wall of floor.walls) drawFreeWall(draw, wall);
  for (const room of floor.rooms) {
    if (room.polygon) {
      const walls = polygons.get(room.id);
      if (walls) {
        drawPolygonRoom(draw, walls, edgeBands);
        for (const symbol of room.symbols) {
          if (symbol.position_m) drawSymbol(draw, symbol, room);
        }
        dimensionTextPolygon(draw, room, walls, size, technicalPassport);
      } else {
        for (const [index, point] of room.polygon.entries()) {
          draw.line("VERANDA", ...point, ...room.polygon[(index + 1) % room.polygon.length]);
        }
      }
      continue;
    }
    for (const symbol of room.symbols ?? []) drawSymbol(draw, symbol, room);
    dimensionText(draw, room, size, technicalPassport);
  }
  const seen = new Set();
  return draw.items.filter((item) => {
    if (item.kind !== "line") return true;
    const ends = [[mm(item.x1), mm(item.y1)], [mm(item.x2), mm(item.y2)]].map((point) => point.join(",")).sort();
    const key = [item.layer, ...ends].join("|");
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function modelBounds(items) {
  let left = Infinity;
  let top = Infinity;
  let right = -Infinity;
  let bottom = -Infinity;
  for (const item of items) {
    if (item.kind === "line") {
      left = Math.min(left, item.x1, item.x2);
      top = Math.min(top, item.y1, item.y2);
      right = Math.max(right, item.x1, item.x2);
      bottom = Math.max(bottom, item.y1, item.y2);
    }
    if (item.kind === "circle") {
      left = Math.min(left, item.x - item.radius);
      top = Math.min(top, item.y - item.radius);
      right = Math.max(right, item.x + item.radius);
      bottom = Math.max(bottom, item.y + item.radius);
    }
  }
  return Number.isFinite(left) ? { left, top, width: right - left, height: bottom - top }
    : { left: 0, top: 0, width: 0, height: 0 };
}

function scaleOf(profile) {
  return ["1:50", "1:100", "1:200"].includes(profile?.scale) ? Number(profile.scale.slice(2)) : 100;
}

function svgItem(item, offsetX = 0, offsetY = 0, factor = 1) {
  const x = (value) => (value * factor + offsetX).toFixed(3);
  const y = (value) => (value * factor + offsetY).toFixed(3);
  const strokeWidth = ["WALL_MAIN", "LEGEND"].includes(item.layer) ? 0.27 : 0.16;
  if (item.kind === "line") {
    return `<line x1="${x(item.x1)}" y1="${y(item.y1)}" x2="${x(item.x2)}" y2="${y(item.y2)}" stroke="#111" stroke-width="${strokeWidth}"/>`;
  }
  if (item.kind === "circle") {
    return `<circle cx="${x(item.x)}" cy="${y(item.y)}" r="${(item.radius * factor).toFixed(3)}" fill="none" stroke="#111" stroke-width="${strokeWidth}"/>`;
  }
  const rotation = item.rotation ? ` transform="rotate(${item.rotation} ${x(item.x)} ${y(item.y)})"` : "";
  const weight = item.layer === "TEXT_ROOM" ? ' font-weight="bold"' : "";
  return `<text x="${x(item.x)}" y="${y(item.y)}"${rotation}${weight} font-size="${(item.size * factor).toFixed(3)}" text-anchor="${item.anchor}" dominant-baseline="central">${safeText(item.value)}</text>`;
}

const svgRoot = (body) => `<svg xmlns="http://www.w3.org/2000/svg" width="210mm" height="297mm" viewBox="0 0 210 297" role="img" aria-label="Лист A4"><rect width="210" height="297" fill="white"/><g fill="#111" font-family="'Courier New', 'Courier Prime', monospace">${body}</g></svg>`;

function drawLegend(draw, layers, startY) {
  const keys = Object.keys(LAYERS).filter((key) => layers.has(key) && key !== "TEXT_ROOM" && key !== "LEGEND");
  keys.forEach((key, index) => {
    const x = 18 + index % 2 * 95;
    const y = startY + Math.floor(index / 2) * 5.7;
    if (key === "TEXT_DIM") {
      draw.text("LEGEND", x + 6.5, y, "2.97", 2.4);
    } else if (key === "WINDOWS" || key.startsWith("DOOR")) {
      const type = key === "WINDOWS" ? "WINDOW_STD" : key === "DOORS" ? "DOOR_SINGLE" : "DOOR_GLAZED";
      drawOpening(draw, (along, across) => [along, across], { type, from: x + 2, to: x + 11 }, y - 0.72, y + 0.72);
    } else if (key === "COLUMNS") {
      draw.rect("COLUMNS", x + 5.5, y - 1.15, 2.3, 2.3);
    } else if (key === "STAIRS" || key === "EQUIPMENT") {
      draw.rect(key, x + 2, y - 1.4, 9, 2.8);
      for (const position of key === "STAIRS" ? [4, 6, 8, 10] : [6]) {
        draw.line(key, x + position, y - 1.4, x + position, y + 1.4);
      }
    } else {
      draw.line(key, x + 2, y - 0.65, x + 11, y - 0.65);
      if (key !== "VERANDA") draw.line(key, x + 2, y + 0.65, x + 11, y + 0.65);
    }
    draw.text("LEGEND", x + 16, y, LAYERS[key], 2.65, 0, "start");
  });
}

function sheetDetails(floor, profile) {
  const scale = scaleOf(profile);
  const factor = 1000 / scale;
  const technicalPassport = profile?.target_profile === "BTI";
  const items = modelGeometry(floor, 2.4 / factor, technicalPassport);
  const bounds = modelBounds(items);
  const fits = factor * bounds.width <= PLAN.width && factor * bounds.height <= PLAN.height;
  return { scale, factor, items, bounds, fits, technicalPassport };
}

export function renderSheet(solved, floorIndex, profile = {}) {
  const floor = solved?.floors?.[floorIndex];
  if (!floor) return "";
  const details = sheetDetails(floor, profile);
  const { items, bounds, scale, fits, technicalPassport } = details;
  const factor = fits ? details.factor : Math.min(PLAN.width / bounds.width, PLAN.height / bounds.height);
  const dx = PLAN.x + (PLAN.width - bounds.width * factor) / 2 - bounds.left * factor;
  const dy = PLAN.y + (PLAN.height - bounds.height * factor) / 2 - bounds.top * factor;
  const draw = drawing();
  draw.rect("LEGEND", FRAME.x, FRAME.y, FRAME.width, FRAME.height);
  draw.text("TEXT_ROOM", 105, 21, "ПЛАН ЭТАЖА", 6);
  draw.text("LEGEND", 105, 31, floor.title, 4.05);
  if (technicalPassport) {
    const height = floor.height_m ?? profile.height_m;
    draw.text("LEGEND", 105, 39, validRange(height, 0.2, 50)
      ? `Технический паспорт  ·  H = ${display(height)} м` : "Технический паспорт", 2.8);
  }
  draw.line("LEGEND", FRAME.x, 233, FRAME.x + FRAME.width, 233);
  draw.text("TEXT_ROOM", 18, 240, "Условные обозначения", 3.15, 0, "start");
  draw.text("LEGEND", 191, 240, fits ? `Масштаб 1:${scale}` : `Выберите меньший масштаб (1:${scale})`, 2.85, 0, "end");
  drawLegend(draw, new Set(items.map((item) => item.layer)), 250);
  if (!floor.valid || !fits) {
    draw.text("LEGEND", 105, 45, "ЧЕРНОВИК", 2.65);
  }
  return svgRoot([...draw.items.map((item) => svgItem(item)), ...items.map((item) => svgItem(item, dx, dy, factor))].join(""));
}

function renderSchedule(floor, page, pageCount, rows) {
  const draw = drawing();
  draw.rect("LEGEND", FRAME.x, FRAME.y, FRAME.width, FRAME.height);
  draw.text("TEXT_ROOM", 105, 23, "ЭКСПЛИКАЦИЯ ПОМЕЩЕНИЙ", 4.8);
  draw.text("LEGEND", 105, 33, floor.title, 3.8);
  draw.text("LEGEND", 105, 42, `Лист ${page + 1} из ${pageCount}`, 2.8);
  const columns = [18, 44, 98, 154, 192];
  const y = 48;
  const header = ["№", "Ширина, м", "Длина, м", "Площадь, м²"];
  const rowsHeight = 7.1;
  const bottomY = y + rowsHeight * (rows.length + 1);
  for (const x of columns) draw.line("LEGEND", x, y, x, bottomY);
  [header, ...rows].forEach((values, index) => {
    const rowY = y + rowsHeight * index;
    draw.line("LEGEND", columns[0], rowY, columns.at(-1), rowY);
    values.forEach((value, column) => {
      draw.text(index ? "LEGEND" : "TEXT_ROOM", (columns[column] + columns[column + 1]) / 2, rowY + rowsHeight / 2, value, 3);
    });
  });
  draw.line("LEGEND", columns[0], bottomY, columns.at(-1), bottomY);
  if (page + 1 === pageCount) {
    const total = floor.rooms.filter((room) => room.number !== null).reduce((area, room) => area + room.area_m2, 0);
    draw.text("TEXT_ROOM", columns.at(-1), bottomY + 10, `Всего ${display(total)} м²`, 3.25, 0, "end");
  }
  return svgRoot(draw.items.map((item) => svgItem(item)).join(""));
}

function dxfText(value, unicode) {
  const singleLine = String(value).replace(/[\r\n\x00-\x1f\\]/gu, " ");
  if (unicode) return singleLine;
  return singleLine.replace(/[^\x20-\x7e]/gu, (character) => {
    return character.split("").map((unit) => "\\U+" + unit.charCodeAt(0).toString(16).toUpperCase().padStart(4, "0")).join("");
  });
}

export function buildDxf(solved, floorIndex, profile = {}, { unicode = false } = {}) {
  const floor = requireFloor(solved, floorIndex);
  const model = modelGeometry(floor, 0.24, profile.target_profile === "BTI");
  const legend = drawing();
  legend.text("LEGEND", 18, 240, "Условные обозначения", 3.15, 0, "start");
  drawLegend(legend, new Set(model.map((item) => item.layer)), 250);
  const items = [...model, ...legend.items.map((item) => ({ ...item, layer: "LEGEND", paperSpace: true }))];
  const lines = [];
  let serial = 0x100;
  const handle = () => (serial++).toString(16).toUpperCase();
  const fields = (...pairs) => lines.push(...pairs.map(String));
  const section = (name) => fields(0, "SECTION", 2, name);
  const endSection = () => fields(0, "ENDSEC");
  const table = (name, owner, count) => fields(0, "TABLE", 2, name, 5, owner, 330, 0, 100, "AcDbSymbolTable", 70, count);
  const endTable = () => fields(0, "ENDTAB");
  const tableRecord = (name, owner, subclass) => fields(0, name, 5, handle(), 330, owner, 100, "AcDbSymbolTableRecord", 100, subclass);
  const point = (x, y) => [10, mm(x), 20, mm(-y), 30, 0];
  const onlyLayers = [...new Set(["0", ...Object.keys(LAYERS)])];
  const weight = (layer) => layer === "WALL_MAIN" ? 25 : layer === "WALL_PART" ? 18 : 13;

  section("HEADER");
  fields(9, "$ACADVER", 1, "AC1015", 9, "$DWGCODEPAGE", 3, unicode ? "ANSI_1251" : "ANSI_1252",
    9, "$HANDSEED", 5, "FFFFF", 9, "$INSUNITS", 70, 4, 9, "$MEASUREMENT", 70, 1,
    9, "$LUNITS", 70, 2, 9, "$LUPREC", 70, 2);
  endSection();

  section("TABLES");
  table("LTYPE", "2", 3);
  for (const name of ["BYBLOCK", "BYLAYER", "CONTINUOUS"]) {
    tableRecord("LTYPE", "2", "AcDbLinetypeTableRecord");
    fields(2, name, 70, 0, 3, name === "CONTINUOUS" ? "Solid line" : "", 72, 65, 73, 0, 40, 0);
  }
  endTable();
  table("LAYER", "3", onlyLayers.length);
  for (const name of onlyLayers) {
    tableRecord("LAYER", "3", "AcDbLayerTableRecord");
    fields(2, name, 70, 0, 62, 7, 6, "CONTINUOUS", 290, 1, 370, weight(name));
  }
  endTable();
  table("STYLE", "5", 1);
  tableRecord("STYLE", "5", "AcDbTextStyleTableRecord");
  fields(2, "STANDARD", 70, 0, 40, 0, 41, 1, 50, 0, 71, 0, 42, 2.5, 3, "arial.ttf", 4, "");
  endTable();
  table("BLOCK_RECORD", "1", 2);
  const blockRecords = [["*Model_Space", "1F"], ["*Paper_Space", "1B"]];
  for (const [name, owner] of blockRecords) {
    fields(0, "BLOCK_RECORD", 5, owner, 330, 1, 100, "AcDbSymbolTableRecord", 100, "AcDbBlockTableRecord",
      2, name, 70, 0);
  }
  endTable();
  endSection();

  section("BLOCKS");
  for (const [name, owner] of blockRecords) {
    fields(0, "BLOCK", 5, handle(), 330, owner, 100, "AcDbEntity", 8, 0, 100, "AcDbBlockBegin", 2, name, 70, 0,
      ...point(0, 0), 3, name, 1, "");
    fields(0, "ENDBLK", 5, handle(), 330, owner, 100, "AcDbEntity", 8, 0, 100, "AcDbBlockEnd");
  }
  endSection();

  section("ENTITIES");
  for (const item of items) {
    const entity = item.kind.toUpperCase();
    fields(0, entity, 5, handle(), 330, item.paperSpace ? "1B" : "1F", 100, "AcDbEntity");
    if (item.paperSpace) fields(67, 1);
    fields(8, item.layer);
    const units = item.paperSpace ? (value) => Number(value.toFixed(3)) : mm;
    const vertical = item.paperSpace ? (value) => units(A4[1] - value) : (value) => mm(-value);
    const insertion = (x, y) => [10, units(x), 20, vertical(y), 30, 0];
    if (entity === "LINE") {
      fields(100, "AcDbLine", ...insertion(item.x1, item.y1), 11, units(item.x2), 21, vertical(item.y2), 31, 0);
    } else if (entity === "CIRCLE") {
      fields(100, "AcDbCircle", ...insertion(item.x, item.y), 40, units(item.radius));
    } else {
      const horizontal = item.anchor === "middle" ? 1 : item.anchor === "end" ? 2 : 0;
      fields(100, "AcDbText", ...insertion(item.x, item.y), 40, units(item.size), 1, dxfText(item.value, unicode),
        50, -item.rotation, 7, "STANDARD", 72, horizontal, 11, units(item.x), 21, vertical(item.y), 31, 0, 100, "AcDbText", 73, 2);
    }
  }
  endSection();
  fields(0, "EOF");
  return lines.join("\r\n") + "\r\n";
}

function requireFloor(solved, index) {
  const floor = solved?.floors?.[index];
  if (!floor) throw new Error("Выберите этаж.");
  if (!floor.valid) {
    const first = floor.issues.find((issue) => issue.severity === "error");
    throw new Error(first?.message ?? "Сначала исправьте ошибки плана.");
  }
  return floor;
}

function requireSheet(solved, index, profile) {
  const floor = requireFloor(solved, index);
  if (!sheetDetails(floor, profile).fits) {
    throw new Error(`${floor.title}. План не помещается на A4 в масштабе 1:${scaleOf(profile)}. Выберите меньший масштаб.`);
  }
  return floor;
}

function densityJpeg(bytes) {
  // The browser encodes density as 96 dpi. Preserve image data and replace the JFIF density.
  const jfif = new Uint8Array([255, 224, 0, 16, 74, 70, 73, 70, 0, 1, 1, 1, 1, 44, 1, 44, 0, 0]);
  const existing = bytes[2] === 255 && bytes[3] === 224 && bytes[6] === 74 && bytes[7] === 70
    && bytes[8] === 73 && bytes[9] === 70 && bytes[10] === 0;
  if (existing && bytes.length > 17) {
    bytes.set(jfif.subarray(11, 16), 13);
    return bytes;
  }
  const result = new Uint8Array(bytes.length + jfif.length);
  result.set(bytes.subarray(0, 2), 0);
  result.set(jfif, 2);
  result.set(bytes.subarray(2), 2 + jfif.length);
  return result;
}

async function rasterSheet(svg) {
  const source = URL.createObjectURL(new Blob([svg], { type: "image/svg+xml;charset=utf-8" }));
  const canvas = document.createElement("canvas");
  canvas.width = Math.round(A4[0] * PDF_DPI / 25.4);
  canvas.height = Math.round(A4[1] * PDF_DPI / 25.4);
  const image = new Image();
  try {
    await new Promise((resolve, reject) => {
      image.onload = resolve;
      image.onerror = () => reject(new Error("Не удалось отрисовать лист."));
      image.src = source;
    });
    const context = canvas.getContext("2d", { alpha: false });
    context.drawImage(image, 0, 0, canvas.width, canvas.height);
    const jpeg = await new Promise((resolve, reject) => canvas.toBlob(
      (result) => result ? resolve(result) : reject(new Error("Не удалось записать JPEG.")), "image/jpeg", 0.96,
    ));
    return { width: canvas.width, height: canvas.height, bytes: densityJpeg(new Uint8Array(await jpeg.arrayBuffer())) };
  } finally {
    URL.revokeObjectURL(source);
    canvas.width = 1;
    canvas.height = 1;
  }
}

function writePdf(pages) {
  const encoder = new TextEncoder();
  const pieces = [];
  const offsets = [0];
  let length = 0;
  const append = (part) => {
    const bytes = typeof part === "string" ? encoder.encode(part) : part;
    pieces.push(bytes);
    length += bytes.length;
  };
  const object = (number, ...parts) => {
    offsets[number] = length;
    append(`${number} 0 obj\n`);
    for (const part of parts) append(part);
    append("\nendobj\n");
  };
  const [width, height] = A4.map((millimetres) => (millimetres * 72 / 25.4).toFixed(6));
  append("%PDF-1.4\n");
  append(new Uint8Array([37, 226, 227, 207, 211, 10]));
  object(1, "<< /Type /Catalog /Pages 2 0 R >>");
  object(2, `<< /Type /Pages /Count ${pages.length} /Kids [${pages.map((_, index) => `${3 + index * 3} 0 R`).join(" ")}] >>`);
  for (const [index, page] of pages.entries()) {
    const number = 3 + index * 3;
    object(number, `<< /Type /Page /Parent 2 0 R /MediaBox [0 0 ${width} ${height}] /Resources << /XObject << /Plan ${number + 2} 0 R >> >> /Contents ${number + 1} 0 R >>`);
    const content = `q\n${width} 0 0 ${height} 0 0 cm\n/Plan Do\nQ\n`;
    object(number + 1, `<< /Length ${encoder.encode(content).length} >>\nstream\n${content}endstream`);
    object(number + 2, `<< /Type /XObject /Subtype /Image /Width ${page.width} /Height ${page.height} /ColorSpace /DeviceRGB /BitsPerComponent 8 /Filter /DCTDecode /Length ${page.bytes.length} >>\nstream\n`,
      page.bytes, "\nendstream");
  }
  const xref = length;
  append(`xref\n0 ${offsets.length}\n0000000000 65535 f \n`);
  for (const offset of offsets.slice(1)) append(`${String(offset).padStart(10, "0")} 00000 n \n`);
  append(`trailer\n<< /Size ${offsets.length} /Root 1 0 R >>\nstartxref\n${xref}\n%%EOF\n`);
  return new Blob(pieces, { type: "application/pdf" });
}

function saveFile(blob, filename) {
  const link = document.createElement("a");
  const url = URL.createObjectURL(blob);
  link.href = url;
  link.download = filename;
  document.body.append(link);
  link.click();
  link.remove();
  setTimeout(() => URL.revokeObjectURL(url), 30000);
}

function safeName(value) {
  return String(value).replace(/[^\p{L}\p{N}_-]+/gu, "-").replace(/^-+|-+$/gu, "").slice(0, 60) || "abris";
}

export async function downloadPlan(format, solved, floorIndex, profile = {}) {
  if (format === "pdf") {
    const indices = floorIndex === null ? solved.floors.map((_, index) => index) : [floorIndex];
    if (!indices.length) throw new Error("Добавьте этаж.");
    const floors = indices.map((index) => requireSheet(solved, index, profile));
    const pages = [];
    for (const [index, floor] of floors.entries()) {
      pages.push(await rasterSheet(renderSheet(solved, indices[index], profile)));
      if (profile.target_profile !== "BTI") continue;
      const rows = floor.rooms.filter((room) => room.number !== null).map((room) => [
        room.number, display(room.width), display(room.length), display(room.area_m2),
      ]);
      const perPage = 29;
      const count = Math.max(1, Math.ceil(rows.length / perPage));
      for (let page = 0; page < count; page += 1) {
        pages.push(await rasterSheet(renderSchedule(floor, page, count, rows.slice(page * perPage, (page + 1) * perPage))));
      }
    }
    const title = floorIndex === null ? "Все-этажи" : safeName(floors[0].title);
    saveFile(writePdf(pages), `Абрис-${title}.pdf`);
    return;
  }

  const floor = requireFloor(solved, floorIndex);
  const filename = `Абрис-${safeName(floor.title)}`;
  if (format === "jpeg" || format === "jpg") {
    requireSheet(solved, floorIndex, profile);
    const page = await rasterSheet(renderSheet(solved, floorIndex, profile));
    saveFile(new Blob([page.bytes], { type: "image/jpeg" }), filename + ".jpg");
    return;
  }
  if (format === "dxf") {
    saveFile(new Blob([buildDxf(solved, floorIndex, profile)], { type: "application/dxf" }), filename + ".dxf");
    return;
  }
  if (format === "dwg") {
    const response = await fetch("/abris/api/dwg", {
      method: "POST",
      headers: { "Content-Type": "application/octet-stream" },
      body: buildDxf(solved, floorIndex, profile, { unicode: true }),
    });
    if (!response.ok) {
      const error = await response.json().catch(() => ({}));
      throw new Error(error.error || error.detail || "Сервис экспорта DWG временно недоступен.");
    }
    const dwg = await response.blob();
    if (!(await dwg.slice(0, 6).text()).match(/^AC10\d\d$/u)) {
      throw new Error("Сервис вернул повреждённый DWG.");
    }
    saveFile(dwg, filename + ".dwg");
    return;
  }
  throw new Error("Неизвестный формат экспорта.");
}
