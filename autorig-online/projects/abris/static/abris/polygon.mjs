const EPSILON = 1e-8;
const RIGHT_ANGLE = Math.PI / 2;
const ANGLE_TOLERANCE = Math.PI / 12;
const CLOSURE_TOLERANCE_M = 0.03;
const MIN_EDGE_M = 0.01;
const MAX_LENGTH_CM = 10000;
const coordinates = (point) => Array.isArray(point) && point.length === 2 && point.every(Number.isFinite);
const distance = (a, b) => Math.hypot(b[0] - a[0], b[1] - a[1]);
const cross = (a, b, c) => (b[0] - a[0]) * (c[1] - a[1]) - (b[1] - a[1]) * (c[0] - a[0]);
const comparableLength = (value) => Number.isFinite(value) && value >= MIN_EDGE_M * 100 && value <= MAX_LENGTH_CM;

export function boundsOfPoints(points) {
  const x = points.map(([value]) => value);
  const y = points.map(([, value]) => value);
  return [Math.min(...x), Math.min(...y), Math.max(...x), Math.max(...y)];
}

export function signedArea(points) {
  const first = points[0];
  return points.slice(1, -1).reduce((area, point, index) => area + cross(first, point, points[index + 2]) / 2, 0);
}

function onSegment(a, b, point) {
  return Math.abs(cross(a, b, point)) < EPSILON
    && Math.min(a[0], b[0]) - EPSILON <= point[0] && point[0] <= Math.max(a[0], b[0]) + EPSILON
    && Math.min(a[1], b[1]) - EPSILON <= point[1] && point[1] <= Math.max(a[1], b[1]) + EPSILON;
}

export function pointInPolygon(point, points) {
  let inside = false;
  for (const [index, first] of points.entries()) {
    const second = points[(index + 1) % points.length];
    if (onSegment(first, second, point)) return true;
    const [x, y] = point;
    const heightChangesSide = (first[1] > y) !== (second[1] > y);
    if (heightChangesSide && x < first[0] + (second[0] - first[0]) * (y - first[1]) / (second[1] - first[1])) {
      inside = !inside;
    }
  }
  return inside;
}

function crossing(a, b, c, d) {
  return onSegment(a, b, c) || onSegment(a, b, d) || onSegment(c, d, a) || onSegment(c, d, b)
    || (Math.sign(cross(a, b, c)) * Math.sign(cross(a, b, d)) < 0
      && Math.sign(cross(c, d, a)) * Math.sign(cross(c, d, b)) < 0);
}

function simple(points) {
  return points.every((start, index) => {
    const end = points[(index + 1) % points.length];
    if (distance(start, end) <= EPSILON) return false;
    const previous = points[(index + points.length - 1) % points.length];
    if (onSegment(previous, start, end) || onSegment(start, end, previous)) return false;
    for (let other = index + 2; other < points.length; other += 1) {
      if (index === 0 && other === points.length - 1) continue;
      if (crossing(start, end, points[other], points[(other + 1) % points.length])) return false;
    }
    return true;
  });
}

function orthogonalProfile(points) {
  if (!Array.isArray(points) || points.length < 4 || !points.every(coordinates)) return null;
  const directions = points.map((point, index) => {
    const next = points[(index + 1) % points.length];
    return Math.atan2(next[1] - point[1], next[0] - point[0]);
  });
  const sin = directions.reduce((value, direction) => value + Math.sin(4 * direction), 0);
  const cos = directions.reduce((value, direction) => value + Math.cos(4 * direction), 0);
  const dominant = Math.atan2(sin, cos) / 4;
  const steps = directions.map((direction) => Math.round((direction - dominant) / RIGHT_ANGLE));
  if (directions.some((direction, index) => Math.abs(direction - dominant - steps[index] * RIGHT_ANGLE) > ANGLE_TOLERANCE + EPSILON)) return null;
  const orientation = Math.abs(dominant) <= ANGLE_TOLERANCE ? 0 : dominant;
  const cardinal = [[1, 0], [0, 1], [-1, 0], [0, -1]];
  return { orientation, directions: steps.map((step) => cardinal[(step % 4 + 4) % 4]) };
}

export function requiresPolygonDiagonals(points) {
  return points?.length > 3 && !orthogonalProfile(points);
}

export function usesMeasuredPolygon(room) {
  const points = room.polygon_px;
  if (!Array.isArray(points) || points.length < 3) return false;
  if (Array.isArray(room.edge_lengths_cm)) return true;
  if (points.length !== 4 || !points.every(coordinates)) return true;
  const [left, top, right, bottom] = boundsOfPoints(points);
  const rectangular = new Set(points.map((point) => point.join(","))).size === 4
    && points.every(([x, y], index) => {
      const [nx, ny] = points[(index + 1) % points.length];
      return (x === left || x === right) && (y === top || y === bottom) && ((x === nx) !== (y === ny));
    });
  const measurements = room.raw_measurements_cm;
  return !rectangular || ![["top", "bottom"], ["left", "right"]].every((sides) => sides.some((side) => comparableLength(measurements?.[side])));
}

function issue(code, message, target = {}) {
  return { code, message, severity: "error", ...target };
}

function orthogonalPoints(profile, lengths, issues) {
  const adjusted = [...lengths];
  for (const axis of [0, 1]) {
    const spans = profile.directions.flatMap((direction, index) => direction[axis] ? [{ direction: direction[axis], index }] : []);
    const closure = spans.reduce((total, { direction, index }) => total + direction * lengths[index], 0);
    if (!spans.length || Math.abs(closure) > CLOSURE_TOLERANCE_M + EPSILON) {
      issues.push(issue("polygon-closure", `Невязка контура по ${axis === 0 ? "горизонтали" : "вертикали"} ${(Math.abs(closure) * 100).toFixed(1)} см. Допуск 3 см.`));
      continue;
    }
    for (const { index, direction } of spans) adjusted[index] -= direction * closure / spans.length;
    if (Math.abs(closure) > EPSILON) {
      issues.push(issue("polygon-adjusted", `Уравнена невязка ${(Math.abs(closure) * 100).toFixed(1)} см по ${axis === 0 ? "горизонтали" : "вертикали"}.`,
        { severity: "warning" }));
    }
  }
  for (const [index, length] of adjusted.entries()) {
    if (length < MIN_EDGE_M - EPSILON) {
      issues.push(issue("polygon-edge", `Сторона ${index + 1} слишком короткая после уравнивания.`, { edge_index: index }));
    }
  }
  if (issues.some(({ severity }) => severity === "error")) return null;
  const points = [];
  let x = 0;
  let y = 0;
  const cosine = Math.cos(profile.orientation);
  const sine = Math.sin(profile.orientation);
  for (const [index, direction] of profile.directions.entries()) {
    points.push([x * cosine - y * sine, x * sine + y * cosine]);
    x += direction[0] * adjusted[index];
    y += direction[1] * adjusted[index];
  }
  return { points, adjusted };
}

function triangulatedPoints(sketch, lengths, diagonals, issues) {
  const end = sketch.length - 1;
  const radii = [0, lengths[0]];
  const start = sketch[0];
  for (let index = 2; index < end; index += 1) {
    const value = diagonals?.[index];
    if (!comparableLength(value)) {
      issues.push(issue("polygon-diagonal", `Введите диагональ 1–${index + 1} в сантиметрах.`, { diagonal_index: index }));
      continue;
    }
    radii[index] = value / 100;
  }
  if (issues.some(({ severity }) => severity === "error")) return null;
  radii[end] = lengths[end];
  let angle = Math.atan2(sketch[1][1] - start[1], sketch[1][0] - start[0]);
  const points = [[0, 0], [radii[1] * Math.cos(angle), radii[1] * Math.sin(angle)]];
  for (let index = 1; index < end; index += 1) {
    const first = radii[index];
    const second = radii[index + 1];
    const third = lengths[index];
    const diagonalIndex = index + 1 === end ? index : index + 1;
    if (third <= Math.abs(first - second) + EPSILON || third >= first + second - EPSILON) {
      issues.push(issue("polygon-triangle", `Треугольник 1–${index + 1}–${index + 2} не складывается из этих промеров.`,
        end > 2 ? { diagonal_index: diagonalIndex } : { edge_index: index }));
      return null;
    }
    const sign = Math.sign(cross(start, sketch[index], sketch[index + 1]));
    const normalizedTurn = Math.abs(cross(start, sketch[index], sketch[index + 1]))
      / (distance(start, sketch[index]) * distance(start, sketch[index + 1]));
    if (normalizedTurn < 0.002) {
      issues.push(issue("polygon-ambiguous", `Разведите вершины треугольника 1–${index + 1}–${index + 2} на абрисе.`,
        end > 2 ? { diagonal_index: diagonalIndex } : { edge_index: index }));
      return null;
    }
    const cosine = (first * first + second * second - third * third) / (2 * first * second);
    angle += sign * Math.acos(Math.max(-1, Math.min(1, cosine)));
    points.push([second * Math.cos(angle), second * Math.sin(angle)]);
  }
  return { points, adjusted: lengths };
}

export function triangulate(points) {
  const triangles = [];
  const indices = points.map((_, index) => index);
  const handedness = Math.sign(signedArea(points));
  if (!handedness) return triangles;
  while (indices.length > 3) {
    const ear = indices.findIndex((current, index) => {
      const previous = indices[(index - 1 + indices.length) % indices.length];
      const next = indices[(index + 1) % indices.length];
      const a = points[previous];
      const b = points[current];
      const c = points[next];
      if (handedness * cross(a, b, c) <= EPSILON) return false;
      return !indices.some((other) => {
        if (other === current || other === previous || other === next) return false;
        const point = points[other];
        return [cross(a, b, point), cross(b, c, point), cross(c, a, point)]
          .every((turn) => turn * handedness >= -EPSILON);
      });
    });
    if (ear < 0) {
      const inline = indices.findIndex((current, index) => onSegment(
        points[indices[(index - 1 + indices.length) % indices.length]], points[indices[(index + 1) % indices.length]], points[current],
      ));
      if (inline < 0) return [];
      indices.splice(inline, 1);
      continue;
    }
    triangles.push([indices[(ear - 1 + indices.length) % indices.length], indices[ear], indices[(ear + 1) % indices.length]]);
    indices.splice(ear, 1);
  }
  if (indices.length === 3 && Math.abs(cross(...indices.map((index) => points[index]))) > EPSILON) triangles.push(indices);
  return triangles;
}

export function solveMeasuredPolygon(room) {
  const sketch = room.polygon_px;
  const issues = [];
  if (!Array.isArray(sketch) || sketch.length < 3 || sketch.length > 200 || !sketch.every(coordinates)
      || Math.abs(signedArea(sketch)) <= EPSILON || !simple(sketch)) {
    issues.push(issue("polygon-sketch", "Проверьте порядок вершин и пересечения контура на абрисе."));
    return { issues };
  }
  const lengths = sketch.map((_, index) => {
    const value = room.edge_lengths_cm?.[index];
    if (comparableLength(value)) return value / 100;
    issues.push(issue("polygon-edge", `Введите промер стороны ${index + 1} в сантиметрах.`, { edge_index: index }));
    return null;
  });
  const profile = orthogonalProfile(sketch);
  if (!profile) {
    for (let index = 2; index < sketch.length - 1; index += 1) {
      if (!comparableLength(room.diagonals_cm?.[index])) {
        issues.push(issue("polygon-diagonal", `Введите диагональ 1–${index + 1} в сантиметрах.`, { diagonal_index: index }));
      }
    }
  }
  if (issues.length) return { issues };
  const requestedDiagonals = Array.from({ length: Math.max(0, sketch.length - 3) }, (_, index) => room.diagonals_cm?.[index + 2]);
  const fullFan = requestedDiagonals.length > 0 && requestedDiagonals.every(comparableLength);
  const useFan = !profile || fullFan;
  const measured = useFan
    ? triangulatedPoints(sketch, lengths, room.diagonals_cm, issues)
    : orthogonalPoints(profile, lengths, issues);
  if (!measured) return { issues };
  if (!useFan) {
    for (const [offset, value] of requestedDiagonals.entries()) {
      if (value === null || value === undefined) continue;
      const index = offset + 2;
      if (!comparableLength(value) || Math.abs(distance(measured.points[0], measured.points[index]) - value / 100) > CLOSURE_TOLERANCE_M + EPSILON) {
        issues.push(issue("polygon-diagonal-conflict", `Диагональ 1–${index + 1} расходится с промерами стен.`, { diagonal_index: index }));
      }
    }
  }
  const area = signedArea(measured.points);
  const triangles = triangulate(measured.points);
  if (Math.abs(area) < 0.001 || !simple(measured.points) || !triangles.length || Math.sign(area) !== Math.sign(signedArea(sketch))) {
    issues.push(issue("polygon-self-intersection", "Промеры образуют пересекающийся, перевёрнутый или вырожденный контур."));
  }
  if (issues.some(({ severity }) => severity === "error")) return { issues };
  const [left, top, right, bottom] = boundsOfPoints(measured.points);
  return {
    issues,
    method: useFan ? "triangulated" : "orthogonal",
    points: measured.points.map(([x, y]) => [x - left, y - top]),
    width: right - left,
    length: bottom - top,
    area: Math.abs(area),
    edgeLengths: measured.adjusted,
    residuals_cm: measured.adjusted.map((value, index) => (value - lengths[index]) * 100),
    triangles,
  };
}

export function mapPolygonPoint(point, source, target) {
  for (const indices of triangulate(source)) {
    const [a, b, c] = indices.map((index) => source[index]);
    const total = cross(a, b, c);
    const weights = [cross(b, c, point) / total, cross(c, a, point) / total, cross(a, b, point) / total];
    if (weights.some((weight) => weight < -EPSILON)) continue;
    const mapped = [0, 1].map((axis) => indices.reduce((value, index, offset) => value + target[index][axis] * weights[offset], 0));
    if (pointInPolygon(mapped, target)) return mapped;
  }
  return null;
}

export function polygonLabelPoint(points, triangles) {
  const area = signedArea(points);
  const offset = points[0];
  const sum = [0, 0];
  for (const [index, current] of points.slice(1, -1).entries()) {
    const next = points[index + 2];
    const weight = cross(offset, current, next) / (6 * area);
    for (const axis of [0, 1]) sum[axis] += weight * (offset[axis] + current[axis] + next[axis]);
  }
  if (pointInPolygon(sum, points)) return sum;
  const triangle = triangles.map((indices) => indices.map((index) => points[index]))
    .sort((first, second) => Math.abs(cross(...second)) - Math.abs(cross(...first)))[0];
  return [0, 1].map((axis) => triangle.reduce((total, point) => total + point[axis] / 3, 0));
}

function clipConvex(subject, clip) {
  const side = Math.sign(signedArea(clip));
  let output = subject;
  for (const [index, start] of clip.entries()) {
    const end = clip[(index + 1) % clip.length];
    const input = output;
    output = [];
    for (const [offset, first] of input.entries()) {
      const second = input[(offset + 1) % input.length];
      const firstDistance = cross(start, end, first) * side;
      const secondDistance = cross(start, end, second) * side;
      const firstInside = firstDistance >= -EPSILON;
      const secondInside = secondDistance >= -EPSILON;
      if (firstInside) output.push(first);
      if (firstInside === secondInside || Math.abs(firstDistance - secondDistance) < EPSILON) continue;
      const ratio = firstDistance / (firstDistance - secondDistance);
      output.push([first[0] + (second[0] - first[0]) * ratio, first[1] + (second[1] - first[1]) * ratio]);
    }
  }
  return output;
}

export function polygonIntersectionArea(first, firstTriangles, second, secondTriangles, stopAbove = Infinity) {
  let area = 0;
  for (const a of firstTriangles) {
    const triangleA = a.map((index) => first[index]);
    for (const b of secondTriangles) {
      const clipped = clipConvex(triangleA, b.map((index) => second[index]));
      if (clipped.length >= 3) area += Math.abs(signedArea(clipped));
      if (area > stopAbove) return area;
    }
  }
  return area;
}
