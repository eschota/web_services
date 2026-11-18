const width = 1024;
const height = 766;
const sides = ['top', 'right', 'bottom', 'left'];
const opening = (id, wall, type, width_cm, offset_cm, t, confidence = .93, extra = {}) => ({
  id, wall, type, width_cm, offset_cm, t, confidence, verified: true, ...extra,
});
const windowAt = (id, wall, width, offset, t, extra) => opening(id, wall, 'WINDOW_STD', width, offset, t, .96, extra);
const doorAt = (id, wall, width, offset, t) => opening(id, wall, 'DOOR_SINGLE', width, offset, t, .92);

function room(id, bbox_px, numbers, sharedSides, extra = {}) {
  const [top, bottom, left, right] = numbers;
  const wall_types = {};
  const wall_thickness_cm = {};
  for (const side of sides) {
    const partition = sharedSides.includes(side);
    wall_types[side] = partition ? 'WALL_PARTITION' : 'WALL_CAPITAL';
    wall_thickness_cm[side] = partition ? 12 : 25;
  }
  return {
    id, bbox_px, raw_measurements_cm: { top, bottom, left, right }, wall_types, wall_thickness_cm,
    is_heated: true, confidence: .94, verified: true, openings: [], symbols: [], ...extra,
  };
}

function firstFloor() {
  return [
    room('r1', [142, 266, 455, 447], [298, 297, 230, 230], ['top', 'right', 'bottom'], {
      openings: [windowAt('r1-window', 'left', 106, 56, .47), doorAt('r1-door-2', 'top', 68, 169, .70),
        opening('r1-door-4', 'bottom', 'DOOR_SINGLE', 76, null, .67, .91, { offset_right_cm: 60 })],
      symbols: [{ type: 'STAIRS', u: .14, v: .78, width_cm: 84, depth_cm: 86 }],
    }),
    room('r2', [151, 133, 455, 255], [297, 295, 250, 253], ['right', 'bottom'], {
      measurement_weights: { top: 6, bottom: 1, left: 6, right: 1 },
      openings: [windowAt('r2-window', 'top', 103, null, .44, { offset_right_cm: 95 })],
      symbols: [{ type: 'STOVE', u: .90, v: .81, width_cm: 68, depth_cm: 53 }],
      confidence: .91,
    }),
    room('r3', [466, 133, 697, 455], [305, 301, 494, 491], ['left', 'right'], {
      measurement_weights: { top: 4, bottom: 4, left: 4, right: 4 },
      wall_thickness_cm: { top: 25, right: 12, bottom: 29, left: 12 },
      openings: [windowAt('r3-window', 'top', 107, null, .55, { offset_right_cm: 102 }),
        opening('r3-glazed', 'bottom', 'DOOR_GLAZED', 190, 62, .51, .82, { raw_label: 'створ' }),
        doorAt('r3-door', 'left', 89, 270, .62)],
      confidence: .82,
    }),
    room('r4', [142, 458, 455, 663], [298, 299, 261, 259], ['top', 'right'], {
      measurement_weights: { top: 4, bottom: 1 },
      openings: [windowAt('r4-window-left', 'left', 113, null, .42, { offset_right_cm: 20 }),
        windowAt('r4-window-front', 'bottom', 113, 21, .26), windowAt('r4-window-back', 'bottom', 113, null, .72, { offset_right_cm: 30 }),
        doorAt('r4-door', 'right', 80, 89, .49)],
      symbols: [{ type: 'KITCHEN', u: .09, v: .83, width_cm: 52, depth_cm: 75 }],
    }),
    room('r5', [707, 128, 874, 470], [206, 207, 509, null], ['left'], {
      measurement_weights: { top: 5, bottom: 1 },
      wall_thickness_cm: { top: 25, right: 25, bottom: 12, left: 12 },
      openings: [opening('r5-glazed', 'bottom', 'DOOR_GLAZED', 80, 30, .36, .83, { raw_label: 'створ' })],
      confidence: .86,
    }),
    room('terrace', [466, 475, 615, 663], [206, null, null, 260], [], {
      is_heated: false,
      wall_types: { top: 'VERANDA', right: 'VERANDA', bottom: 'VERANDA', left: 'VERANDA' },
      wall_thickness_cm: { top: 0, right: 0, bottom: 0, left: 0 },
      symbols: [{ type: 'COLUMN', u: .95, v: .96, width_cm: 15, depth_cm: 15 }],
    }),
  ];
}

function atticFloor() {
  return [
    room('r6', [246, 463, 575, 598], [305, null, null, 153], ['top', 'right'], {
      wall_thickness_cm: { top: 10, right: 10, bottom: 15, left: 25 },
      local_measurements_cm: [{ label: 'У лестничного проёма', value_cm: 104, bbox_px: [260, 523, 290, 546] }],
      openings: [windowAt('r6-window', 'bottom', 101, null, .73, { offset_right_cm: 19 }), doorAt('r6-door', 'top', 60, 122, .50)],
      symbols: [{ type: 'STAIRS', u: .13, v: .72, width_cm: 84, depth_cm: 86 }],
    }),
    room('r7', [263, 171, 575, 450], [289, 288, 330, 328], ['right', 'bottom'], {
      measurement_weights: { top: 5, bottom: 1 },
      wall_thickness_cm: { top: 25, right: 10, bottom: 10, left: 25 },
      symbols: [{ type: 'SHAFT', u: .89, v: .65, width_cm: 38, depth_cm: 36 }],
    }),
    room('r8', [588, 171, 801, 583], [205, 202, 482, 482], ['left'], {
      wall_thickness_cm: { top: 25, right: 25, bottom: 25, left: 10 },
      openings: [windowAt('r8-window-top', 'top', 110, 15, .34), windowAt('r8-window-bottom', 'bottom', 113, 15, .34),
        doorAt('r8-door', 'left', 69, 332, .76)],
      confidence: .91,
    }),
  ];
}

// Open paths keep the numerals legible without a handwriting-font download.
const digits = [
  'M7 1 Q12 0 11 7 Q10 17 4 18 Q0 17 1 10 Q2 3 7 1', 'M2 5 L8 1 Q5 9 4 18',
  'M1 6 Q5 0 9 2 Q13 4 7 9 L.5 17 Q6 16 11 17', 'M1 3 Q6 0 10 2 Q11 5 6 8 Q11 8 10 12 Q10 18 2 18',
  'M7 1 L.5 12 L11 11 M9 3 L7 18', 'M11 2 L3 2 L2 9 Q10 7 10 12 Q8 18 1 17',
  'M10 1 Q2 5 1 13 Q1 19 7 17 Q12 14 9 10 Q6 7 2 11', 'M1 3 Q5 2 12 2 Q7 8 5 18 M2 10 L10 9',
  'M7 1 Q15 1 6 9 Q-3 16 3 18 Q11 17 11 12 Q11 9 5 7 Q-2 3 7 1', 'M10 7 Q7 12 2 9 Q-1 7 3 2 Q8-1 11 3 Q12 10 5 18',
];

function handNumber(value, x, y, angle = 0, ratio = 1) {
  const marks = [...String(value)].map((digit, i) => `<path transform="translate(${i * 13} ${i % 2}) rotate(${(i % 3) * 3 - 4})" d="${digits[digit]}"/>`).join('');
  return `<g transform="translate(${x} ${y}) rotate(${angle}) scale(${ratio}) translate(${-String(value).length * 6} -8)" fill="none" stroke="#344f81" stroke-width="1.35" vector-effect="non-scaling-stroke">${marks}</g>`;
}

function inkLine(x1, y1, x2, y2, seed, bold = false) {
  const dip = Math.sin(seed * 7.11) * 2.6;
  const horizontal = Math.abs(x2 - x1) > Math.abs(y2 - y1);
  const cx = (x1 + x2) / 2 + (horizontal ? 0 : dip);
  const cy = (y1 + y2) / 2 + (horizontal ? dip : 0);
  return `<path fill="none" stroke="#375681" stroke-width="${bold ? 1.8 : 1.35}" d="M${x1} ${y1} Q${cx} ${cy} ${x2} ${y2}"/>`;
}

function symbolMarkup(symbol, x, y, w, h) {
  const px = (x + w * symbol.u).toFixed(1);
  const py = (y + h * symbol.v).toFixed(1);
  const blocks = {
    STAIRS: '<path d="M-28-27h24v35h36v23h-60z M-28-13h24m-24 13h24m-24 12 27-5m-27 17L0 8m11 0v23m10-23v23 M-17-20v38q0 3 3 3h39m-8-6 8 6-8 5"/>',
    STOVE: '<path d="M-24-17h48v35h-48z M-24 18l48-35M-10 18q6-16 18 0"/>',
    SHAFT: '<path d="M-16-15h32v30h-32zM-16 15l32-30"/>',
    COLUMN: '<path d="M-7-7h14V7H-7z"/>',
    KITCHEN: '<path d="M-15-33h30v66h-30zM-15 1h30m-24 17h19m-9-9v19"/><circle cx="-7" cy="-18" r="4"/><circle cx="6" cy="-18" r="5"/>',
  };
  return `<g transform="translate(${px} ${py}) rotate(-1)" fill="none" stroke="#355580" stroke-width="1.55">${blocks[symbol.type] ?? ''}</g>`;
}

function buildSketch(floor, index) {
  const walls = [], openings = [], notes = [];
  floor.rooms.forEach((entry, i) => {
    const [x, y, right, bottom] = entry.bbox_px;
    const w = right - x;
    const h = bottom - y;
    const locations = {
      top: { x, y, endX: right, endY: y, span: w, angle: 0, outside: -1 },
      bottom: { x, y: bottom, endX: right, endY: bottom, span: w, angle: 0, outside: 1 },
      left: { x, y, endX: x, endY: bottom, span: h, angle: 90, outside: 1 },
      right: { x: right, y, endX: right, endY: bottom, span: h, angle: 90, outside: -1 },
    };
    entry.raw_measurement_boxes_px = {};
    entry.measurement_confidences = {};
    for (const [j, side] of sides.entries()) {
      const edge = locations[side];
      walls.push(inkLine(edge.x, edge.y, edge.endX, edge.endY, i * 5 + j, true));
      if (entry.wall_types[side] === 'WALL_CAPITAL') {
        const dx = edge.angle === 90 ? -13 * edge.outside : 0;
        const dy = edge.angle === 0 ? 13 * edge.outside : 0;
        walls.push(inkLine(edge.x + dx - (dx === 0 ? 11 : 0), edge.y + dy - (dy === 0 ? 11 : 0), edge.endX + dx + (dx === 0 ? 11 : 0), edge.endY + dy + (dy === 0 ? 11 : 0), 42 + i * 5 + j));
      }
      const measurement = entry.raw_measurements_cm[side];
      if (measurement == null) continue;
      const inset = 27 + (j % 2) * 5;
      const px = edge.angle === 0 ? x + w * .51 : edge.x + edge.outside * inset;
      const py = edge.angle === 0 ? edge.y - edge.outside * inset : y + h * .53;
      notes.push(handNumber(measurement, px, py, edge.angle === 0 ? (i % 3) * 3 - 4 : (side === 'left' ? -94 : 88), j % 2 ? .94 : 1.03));
      entry.raw_measurement_boxes_px[side] = edge.angle ? [px - 13, py - 23, px + 13, py + 23] : [px - 23, py - 13, px + 23, py + 13];
      entry.measurement_confidences[side] = entry.id === 'r3' && side === 'right' ? .79 : .96;
    }
    for (const obj of entry.openings) {
      const edge = locations[obj.wall];
      const depth = edge.outside;
      const span = Math.min(102, Math.max(25, obj.width_cm * .57));
      const begin = edge.span * obj.t - span / 2;
      const end = begin + span;
      const window = obj.type === 'WINDOW_STD' || obj.type === 'DOOR_GLAZED';
      const mark = window
        ? `<path d="M${begin} 0v${depth * 15}h${span}V0 M${begin} ${depth * 6.2}h${span}m${-span} ${depth * 3.2}h${span}"/>`
        : `<path d="M${begin} ${depth * 12}v${depth * -23}M${end} ${depth * 10}v${depth * -23}M${begin-6} ${depth * -5}l${span+12} ${depth * -1.2}"/>`;
      const label = obj.type === 'DOOR_GLAZED' ? `<text x="${end + 6}" y="${depth * 22}" fill="#345482" stroke="none">створ</text>` : '';
      const textAngle = depth > 0 && edge.angle ? 180 : 0;
      const number = handNumber(obj.width_cm, (begin + end) / 2, depth * (window ? 26 : 18), textAngle, .66);
      const fromEnd = obj.offset_right_cm != null;
      const offset = fromEnd ? obj.offset_right_cm : obj.offset_cm;
      const clearance = fromEnd ? edge.span - end : begin;
      const offsetNote = clearance > 62 && offset > 0 && obj.type !== 'DOOR_GLAZED'
        ? handNumber(offset, fromEnd ? end + clearance * .52 : begin * .48, depth * 20, textAngle - 3, .56)
        : '';
      openings.push(`<g transform="translate(${edge.x} ${edge.y}) rotate(${edge.angle})" fill="none" stroke="#2c527f" stroke-width="1.55">${mark}${number}${label}${offsetNote}</g>`);
    }
    for (const [symbolIndex, symbol] of entry.symbols.entries()) {
      symbol.id = `${entry.id}-symbol-${symbolIndex + 1}`;
      openings.push(symbolMarkup(symbol, x, y, w, h));
    }
  });
  const section = index === 1 ? `<g transform="translate(689 637) rotate(-2)">
    <path fill="none" stroke="#375681" stroke-width="1.6" d="M0 62Q44 27 77 16L77 63Z M84 59V20m-4 7 4-7 4 7m-7 29 3 6 4-6"/>
    <text x="94" y="56">h = 208</text><text x="31" y="37" transform="rotate(-34 31 37)">570</text>
  </g>` : '';
  const localNotes = floor.rooms.flatMap(entry => (entry.local_measurements_cm || []).map(measurement => {
    const [x1, y1, x2, y2] = measurement.bbox_px;
    return handNumber(measurement.value_cm, (x1 + x2) / 2, (y1 + y2) / 2, -8, .73);
  })).join('');
  return `<svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}" viewBox="0 0 ${width} ${height}">
  <defs>
    <pattern id="grid" width="19" height="19" patternUnits="userSpaceOnUse"><path d="M19 0H0V19" fill="none" stroke="#d0dce6" stroke-width=".78" opacity=".80"/></pattern>
    <linearGradient id="paper" x2=".78" y2="1"><stop stop-color="#f9f8f0"/><stop offset="1" stop-color="#fffdf5"/></linearGradient>
  </defs>
  <path fill="url(#paper)" d="M0 0h${width}v${height}H0z"/>
  <path fill="url(#grid)" opacity=".74" d="M30 98h964v623H30z"/>
  <path stroke="#d3d8d6" stroke-width="1" d="M50 89h924m-924 631h924"/>
  <text x="51" y="53" fill="#4c5460" font-family="monospace" font-size="19" font-weight="bold" letter-spacing="2.3">ПОЛЕВОЙ АБРИС</text>
  <text x="51" y="75" fill="#68727d" font-family="monospace" font-size="11" letter-spacing="1.2">ДЕМО-ПРОЕКТ · ОБМЕРЫ В СМ</text>
  <g font-family="Georgia, serif" font-style="italic" font-size="17" fill="#345482" stroke="none" stroke-linecap="round" stroke-linejoin="round">
    <text x="786" y="63" transform="rotate(-5 786 63)" font-size="31">${index ? 'Мансарда' : 'Этаж 1'}</text>
    ${walls.join('')}${openings.join('')}${notes.join('')}${section}${localNotes}
  </g>
  <text x="50" y="744" fill="#727982" font-family="monospace" font-size="11" letter-spacing="1.2">ОБМЕРНЫЙ ЛИСТ</text>
  <text x="974" y="744" text-anchor="end" fill="#727982" font-family="monospace" font-size="11" letter-spacing="1.2">0${index + 1} / 02</text>
</svg>`;
}

export function createDemo() {
  const floors = [
    { floor_id: 'floor_1', title: 'Этаж 1', rooms: firstFloor(),
      exterior_alignments: [{ side: 'bottom', room_a: 'r3', room_b: 'r5' }] },
    { floor_id: 'floor_attic', title: 'Этаж мансардный', rooms: atticFloor(), height_m: 2.08,
      exterior_alignments: [{ side: 'bottom', room_a: 'r6', room_b: 'r8' }] },
  ].map((entry, i) => ({
    ...entry,
    source_bbox_px: [0, 0, width, height],
    image_width: width,
    image_height: height,
    image: `data:image/svg+xml;charset=utf-8,${encodeURIComponent(buildSketch(entry, i))}`,
    walls: [],
  }));
  return {
    name: 'Жилой дом с мансардой',
    project_meta: {
      scale: '1:100', target_profile: 'TECH_PLAN_P0082', height_m: 2.08,
      contract_number: '', customer: '', cadastral_number: '', address: '',
    },
    floors,
  };
}
