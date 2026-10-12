// Builds apps/api/data/roads.json, the street network couriers ride on, from OpenStreetMap.
//
//   node scripts/build-roads.mjs              downloads central Bangkok from the Overpass API (about 35 MB)
//   node scripts/build-roads.mjs ways.json    uses an Overpass JSON file you already have
//
// Main roads only (trunk down to unclassified, no expressways, no side streets): the side streets would make the file
// four times bigger. Points within 4 m of a straight line are dropped, and only the largest connected piece is kept,
// so a route exists between any two points. Map data © OpenStreetMap contributors, available under the ODbL.
import fs from 'fs';

const BBOX = [13.66, 100.45, 13.84, 100.65];   // south, west, north, east: the city box on the map plus a margin
const TYPES = 'trunk|primary|secondary|tertiary|unclassified|trunk_link|primary_link|secondary_link|tertiary_link';
const MIRRORS = ['https://overpass-api.de/api/interpreter', 'https://maps.mail.ru/osm/tools/overpass/api/interpreter'];
const TOLERANCE_M = 4;
const OUT = new URL('../apps/api/data/roads.json', import.meta.url);

async function download() {
  const q = `[out:json][timeout:180];way["highway"~"^(${TYPES})$"](${BBOX.join(',')});out geom qt;`;
  for (const url of MIRRORS) {
    const r = await fetch(url, { method: 'POST', body: new URLSearchParams({ data: q }), headers: { 'user-agent': 'food-delivery-lab' } });
    const text = await r.text();
    if (r.ok && text.startsWith('{')) return JSON.parse(text);
    console.error(`${url}: ${r.status}, trying the next mirror`);
  }
  throw new Error('every Overpass mirror refused; try again later');
}

const data = process.argv[2] ? JSON.parse(fs.readFileSync(process.argv[2], 'utf8')) : await download();
const ways = data.elements.filter(w => w.type === 'way' && new RegExp(`^(${TYPES})$`).test(w.tags?.highway))
  .map(w => w.geometry.map(p => [Math.round(p.lat * 1e5), Math.round(p.lon * 1e5)]));   // 1e-5 degrees, about 1 m

// A point shared by two ways, or the end of a way, is a junction and always kept.
const key = p => p[0] * 1e8 + p[1];
const uses = new Map();
for (const w of ways) w.forEach((p, i) => uses.set(key(p), (uses.get(key(p)) || 0) + (i === 0 || i === w.length - 1 ? 2 : 1)));

const M_PER_UNIT = 1.11;   // metres per 1e-5 degree of latitude; longitude is scaled by cos(latitude)
const COS = Math.cos(13.75 * Math.PI / 180);
function offLine(p, a, b) {   // distance in metres from p to the segment a-b
  const [px, py, ax, ay, bx, by] = [p[1] * COS, p[0], a[1] * COS, a[0], b[1] * COS, b[0]];
  const dx = bx - ax, dy = by - ay, len = dx * dx + dy * dy;
  const t = len ? Math.max(0, Math.min(1, ((px - ax) * dx + (py - ay) * dy) / len)) : 0;
  return Math.hypot(px - ax - t * dx, py - ay - t * dy) * M_PER_UNIT;
}
function simplify(pts) {   // Douglas-Peucker
  if (pts.length < 3) return pts;
  let worst = 0, at = 0;
  for (let i = 1; i < pts.length - 1; i++) { const d = offLine(pts[i], pts[0], pts.at(-1)); if (d > worst) { worst = d; at = i; } }
  if (worst <= TOLERANCE_M) return [pts[0], pts.at(-1)];
  return [...simplify(pts.slice(0, at + 1)).slice(0, -1), ...simplify(pts.slice(at))];
}

const ids = new Map(), nodes = [], edges = new Set();
const id = p => { const k = key(p); if (!ids.has(k)) { ids.set(k, nodes.length); nodes.push(p); } return ids.get(k); };
for (const w of ways) {
  let piece = [w[0]];
  for (let i = 1; i < w.length; i++) {
    piece.push(w[i]);
    if (uses.get(key(w[i])) > 1 || i === w.length - 1) {
      const s = simplify(piece);
      for (let j = 1; j < s.length; j++) {
        const a = id(s[j - 1]), b = id(s[j]);
        if (a !== b) edges.add(a < b ? `${a},${b}` : `${b},${a}`);
      }
      piece = [w[i]];
    }
  }
}

// Keep the largest connected piece (one-way streets are ignored: a courier on a scooter, not a car).
const adj = nodes.map(() => []);
for (const e of edges) { const [a, b] = e.split(',').map(Number); adj[a].push(b); adj[b].push(a); }
const comp = new Int32Array(nodes.length).fill(-1), sizes = [];
for (let s = 0; s < nodes.length; s++) {
  if (comp[s] >= 0) continue;
  const stack = [s]; comp[s] = sizes.length; let n = 0;
  while (stack.length) { const u = stack.pop(); n++; for (const v of adj[u]) if (comp[v] < 0) { comp[v] = sizes.length; stack.push(v); } }
  sizes.push(n);
}
const main = sizes.indexOf(Math.max(...sizes));
const keep = new Int32Array(nodes.length).fill(-1), kept = [];
nodes.forEach((p, i) => { if (comp[i] === main) { keep[i] = kept.length; kept.push(p); } });
const outEdges = [];
for (const e of edges) { const [a, b] = e.split(',').map(Number); if (keep[a] >= 0) outEdges.push(keep[a], keep[b]); }

// Delta-encoded so the numbers stay short: lat/lon in 1e-5 degrees, edges as pairs of node indexes.
const delta = xs => xs.map((x, i) => i ? x - xs[i - 1] : x);
const out = { attribution: 'Map data © OpenStreetMap contributors, ODbL (https://www.openstreetmap.org/copyright)',
  bbox: BBOX, scale: 1e5, lat: delta(kept.map(p => p[0])), lon: delta(kept.map(p => p[1])), edges: delta(outEdges) };
fs.mkdirSync(new URL('.', OUT), { recursive: true });
fs.writeFileSync(OUT, JSON.stringify(out));
console.log(`${kept.length} nodes, ${outEdges.length / 2} road segments (${sizes.length - 1} small disconnected pieces dropped), ` +
  `${(fs.statSync(OUT).size / 1024).toFixed(0)} KB -> apps/api/data/roads.json`);
