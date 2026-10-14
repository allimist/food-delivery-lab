// Street routing: the shortest ride along real roads (central Bangkok main roads from OpenStreetMap, built by
// scripts/build-roads.mjs). A point off the network joins it at the nearest spot on the nearest road, so a route is
// [from, where it joins the road, ...junctions, where it leaves the road, to]. A* over about 13k junctions takes a few ms.
import fs from 'fs';
import path from 'path';
import { haversineKm } from './geo';

export type LatLon = [number, number];

const file = JSON.parse(fs.readFileSync(path.join(__dirname, '../data/roads.json'), 'utf8'));
const undelta = (xs: number[]) => { let s = 0; return xs.map(x => (s += x)); };
const LAT = Float64Array.from(undelta(file.lat), v => v / file.scale);
const LON = Float64Array.from(undelta(file.lon), v => v / file.scale);
const N = LAT.length;
const EDGES = undelta(file.edges);

// Adjacency in compressed form: the neighbours of u are NB[START[u] .. START[u+1]), with the length of each road piece.
const START = new Int32Array(N + 1), NB = new Int32Array(EDGES.length), LEN = new Float64Array(EDGES.length);
for (const u of EDGES) START[u + 1]++;
for (let i = 0; i < N; i++) START[i + 1] += START[i];
{
  const fill = START.slice(0, N);
  for (let i = 0; i < EDGES.length; i += 2) {
    const a = EDGES[i], b = EDGES[i + 1], d = haversineKm(LAT[a], LON[a], LAT[b], LON[b]);
    NB[fill[a]] = b; LEN[fill[a]++] = d;
    NB[fill[b]] = a; LEN[fill[b]++] = d;
  }
}

// Grid of road pieces by 0.005 degree cell (about 550 m), to find the nearest road without scanning them all.
const CELL = 0.005;
const grid = new Map<string, number[]>();   // cell -> edge index (into EDGES, step 2)
const cellOf = (lat: number, lon: number) => `${Math.floor(lat / CELL)},${Math.floor(lon / CELL)}`;
for (let i = 0; i < EDGES.length; i += 2) {
  const a = EDGES[i], b = EDGES[i + 1];
  for (let y = Math.floor(Math.min(LAT[a], LAT[b]) / CELL); y <= Math.floor(Math.max(LAT[a], LAT[b]) / CELL); y++)
    for (let x = Math.floor(Math.min(LON[a], LON[b]) / CELL); x <= Math.floor(Math.max(LON[a], LON[b]) / CELL); x++) {
      const k = `${y},${x}`; (grid.get(k) || grid.set(k, []).get(k)!).push(i);
    }
}

type Snap = { a: number; b: number; lat: number; lon: number; toA: number; toB: number };
/** The nearest point on any road, and how far it is along that road piece to either end. */
function snap(lat: number, lon: number): Snap {
  const cos = Math.cos(lat * Math.PI / 180);
  let best: Snap | null = null, bestD = Infinity, hitRing = Infinity;
  for (let ring = 0; ring < 40 && ring <= hitRing + 1; ring++) {   // one ring past the first hit: a closer road can sit in the next cell
    const cy = Math.floor(lat / CELL), cx = Math.floor(lon / CELL);
    for (let y = cy - ring; y <= cy + ring; y++) for (let x = cx - ring; x <= cx + ring; x++) {
      if (Math.max(Math.abs(y - cy), Math.abs(x - cx)) !== ring) continue;
      for (const i of grid.get(`${y},${x}`) || []) {
        const a = EDGES[i], b = EDGES[i + 1];
        const dx = (LON[b] - LON[a]) * cos, dy = LAT[b] - LAT[a], len = dx * dx + dy * dy;
        const t = len ? Math.max(0, Math.min(1, (((lon - LON[a]) * cos) * dx + (lat - LAT[a]) * dy) / len)) : 0;
        const pLat = LAT[a] + t * dy, pLon = LON[a] + t * (LON[b] - LON[a]);
        const d = (pLat - lat) ** 2 + ((pLon - lon) * cos) ** 2;
        if (d < bestD) {
          const piece = haversineKm(LAT[a], LON[a], LAT[b], LON[b]);
          bestD = d; best = { a, b, lat: pLat, lon: pLon, toA: t * piece, toB: (1 - t) * piece }; hitRing = Math.min(hitRing, ring);
        }
      }
    }
  }
  return best!;
}

/** Min-heap of [priority, node]. */
class Heap {
  private p: number[] = []; private v: number[] = [];
  get size() { return this.v.length; }
  push(pri: number, node: number) {
    let i = this.v.length; this.p.push(pri); this.v.push(node);
    while (i > 0) { const up = (i - 1) >> 1; if (this.p[up] <= pri) break; this.p[i] = this.p[up]; this.v[i] = this.v[up]; i = up; }
    this.p[i] = pri; this.v[i] = node;
  }
  pop() {
    const top = this.v[0], pri = this.p.pop()!, node = this.v.pop()!, n = this.v.length;
    if (n) {
      let i = 0;
      for (;;) {
        let c = 2 * i + 1; if (c >= n) break;
        if (c + 1 < n && this.p[c + 1] < this.p[c]) c++;
        if (this.p[c] >= pri) break;
        this.p[i] = this.p[c]; this.v[i] = this.v[c]; i = c;
      }
      this.p[i] = pri; this.v[i] = node;
    }
    return top;
  }
}

const dist = new Float64Array(N + 1), prev = new Int32Array(N + 1), done = new Uint8Array(N + 1);
const GOAL = N;   // a virtual node: the spot where the route leaves the road

/** Shortest ride from one point to another along the roads, as [lat, lon] points (ends included). */
export function route(fromLat: number, fromLon: number, toLat: number, toLon: number): LatLon[] {
  const s = snap(fromLat, fromLon), t = snap(toLat, toLon);
  const ends = (mid: LatLon[]): LatLon[] => dedupe([[fromLat, fromLon], [s.lat, s.lon], ...mid, [t.lat, t.lon], [toLat, toLon]]);
  if (s.a === t.a && s.b === t.b) return ends([]);   // both on the same stretch of road

  dist.fill(Infinity); prev.fill(-1); done.fill(0);
  const h = (u: number) => u === GOAL ? 0 : haversineKm(LAT[u], LON[u], t.lat, t.lon);
  const heap = new Heap();
  const start = (u: number, d: number) => { if (d < dist[u]) { dist[u] = d; prev[u] = -1; heap.push(d + h(u), u); } };
  start(s.a, s.toA); start(s.b, s.toB);
  while (heap.size) {
    const u = heap.pop();
    if (done[u]) continue;
    done[u] = 1;
    if (u === GOAL) break;
    const relax = (v: number, d: number) => { if (d < dist[v]) { dist[v] = d; prev[v] = u; heap.push(d + h(v), v); } };
    if (u === t.a) relax(GOAL, dist[u] + t.toA);
    if (u === t.b) relax(GOAL, dist[u] + t.toB);
    for (let k = START[u]; k < START[u + 1]; k++) relax(NB[k], dist[u] + LEN[k]);
  }
  const mid: LatLon[] = [];
  for (let u = prev[GOAL]; u >= 0; u = prev[u]) mid.push([LAT[u], LON[u]]);
  return ends(mid.reverse());
}

/** Length of a route in km. */
export const lengthKm = (r: LatLon[]) => r.reduce((s, p, i) => i ? s + haversineKm(r[i - 1][0], r[i - 1][1], p[0], p[1]) : 0, 0);
/** Ride distance along the roads. */
export const roadKm = (fromLat: number, fromLon: number, toLat: number, toLon: number) => lengthKm(route(fromLat, fromLon, toLat, toLon));
/** The nearest point on a road: where a customer's door meets the street. */
export const onRoad = (lat: number, lon: number) => { const s = snap(lat, lon); return { lat: s.lat, lon: s.lon }; };

const r6 = (x: number) => Math.round(x * 1e6) / 1e6;
function dedupe(pts: LatLon[]) {   // drops repeats closer than about 2 m, rounds to about 10 cm
  const out: LatLon[] = [];
  for (const [lat, lon] of pts) {
    const last = out.at(-1);
    if (!last || Math.abs(last[0] - lat) + Math.abs(last[1] - lon) > 0.00002) out.push([r6(lat), r6(lon)]);
  }
  return out;
}
