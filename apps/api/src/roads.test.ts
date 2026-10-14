// Street routing checks. Run: npm test
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { route, lengthKm, onRoad, roadKm } from './roads';
import { haversineKm } from './geo';

const LUMPINI = [13.7314, 100.5414] as const, THONGLOR = [13.7326, 100.5800] as const, ARI = [13.7797, 100.5446] as const;
const m = (a: readonly number[], b: readonly number[]) => haversineKm(a[0], a[1], b[0], b[1]) * 1000;

test('a route starts and ends at the given points', () => {
  const r = route(LUMPINI[0], LUMPINI[1], THONGLOR[0], THONGLOR[1]);
  assert.ok(r.length > 2, 'goes through junctions');
  assert.ok(m(r[0], LUMPINI) < 1);
  assert.ok(m(r.at(-1)!, THONGLOR) < 1);
});

test('the ride along the roads is never shorter than the straight line, and not absurdly longer', () => {
  for (const [a, b] of [[LUMPINI, THONGLOR], [THONGLOR, ARI], [ARI, LUMPINI]]) {
    const road = roadKm(a[0], a[1], b[0], b[1]), straight = m(a, b) / 1000;
    assert.ok(road >= straight - 1e-9, `${road} >= ${straight}`);
    assert.ok(road < straight * 2.5, `${road} < 2.5 x ${straight}`);
  }
});

test('a route is symmetric in length', () => {
  const there = roadKm(LUMPINI[0], LUMPINI[1], ARI[0], ARI[1]), back = roadKm(ARI[0], ARI[1], LUMPINI[0], LUMPINI[1]);
  assert.ok(Math.abs(there - back) < 0.01, `${there} vs ${back}`);
});

test('two points on the same stretch of road ride straight along it', () => {
  const p = onRoad(...LUMPINI);
  const r = route(p.lat, p.lon, p.lat + 0.00001, p.lon);
  assert.ok(lengthKm(r) < 0.05);
});

test('onRoad lands on a road: snapping again does not move it', () => {
  for (let i = 0; i < 50; i++) {
    const p = onRoad(13.70 + Math.random() * 0.1, 100.49 + Math.random() * 0.12), q = onRoad(p.lat, p.lon);
    assert.ok(m([p.lat, p.lon], [q.lat, q.lon]) < 1);
  }
});

test('routing is fast: under 5 ms a ride across the city', () => {
  const t0 = performance.now();
  for (let i = 0; i < 100; i++) route(13.70 + Math.random() * 0.1, 100.49 + Math.random() * 0.12, 13.70 + Math.random() * 0.1, 100.49 + Math.random() * 0.12);
  assert.ok((performance.now() - t0) / 100 < 5);
});
