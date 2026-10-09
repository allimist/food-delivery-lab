// Simulated couriers. Each one is a normal courier account using the public API, like a phone app would:
// go online, ping its position, ride to the restaurant, pick up (once the food is ready), ride to the customer, deliver.
// The admin sets how many are online (GET /api/sim/settings); the others finish their order and go offline.
const API = process.env.API_URL || 'http://localhost:3030/api';
const COUNT = Number(process.env.SIM_COURIERS || 40);
const TICK_MS = 1000;
const IDLE_PING_MS = 3000;

type Pos = { lat: number; lon: number };
type Courier = { n: number; token?: string; pos: Pos; online: boolean; lastPing: number; busy: boolean; drift: Pos };

let cfg = { timeScale: 20, courierKmh: 25, atPlaceKm: 0.15 };
const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

async function call(path: string, token?: string, body?: any) {
  const r = await fetch(API + path, { method: body ? 'POST' : 'GET',
    headers: { ...(body ? { 'content-type': 'application/json' } : {}), ...(token ? { authorization: `Bearer ${token}` } : {}) },
    body: body ? JSON.stringify(body) : undefined });
  const data: any = await r.json().catch(() => ({}));
  if (!r.ok) throw Object.assign(new Error(data.error || r.statusText), { status: r.status });
  return data;
}

function km(a: Pos, b: Pos) {
  const R = 6371, rad = Math.PI / 180, dLat = (b.lat - a.lat) * rad, dLon = (b.lon - a.lon) * rad;
  const h = Math.sin(dLat / 2) ** 2 + Math.cos(a.lat * rad) * Math.cos(b.lat * rad) * Math.sin(dLon / 2) ** 2;
  return 2 * R * Math.asin(Math.sqrt(h));
}
/** One tick of movement toward the target, at the scaled courier speed. */
function moveToward(p: Pos, t: Pos): Pos {
  const step = cfg.courierKmh * cfg.timeScale / 3600 * TICK_MS / 1000;
  const d = km(p, t);
  if (d <= step) return { ...t };
  const f = step / d;
  return { lat: p.lat + (t.lat - p.lat) * f, lon: p.lon + (t.lon - p.lon) * f };
}
// Couriers start spread over central Bangkok and drift slowly while waiting.
const randomStart = (): Pos => ({ lat: 13.725 + Math.random() * 0.07, lon: 100.505 + Math.random() * 0.08 });
const randomDrift = (): Pos => ({ lat: (Math.random() - 0.5) * 0.0006, lon: (Math.random() - 0.5) * 0.0006 });

async function tick(c: Courier, wantOnline: boolean) {
  if (!c.token) c.token = (await call('/auth/login', undefined, { email: `courier${c.n}@example.com`, password: 'courier123' })).token;
  const cur = await call('/courier/current', c.token);
  if (cur.position && !c.online && !cur.online) c.pos = cur.position;
  c.online = cur.online; c.busy = !!cur.order;

  if (!cur.order) {
    if (wantOnline && !cur.online) { await call('/courier/online', c.token, c.pos); c.lastPing = Date.now(); return; }
    if (!wantOnline && cur.online) { await call('/courier/offline', c.token, {}); return; }
    if (cur.online && Date.now() - c.lastPing >= IDLE_PING_MS) {
      c.pos = { lat: c.pos.lat + c.drift.lat, lon: c.pos.lon + c.drift.lon };
      if (Math.random() < 0.1) c.drift = randomDrift();
      await call('/courier/location', c.token, c.pos); c.lastPing = Date.now();
    }
    return;
  }

  const o = cur.order;
  const target: Pos = o.status === 'COURIER_ASSIGNED' ? { lat: o.restLat, lon: o.restLon } : { lat: o.dropLat, lon: o.dropLon };
  c.pos = moveToward(c.pos, target);
  await call('/courier/location', c.token, c.pos); c.lastPing = Date.now();
  if (km(c.pos, target) > cfg.atPlaceKm) return;
  try {
    if (o.status === 'COURIER_ASSIGNED' && o.readyAt) await call(`/courier/orders/${o.id}/pickup`, c.token, {});
    else if (o.status === 'PICKED_UP') await call(`/courier/orders/${o.id}/deliver`, c.token, {});
  } catch (e: any) { if (e.status !== 409) throw e; }   // food not ready yet: wait at the restaurant
}

async function main() {
  for (;;) {
    try { cfg = await call('/config'); break; } catch { console.log('waiting for the API...'); await sleep(3000); }
  }
  const fleet: Courier[] = Array.from({ length: COUNT }, (_, i) => ({ n: i + 1, pos: randomStart(), online: false, lastPing: 0, busy: false, drift: randomDrift() }));
  console.log(`courier-sim: ${COUNT} couriers, x${cfg.timeScale} time, ${cfg.courierKmh} km/h`);
  let last = '';
  for (;;) {
    const t0 = Date.now();
    let online = 20;
    try { online = (await call('/sim/settings')).onlineCouriers; } catch { /* API restarting */ }
    await Promise.all(fleet.map(c => tick(c, c.n <= online).catch(e => {
      if (e.status === 401) c.token = undefined;
      else if (!String(e.message).includes('fetch failed')) console.error(`courier${c.n}: ${e.message}`);
    })));
    const status = `${fleet.filter(c => c.online).length} online, ${fleet.filter(c => c.busy).length} busy`;
    if (status !== last) { console.log(status); last = status; }
    await sleep(Math.max(0, TICK_MS - (Date.now() - t0)));
  }
}
main();
