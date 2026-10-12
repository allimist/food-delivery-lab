// Load simulator: many customers order at the same instant, the courier-sim worker delivers, and the report shows
// where the time goes and checks the invariants (no double orders, no courier carrying two orders, Redis = PostgreSQL).
import crypto from 'crypto';
import { pool, redis, stats, sleep, TIME_SCALE, DISPATCH_TIMEOUT_S } from './ctx';
import { placeOrder, probe, TERMINAL, reconcileCouriers } from './saga';
import * as couriers from './dispatch';
import { onRoad } from './roads';

const MAX_WAIT_S = 300;
let running: string | null = null;

/** Random point 1-4 km from a restaurant, moved onto the nearest street (the customer's door). */
function dropNear(lat: number, lon: number) {
  const km = 1 + Math.random() * 3, angle = Math.random() * 2 * Math.PI;
  return onRoad(lat + km * Math.cos(angle) / 111, lon + km * Math.sin(angle) / (111 * Math.cos(lat * Math.PI / 180)));
}

async function loadCustomers(n: number) {
  await pool.query(`INSERT INTO users(email,password_hash,name,role,is_load_test)
    SELECT 'load'||i||'@example.com','load123','Load customer '||i,'CUSTOMER',TRUE FROM generate_series(1,$1) i
    ON CONFLICT (email) DO NOTHING`, [n]);
  return (await pool.query(`SELECT id FROM users WHERE is_load_test ORDER BY (regexp_replace(email,'\\D','','g'))::int LIMIT $1`, [n])).rows.map(r => r.id);
}

export async function consistency() {
  const active = new Map((await pool.query(`SELECT courier_id, id FROM orders WHERE status IN ('COURIER_ASSIGNED','PICKED_UP')`)).rows.map(r => [r.courier_id, r.id]));
  const problems: string[] = [];
  const list = await couriers.allCouriers();
  for (const c of list) {
    const want = active.get(c.id) || null;
    if (c.free && want) problems.push(`courier ${c.id.slice(0, 8)} is in the free set but carries order ${want.slice(0, 8)}`);
    if ((c.orderId || null) !== want) problems.push(`courier ${c.id.slice(0, 8)}: Redis says ${c.orderId?.slice(0, 8) || 'no order'}, PostgreSQL says ${want?.slice(0, 8) || 'no order'}`);
    if (c.free && !c.online) problems.push(`courier ${c.id.slice(0, 8)} is free but offline`);
  }
  for (const [cid, oid] of active) if (!list.find(c => c.id === cid)) problems.push(`courier ${cid.slice(0, 8)} carries ${oid.slice(0, 8)} but Redis has no position`);
  return { ok: problems.length === 0, checkedCouriers: list.length, activeOrders: active.size, problems };
}

export function simRunning() { return running; }

export async function startSimulation(adminId: string, params: { orders?: number; duplicatePct?: number }) {
  if (running) return { id: running, alreadyRunning: true };
  const orders = Math.max(1, Math.min(500, Math.floor(Number(params.orders) || 50)));
  const duplicatePct = Math.max(0, Math.min(100, Number(params.duplicatePct ?? 20)));
  const onlineCouriers = Number(await redis.get('sim:online') || 0);
  const id = (await pool.query(`INSERT INTO simulation_runs(status,params,created_by) VALUES('RUNNING',$1,$2) RETURNING id`,
    [JSON.stringify({ orders, duplicatePct, onlineCouriers, timeScale: TIME_SCALE, dispatchTimeoutS: DISPATCH_TIMEOUT_S }), adminId])).rows[0].id;
  running = id;
  run(id, orders, duplicatePct).catch(async e => {
    await pool.query(`UPDATE simulation_runs SET status='FAILED', report=$2, finished_at=now() WHERE id=$1`, [id, JSON.stringify({ error: String(e?.message || e) })]);
  }).finally(() => { running = null; probe.claimMs = probe.placeMs = probe.authMs = null; });
  return { id };
}

async function run(id: string, n: number, duplicatePct: number) {
  const log: string[] = [];
  const note = async (msg: string, extra: any = {}) => {
    log.push(`${new Date().toISOString().slice(11, 19)} ${msg}`);
    await pool.query(`UPDATE simulation_runs SET report = report || $2 WHERE id=$1`, [id, JSON.stringify({ log, ...extra })]);
  };
  probe.claimMs = []; probe.placeMs = []; probe.authMs = [];
  const customers = await loadCustomers(n);
  const rests = (await pool.query(`SELECT r.*, array_agg(m.id) AS items FROM restaurants r JOIN menu_items m ON m.restaurant_id=r.id
    WHERE r.auto_accept GROUP BY r.id`)).rows;
  await note(`${n} customers order at once from ${rests.length} auto-accept restaurants; ${duplicatePct}% send the same request twice`);

  // Fire every order at the same instant. Duplicates reuse the key, like a double-clicked "Place order".
  const started = Date.now();
  const requests = customers.map(cid => {
    const r = rests[Math.floor(Math.random() * rests.length)];
    const d = dropNear(r.lat, r.lon);
    const body = { restaurantId: r.id, items: [{ menuItemId: r.items[Math.floor(Math.random() * r.items.length)], qty: 1 + Math.floor(Math.random() * 2) }],
      dropLat: d.lat, dropLon: d.lon, address: 'Simulated drop-off' };
    const key = crypto.randomUUID();
    const twice = Math.random() * 100 < duplicatePct;
    return [placeOrder(cid, key, body, true), ...(twice ? [placeOrder(cid, key, body, true)] : [])];
  });
  const results = await Promise.allSettled(requests.flat());
  const sent = results.length, failed = results.filter(r => r.status === 'rejected');
  const ok = results.filter(r => r.status === 'fulfilled').map(r => (r as PromiseFulfilledResult<any>).value);
  const replayed = ok.filter(r => r.replayed).length;
  const orderIds = [...new Set(ok.map(r => r.orderId))];
  await note(`${sent} requests sent: ${orderIds.length} orders created, ${replayed} answered as replays, ${failed.length} errors`, { progressPct: 10 });

  // Wait for the saga to finish every order.
  let open = orderIds.length;
  while (open > 0 && Date.now() - started < MAX_WAIT_S * 1000) {
    await sleep(1000);
    const r = (await pool.query(`SELECT status, count(*)::int AS n FROM orders WHERE id = ANY($1) GROUP BY status`, [orderIds])).rows;
    open = r.filter(x => !TERMINAL.includes(x.status)).reduce((s, x) => s + x.n, 0);
    const byStatus = Object.fromEntries(r.map(x => [x.status, x.n]));
    await pool.query(`UPDATE simulation_runs SET report = report || $2 WHERE id=$1`, [id, JSON.stringify({ byStatus,
      progressPct: Math.round(10 + 85 * (1 - open / orderIds.length)) })]);
  }
  if (open) await note(`${open} orders still open after ${MAX_WAIT_S}s`);
  await note(`all orders finished in ${((Date.now() - started) / 1000).toFixed(1)}s, waiting for the outbox to drain`);
  for (let i = 0; i < 30; i++) {
    const p = (await pool.query(`SELECT count(*)::int AS n FROM outbox_events WHERE published_at IS NULL`)).rows[0].n;
    if (!p) break;
    await sleep(500);
  }

  const rows = (await pool.query(`SELECT * FROM orders WHERE id = ANY($1)`, [orderIds])).rows;
  const t = (a: any, b: any) => (new Date(b).getTime() - new Date(a).getTime()) / 1000;
  const delivered = rows.filter(o => o.status === 'DELIVERED');
  const accepted = rows.filter(o => o.accepted_at);
  const waits = accepted.filter(o => o.assigned_at).map(o => t(o.accepted_at, o.assigned_at));
  const outbox = (await pool.query(`SELECT extract(epoch from published_at - created_at)*1000 AS lag FROM outbox_events
    WHERE event_key = ANY($1) AND published_at IS NOT NULL`, [orderIds])).rows.map(r => Number(r.lag));

  // A courier carried two orders at once if two of its orders overlap between assignment and finish.
  const byCourier = new Map<string, any[]>();
  for (const o of rows.filter(o => o.courier_id)) { if (!byCourier.has(o.courier_id)) byCourier.set(o.courier_id, []); byCourier.get(o.courier_id)!.push(o); }
  let overlaps = 0;
  for (const list of byCourier.values()) {
    list.sort((a, b) => new Date(a.assigned_at).getTime() - new Date(b.assigned_at).getTime());
    for (let i = 1; i < list.length; i++) if (new Date(list[i].assigned_at) < new Date(list[i - 1].finished_at || Date.now())) overlaps++;
  }
  const perCourier = [...byCourier.values()].map(l => l.filter(o => o.status === 'DELIVERED').length);
  const byStatus: Record<string, number> = {};
  for (const o of rows) byStatus[o.status] = (byStatus[o.status] || 0) + 1;
  await reconcileCouriers().catch(() => 0);
  const check = await consistency();

  const timings = {
    'Place order (PG insert + payment auth + PG step)': stats(probe.placeMs!),
    'Payment authorize (fake gateway, 30-100 ms on purpose)': stats(probe.authMs!),
    'Redis dispatch claim (GEOSEARCH + claim, one Lua script)': stats(probe.claimMs!),
  };
  const lifecycle = {
    'Placed -> accepted (s)': stats(accepted.map(o => t(o.created_at, o.accepted_at))),
    'Accepted -> courier assigned (s)': stats(waits),
    'Assigned -> picked up (s)': stats(delivered.map(o => t(o.assigned_at, o.picked_up_at))),
    'Placed -> delivered (s)': stats(delivered.map(o => t(o.created_at, o.delivered_at))),
    'ETA error, |delivered - quoted| (s)': stats(delivered.map(o => Math.abs(t(o.eta_quoted_at, o.delivered_at)))),
    'Outbox -> Kafka lag (ms)': stats(outbox),
  };
  const bottlenecks: string[] = [];
  const noCourier = byStatus.CANCELLED_NO_COURIER || 0;
  if (noCourier) bottlenecks.push(`${noCourier} orders found no courier within ${DISPATCH_TIMEOUT_S}s: too few couriers online for the burst. Raise "couriers online" or the dispatch timeout.`);
  const waitP95 = lifecycle['Accepted -> courier assigned (s)'].p95;
  if (waitP95 && waitP95 > 5) bottlenecks.push(`Orders waited up to ${waitP95}s (p95) for a courier: dispatch is courier-bound, not CPU-bound (the claim itself takes ${timings['Redis dispatch claim (GEOSEARCH + claim, one Lua script)'].p95} ms).`);
  const placeP95 = timings['Place order (PG insert + payment auth + PG step)'].p95;
  if (placeP95 && placeP95 > 500) bottlenecks.push(`Placing an order took ${placeP95} ms at p95: ${n} transactions share a PostgreSQL pool of ${process.env.PG_POOL || 20} connections.`);
  if (!bottlenecks.length) bottlenecks.push('No bottleneck at this load: try more orders or fewer couriers.');

  const report = {
    summary: { requests: sent, ordersCreated: orderIds.length, duplicateRequests: sent - customers.length, replayed,
      duplicateOrders: rows.length - new Set(rows.map(o => o.customer_id)).size, errors: failed.length,
      couriersUsed: byCourier.size, deliveriesPerCourier: stats(perCourier), courierOverlaps: overlaps, consistent: check.ok },
    byStatus, timings, lifecycle, consistency: check, bottlenecks, progressPct: 100,
    errors: failed.slice(0, 5).map(f => String((f as PromiseRejectedResult).reason?.message || (f as PromiseRejectedResult).reason)),
  };
  await note(`done: ${byStatus.DELIVERED || 0} delivered, ${overlaps} courier overlaps, Redis ${check.ok ? 'matches' : 'does NOT match'} PostgreSQL`, report);
  await pool.query(`UPDATE simulation_runs SET status='DONE', finished_at=now() WHERE id=$1`, [id]);
}
