import Fastify from 'fastify';
import cors from '@fastify/cors';
import jwt from '@fastify/jwt';
import { pool, redis, HttpError, ADDRESSES, TIME_SCALE, COURIER_KMH, DISPATCH_TIMEOUT_S, RESTAURANT_TIMEOUT_S, AT_PLACE_KM } from './ctx';
import * as saga from './saga';
import * as couriers from './dispatch';
import { startSimulation, consistency, simRunning } from './sim';

const app = Fastify({ logger: { level: process.env.LOG_LEVEL || 'info' } });
// No per-request log lines: 40 simulated couriers ping every second.
app.addHook('onRoute', route => { if (!route.logLevel) route.logLevel = 'warn'; });
app.register(cors, { origin: process.env.CORS_ORIGIN || true });
app.register(jwt, { secret: process.env.JWT_SECRET || 'dev-secret' });

type Role = 'CUSTOMER' | 'RESTAURANT' | 'COURIER' | 'ADMIN';
type User = { id: string; role: Role; impersonatedBy?: string };

async function auth(req: any, roles?: Role[]): Promise<User> {
  let p: any;
  try { p = await req.jwtVerify(); } catch { throw new HttpError(401, 'Unauthorized'); }
  if (roles && !roles.includes(p.role)) throw new HttpError(403, 'Forbidden for this role');
  return { id: p.id, role: p.role, impersonatedBy: p.impersonatedBy };
}

app.setErrorHandler((err: any, _req, reply) => {
  const code = err.statusCode && err.statusCode < 500 ? err.statusCode : 500;
  if (code === 500) app.log.error(err);
  reply.code(code).send({ error: code === 500 ? 'Internal error' : err.message });
});

const ORDER_COLS = `o.id, o.status, o.address, o.drop_lat AS "dropLat", o.drop_lon AS "dropLon", o.subtotal::float, o.delivery_fee::float AS "deliveryFee",
  o.total::float, o.cancel_reason AS "cancelReason", o.eta_quoted_at AS "etaQuotedAt", o.eta_at AS "etaAt", o.created_at AS "createdAt",
  o.accepted_at AS "acceptedAt", o.ready_at AS "readyAt", o.assigned_at AS "assignedAt", o.picked_up_at AS "pickedUpAt", o.delivered_at AS "deliveredAt",
  o.courier_id AS "courierId", o.customer_id AS "customerId", o.restaurant_id AS "restaurantId", r.name AS "restaurant",
  cu.name AS "customer", co.name AS "courier",
  (SELECT json_agg(json_build_object('name', i.name, 'qty', i.qty, 'unitPrice', i.unit_price::float)) FROM order_items i WHERE i.order_id=o.id) AS items`;
const ORDER_FROM = `FROM orders o JOIN restaurants r ON r.id=o.restaurant_id JOIN users cu ON cu.id=o.customer_id LEFT JOIN users co ON co.id=o.courier_id`;

// ---- Public -----------------------------------------------------------------------------------------------------------
app.get('/api/health', async () => {
  await pool.query('SELECT 1'); await redis.ping();
  return { ok: true };
});

app.get('/api/config', async () => ({ timeScale: TIME_SCALE, courierKmh: COURIER_KMH, dispatchTimeoutS: DISPATCH_TIMEOUT_S,
  restaurantTimeoutS: RESTAURANT_TIMEOUT_S, atPlaceKm: AT_PLACE_KM, addresses: ADDRESSES }));

app.post('/api/auth/login', async (req: any) => {
  const { email, password } = req.body || {};
  const u = (await pool.query(`SELECT id,email,name,role,password_hash FROM users WHERE email=$1`, [String(email || '').toLowerCase().trim()])).rows[0];
  if (!u || u.password_hash !== password) throw new HttpError(401, 'Wrong email or password');
  return { token: app.jwt.sign({ id: u.id, role: u.role }), user: { id: u.id, email: u.email, name: u.name, role: u.role } };
});

app.get('/api/restaurants', async () => {
  const rows = (await pool.query(`SELECT id,name,cuisine,area,lat,lon,base_prep_min::float AS "basePrepMin",auto_accept AS "autoAccept" FROM restaurants ORDER BY name`)).rows;
  for (const r of rows) r.prepAvgMin = Math.round(await couriers.prepAverage(r.id, r.basePrepMin) * 10) / 10;
  return rows;
});

app.get('/api/restaurants/:id', async (req: any) => {
  const r = (await pool.query(`SELECT id,name,cuisine,area,lat,lon,base_prep_min::float AS "basePrepMin",auto_accept AS "autoAccept" FROM restaurants WHERE id=$1`, [req.params.id])).rows[0];
  if (!r) throw new HttpError(404, 'Restaurant not found');
  r.prepAvgMin = Math.round(await couriers.prepAverage(r.id, r.basePrepMin) * 10) / 10;
  r.menu = (await pool.query(`SELECT id,name,description,price::float FROM menu_items WHERE restaurant_id=$1 ORDER BY price DESC`, [r.id])).rows;
  return r;
});

/** The courier-sim worker reads how many simulated couriers should be online. */
app.get('/api/sim/settings', async () => ({ onlineCouriers: Number(await redis.get('sim:online') ?? process.env.SIM_ONLINE_DEFAULT ?? 20) }));

// ---- Orders (customer, plus admin read) --------------------------------------------------------------------------------
app.post('/api/orders', async (req: any) => {
  const u = await auth(req, ['CUSTOMER']);
  return saga.placeOrder(u.id, String(req.headers['idempotency-key'] || ''), req.body);
});

app.get('/api/orders', async (req: any) => {
  const u = await auth(req, ['CUSTOMER', 'ADMIN']);
  // Admin sees every order (load-test orders only with ?all=1); a customer sees their own.
  const all = req.query.all === '1';
  const r = u.role === 'ADMIN'
    ? await pool.query(`SELECT ${ORDER_COLS} ${ORDER_FROM} WHERE $1 OR NOT o.is_load_test ORDER BY o.created_at DESC LIMIT 100`, [all])
    : await pool.query(`SELECT ${ORDER_COLS} ${ORDER_FROM} WHERE o.customer_id=$1 ORDER BY o.created_at DESC LIMIT 100`, [u.id]);
  return r.rows;
});

async function orderDetail(id: string) {
  const o = (await pool.query(`SELECT ${ORDER_COLS}, r.lat AS "restLat", r.lon AS "restLon" ${ORDER_FROM} WHERE o.id=$1`, [id])).rows[0];
  if (!o) throw new HttpError(404, 'Order not found');
  o.steps = (await pool.query(`SELECT status, detail, at FROM order_steps WHERE order_id=$1 ORDER BY id`, [id])).rows;
  o.payment = (await pool.query(`SELECT status, amount::float FROM payments WHERE order_id=$1`, [id])).rows[0] || null;
  o.courierPos = o.courierId && ['COURIER_ASSIGNED', 'PICKED_UP'].includes(o.status) ? await couriers.position(o.courierId) : null;
  o.trail = o.courierId ? await couriers.trail(o.id) : [];
  o.plan = o.courierId ? await couriers.plan(o) : null;
  if (!saga.TERMINAL.includes(o.status)) {
    const raw = (await pool.query(`SELECT * FROM orders WHERE id=$1`, [id])).rows[0];
    const rest = (await pool.query(`SELECT * FROM restaurants WHERE id=$1`, [raw.restaurant_id])).rows[0];
    o.etaLive = await saga.estimate(raw, rest);
  }
  o.serverTime = new Date();   // the tracker counts down on this clock, not the browser's
  return o;
}

app.get('/api/orders/:id', async (req: any) => {
  const u = await auth(req);
  const o = await orderDetail(req.params.id);
  const owner = (await pool.query(`SELECT owner_user_id FROM restaurants WHERE id=$1`, [o.restaurantId])).rows[0]?.owner_user_id;
  if (u.role !== 'ADMIN' && o.customerId !== u.id && o.courierId !== u.id && owner !== u.id) throw new HttpError(404, 'Order not found');
  return o;
});

app.post('/api/orders/:id/cancel', async (req: any) => {
  const u = await auth(req, ['CUSTOMER']);
  await saga.cancelByCustomer(req.params.id, u.id);
  return { ok: true };
});

// ---- Restaurant ------------------------------------------------------------------------------------------------------
async function myRestaurant(userId: string) {
  const r = (await pool.query(`SELECT * FROM restaurants WHERE owner_user_id=$1`, [userId])).rows[0];
  if (!r) throw new HttpError(404, 'You do not own a restaurant');
  return r;
}
async function ownOrder(userId: string, orderId: string) {
  const r = await myRestaurant(userId);
  const ok = (await pool.query(`SELECT 1 FROM orders WHERE id=$1 AND restaurant_id=$2`, [orderId, r.id])).rows[0];
  if (!ok) throw new HttpError(404, 'Order not found');
}

app.get('/api/restaurant/orders', async (req: any) => {
  const u = await auth(req, ['RESTAURANT']);
  const r = await myRestaurant(u.id);
  const orders = (await pool.query(`SELECT ${ORDER_COLS}, o.updated_at AS "updatedAt" ${ORDER_FROM} WHERE o.restaurant_id=$1 ORDER BY o.created_at DESC LIMIT 50`, [r.id])).rows;
  return { restaurant: { id: r.id, name: r.name, autoAccept: r.auto_accept, basePrepMin: Number(r.base_prep_min),
    prepAvgMin: Math.round(await couriers.prepAverage(r.id, Number(r.base_prep_min)) * 10) / 10 }, orders, restaurantTimeoutS: RESTAURANT_TIMEOUT_S };
});

app.post('/api/restaurant/orders/:id/:action', async (req: any) => {
  const u = await auth(req, ['RESTAURANT']);
  await ownOrder(u.id, req.params.id);
  const { id, action } = req.params;
  if (action === 'accept') await saga.accept(id, 'restaurant');
  else if (action === 'reject') await saga.reject(id, String(req.body?.reason || 'Rejected by the restaurant'));
  else if (action === 'ready') await saga.markReady(id, 'restaurant');
  else throw new HttpError(404, 'Unknown action');
  return { ok: true };
});

// ---- Courier ---------------------------------------------------------------------------------------------------------
function latLon(body: any) {
  const lat = Number(body?.lat), lon = Number(body?.lon);
  if (!(lat > 13.4 && lat < 14.1 && lon > 100.2 && lon < 100.9)) throw new HttpError(400, 'Position must be in Bangkok');
  return { lat, lon };
}

app.post('/api/courier/online', async (req: any) => {
  const u = await auth(req, ['COURIER']);
  const p = latLon(req.body);
  await couriers.goOnline(u.id, p.lat, p.lon);
  await pool.query(`INSERT INTO outbox_events(topic,event_key,payload) VALUES('couriers.events',$1,$2)`, [u.id, JSON.stringify({ type: 'ONLINE', courierId: u.id, ...p })]);
  return couriers.courierState(u.id);
});
app.post('/api/courier/offline', async (req: any) => {
  const u = await auth(req, ['COURIER']);
  await couriers.goOffline(u.id);
  await pool.query(`INSERT INTO outbox_events(topic,event_key,payload) VALUES('couriers.events',$1,$2)`, [u.id, JSON.stringify({ type: 'OFFLINE', courierId: u.id })]);
  return couriers.courierState(u.id);
});
// High-frequency position pings go to Redis only (not PostgreSQL, not Kafka).
// via: up to 300 corners, [lat, lon], passed since the last ping, so the order's trail follows the streets.
app.post('/api/courier/location', async (req: any) => {
  const u = await auth(req, ['COURIER']);
  const p = latLon(req.body);
  const via = (Array.isArray(req.body.via) ? req.body.via.slice(0, 300) : []).map((v: any) => {
    const q = latLon({ lat: v?.[0], lon: v?.[1] }); return [q.lat, q.lon] as [number, number];
  });
  await couriers.ping(u.id, p.lat, p.lon, via);
  return { ok: true };
});

app.get('/api/courier/current', async (req: any) => {
  const u = await auth(req, ['COURIER']);
  const state = await couriers.courierState(u.id);
  const o = (await pool.query(`SELECT ${ORDER_COLS}, r.lat AS "restLat", r.lon AS "restLon", r.area AS "restArea" ${ORDER_FROM}
    WHERE o.courier_id=$1 AND o.status IN ('COURIER_ASSIGNED','PICKED_UP')`, [u.id])).rows[0] || null;
  const position = await couriers.position(u.id);
  if (o) { o.trail = await couriers.trail(o.id); o.plan = await couriers.plan(o); }
  const delivered = (await pool.query(`SELECT count(*)::int AS n FROM orders WHERE courier_id=$1 AND status='DELIVERED'`, [u.id])).rows[0].n;
  return { ...state, position, order: o, delivered };
});

app.post('/api/courier/orders/:id/:action', async (req: any) => {
  const u = await auth(req, ['COURIER']);
  if (req.params.action === 'pickup') await saga.pickUp(req.params.id, u.id);
  else if (req.params.action === 'deliver') await saga.deliver(req.params.id, u.id);
  else throw new HttpError(404, 'Unknown action');
  return { ok: true };
});

// ---- Admin -----------------------------------------------------------------------------------------------------------
app.get('/api/admin/users', async (req: any) => {
  await auth(req, ['ADMIN']);
  return (await pool.query(`SELECT u.id,u.email,u.name,u.role,u.is_sim AS "isSim",u.password_hash AS password, r.name AS restaurant
    FROM users u LEFT JOIN restaurants r ON r.owner_user_id=u.id WHERE NOT u.is_load_test ORDER BY u.role, u.is_sim, u.email`)).rows;
});

app.post('/api/admin/impersonate/:userId', async (req: any) => {
  const me = await auth(req, ['ADMIN']);
  const t = (await pool.query(`SELECT id,email,name,role FROM users WHERE id=$1`, [req.params.userId])).rows[0];
  if (!t) throw new HttpError(404, 'User not found');
  await pool.query(`INSERT INTO audit_logs(actor_user_id,action,target_user_id) VALUES($1,'IMPERSONATION_STARTED',$2)`, [me.id, t.id]);
  return { token: app.jwt.sign({ id: t.id, role: t.role, impersonatedBy: me.id }), user: t };
});

app.get('/api/admin/map', async (req: any) => {
  await auth(req, ['ADMIN']);
  const [rests, orders, list] = await Promise.all([
    pool.query(`SELECT id,name,lat,lon,auto_accept AS "autoAccept" FROM restaurants`),
    pool.query(`SELECT o.id,o.status,o.courier_id AS "courierId",o.drop_lat AS "dropLat",o.drop_lon AS "dropLon",r.lat AS "restLat",r.lon AS "restLon",
        o.ready_at IS NOT NULL AS ready
      FROM orders o JOIN restaurants r ON r.id=o.restaurant_id WHERE o.status IN ('PLACED','PAYMENT_AUTHORIZED','RESTAURANT_ACCEPTED','COURIER_ASSIGNED','PICKED_UP')`),
    couriers.allCouriers()]);
  // Orders with a courier carry their planned road from the restaurant to the customer (the others: none yet).
  await Promise.all(orders.rows.map(async o => { if (o.courierId) o.road = (await couriers.plan(o))?.toCustomer; }));
  return { restaurants: rests.rows, orders: orders.rows, couriers: list.filter(c => c.online || c.orderId) };
});

app.get('/api/admin/stats', async (req: any) => {
  await auth(req, ['ADMIN']);
  const [byStatus, outbox, pays, list] = await Promise.all([
    pool.query(`SELECT status, count(*)::int AS n FROM orders GROUP BY status`),
    pool.query(`SELECT count(*) FILTER (WHERE published_at IS NULL)::int AS pending, count(*) FILTER (WHERE published_at IS NOT NULL)::int AS published,
        (SELECT json_object_agg(topic, n) FROM (SELECT topic, count(*)::int AS n FROM outbox_events GROUP BY topic) t) AS "byTopic" FROM outbox_events`),
    pool.query(`SELECT status, count(*)::int AS n, sum(amount)::float AS amount FROM payments GROUP BY status`),
    couriers.allCouriers()]);
  return { orders: Object.fromEntries(byStatus.rows.map(r => [r.status, r.n])), outbox: outbox.rows[0], payments: pays.rows,
    couriers: { online: list.filter(c => c.online).length, free: list.filter(c => c.free).length, busy: list.filter(c => c.orderId).length },
    onlineTarget: Number(await redis.get('sim:online') ?? process.env.SIM_ONLINE_DEFAULT ?? 20), simulationRunning: simRunning() };
});

app.get('/api/admin/consistency', async (req: any) => { await auth(req, ['ADMIN']); return consistency(); });
app.post('/api/admin/reconcile', async (req: any) => { await auth(req, ['ADMIN']); return { fixed: await saga.reconcileCouriers() }; });

app.put('/api/admin/sim-settings', async (req: any) => {
  const me = await auth(req, ['ADMIN']);
  const n = Math.floor(Number(req.body?.onlineCouriers));
  if (!(n >= 0 && n <= 40)) throw new HttpError(400, 'onlineCouriers must be 0-40');
  await redis.set('sim:online', n);
  await pool.query(`INSERT INTO audit_logs(actor_user_id,action,metadata) VALUES($1,'SIM_COURIERS_SET',$2)`, [me.id, JSON.stringify({ onlineCouriers: n })]);
  return { onlineCouriers: n };
});

app.post('/api/admin/simulate', async (req: any) => {
  const me = await auth(req, ['ADMIN']);
  return startSimulation(me.id, req.body || {});
});
app.get('/api/admin/simulations', async (req: any) => {
  await auth(req, ['ADMIN']);
  return (await pool.query(`SELECT id,status,params,report,created_at AS "createdAt",finished_at AS "finishedAt" FROM simulation_runs ORDER BY created_at DESC LIMIT 10`)).rows;
});

/** Deletes every order (and its events) so the lab starts clean. Couriers keep their positions. */
app.post('/api/admin/reset', async (req: any) => {
  const me = await auth(req, ['ADMIN']);
  if (simRunning()) throw new HttpError(409, 'A simulation is running');
  const n = (await pool.query(`WITH d AS (DELETE FROM orders RETURNING 1) SELECT count(*)::int AS n FROM d`)).rows[0].n;
  await pool.query(`DELETE FROM outbox_events WHERE published_at IS NOT NULL`);
  await pool.query(`DELETE FROM simulation_runs`);
  await pool.query(`DELETE FROM users WHERE is_load_test`);
  await couriers.resetCouriers();
  const keys = await redis.keys('prep:ewma:*');
  if (keys.length) await redis.del(...keys);
  await pool.query(`INSERT INTO audit_logs(actor_user_id,action,metadata) VALUES($1,'RESET',$2)`, [me.id, JSON.stringify({ orders: n })]);
  return { deletedOrders: n };
});

async function start() {
  const fixed = await saga.reconcileCouriers().catch(e => { app.log.error(e, 'reconcile failed'); return 0; });
  if (fixed) app.log.info({ fixed }, 'Redis courier state rebuilt from PostgreSQL');
  await pool.query(`UPDATE simulation_runs SET status='FAILED', report = report || '{"error":"API restarted while running"}' WHERE status='RUNNING'`);
  await app.listen({ port: Number(process.env.PORT || 3030), host: '0.0.0.0' });
  saga.sagaLoop(app.log);
  saga.outboxLoop(app.log);
}
start();
