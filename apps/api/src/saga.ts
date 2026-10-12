// The order saga. One orchestrator (this file) moves every order through its steps and runs the compensating step when
// a later step fails:
//
//   PLACED -> PAYMENT_AUTHORIZED -> RESTAURANT_ACCEPTED -> COURIER_ASSIGNED -> PICKED_UP -> DELIVERED
//      |              |                     |
//      |              |                     +-- no courier within DISPATCH_TIMEOUT_S -> CANCELLED_NO_COURIER (refund)
//      |              +-- restaurant rejects or does not answer                   -> REJECTED (void)
//      |              +-- customer cancels before the restaurant accepts          -> CANCELLED (void)
//      +-- card declined                                                          -> PAYMENT_FAILED
//
// Every step is a conditional UPDATE (WHERE status = expected), a saga-log row and an outbox event in one transaction,
// so a step happens exactly once even when two workers or a retried request try it at the same time.
import crypto from 'crypto';
import { PoolClient } from 'pg';
import { Kafka } from 'kafkajs';
import {
  pool, tx, addOutbox, sleep, haversineKm, kmPerSecond, HttpError, TIME_SCALE, RESTAURANT_TIMEOUT_S, DISPATCH_TIMEOUT_S,
  AUTO_ACCEPT_DELAY_MS, AT_PLACE_KM, ADDRESSES,
} from './ctx';
import * as couriers from './dispatch';
import * as roads from './roads';

export type Status = 'PLACED' | 'PAYMENT_AUTHORIZED' | 'RESTAURANT_ACCEPTED' | 'COURIER_ASSIGNED' | 'PICKED_UP' | 'DELIVERED'
  | 'PAYMENT_FAILED' | 'REJECTED' | 'CANCELLED' | 'CANCELLED_NO_COURIER';
export const TERMINAL: Status[] = ['DELIVERED', 'PAYMENT_FAILED', 'REJECTED', 'CANCELLED', 'CANCELLED_NO_COURIER'];

// Latencies recorded while a simulation runs (null = not recording).
export const probe: { claimMs: number[] | null; placeMs: number[] | null; authMs: number[] | null } = { claimMs: null, placeMs: null, authMs: null };

const PATCHABLE = ['courier_id', 'accepted_at', 'ready_at', 'assigned_at', 'picked_up_at', 'delivered_at', 'finished_at',
  'cancel_reason', 'eta_at', 'prep_due_at'] as const;
type Patch = Partial<Record<typeof PATCHABLE[number], any>>;

/** One saga step. Returns the updated order, or null if the order was no longer in one of the `from` states. */
async function step(c: PoolClient, orderId: string, from: Status[], to: Status, patch: Patch = {}, detail: any = {}) {
  if (TERMINAL.includes(to)) patch.finished_at ??= new Date();
  const cols = Object.keys(patch).filter(k => (PATCHABLE as readonly string[]).includes(k));
  const sets = cols.map((k, i) => `${k}=$${i + 4}`);
  const r = await c.query(`UPDATE orders SET status=$2, updated_at=now()${sets.length ? ', ' + sets.join(', ') : ''}
    WHERE id=$1 AND status = ANY($3) RETURNING *`, [orderId, to, from, ...cols.map(k => (patch as any)[k])]);
  const o = r.rows[0];
  if (!o) return null;
  await c.query(`INSERT INTO order_steps(order_id,status,detail) VALUES($1,$2,$3)`, [orderId, to, JSON.stringify(detail)]);
  await addOutbox(c, 'orders.events', orderId, { type: to, orderId, restaurantId: o.restaurant_id, customerId: o.customer_id,
    courierId: o.courier_id, at: new Date().toISOString(), ...detail });
  return o;
}

// ---- Fake payment gateway --------------------------------------------------------------------------------------------
// A table in the same database to keep the lab small, but used like a remote API: called outside the order
// transaction, with the order id as idempotency key, so a retry after a crash never charges twice.
const payment = {
  async authorize(orderId: string, amount: number, card: string) {
    const t0 = performance.now();
    await sleep(30 + Math.random() * 70);   // network + bank
    await pool.query(`INSERT INTO payments(order_id,amount,status) VALUES($1,$2,$3) ON CONFLICT (order_id) DO NOTHING`,
      [orderId, amount, card === 'decline' ? 'DECLINED' : 'AUTHORIZED']);
    probe.authMs?.push(performance.now() - t0);
    return (await pool.query(`SELECT status FROM payments WHERE order_id=$1`, [orderId])).rows[0].status as string;
  },
  async move(c: PoolClient, orderId: string, from: string, to: string) {
    const r = await c.query(`UPDATE payments SET status=$3, updated_at=now() WHERE order_id=$1 AND status=$2 RETURNING amount`, [orderId, from, to]);
    if (r.rows[0]) await addOutbox(c, 'payments.events', orderId, { type: to, orderId, amount: Number(r.rows[0].amount), at: new Date().toISOString() });
    return !!r.rows[0];
  },
};

// ---- ETA ---------------------------------------------------------------------------------------------------------------
// ETA = when the food can leave the restaurant (later of: food ready, courier there) + the ride to the customer.
// Rides are measured along the roads (roads.ts), not as the crow flies.
// Prep time is the restaurant's rolling average of real prep times; it learns as orders are marked ready.
export async function estimate(o: any, rest: any, now = Date.now()) {
  const speed = kmPerSecond();
  const ride = roads.roadKm(rest.lat, rest.lon, o.drop_lat, o.drop_lon) / speed;
  if (o.status === 'PICKED_UP' && o.courier_id) {
    const p = await couriers.position(o.courier_id);
    return new Date(now + (p ? roads.roadKm(p.lat, p.lon, o.drop_lat, o.drop_lon) / speed : ride) * 1000);
  }
  const prepS = (await couriers.prepAverage(rest.id, Number(rest.base_prep_min))) * 60 / TIME_SCALE;
  const since = o.accepted_at ? (now - ms(o.accepted_at)) / 1000 : -AUTO_ACCEPT_DELAY_MS / 1000;
  const prepLeft = o.ready_at ? 0 : Math.max(0, prepS - since);
  let toRestaurant = 2 / speed;   // typical first-mile ride before a courier is known
  if (o.courier_id) {
    const p = await couriers.position(o.courier_id);
    if (p) toRestaurant = roads.roadKm(p.lat, p.lon, rest.lat, rest.lon) / speed;
  }
  return new Date(now + (Math.max(prepLeft, toRestaurant) + ride) * 1000);
}

const ms = (d: any) => new Date(d).getTime();
const restaurantOf = async (c: PoolClient | typeof pool, id: string) => (await c.query(`SELECT * FROM restaurants WHERE id=$1`, [id])).rows[0];

// ---- Place order -------------------------------------------------------------------------------------------------------
export type PlaceInput = { restaurantId: string; items: { menuItemId: string; qty: number }[]; addressId?: string;
  dropLat?: number; dropLon?: number; address?: string; card?: string };

export async function placeOrder(customerId: string, key: string, input: PlaceInput, loadTest = false) {
  const t0 = performance.now();
  if (!key || key.length > 100) throw new HttpError(400, 'Idempotency-Key header required');
  if (!input?.restaurantId || !Array.isArray(input.items) || !input.items.length) throw new HttpError(400, 'restaurantId and items required');
  const requestHash = crypto.createHash('sha256').update(JSON.stringify([input.restaurantId, input.items, input.addressId, input.dropLat, input.dropLon, input.card || 'ok'])).digest('hex');

  const placed = await tx(async c => {
    const rest = await restaurantOf(c, input.restaurantId);
    if (!rest) throw new HttpError(404, 'Restaurant not found');
    const addr = input.addressId ? ADDRESSES.find(a => a.id === input.addressId) : null;
    const drop = addr || (Number.isFinite(input.dropLat) && Number.isFinite(input.dropLon)
      ? { lat: input.dropLat!, lon: input.dropLon!, label: input.address || 'Pinned location' } : null);
    if (!drop) throw new HttpError(400, 'addressId or dropLat/dropLon required');
    const km = haversineKm(rest.lat, rest.lon, drop.lat, drop.lon);
    if (km > 15) throw new HttpError(400, 'Too far: this restaurant delivers within 15 km');
    const menu = new Map((await c.query(`SELECT id,name,price FROM menu_items WHERE restaurant_id=$1`, [rest.id])).rows.map(m => [m.id, m]));
    let subtotal = 0;
    for (const it of input.items) {
      const m = menu.get(it.menuItemId);
      if (!m) throw new HttpError(400, 'Item is not on this menu');
      if (!(Number.isInteger(it.qty) && it.qty >= 1 && it.qty <= 20)) throw new HttpError(400, 'Quantity must be 1-20');
      subtotal += Number(m.price) * it.qty;
    }
    const fee = Math.round(15 + 7 * km);
    const quoted = await estimate({ status: 'PLACED', drop_lat: drop.lat, drop_lon: drop.lon }, rest);
    // The unique (customer_id, idempotency_key) index decides. A concurrent duplicate waits here for the first
    // transaction to commit, then gets no row back and returns the first order instead of creating a second one.
    const ins = await c.query(`INSERT INTO orders(customer_id,restaurant_id,status,idempotency_key,request_hash,address,drop_lat,drop_lon,
        subtotal,delivery_fee,total,card,eta_quoted_at,eta_at,is_load_test)
      VALUES($1,$2,'PLACED',$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$12,$13) ON CONFLICT (customer_id, idempotency_key) DO NOTHING RETURNING *`,
      [customerId, rest.id, key, requestHash, drop.label, drop.lat, drop.lon, subtotal, fee, subtotal + fee, input.card === 'decline' ? 'decline' : 'ok', quoted, loadTest]);
    if (!ins.rows[0]) return null;
    const o = ins.rows[0];
    const pairs = input.items.map(it => [it.menuItemId, it.qty] as const);
    for (const [mid, qty] of pairs) {
      const m = menu.get(mid)!;
      await c.query(`INSERT INTO order_items(order_id,menu_item_id,name,unit_price,qty) VALUES($1,$2,$3,$4,$5)
        ON CONFLICT (order_id,menu_item_id) DO UPDATE SET qty = order_items.qty + EXCLUDED.qty`, [o.id, mid, m.name, m.price, qty]);
    }
    await c.query(`INSERT INTO order_steps(order_id,status,detail) VALUES($1,'PLACED',$2)`, [o.id, JSON.stringify({ total: Number(o.total), etaQuoted: quoted })]);
    await addOutbox(c, 'orders.events', o.id, { type: 'PLACED', orderId: o.id, restaurantId: rest.id, customerId, total: Number(o.total), at: new Date().toISOString() });
    return o;
  });

  if (!placed) {
    const existing = (await pool.query(`SELECT id,status,request_hash FROM orders WHERE customer_id=$1 AND idempotency_key=$2`, [customerId, key])).rows[0];
    if (existing.request_hash !== requestHash) throw new HttpError(422, 'This Idempotency-Key was already used for a different order');
    return { orderId: existing.id, status: existing.status, replayed: true };
  }
  const status = await authorizeAndAdvance(placed);
  probe.placeMs?.push(performance.now() - t0);
  return { orderId: placed.id, status, replayed: false, total: Number(placed.total), etaQuotedAt: placed.eta_quoted_at };
}

/** Saga step 1: hold the money. Also used by the recovery loop for orders a crash left in PLACED. */
async function authorizeAndAdvance(o: any): Promise<Status> {
  const result = await payment.authorize(o.id, Number(o.total), o.card);
  return tx(async c => {
    if (result === 'AUTHORIZED') {
      await addOutbox(c, 'payments.events', o.id, { type: 'AUTHORIZED', orderId: o.id, amount: Number(o.total), at: new Date().toISOString() });
      const u = await step(c, o.id, ['PLACED'], 'PAYMENT_AUTHORIZED', {}, { amount: Number(o.total) });
      return (u?.status || o.status) as Status;
    }
    const u = await step(c, o.id, ['PLACED'], 'PAYMENT_FAILED', { cancel_reason: 'Card declined' }, { reason: 'Card declined' });
    return (u?.status || o.status) as Status;
  });
}

// ---- Restaurant ---------------------------------------------------------------------------------------------------------
export async function accept(orderId: string, by: 'restaurant' | 'auto') {
  return tx(async c => {
    const o = (await c.query(`SELECT o.*, r.base_prep_min, r.auto_accept FROM orders o JOIN restaurants r ON r.id=o.restaurant_id WHERE o.id=$1 FOR UPDATE OF o`, [orderId])).rows[0];
    if (!o) throw new HttpError(404, 'Order not found');
    // Auto kitchens finish after their usual prep time, give or take 30%. People press "Food ready" themselves.
    const prepS = Number(o.base_prep_min) * (0.7 + Math.random() * 0.6) * 60 / TIME_SCALE;
    const now = new Date();
    const u = await step(c, orderId, ['PAYMENT_AUTHORIZED'], 'RESTAURANT_ACCEPTED',
      { accepted_at: now, prep_due_at: o.auto_accept ? new Date(now.getTime() + prepS * 1000) : null }, { by });
    if (!u) throw new HttpError(409, `Order is ${o.status}, it cannot be accepted`);
    await payment.move(c, orderId, 'AUTHORIZED', 'CAPTURED');   // the money is taken once the kitchen commits
    await c.query(`UPDATE orders SET eta_at=$2 WHERE id=$1`, [orderId, await estimate(u, await restaurantOf(c, u.restaurant_id))]);
    return u;
  });
}

export async function reject(orderId: string, reason: string) {
  return tx(async c => {
    const u = await step(c, orderId, ['PAYMENT_AUTHORIZED'], 'REJECTED', { cancel_reason: reason }, { reason, compensation: 'payment voided' });
    if (!u) throw new HttpError(409, 'Only an order waiting for the restaurant can be rejected');
    await payment.move(c, orderId, 'AUTHORIZED', 'VOIDED');
    return u;
  });
}

export async function markReady(orderId: string, by: 'restaurant' | 'auto') {
  const u = await tx(async c => {
    const r = await c.query(`UPDATE orders SET ready_at=now(), updated_at=now() WHERE id=$1 AND ready_at IS NULL
      AND status IN ('RESTAURANT_ACCEPTED','COURIER_ASSIGNED') RETURNING *`, [orderId]);
    const o = r.rows[0];
    if (!o) return null;
    await c.query(`INSERT INTO order_steps(order_id,status,detail) VALUES($1,'FOOD_READY',$2)`, [orderId, JSON.stringify({ by })]);
    await addOutbox(c, 'orders.events', orderId, { type: 'FOOD_READY', orderId, restaurantId: o.restaurant_id, at: new Date().toISOString() });
    return o;
  });
  if (!u) throw new HttpError(409, 'Food can be marked ready only once, after the order is accepted');
  const rest = await restaurantOf(pool, u.restaurant_id);
  const minutes = (ms(u.ready_at) - ms(u.accepted_at)) / 1000 * TIME_SCALE / 60;
  await couriers.recordPrep(rest.id, minutes, Number(rest.base_prep_min));
  return u;
}

// ---- Customer -----------------------------------------------------------------------------------------------------------
export async function cancelByCustomer(orderId: string, customerId: string) {
  return tx(async c => {
    const u = await step(c, orderId, ['PAYMENT_AUTHORIZED'], 'CANCELLED', { cancel_reason: 'Cancelled by customer' }, { reason: 'Cancelled by customer', compensation: 'payment voided' });
    if (!u || u.customer_id !== customerId) throw new HttpError(409, 'An order can be cancelled only before the restaurant accepts it');
    await payment.move(c, orderId, 'AUTHORIZED', 'VOIDED');
    return u;
  });
}

// ---- Dispatch -----------------------------------------------------------------------------------------------------------
// The search widens while the order waits: 3 km, then 6 km after 10 s, then 12 km after 20 s.
export async function dispatch(o: any, rest: any) {
  const waited = (Date.now() - ms(o.accepted_at)) / 1000;
  const radius = waited < 10 ? 3 : waited < 20 ? 6 : 12;
  const t0 = performance.now();
  const claim = await couriers.claimNearest(rest.lat, rest.lon, radius, o.id);
  probe.claimMs?.push(performance.now() - t0);
  if (!claim) return false;
  try {
    const u = await tx(async c => {
      const u = await step(c, o.id, ['RESTAURANT_ACCEPTED'], 'COURIER_ASSIGNED', { courier_id: claim.courierId, assigned_at: new Date() },
        { distanceKm: Math.round(claim.distanceKm * 100) / 100, radiusKm: radius });
      if (u) await c.query(`UPDATE orders SET eta_at=$2 WHERE id=$1`, [o.id, await estimate(u, rest)]);
      return u;
    });
    if (!u) { await couriers.release(claim.courierId, o.id); return false; }   // order moved on (cancelled) meanwhile
    await couriers.startTrail(o.id, claim.courierId);
    await couriers.plan({ id: o.id, status: 'COURIER_ASSIGNED', courierId: claim.courierId, restLat: rest.lat, restLon: rest.lon, dropLat: o.drop_lat, dropLon: o.drop_lon });
    return true;
  } catch (e: any) {
    // 23505 = one_active_order_per_courier said no. Redis was wrong; PostgreSQL wins and the claim is undone.
    await couriers.release(claim.courierId, o.id);
    if (e.code === '23505') return false;
    throw e;
  }
}

async function cancelNoCourier(orderId: string) {
  await tx(async c => {
    const u = await step(c, orderId, ['RESTAURANT_ACCEPTED'], 'CANCELLED_NO_COURIER',
      { cancel_reason: `No courier found within ${DISPATCH_TIMEOUT_S}s` }, { compensation: 'payment refunded' });
    if (u) await payment.move(c, orderId, 'CAPTURED', 'REFUNDED');
  });
}

// ---- Courier ------------------------------------------------------------------------------------------------------------
async function courierOrder(orderId: string, courierId: string) {
  const o = (await pool.query(`SELECT o.*, r.lat AS r_lat, r.lon AS r_lon FROM orders o JOIN restaurants r ON r.id=o.restaurant_id WHERE o.id=$1`, [orderId])).rows[0];
  if (!o || o.courier_id !== courierId) throw new HttpError(404, 'This is not your order');
  return o;
}

export async function pickUp(orderId: string, courierId: string) {
  const o = await courierOrder(orderId, courierId);
  if (!o.ready_at) throw new HttpError(409, 'The food is not ready yet');
  const p = await couriers.position(courierId);
  if (!p || haversineKm(p.lat, p.lon, o.r_lat, o.r_lon) > AT_PLACE_KM) throw new HttpError(409, 'You are not at the restaurant');
  return tx(async c => {
    const u = await step(c, orderId, ['COURIER_ASSIGNED'], 'PICKED_UP', { picked_up_at: new Date() });
    if (!u) throw new HttpError(409, `Order is ${o.status}`);
    await c.query(`UPDATE orders SET eta_at=$2 WHERE id=$1`, [orderId, await estimate(u, await restaurantOf(c, u.restaurant_id))]);
    return u;
  });
}

export async function deliver(orderId: string, courierId: string) {
  const o = await courierOrder(orderId, courierId);
  const p = await couriers.position(courierId);
  if (!p || haversineKm(p.lat, p.lon, o.drop_lat, o.drop_lon) > AT_PLACE_KM) throw new HttpError(409, 'You are not at the customer');
  const u = await tx(async c => {
    const u = await step(c, orderId, ['PICKED_UP'], 'DELIVERED', { delivered_at: new Date() },
      { lateBySeconds: Math.round((Date.now() - ms(o.eta_quoted_at)) / 1000) });
    if (!u) throw new HttpError(409, `Order is ${o.status}`);
    return u;
  });
  await couriers.release(courierId, orderId);   // after commit: free again only once PostgreSQL agrees
  return u;
}

// ---- Loops -------------------------------------------------------------------------------------------------------------
type Logger = { error: (...a: any[]) => void; info: (...a: any[]) => void };

/** Finds orders whose next step is due and runs it. Safe to run in several API replicas: every step is conditional. */
async function sagaTick() {
  const due = (await pool.query(`
    SELECT o.*, r.auto_accept FROM orders o JOIN restaurants r ON r.id=o.restaurant_id
    WHERE (o.status='PLACED' AND o.created_at < now() - interval '5 seconds')
       OR (o.status='PAYMENT_AUTHORIZED' AND (r.auto_accept AND o.updated_at < now() - make_interval(secs => $1)
                                              OR o.updated_at < now() - make_interval(secs => $2)))
       OR (o.status='RESTAURANT_ACCEPTED')
       OR (o.status IN ('RESTAURANT_ACCEPTED','COURIER_ASSIGNED') AND o.ready_at IS NULL AND o.prep_due_at <= now())
    ORDER BY o.created_at LIMIT 500`, [AUTO_ACCEPT_DELAY_MS / 1000, RESTAURANT_TIMEOUT_S])).rows;
  const rests = new Map((await pool.query(`SELECT * FROM restaurants`)).rows.map(r => [r.id, r]));
  for (const o of due) {
    try {
      if (o.status === 'PLACED') await authorizeAndAdvance(o);   // crash between insert and payment: retry, idempotent
      else if (o.status === 'PAYMENT_AUTHORIZED') {
        const late = Date.now() - ms(o.updated_at) >= RESTAURANT_TIMEOUT_S * 1000;
        if (late && !o.auto_accept) await reject(o.id, `Restaurant did not answer within ${RESTAURANT_TIMEOUT_S}s`);
        else await accept(o.id, 'auto');
      } else {
        if (!o.ready_at && o.prep_due_at && ms(o.prep_due_at) <= Date.now()) await markReady(o.id, 'auto').catch(() => {});
        if (o.status === 'RESTAURANT_ACCEPTED' && !o.courier_id) {
          const ok = await dispatch(o, rests.get(o.restaurant_id));
          if (!ok && Date.now() - ms(o.accepted_at) > DISPATCH_TIMEOUT_S * 1000) await cancelNoCourier(o.id);
        }
      }
    } catch (e: any) { if (!(e instanceof HttpError)) throw e; }   // another worker got there first
  }
}

export async function sagaLoop(log: Logger) {
  while (true) {
    try { await sagaTick(); } catch (e) { log.error(e, 'saga tick failed'); }
    await sleep(500);
  }
}

// Outbox publisher. Kafka being down never stops orders: events wait in outbox_events and go out once the producer
// (re)connects. One poll sends one batch (the flight lab sent one message per event, which was its bottleneck).
const kafka = new Kafka({ clientId: 'food-delivery-lab-api', brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(',') });
const producer = kafka.producer({ idempotent: true, maxInFlightRequests: 1 });

export async function outboxLoop(log: Logger) {
  let connected = false;
  while (true) {
    let sent = 0;
    try {
      if (!connected) { await producer.connect(); connected = true; log.info('Kafka producer connected'); }
      const rows = (await pool.query(`SELECT * FROM outbox_events WHERE published_at IS NULL ORDER BY id LIMIT 500`)).rows;
      if (rows.length) {
        const byTopic = new Map<string, any[]>();
        for (const e of rows) {
          if (!byTopic.has(e.topic)) byTopic.set(e.topic, []);
          byTopic.get(e.topic)!.push({ key: e.event_key, value: JSON.stringify(e.payload), headers: { outboxId: String(e.id) } });
        }
        await producer.sendBatch({ topicMessages: [...byTopic].map(([topic, messages]) => ({ topic, messages })) });
        await pool.query(`UPDATE outbox_events SET published_at=now() WHERE id = ANY($1)`, [rows.map(r => r.id)]);
        sent = rows.length;
      }
    } catch (err) { connected = false; log.error(err, 'outbox publish failed'); await producer.disconnect().catch(() => {}); await sleep(2000); }
    if (sent < 500) await sleep(500);
  }
}

/** On start: Redis must agree with PostgreSQL about who carries what (Redis may have been flushed or restarted). */
export async function reconcileCouriers() {
  const rows = (await pool.query(`SELECT courier_id, id FROM orders WHERE status IN ('COURIER_ASSIGNED','PICKED_UP')`)).rows;
  return couriers.reconcile(new Map(rows.map(r => [r.courier_id, r.id])));
}
