// Courier state in Redis and the atomic dispatch claim.
//
//   couriers:pos      GEO set  last known position of every courier that has been online
//   couriers:free     GEO set  online couriers without an order: the only set dispatch searches
//   couriers:online   SET      who is online
//   courier:order:ID  STRING   the order a courier is carrying
//   order:trail:ID    LIST     where the courier has been while carrying that order, "lon,lat" per ping (for the map only)
//   order:plan:ID     STRING   the roads it will take to the restaurant and to the customer (JSON; remade if lost)
//
// The invariant is "in couriers:free  <=>  online and no courier:order key". Every write that touches more than one
// of these keys is a Lua script, so it runs as one step and nothing can run in between. The race this prevents: a
// position update reads "free", dispatch claims the courier, then the update writes the courier back into the free set,
// and the courier gets a second order.
import { redis } from './ctx';
import * as roads from './roads';

const POS = 'couriers:pos', FREE = 'couriers:free', ONLINE = 'couriers:online';
const orderKey = (courierId: string) => `courier:order:${courierId}`;

// Nearest free courier inside the radius: take it out of the free set and record the order, all in one step.
// Two orders racing for the same courier: the second script runs after the first and no longer finds it.
const CLAIM = `
local hits = redis.call('GEOSEARCH', KEYS[1], 'FROMLONLAT', ARGV[1], ARGV[2], 'BYRADIUS', ARGV[3], 'km', 'ASC', 'COUNT', 1, 'WITHDIST')
if #hits == 0 then return false end
local id = hits[1][1]
redis.call('ZREM', KEYS[1], id)
redis.call('SET', ARGV[5] .. id, ARGV[4])
return {id, hits[1][2]}`;

// Position ping. The courier goes (back) into the free set only if it is online and carries nothing.
// A courier carrying an order also extends that order's trail: the corners it turned since the last ping (ARGV[5..],
// so the trail follows the streets), then where it is now; repeats are skipped while it waits at the restaurant.
const PING = `
redis.call('GEOADD', KEYS[1], ARGV[1], ARGV[2], ARGV[3])
local order = redis.call('GET', KEYS[4])
if order then
  local trail = ARGV[4] .. order
  local last = redis.call('LINDEX', trail, -1)
  for i = 5, #ARGV + 1 do
    local pt = (i <= #ARGV) and ARGV[i] or (ARGV[1] .. ',' .. ARGV[2])
    if pt ~= last then redis.call('RPUSH', trail, pt); last = pt end
  end
  redis.call('LTRIM', trail, -2000, -1)
  redis.call('EXPIRE', trail, 86400)
end
if redis.call('SISMEMBER', KEYS[3], ARGV[3]) == 1 and not order then
  redis.call('GEOADD', KEYS[2], ARGV[1], ARGV[2], ARGV[3])
  return 1
end
return 0`;

// Order finished or rolled back: drop the order key (only if it still points at this order) and, if the courier is
// online, put it back into the free set at its last position.
const RELEASE = `
if ARGV[2] ~= '' and redis.call('GET', KEYS[4]) ~= ARGV[2] then return 0 end
redis.call('DEL', KEYS[4])
if redis.call('SISMEMBER', KEYS[3], ARGV[1]) == 1 then
  local p = redis.call('GEOPOS', KEYS[1], ARGV[1])[1]
  if p then redis.call('GEOADD', KEYS[2], p[1], p[2], ARGV[1]) end
end
return 1`;

const OFFLINE = `
redis.call('SREM', KEYS[2], ARGV[1])
redis.call('ZREM', KEYS[1], ARGV[1])
return 1`;

// Rolling average of real prep times per restaurant (exponentially weighted, alpha = 0.2), in lab minutes.
const EWMA = `
local v = tonumber(redis.call('GET', KEYS[1]) or ARGV[2])
local n = 0.2 * tonumber(ARGV[1]) + 0.8 * v
redis.call('SET', KEYS[1], tostring(n))
return tostring(n)`;

redis.defineCommand('fdClaim', { numberOfKeys: 1, lua: CLAIM });
redis.defineCommand('fdPing', { numberOfKeys: 4, lua: PING });
redis.defineCommand('fdRelease', { numberOfKeys: 4, lua: RELEASE });
redis.defineCommand('fdOffline', { numberOfKeys: 2, lua: OFFLINE });
redis.defineCommand('fdEwma', { numberOfKeys: 1, lua: EWMA });
const r = redis as any;

export async function claimNearest(lat: number, lon: number, radiusKm: number, orderId: string) {
  const res = await r.fdClaim(FREE, lon, lat, radiusKm, orderId, 'courier:order:');
  return res ? { courierId: res[0] as string, distanceKm: Number(res[1]) } : null;
}
/** via: corners passed since the last ping, [lat, lon], oldest first. */
export async function ping(courierId: string, lat: number, lon: number, via: roads.LatLon[] = []) {
  return r.fdPing(POS, FREE, ONLINE, orderKey(courierId), String(lon), String(lat), courierId, TRAIL, ...via.map(([la, lo]) => `${lo},${la}`));
}
const TRAIL = 'order:trail:';
/** Where the courier has been while carrying the order, oldest first. Kept for a day after delivery. */
export async function trail(orderId: string): Promise<[number, number][]> {
  return (await redis.lrange(TRAIL + orderId, 0, -1)).map(p => { const [lon, lat] = p.split(',').map(Number); return [lat, lon]; });
}
/** Starts the trail where the courier was when it got the order. */
export async function startTrail(orderId: string, courierId: string) {
  const p = await position(courierId);
  if (p) await redis.rpush(TRAIL + orderId, `${p.lon},${p.lat}`);
}
const PLAN = 'order:plan:';
export type Plan = { toRestaurant: roads.LatLon[]; toCustomer: roads.LatLon[] };
type PlanFor = { id: string; status: string; courierId: string; restLat: number; restLon: number; dropLat: number; dropLon: number };
/** The roads the courier takes for this order. Made when the courier is assigned, from where it was; if Redis lost it,
 *  made again from where it is now (before pick-up: to the restaurant, then to the customer; after: to the customer). */
export async function plan(o: PlanFor): Promise<Plan | null> {
  if (!['COURIER_ASSIGNED', 'PICKED_UP'].includes(o.status)) return null;
  const saved = await redis.get(PLAN + o.id);
  if (saved) return JSON.parse(saved);
  const p = await position(o.courierId);
  if (!p) return null;
  const made: Plan = o.status === 'COURIER_ASSIGNED'
    ? { toRestaurant: roads.route(p.lat, p.lon, o.restLat, o.restLon), toCustomer: roads.route(o.restLat, o.restLon, o.dropLat, o.dropLon) }
    : { toRestaurant: [], toCustomer: roads.route(p.lat, p.lon, o.dropLat, o.dropLon) };
  await redis.set(PLAN + o.id, JSON.stringify(made), 'EX', 86400);
  return made;
}
export async function goOnline(courierId: string, lat: number, lon: number) {
  await redis.sadd(ONLINE, courierId);
  return ping(courierId, lat, lon);
}
export const goOffline = (courierId: string) => r.fdOffline(FREE, ONLINE, courierId);
/** orderId: release only if the courier still carries this order ('' = whatever it carries). */
export const release = (courierId: string, orderId = '') => r.fdRelease(POS, FREE, ONLINE, orderKey(courierId), courierId, orderId);

export async function position(courierId: string) {
  const p = (await redis.geopos(POS, courierId))[0];
  return p ? { lat: Number(p[1]), lon: Number(p[0]) } : null;
}
export async function courierState(courierId: string) {
  const [online, order, free] = await Promise.all([redis.sismember(ONLINE, courierId), redis.get(orderKey(courierId)), redis.zscore(FREE, courierId)]);
  return { online: online === 1, orderId: order, inFreeSet: free !== null };
}

/** Every courier Redis knows about, for the live map and the consistency check. */
export async function allCouriers() {
  const ids = await redis.zrange(POS, 0, -1);
  if (!ids.length) return [];
  const [positions, online, free, orders] = await Promise.all([
    redis.geopos(POS, ...ids), redis.smembers(ONLINE), redis.zrange(FREE, 0, -1), redis.mget(...ids.map(orderKey))]);
  const on = new Set(online), fr = new Set(free);
  return ids.map((id, i) => ({ id, lat: Number(positions[i]?.[1]), lon: Number(positions[i]?.[0]),
    online: on.has(id), free: fr.has(id), orderId: orders[i] }));
}

export async function prepAverage(restaurantId: string, fallbackMin: number) {
  const v = await redis.get(`prep:ewma:${restaurantId}`);
  return v ? Number(v) : fallbackMin;
}
export const recordPrep = async (restaurantId: string, minutes: number, fallbackMin: number) =>
  Number(await r.fdEwma(`prep:ewma:${restaurantId}`, minutes, fallbackMin));

/** Makes Redis agree with PostgreSQL: busy couriers carry exactly their active order, nobody busy is in the free set. */
export async function reconcile(active: Map<string, string>) {
  let fixed = 0;
  for (const c of await allCouriers()) {
    const want = active.get(c.id) || null;
    if (want && (c.orderId !== want || c.free)) { await redis.set(orderKey(c.id), want); await redis.zrem(FREE, c.id); fixed++; }
    else if (!want && c.orderId) { await release(c.id); fixed++; }
    else if (!want && c.online && !c.free) { await release(c.id); fixed++; }
    else if (!c.online && c.free) { await redis.zrem(FREE, c.id); fixed++; }
  }
  for (const [courierId, orderId] of active) if ((await redis.get(orderKey(courierId))) !== orderId) { await redis.set(orderKey(courierId), orderId); fixed++; }
  return fixed;
}

export async function resetCouriers() {
  const keys = await redis.keys('courier:order:*');
  if (keys.length) await redis.del(...keys);
  for (const id of await redis.smembers(ONLINE)) await release(id);
}
