// Shared clients, settings and small helpers.
import { Pool, PoolClient } from 'pg';
import Redis from 'ioredis';

export const pool = new Pool({ connectionString: process.env.DATABASE_URL, max: Number(process.env.PG_POOL || 20) });
export const redis = new Redis(process.env.REDIS_URL || 'redis://localhost:6379');

// The lab runs faster than real life: with TIME_SCALE=20 a 15-minute prep takes 45 seconds and a courier at
// 25 km/h covers 3 km in about 22 seconds. Timeouts below are in real seconds.
export const TIME_SCALE = Number(process.env.TIME_SCALE || 20);
export const COURIER_KMH = Number(process.env.COURIER_KMH || 25);
export const RESTAURANT_TIMEOUT_S = Number(process.env.RESTAURANT_TIMEOUT_S || 60);
export const DISPATCH_TIMEOUT_S = Number(process.env.DISPATCH_TIMEOUT_S || 90);
export const AUTO_ACCEPT_DELAY_MS = 1500;
export const AT_PLACE_KM = 0.15;      // a courier this close to the restaurant / customer may pick up / deliver

/** Courier speed in km per real second. */
export const kmPerSecond = () => COURIER_KMH * TIME_SCALE / 3600;

export const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

export { haversineKm } from './geo';

export async function tx<T>(fn: (c: PoolClient) => Promise<T>): Promise<T> {
  const c = await pool.connect();
  try {
    await c.query('BEGIN');
    const out = await fn(c);
    await c.query('COMMIT');
    return out;
  } catch (e) {
    await c.query('ROLLBACK').catch(() => {});
    throw e;
  } finally { c.release(); }
}

export async function addOutbox(c: PoolClient, topic: string, key: string, payload: any) {
  await c.query(`INSERT INTO outbox_events(topic,event_key,payload) VALUES($1,$2,$3)`, [topic, key, JSON.stringify(payload)]);
}

export function percentile(values: number[], p: number) {
  if (!values.length) return null;
  const s = [...values].sort((a, b) => a - b);
  return Math.round(s[Math.min(s.length - 1, Math.ceil(p / 100 * s.length) - 1)] * 10) / 10;
}
export const stats = (v: number[]) => ({ n: v.length, p50: percentile(v, 50), p95: percentile(v, 95), max: v.length ? Math.round(Math.max(...v) * 10) / 10 : null });

/** Thrown from routes; the error handler turns it into { error } with this status. */
export class HttpError extends Error { constructor(public statusCode: number, message: string) { super(message); } }

// Drop-off addresses customers pick from (no geocoding API needed).
export const ADDRESSES = [
  { id: 'lumpini', label: 'Lumpini Park gate, Rama IV', lat: 13.7314, lon: 100.5414 },
  { id: 'victory', label: 'Victory Monument', lat: 13.7649, lon: 100.5383 },
  { id: 'chatuchak', label: 'Chatuchak Market, gate 1', lat: 13.7998, lon: 100.5502 },
  { id: 'asok', label: 'Terminal 21, Asok', lat: 13.7370, lon: 100.5603 },
  { id: 'rama9', label: 'Central Rama 9', lat: 13.7570, lon: 100.5650 },
  { id: 'khaosan', label: 'Khao San Road', lat: 13.7589, lon: 100.4977 },
  { id: 'onnut', label: 'On Nut BTS', lat: 13.7056, lon: 100.6013 },
  { id: 'taksin', label: 'Saphan Taksin pier', lat: 13.7187, lon: 100.5142 },
];
