// Distance on the Earth's surface. Kept apart from ctx.ts so roads.ts (and its tests) load without database clients.
export function haversineKm(lat1: number, lon1: number, lat2: number, lon2: number) {
  const R = 6371, rad = Math.PI / 180;
  const dLat = (lat2 - lat1) * rad, dLon = (lon2 - lon1) * rad;
  const a = Math.sin(dLat / 2) ** 2 + Math.cos(lat1 * rad) * Math.cos(lat2 * rad) * Math.sin(dLon / 2) ** 2;
  return 2 * R * Math.asin(Math.sqrt(a));
}
