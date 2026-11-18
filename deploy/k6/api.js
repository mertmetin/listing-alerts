// k6 load test of the saved-search API: sites creating, listing, updating
// and deleting their users' searches. Run with `make k6` (scale stack up).
//
//   k6 run -e BASE=http://localhost:18081 -e KEY=<site key> deploy/k6/api.js
import http from "k6/http";
import { check } from "k6";

const BASE = __ENV.BASE || "http://localhost:18081";
const KEY = __ENV.KEY;
const RATE = parseInt(__ENV.RATE || "300", 10); // creates per second

export const options = {
  scenarios: {
    create: { executor: "constant-arrival-rate", rate: RATE, timeUnit: "1s", duration: "60s", preAllocatedVUs: 50, maxVUs: 200, exec: "create" },
    list: { executor: "constant-arrival-rate", rate: RATE * 2, timeUnit: "1s", duration: "60s", preAllocatedVUs: 50, maxVUs: 200, exec: "list" },
  },
  thresholds: {
    "http_req_failed": ["rate<0.01"],
    "http_req_duration{scenario:create}": ["p(99)<100"],
    "http_req_duration{scenario:list}": ["p(99)<50"],
  },
};

const headers = { Authorization: `Bearer ${KEY}`, "Content-Type": "application/json" };

function circle(lat, lng, km) {
  const pts = [];
  for (let i = 0; i < 16; i++) {
    const a = (2 * Math.PI * i) / 16;
    pts.push({ lat: lat + (km / 111.32) * Math.sin(a), lng: lng + (km / (111.32 * Math.cos((lat * Math.PI) / 180))) * Math.cos(a) });
  }
  return pts;
}

const cities = [[43.6532, -79.3832], [45.4215, -75.6972], [43.589, -79.6441], [43.2557, -79.8711], [42.9849, -81.2453]];

export function create() {
  const user = `k6-${__VU}-${Math.floor(__ITER / 40)}`; // up to 40 searches per user (limit 50)
  const [lat, lng] = cities[__ITER % cities.length];
  const body = JSON.stringify({
    user_id: user, email: `${user}@example.com`, audience: __ITER % 4 ? "public" : "vow", name: "k6",
    transaction: __ITER % 3 ? "sale" : "lease", max_price: 900000, min_beds: 2,
    areas: [circle(lat + (Math.random() - 0.5) * 0.2, lng + (Math.random() - 0.5) * 0.2, 1 + Math.random() * 3)],
  });
  const r = http.post(`${BASE}/v1/searches`, body, { headers });
  check(r, { "created": (res) => res.status === 201 });
  if (r.status === 201 && __ITER % 5 === 0) { // some users change their mind
    const id = r.json("id");
    const del = http.del(`${BASE}/v1/searches/${id}`, null, { headers });
    check(del, { "deleted": (res) => res.status === 204 });
  }
}

export function list() {
  const r = http.get(`${BASE}/v1/searches?user_id=k6-${1 + (__ITER % 50)}-0`, { headers });
  check(r, { "listed": (res) => res.status === 200 });
}
