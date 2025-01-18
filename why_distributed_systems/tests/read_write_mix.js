// Why: Many distributed systems separate read vs. write paths (e.g., read replicas, sharding, etc.). This test sends both POST (writes) and GET (reads) traffic.
// What it tests: Your DB’s ability to handle mixed read/write workloads. In a distributed environment, you might see how well replication or partitioning (shards) handle concurrency,
// ensuring writes propagate and reads stay fast.

import http from 'k6/http';
import { check, sleep } from 'k6';

export let options = {
  vus: 50,
  duration: '1m',
};

function getRandomProductId() {
  return Math.floor(Math.random() * 1000) + 1; // 1 to 1000
}

export default function () {
  // 1. Simulate a read
  let productId = getRandomProductId();
  let resGet = http.get(`http://localhost:80/products/${productId}`);
  check(resGet, {
    'GET: status is 200': (r) => r.status === 200,
  });

  // 2. Simulate a write (e.g., update or create)
  let payload = JSON.stringify({ name: `NewName${productId}`, price: 19.99 });
  let params = { headers: { 'Content-Type': 'application/json' } };
  let resPost = http.post(`http://localhost:80/products`, payload, params);
  check(resPost, {
    'POST: status is 201 or 200': (r) => r.status === 201 || r.status === 200,
  });

  sleep(1);
}
