// Why: To detect memory leaks or performance degradations over a longer period, ensuring your distributed system can handle continuous load without crashing.
// What it tests: System stability over time. A distributed system might show resilience if one node restarts or if it rebalances traffic.
// You can watch memory usage, CPU usage, etc. in your monitoring to catch slow drifts or leaks.

import http from 'k6/http';
import { check, sleep } from 'k6';

export let options = {
  vus: 10,
  duration: '30m', // run for 30 minutes (or more)
};

export default function () {
  let res = http.get('http://localhost:80/healthcheck');
  check(res, {
    'status is 200': (r) => r.status === 200,
  });
  sleep(1);
}
