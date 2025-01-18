// Why: To see how your system handles increasing traffic over time, and if it can scale horizontally (e.g., adding more servers) without failing or degrading too much.
// What it tests: Your service’s ability to handle a gradually increasing load (scalability). In a distributed system, you might observe more instances spinning up or auto-scaling happening seamlessly.

import http from 'k6/http';
import { check, sleep } from 'k6';

export let options = {
  scenarios: {
    ramping_load: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '1s', target: 50 },   // ramp up to 50 VUs in 1 min
        { duration: '10s', target: 500 },  // then to 500 VUs in next 2 min
        { duration: '30s', target: 1500 },
        { duration: '1m', target: 2500 },
        { duration: '2m', target: 5000 },
        { duration: '4m', target: 10000 },
        { duration: '1m', target: 0 },    // ramp down to 0
      ],
      gracefulRampDown: '30s',
    },
  },
};

export default function () {
  const res = http.get('http://localhost:80/products');
  check(res, {
    'status is 200': (r) => r.status === 200,
  });
  sleep(0.5);
}
