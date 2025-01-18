// Why: A sudden surge of traffic can reveal bottlenecks or how well the system handles ephemeral bursts.
// A truly fault-tolerant distributed system can handle or gracefully degrade under spikes.
// What it tests: How your system copes when traffic suddenly spikes to a high level. In a distributed system,
// this might trigger auto-scaling events or overflow logic. If the system is fault tolerant, it should degrade gracefully (maybe queue requests)
// rather than completely failing.

import http from 'k6/http';
import { check, sleep } from 'k6';

export let options = {
  scenarios: {
    spike_test: {
      executor: 'shared-iterations',
      vus: 1000,
      iterations: 10000, // total requests
      maxDuration: '1m', // or time limit
    },
  },
};

export default function () {
  let res = http.get('http://localhost:80/login'); // or some "expensive" endpoint
  check(res, {
    'status is 200': (r) => r.status === 200,
  });
  sleep(0.1);
}
