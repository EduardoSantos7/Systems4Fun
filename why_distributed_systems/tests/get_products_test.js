import http from 'k6/http';
import { check, sleep } from 'k6';

export let options = {
    vus: 100000, // number of virtual users
    duration: '60s', // duration of the test
};

export default function () {
    let res = http.get('http://host.docker.internal:80/products');
    check(res, {
        'is status 200': (r) => r.status === 200,
        'response time is less than 200ms': (r) => r.timings.duration < 200,
    });
    sleep(0.5);
}

// k6 run --out influxdb=http://admin:admin123@influxdb:8086/k6db tests/get_products_test.js