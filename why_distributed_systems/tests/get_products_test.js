import http from 'k6/http';
import { check, sleep } from 'k6';

export let options = {
    vus: 10000, // number of virtual users
    duration: '30s', // duration of the test
};

export default function () {
    let res = http.get('http://localhost:80/products');
    check(res, {
        'is status 200': (r) => r.status === 200,
        'response time is less than 200ms': (r) => r.timings.duration < 200,
    });
    sleep(0.5);
}

// k6 run --out influxdb=http://admin:admin123@influxdb:8086/k6db tests/get_products_test.js