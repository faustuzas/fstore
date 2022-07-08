import http from 'k6/http';
import { check, sleep } from 'k6';
import { randomString } from 'https://jslib.k6.io/k6-utils/1.1.0/index.js';

// export const options = {
//     vus: 1,
//     duration: '1s',
// };

// export const options = {
//     vus: 10,
//     duration: '5m',
// };

export const options = {
    vus: 100,
    duration: '30s',
    summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(95)', 'p(99)', 'p(99.99)', 'count'],
};

const baseUrl = 'http://localhost:8001/v1/'

export default function () {
    const key = randomString(12)
    const value = randomString(255)
    const url = baseUrl + key

    const writeRes = http.post(url, value)
    check(writeRes, { 'status should be 200 on write': (r) => r.status === 200 });

    // sleep(1)
}

// SCENARIO #1: Running in memory single-node store
// running (0m30.0s), 000/100 VUs, 313264 complete and 0 interrupted iterations
// checks.........................: 100.00% ✓ 313264       ✗ 0
// data_received..................: 37 MB   1.2 MB/s
// data_sent......................: 117 MB  3.9 MB/s
// http_req_blocked...............: avg=130.66µs min=1µs      med=2µs    max=436.14ms p(95)=4µs     p(99)=9µs     p(99.99)=433.98ms count=313264
// http_req_connecting............: avg=115.24µs min=0s       med=0s     max=420.33ms p(95)=0s      p(99)=0s      p(99.99)=396.99ms count=313264
// http_req_duration..............: avg=7.44ms   min=77µs     med=4.25ms max=380ms    p(95)=24.84ms p(99)=40.75ms p(99.99)=180.13ms count=313264
// { expected_response:true }...: avg=7.44ms   min=77µs     med=4.25ms max=380ms    p(95)=24.84ms p(99)=40.75ms p(99.99)=180.13ms count=313264
// http_req_failed................: 0.00%   ✓ 0            ✗ 313264
// http_req_receiving.............: avg=174.12µs min=9µs      med=21µs   max=195.81ms p(95)=162µs   p(99)=2.58ms  p(99.99)=88.02ms  count=313264
// http_req_sending...............: avg=46.13µs  min=5µs      med=11µs   max=196ms    p(95)=28µs    p(99)=245µs   p(99.99)=28.37ms  count=313264
// http_req_tls_handshaking.......: avg=0s       min=0s       med=0s     max=0s       p(95)=0s      p(99)=0s      p(99.99)=0s       count=313264
// http_req_waiting...............: avg=7.22ms   min=55µs     med=4.14ms max=379.83ms p(95)=24.31ms p(99)=38.43ms p(99.99)=148.04ms count=313264
// http_reqs......................: 313264  10440.955246/s
// iteration_duration.............: avg=9.53ms   min=373.83µs med=5.58ms max=471.98ms p(95)=29.39ms p(99)=51.37ms p(99.99)=448.24ms count=313264
// iterations.....................: 313264  10440.955246/s
// vus............................: 100     min=100        max=100
// vus_max........................: 100     min=100        max=100

// SCENARIO #2: Running in memory 3-nodes cluster using JSON for internal communication
// running (0m30.3s), 000/100 VUs, 4494 complete and 0 interrupted iterations
// checks.........................: 100.00% ✓ 4494       ✗ 0
// data_received..................: 530 kB  18 kB/s
// data_sent......................: 1.7 MB  55 kB/s
// http_req_blocked...............: avg=10.5ms   min=1µs      med=2µs      max=518.11ms p(95)=4µs      p(99)=514.23ms p(99.99)=517.93ms count=4494
// http_req_connecting............: avg=10.05ms  min=0s       med=0s       max=510.51ms p(95)=0s       p(99)=482.74ms p(99.99)=510.47ms count=4494
// http_req_duration..............: avg=661ms    min=15.25ms  med=642.82ms max=1.16s    p(95)=989.82ms p(99)=1.08s    p(99.99)=1.16s    count=4494
// { expected_response:true }...: avg=661ms    min=15.25ms  med=642.82ms max=1.16s    p(95)=989.82ms p(99)=1.08s    p(99.99)=1.16s    count=4494
// http_req_failed................: 0.00%   ✓ 0          ✗ 4494
// http_req_receiving.............: avg=54.32µs  min=14µs     med=24µs     max=15.78ms  p(95)=124.34µs p(99)=457.13µs p(99.99)=12.57ms  count=4494
// http_req_sending...............: avg=23.35µs  min=6µs      med=12µs     max=9.71ms   p(95)=57µs     p(99)=141.13µs p(99.99)=9.13ms   count=4494
// http_req_tls_handshaking.......: avg=0s       min=0s       med=0s       max=0s       p(95)=0s       p(99)=0s       p(99.99)=0s       count=4494
// http_req_waiting...............: avg=660.92ms min=15.09ms  med=642.79ms max=1.16s    p(95)=989.78ms p(99)=1.08s    p(99.99)=1.16s    count=4494
// http_reqs......................: 4494    148.198601/s
// iteration_duration.............: avg=672.25ms min=332.18ms med=648.69ms max=1.16s    p(95)=990.43ms p(99)=1.09s    p(99.99)=1.16s    count=4494
// iterations.....................: 4494    148.198601/s
// vus............................: 100     min=100      max=100
// vus_max........................: 100     min=100      max=100

// SCENARIO #3: Running in memory 3-nodes cluster using Protobuf for internal communication
// running (0m30.0s), 000/100 VUs, 46832 complete and 0 interrupted iterations
// checks.........................: 100.00% ✓ 46832       ✗ 0
// data_received..................: 5.5 MB  184 kB/s
// data_sent......................: 17 MB   580 kB/s
// http_req_blocked...............: avg=1.01ms   min=1µs     med=2µs     max=556.41ms p(95)=4µs     p(99)=17µs     p(99.99)=555.83ms count=46832
// http_req_connecting............: avg=994.83µs min=0s      med=0s      max=552.33ms p(95)=0s      p(99)=0s       p(99.99)=551.78ms count=46832
// http_req_duration..............: avg=62.38ms  min=12.02ms med=57.58ms max=517.64ms p(95)=93.78ms p(99)=118.64ms p(99.99)=473.48ms count=46832
// { expected_response:true }...: avg=62.38ms  min=12.02ms med=57.58ms max=517.64ms p(95)=93.78ms p(99)=118.64ms p(99.99)=473.48ms count=46832
// http_req_failed................: 0.00%   ✓ 0           ✗ 46832
// http_req_receiving.............: avg=42.82µs  min=12µs    med=23µs    max=14.58ms  p(95)=83µs    p(99)=348.69µs p(99.99)=8.45ms   count=46832
// http_req_sending...............: avg=17.87µs  min=5µs     med=11µs    max=10.03ms  p(95)=35µs    p(99)=132µs    p(99.99)=3.89ms   count=46832
// http_req_tls_handshaking.......: avg=0s       min=0s      med=0s      max=0s       p(95)=0s      p(99)=0s       p(99.99)=0s       count=46832
// http_req_waiting...............: avg=62.32ms  min=11.82ms med=57.52ms max=517.34ms p(95)=93.71ms p(99)=118.6ms  p(99.99)=473.27ms count=46832
// http_reqs......................: 46832   1558.535294/s
// iteration_duration.............: avg=64.04ms  min=28.85ms med=58.34ms max=608.33ms p(95)=94.87ms p(99)=121.6ms  p(99.99)=606.41ms count=46832
// iterations.....................: 46832   1558.535294/s
// vus............................: 100     min=100       max=100
// vus_max........................: 100     min=100       max=100

// SCENARIO #4: Running 3-nodes cluster with disk based raft log using json for encoding
// running (0m30.5s), 000/100 VUs, 1213 complete and 0 interrupted iterations
// checks.........................: 100.00% ✓ 1213      ✗ 0
// data_received..................: 143 kB  4.7 kB/s
// data_sent......................: 451 kB  15 kB/s
// http_req_blocked...............: avg=1.36ms  min=1µs      med=3µs   max=29.93ms p(95)=15.52ms  p(99)=21.88ms  p(99.99)=29.64ms  count=1213
// http_req_connecting............: avg=1.35ms  min=0s       med=0s    max=27.44ms p(95)=15.45ms  p(99)=21.82ms  p(99.99)=27.03ms  count=1213
// http_req_duration..............: avg=2.49s   min=255.06ms med=2.49s max=4.6s    p(95)=3.63s    p(99)=4.44s    p(99.99)=4.6s     count=1213
// { expected_response:true }...: avg=2.49s   min=255.06ms med=2.49s max=4.6s    p(95)=3.63s    p(99)=4.44s    p(99.99)=4.6s     count=1213
// http_req_failed................: 0.00%   ✓ 0         ✗ 1213
// http_req_receiving.............: avg=60.44µs min=24µs     med=53µs  max=390µs   p(95)=114.39µs p(99)=158.63µs p(99.99)=373.27µs count=1213
// http_req_sending...............: avg=62.12µs min=8µs      med=20µs  max=10.68ms p(95)=115µs    p(99)=1.11ms   p(99.99)=9.88ms   count=1213
// http_req_tls_handshaking.......: avg=0s      min=0s       med=0s    max=0s      p(95)=0s       p(99)=0s       p(99.99)=0s       count=1213
// http_req_waiting...............: avg=2.49s   min=254.85ms med=2.49s max=4.6s    p(95)=3.63s    p(99)=4.44s    p(99.99)=4.6s     count=1213
// http_reqs......................: 1213    39.751987/s
// iteration_duration.............: avg=2.49s   min=258.56ms med=2.49s max=4.63s   p(95)=3.63s    p(99)=4.46s    p(99.99)=4.62s    count=1213
// iterations.....................: 1213    39.751987/s
// vus............................: 100     min=100     max=100
// vus_max........................: 100     min=100     max=100
