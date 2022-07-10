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
// running (0m33.2s), 000/100 VUs, 652 complete and 0 interrupted iterations
// checks.........................: 100.00% ✓ 652       ✗ 0
// data_received..................: 77 kB   2.3 kB/s
// data_sent......................: 243 kB  7.3 kB/s
// http_req_blocked...............: avg=11.35ms min=1µs      med=3µs   max=87.4ms  p(95)=85.47ms  p(99)=86.99ms  p(99.99)=87.4ms   count=652
// http_req_connecting............: avg=9.7ms   min=0s       med=0s    max=77.03ms p(95)=69.61ms  p(99)=76.29ms  p(99.99)=77.02ms  count=652
// http_req_duration..............: avg=4.82s   min=262.87ms med=5.02s max=6.94s   p(95)=6.51s    p(99)=6.79s    p(99.99)=6.94s    count=652
// { expected_response:true }...: avg=4.82s   min=262.87ms med=5.02s max=6.94s   p(95)=6.51s    p(99)=6.79s    p(99.99)=6.94s    count=652
// http_req_failed................: 0.00%   ✓ 0         ✗ 652
// http_req_receiving.............: avg=60.53µs min=22µs     med=50µs  max=636µs   p(95)=122.44µs p(99)=229.41µs p(99.99)=626.36µs count=652
// http_req_sending...............: avg=44.03µs min=8µs      med=18µs  max=751µs   p(95)=178.34µs p(99)=435.32µs p(99.99)=745.59µs count=652
// http_req_tls_handshaking.......: avg=0s      min=0s       med=0s    max=0s      p(95)=0s       p(99)=0s       p(99.99)=0s       count=652
// http_req_waiting...............: avg=4.82s   min=262.68ms med=5.02s max=6.94s   p(95)=6.51s    p(99)=6.79s    p(99.99)=6.94s    count=652
// http_reqs......................: 652     19.663258/s
// iteration_duration.............: avg=4.84s   min=269.34ms med=5.02s max=6.94s   p(95)=6.51s    p(99)=6.79s    p(99.99)=6.94s    count=652
// iterations.....................: 652     19.663258/s
// vus............................: 4       min=4       max=100
// vus_max........................: 100     min=100     max=100

// SCENARIO #5: Running 3-nodes cluster with disk based raft log and disk based db storage Pebble
// running (0m33.0s), 000/100 VUs, 674 complete and 0 interrupted iterations
// checks.........................: 100.00% ✓ 674       ✗ 0
// data_received..................: 80 kB   2.4 kB/s
// data_sent......................: 251 kB  7.6 kB/s
// http_req_blocked...............: avg=6.49ms  min=1µs      med=3µs   max=62.67ms p(95)=47.82ms  p(99)=61.39ms  p(99.99)=62.66ms  count=674
// http_req_connecting............: avg=6.13ms  min=0s       med=0s    max=56.02ms p(95)=45.04ms  p(99)=53.61ms  p(99.99)=56.02ms  count=674
// http_req_duration..............: avg=4.66s   min=282.41ms med=4.81s max=6.69s   p(95)=6.52s    p(99)=6.63s    p(99.99)=6.69s    count=674
// { expected_response:true }...: avg=4.66s   min=282.41ms med=4.81s max=6.69s   p(95)=6.52s    p(99)=6.63s    p(99.99)=6.69s    count=674
// http_req_failed................: 0.00%   ✓ 0         ✗ 674
// http_req_receiving.............: avg=56.5µs  min=19µs     med=47µs  max=314µs   p(95)=121.05µs p(99)=184.07µs p(99.99)=309.55µs count=674
// http_req_sending...............: avg=54.56µs min=8µs      med=17µs  max=1.42ms  p(95)=237.45µs p(99)=836.11µs p(99.99)=1.41ms   count=674
// http_req_tls_handshaking.......: avg=0s      min=0s       med=0s    max=0s      p(95)=0s       p(99)=0s       p(99.99)=0s       count=674
// http_req_waiting...............: avg=4.66s   min=282.15ms med=4.81s max=6.69s   p(95)=6.52s    p(99)=6.63s    p(99.99)=6.69s    count=674
// http_reqs......................: 674     20.413836/s
// iteration_duration.............: avg=4.67s   min=287.23ms med=4.81s max=6.69s   p(95)=6.53s    p(99)=6.63s    p(99.99)=6.69s    count=674
// iterations.....................: 674     20.413836/s
// vus............................: 2       min=2       max=100
// vus_max........................: 100     min=100     max=100
