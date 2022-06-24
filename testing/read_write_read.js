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
//     summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(95)', 'p(99)', 'p(99.99)', 'count'],
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

    const initialReadRes = http.get(url)
    check(initialReadRes, { 'status should be 404 on initial read': (r) => r.status === 404 });

    const writeRes = http.post(url, value)

    check(writeRes, { 'status should be 200 on write': (r) => r.status === 200 });

    const afterWriteReadRes = http.get(url)
    check(afterWriteReadRes, { 'status should be 200 on after write read': (r) => r.status === 200 });
    check(afterWriteReadRes, { 'read value should be the same that was written': (r) => r.body === value });
}

// SCENARIO #1: Running in memory single-node store
// running (0m30.0s), 000/100 VUs, 221523 complete and 0 interrupted iterations
// checks.........................: 100.00% ✓ 886092       ✗ 0
// data_received..................: 127 MB  4.2 MB/s
// data_sent......................: 125 MB  4.1 MB/s
// http_req_blocked...............: avg=50.45µs min=0s       med=2µs     max=402.2ms  p(95)=3µs     p(99)=8µs      p(99.99)=290.85ms count=664569
// http_req_connecting............: avg=45.08µs min=0s       med=0s      max=375.67ms p(95)=0s      p(99)=0s       p(99.99)=290.76ms count=664569
// http_req_duration..............: avg=3.62ms  min=73µs     med=1.98ms  max=354.54ms p(95)=12.55ms p(99)=22.7ms   p(99.99)=87.54ms  count=664569
// { expected_response:true }...: avg=3.63ms  min=73µs     med=1.99ms  max=182.19ms p(95)=12.58ms p(99)=22.64ms  p(99.99)=85.18ms  count=443046
// http_req_failed................: 33.33%  ✓ 221523       ✗ 443046
// http_req_receiving.............: avg=76.13µs min=6µs      med=18µs    max=93.09ms  p(95)=106µs   p(99)=609.31µs p(99.99)=33.44ms  count=664569
// http_req_sending...............: avg=29.04µs min=3µs      med=9µs     max=91.03ms  p(95)=27µs    p(99)=193µs    p(99.99)=19.56ms  count=664569
// http_req_tls_handshaking.......: avg=0s      min=0s       med=0s      max=0s       p(95)=0s      p(99)=0s       p(99.99)=0s       count=664569
// http_req_waiting...............: avg=3.51ms  min=49µs     med=1.92ms  max=354.43ms p(95)=12.28ms p(99)=22.07ms  p(99.99)=85.03ms  count=664569
// http_reqs......................: 664569  22143.305389/s
// iteration_duration.............: avg=13.5ms  min=806.19µs med=10.32ms max=419.13ms p(95)=33.28ms p(99)=54.32ms  p(99.99)=411.23ms count=221523
// iterations.....................: 221523  7381.101796/s
// vus............................: 100     min=100        max=100
// vus_max........................: 100     min=100        max=100

// SCENARIO #2: First distributed raft implementation
// running (0m30.6s), 000/100 VUs, 1302 complete and 0 interrupted iterations
//
// checks.........................: 100.00% ✓ 5208       ✗ 0
// data_received..................: 766 kB  25 kB/s
// data_sent......................: 732 kB  24 kB/s
// http_req_blocked...............: avg=42.14µs  min=1µs     med=2µs     max=5.58ms p(90)=3µs   p(95)=6µs
// http_req_connecting............: avg=17.12µs  min=0s      med=0s      max=1.79ms p(90)=0s    p(95)=0s
// http_req_duration..............: avg=781.94ms min=135µs   med=5.35ms  max=2.68s  p(90)=2.46s p(95)=2.59s
// { expected_response:true }...: avg=1.17s    min=171µs   med=47.67ms max=2.68s  p(90)=2.49s p(95)=2.59s
// http_req_failed................: 33.33%  ✓ 1302       ✗ 2604
// http_req_receiving.............: avg=40.02µs  min=11µs    med=21µs    max=6.24ms p(90)=43µs  p(95)=101.75µs
// http_req_sending...............: avg=20.87µs  min=5µs     med=10µs    max=1.56ms p(90)=26µs  p(95)=77µs
// http_req_tls_handshaking.......: avg=0s       min=0s      med=0s      max=0s     p(90)=0s    p(95)=0s
// http_req_waiting...............: avg=781.88ms min=103µs   med=5.28ms  max=2.68s  p(90)=2.46s p(95)=2.59s
// http_reqs......................: 3906    127.799094/s
// iteration_duration.............: avg=2.34s    min=86.04ms med=2.29s   max=2.69s  p(90)=2.6s  p(95)=2.68s
// iterations.....................: 1302    42.599698/s
// vus............................: 100     min=100      max=100
// vus_max........................: 100     min=100      max=100

// SCENARIO #3: Second raft implementation using JSON for internal encoding
// running (0m30.3s), 000/100 VUs, 5265 complete and 0 interrupted iterations
// checks.........................: 100.00% ✓ 21060      ✗ 0
// data_received..................: 3.0 MB  100 kB/s
// data_sent......................: 3.0 MB  98 kB/s
// http_req_blocked...............: avg=3.13ms   min=0s       med=2µs      max=588.47ms p(95)=3µs      p(99)=28µs     p(99.99)=587.32ms count=15795
// http_req_connecting............: avg=2.75ms   min=0s       med=0s       max=568.84ms p(95)=0s       p(99)=0s       p(99.99)=562.48ms count=15795
// http_req_duration..............: avg=187.61ms min=82µs     med=1.35ms   max=845.31ms p(95)=688.39ms p(99)=810.88ms p(99.99)=843.77ms count=15795
// { expected_response:true }...: avg=280.12ms min=93µs     med=10.1ms   max=845.31ms p(95)=730.74ms p(99)=815.42ms p(99.99)=843.99ms count=10530
// http_req_failed................: 33.33%  ✓ 5265       ✗ 10530
// http_req_receiving.............: avg=30.04µs  min=7µs      med=18µs     max=6.94ms   p(95)=58.29µs  p(99)=232.11µs p(99.99)=5.99ms   count=15795
// http_req_sending...............: avg=13.02µs  min=4µs      med=8µs      max=6.17ms   p(95)=28µs     p(99)=113.05µs p(99.99)=1.92ms   count=15795
// http_req_tls_handshaking.......: avg=0s       min=0s       med=0s       max=0s       p(95)=0s       p(99)=0s       p(99.99)=0s       count=15795
// http_req_waiting...............: avg=187.57ms min=64µs     med=1.31ms   max=845.27ms p(95)=688.34ms p(99)=810.84ms p(99.99)=843.71ms count=15795
// http_reqs......................: 15795   521.367381/s
// iteration_duration.............: avg=573.03ms min=292.99ms med=571.47ms max=908.48ms p(95)=795.39ms p(99)=837.21ms p(99.99)=907.71ms count=5265
// iterations.....................: 5265    173.789127/s
// vus............................: 100     min=100      max=100
// vus_max........................: 100     min=100      max=100

// SCENARIO #4: Second raft implementation using Protobuf for internal encoding
// running (0m30.0s), 000/100 VUs, 47214 complete and 0 interrupted iterations
// checks.........................: 100.00% ✓ 188856      ✗ 0
// data_received..................: 27 MB   901 kB/s
// data_sent......................: 27 MB   883 kB/s
// http_req_blocked...............: avg=355.59µs min=0s      med=2µs     max=587.19ms p(95)=3µs     p(99)=13µs     p(99.99)=584.7ms  count=141642
// http_req_connecting............: avg=311.73µs min=0s      med=0s      max=551.58ms p(95)=0s      p(99)=0s       p(99.99)=529.67ms count=141642
// http_req_duration..............: avg=20.55ms  min=74µs    med=1.29ms  max=529.18ms p(95)=70.61ms p(99)=92.52ms  p(99.99)=244.22ms count=141642
// { expected_response:true }...: avg=30.36ms  min=85µs    med=23.11ms max=492.86ms p(95)=74.77ms p(99)=96.21ms  p(99.99)=121.02ms count=94428
// http_req_failed................: 33.33%  ✓ 47214       ✗ 94428
// http_req_receiving.............: avg=33.7µs   min=8µs     med=18µs    max=16.32ms  p(95)=62µs    p(99)=257µs    p(99.99)=9.75ms   count=141642
// http_req_sending...............: avg=13.95µs  min=3µs     med=8µs     max=11.13ms  p(95)=26µs    p(99)=119µs    p(99.99)=4.41ms   count=141642
// http_req_tls_handshaking.......: avg=0s       min=0s      med=0s      max=0s       p(95)=0s      p(99)=0s       p(99.99)=0s       count=141642
// http_req_waiting...............: avg=20.51ms  min=57µs    med=1.24ms  max=528.88ms p(95)=70.55ms p(99)=92.48ms  p(99.99)=244.14ms count=141642
// http_reqs......................: 141642  4715.794021/s
// iteration_duration.............: avg=63.57ms  min=34.12ms med=58.93ms max=700.73ms p(95)=87.49ms p(99)=107.41ms p(99.99)=689.36ms count=47214
// iterations.....................: 47214   1571.93134/s
// vus............................: 100     min=100       max=100
// vus_max........................: 100     min=100       max=100
