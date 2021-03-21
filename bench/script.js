import http from 'k6/http';
import { check } from 'k6';

function generateString(length) {
    let result = '';
    const characters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    const charactersLength = characters.length;
    for (let i = 0; i < length; i++ ) {
       result += characters.charAt(Math.floor(Math.random() * charactersLength));
    }
    return result;
}

export let options = {
    vus: 50,
    duration: '30s',
  };

export default function () {
    const key = generateString(100)
    const value = generateString(1000)

    let res = http.get('http://localhost:8082/v1/' + key);
    check(res, { 'status was 404': (r) => r.status === 404 })

    res = http.get('http://localhost:8082/v1/' + key);
    check(res, { 'status was 404': (r) => r.status === 404 })

    res = http.put('http://localhost:8082/v1/' + key, value);
    check(res, { 'status was 201': (r) => r.status === 201 })

    res = http.get('http://localhost:8082/v1/' + key);
    check(res, { 'status was 200': (r) => r.status === 200 })

    res = http.del('http://localhost:8082/v1/' + key);
    check(res, { 'status was 200': (r) => r.status === 200 })

    res = http.get('http://localhost:8082/v1/' + key);
    check(res, { 'status was 404': (r) => r.status === 404 })
}