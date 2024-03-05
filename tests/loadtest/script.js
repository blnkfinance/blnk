import http from "k6/http";
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';
import { check, sleep } from "k6";


export const options = {
    vus: 5,
    duration: '30s',
};

export default function () {
    const url = 'http://localhost:5001/transactions';
    const payload = JSON.stringify({
            "amount": 100,
            "allow_over_draft":true,
            "reference": uuidv4(),
            "currency": "NGN",
            "source": "@cash",
            "destination": "@interests"
        }

    );

    const params = {
        headers: {
            'Content-Type': 'application/json',
        },
    };

     let  response =  http.post(url, payload, params);
    check(response, {
        'is status 201': r => r.status === 201,
    })
}