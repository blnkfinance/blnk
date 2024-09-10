/*
Copyright 2024 Blnk Finance Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

import http from "k6/http";
import { uuidv4 } from "https://jslib.k6.io/k6-utils/1.4.0/index.js";
import { check, sleep } from "k6";

export const options = {
  vus: 5,
  duration: "10s",
};

export default function () {
  const url = "http://localhost:5001/transactions";
  const inflightUrl = "http://localhost:5001/transactions/inflight/";

  // Step 1: Create an inflight transaction
  let payload = JSON.stringify({
    amount: 500,
    description: "test inflight transaction",
    precision: 100,
    allow_overdraft: true,
    reference: uuidv4(),
    currency: "USD",
    source: `@world`,
    destination: `@${uuidv4()}`,
    inflight: true,
  });

  let params = {
    headers: {
      "Content-Type": "application/json",
    },
  };

  let response = http.post(url, payload, params);
  check(response, {
    "is status 201": (r) => r.status === 201,
  });

  let transactionId = JSON.parse(response.body).transaction_id;

  // Step 2: Commit the inflight transaction
  payload = JSON.stringify({
    status: "commit",
  });

  response = http.put(`${inflightUrl}${transactionId}`, payload, params);
  check(response, {
    "is status 200": (r) => r.status === 200,
  });

  // Step 3: Void another inflight transaction
  // payload = JSON.stringify({
  //   amount: 700,
  //   description: "test inflight transaction to be voided",
  //   precision: 100,
  //   allow_overdraft: true,
  //   reference: uuidv4(),
  //   currency: "USD",
  //   source: `@world`,
  //   destination: `@${uuidv4()}`,
  //   inflight: true,
  // });

  // response = http.post(url, payload, params);
  // check(response, {
  //   "is status 201": (r) => r.status === 201,
  // });

  // transactionId = JSON.parse(response.body).transaction_id;

  // payload = JSON.stringify({
  //   status: "void",
  // });

  // response = http.put(`${inflightUrl}${transactionId}`, payload, params);
  // check(response, {
  //   "is status 200": (r) => r.status === 200,
  // });
}
