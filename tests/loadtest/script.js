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
  vus: 10,
  duration: "10s",
};

const randomAmount = Math.floor(Math.random() * (1000 - 100 + 1)) + 100;

export default function () {
  const url = "http://localhost:5001/transactions";
  const payload = JSON.stringify({
    amount: randomAmount,
    description: "test transaction",
    precision: 100,
    allow_overdraft: true,
    reference: uuidv4(),
    currency: "USD",
    source: `@world`,
    destination: `@${uuidv4()}`,
  });

  const params = {
    headers: {
      "Content-Type": "application/json",
    },
  };

  let response = http.post(url, payload, params);
  check(response, {
    "is status 201": (r) => r.status === 201,
  });
}
