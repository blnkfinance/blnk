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
  duration: "5s",
};

export default function () {
  const url = "http://localhost:5001/transactions";

  let payload = JSON.stringify({
    amount: 30000,
    precision: 100,
    description: "multi sources",
    reference: uuidv4(),
    allow_overdraft: true,
    currency: "USD",
    sources: [
      {
        identifier: `@${uuidv4()}`,
        distribution: "10%",
        narration: "Source 1",
      },
      {
        identifier: `@${uuidv4()}`,
        distribution: "20000",
        narration: "Source 2",
      },
      {
        identifier: `@${uuidv4()}`,
        distribution: "left",
        narration: "Source 3",
      },
    ],
    destination: `@${uuidv4()}`,
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
}
