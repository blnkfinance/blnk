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
    description: "multi destinations",
    reference: uuidv4(),
    currency: "USD",
    allow_overdraft: true,
    source: `@${uuidv4()}`,
    destinations: [
      {
        identifier: `@${uuidv4()}`,
        distribution: "20%",
        narration: "Destination 1",
      },
      {
        identifier: `@${uuidv4()}`,
        distribution: "10000",
        narration: "Destination 2",
      },
      {
        identifier: `@${uuidv4()}`,
        distribution: "left",
        narration: "Destination 3",
      },
    ],
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
