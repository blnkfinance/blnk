import http from "k6/http";
import { uuidv4 } from "https://jslib.k6.io/k6-utils/1.4.0/index.js";
import { check, sleep } from "k6";

export const options = {
  vus: 5,
  duration: "10s",
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
