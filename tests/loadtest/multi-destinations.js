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
