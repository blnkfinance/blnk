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

/**
 * Load Test: Bulk Transaction Processing
 *
 * This test simulates high-concurrency(multiple virtual users) bulk transaction processing by creating
 * multiple transactions in atomic batches. Each batch contains a series of
 * credit and debit transactions for a randomly generated account.
 */

import http from "k6/http";
import { uuidv4 } from "https://jslib.k6.io/k6-utils/1.4.0/index.js";
import { check, sleep } from "k6";

// Store the batch ID globally for potential use in other test stages
let batchId;

// Test configuration: Run 100 virtual users simultaneously for 1 second
export const options = {
  scenarios: {
    create_bulk: {
      executor: "constant-vus",
      vus: 1000,
      duration: "1s",
      exec: "createBulk",
    },
  },
};

const BASE_URL = "http://localhost:5001";
const params = {
  headers: {
    "Content-Type": "application/json",
  },
};

/**
 * Main test function that creates a bulk transaction batch.
 * Each batch consists of 4 transactions in the following sequence:
 * 1. Credit sourceAccount from @world1 (initial funding)
 * 2. Debit sourceAccount to @world (spend the funds)
 * 3. Credit sourceAccount from @world with double amount
 * 4. Debit sourceAccount to @world with double amount
 *
 * The transactions are atomic, meaning they either all succeed or all fail.
 * Each virtual user creates a unique account and processes its own batch.
 */
export function createBulk() {
  // Generate random amount between 100 and 1000
  const randomAmount = Math.floor(Math.random() * (1000 - 100 + 1)) + 100;
  // Create a unique account identifier for this test run
  const sourceAccount = "@test_account_" + uuidv4();

  // Create the bulk transaction payload with 4 related transactions
  const bulkPayload = JSON.stringify({
    atomic: true, // Ensures all transactions in the batch succeed or fail together
    transactions: [
      {
        amount: randomAmount,
        precision: 100,
        reference: uuidv4(),
        description: "Credit transaction",
        currency: "NGN",
        source: "@world1",
        allow_overdraft: true,
        destination: sourceAccount,
      },
      {
        amount: randomAmount,
        precision: 100,
        reference: uuidv4(),
        description: "Debit transaction",
        currency: "NGN",
        source: sourceAccount,
        allow_overdraft: true,
        destination: "@world",
      },
      {
        amount: randomAmount * 2,
        precision: 100,
        reference: uuidv4(),
        description: "Credit transaction 2",
        currency: "NGN",
        source: "@world",
        allow_overdraft: true,
        destination: sourceAccount,
      },
      {
        amount: randomAmount * 2,
        precision: 100,
        reference: uuidv4(),
        description: "Debit transaction 2",
        currency: "NGN",
        source: sourceAccount,
        allow_overdraft: true,
        destination: "@world",
      },
    ],
  });

  // Send the bulk transaction request
  let bulkResponse = http.post(
    `${BASE_URL}/transactions/bulk`,
    bulkPayload,
    params
  );

  // Verify the response status is 201 (Created)
  check(bulkResponse, {
    "bulk transaction created": (r) => r.status === 201,
  });

  // If successful, log the response and batch ID for debugging
  if (bulkResponse.status === 201) {
    const responseBody = JSON.parse(bulkResponse.body);
    batchId = responseBody.batch_id;
    console.log("Bulk Response:", JSON.stringify(responseBody, null, 2));
    console.log("Created batch ID:", batchId);
  }
  // Wait 1 second before the next iteration
  sleep(1);
}
