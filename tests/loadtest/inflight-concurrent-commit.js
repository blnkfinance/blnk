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
 * Load Test: Concurrent Inflight Transaction Commits
 *
 * This test creates a single inflight transaction and then simulates multiple users
 * trying to commit the same transaction concurrently. This helps test:
 * 1. Race conditions in inflight transaction handling
 * 2. Proper transaction state management
 * 3. Concurrency control mechanisms
 * 4. Error handling for duplicate commits
 */

import http from "k6/http";
import { uuidv4 } from "https://jslib.k6.io/k6-utils/1.4.0/index.js";
import { check, sleep } from "k6";
import { SharedArray } from "k6/data";

// Alternative approach: Use environment variable to pass transaction ID
// Run with: k6 run -e TRANSACTION_ID=your_id script.js
// Or create the transaction externally and pass it in

// Test configuration with sequential execution
export const options = {
  scenarios: {
    concurrent_commits: {
      executor: "shared-iterations",
      iterations: 100,
      vus: 100,
      exec: "attemptCommit",
      tags: { scenario: "commit" },
    },
  },
};

const BASE_URL = "http://localhost:5001";
const params = {
  headers: {
    "Content-Type": "application/json",
  },
};

// Global variable to store transaction ID (will be set by setup function)
let TRANSACTION_ID = null;

/**
 * Setup function runs once before all scenarios
 * Creates the inflight transaction that will be used by all virtual users
 */
export function setup() {
  console.log("Setting up inflight transaction...");

  const payload = JSON.stringify({
    amount: 1000,
    description: "Concurrent commit test transaction",
    precision: 100,
    allow_overdraft: true,
    skip_queue: true,
    reference: uuidv4(),
    currency: "USD",
    source: "@world",
    destination: `@test_${uuidv4()}`,
    inflight: true,
  });

  const response = http.post(`${BASE_URL}/transactions`, payload, params);

  check(response, {
    "inflight transaction created": (r) => r.status === 201,
  });

  if (response.status === 201) {
    const responseBody = JSON.parse(response.body);
    const transactionId = responseBody.transaction_id;
    console.log("Created inflight transaction:", transactionId);
    return { transactionId: transactionId };
  } else {
    console.error(
      "Failed to create inflight transaction:",
      response.status,
      response.body
    );
    return { transactionId: null };
  }
}

// Removed createInflight() function since we're using setup() instead

/**
 * Multiple virtual users will attempt to commit the same inflight transaction
 * Only one should succeed, while others should receive appropriate error responses
 */
export function attemptCommit(data) {
  const transactionId = data.transactionId;

  if (!transactionId) {
    console.log("No transaction ID available, skipping commit attempt");
    return;
  }

  const commitPayload = JSON.stringify({
    status: "commit",
  });

  const response = http.put(
    `${BASE_URL}/transactions/inflight/${transactionId}`,
    commitPayload,
    params
  );

  // We expect either:
  // - 200 OK (successful commit)
  // - 409 Conflict (transaction already committed)
  // - 404 Not Found (transaction was committed and cleaned up)
  // - 400 Bad Request (invalid transaction state)
  check(response, {
    "commit response valid": (r) => [200, 409, 404, 400].includes(r.status),
    "successful commit": (r) => r.status === 200,
    "already committed": (r) => r.status === 409,
    "transaction not found": (r) => r.status === 404,
  });

  // Log the result for debugging
  if (response.status === 200) {
    console.log(
      `VU ${__VU}: Successfully committed transaction: ${transactionId}`
    );
  } else if (response.status === 409) {
    console.log(`VU ${__VU}: Transaction already committed: ${transactionId}`);
  } else if (response.status === 404) {
    console.log(
      `VU ${__VU}: Transaction not found (may have been cleaned up): ${transactionId}`
    );
  } else {
    console.log(
      `VU ${__VU}: Unexpected response ${response.status}: ${response.body}`
    );
  }
}

/**
 * Teardown function runs once after all scenarios complete
 */
export function teardown(data) {
  console.log("Test completed. Transaction ID was:", data.transactionId);
}
