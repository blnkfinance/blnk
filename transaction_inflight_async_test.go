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

package blnk

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInflightActionReference(t *testing.T) {
	assert.Equal(t, "txn_1_commit_act_9", inflightActionReference("txn_1", InflightActionCommit, "act_9"))
	assert.Equal(t, "txn_1_void_act_9", inflightActionReference("txn_1", InflightActionVoid, "act_9"))
	// An empty action ID yields no reference, so finalize* falls back to a random one.
	assert.Equal(t, "", inflightActionReference("txn_1", InflightActionCommit, ""))
}

func TestIsTerminalInflightError(t *testing.T) {
	l := &Blnk{}

	terminal := []error{
		errors.New("cannot commit. Transaction already committed"),
		errors.New("transaction has already been voided"),
		errors.New("transaction is not in inflight status"),
		errors.New("cannot commit more than the remaining amount"),
		errors.New(`duplicate key value violates unique constraint "idx_transactions_reference_unique"`),
	}
	for _, err := range terminal {
		assert.Truef(t, l.isTerminalInflightError(err), "expected terminal: %v", err)
	}

	transient := []error{
		errors.New("failed to acquire lock for inflight commit"),
		errors.New("transaction not found"),
		errors.New("connection reset by peer"),
		nil,
	}
	for _, err := range transient {
		assert.Falsef(t, l.isTerminalInflightError(err), "expected transient: %v", err)
	}
}
