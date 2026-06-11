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
package model

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestComputeChainHash(t *testing.T) {
	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	row := ChainRow{
		TransactionID: "txn_1", Source: "bln_a", Destination: "bln_b",
		Amount: "100", PreciseAmount: "10000", Currency: "USD", Status: "APPLIED",
		Reference: "ref_1", CreatedAt: at,
	}

	t.Run("deterministic and 64-hex", func(t *testing.T) {
		h1 := ComputeChainHash(ChainGenesisHash, row)
		h2 := ComputeChainHash(ChainGenesisHash, row)
		assert.Equal(t, h1, h2)
		assert.Len(t, h1, 64)
	})

	t.Run("prev hash changes the result", func(t *testing.T) {
		assert.NotEqual(t,
			ComputeChainHash(ChainGenesisHash, row),
			ComputeChainHash("ffff", row),
			"a different head must produce a different hash")
	})

	t.Run("any field change changes the result", func(t *testing.T) {
		base := ComputeChainHash(ChainGenesisHash, row)
		tampered := row
		tampered.Amount = "200"
		assert.NotEqual(t, base, ComputeChainHash(ChainGenesisHash, tampered))
	})

	t.Run("timezone-normalized", func(t *testing.T) {
		other := row
		loc, _ := time.LoadLocation("Africa/Lagos")
		other.CreatedAt = at.In(loc) // same instant, different zone
		assert.Equal(t, ComputeChainHash(ChainGenesisHash, row), ComputeChainHash(ChainGenesisHash, other))
	})
}
