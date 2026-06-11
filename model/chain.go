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
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"time"
)

// ChainGenesisHash is the starting head of the transaction hash chain — 64
// zeros. blnk.chain_state.head_hash begins here.
const ChainGenesisHash = "0000000000000000000000000000000000000000000000000000000000000000"

// ChainRow carries the canonical, immutable fields of a transaction that the
// hash chain seals. The chainer and the verifier both build it from the same DB
// columns so a recomputed hash is always reproducible. NULL columns are empty
// strings. Every field here is enforced immutable by the transactions
// immutability trigger, so a sealed row's hash can never change under a normal
// write.
type ChainRow struct {
	TransactionID string
	Source        string
	Destination   string
	Amount        string // NUMERIC rendered as text
	PreciseAmount string // NUMERIC rendered as text (the authoritative ledger amount)
	Currency      string
	Status        string
	Reference     string
	CreatedAt     time.Time
}

// ComputeChainHash returns SHA256(prevHash || canonical fields). It is
// deterministic and the single source of truth for the chain, shared by the
// background chainer and the verifier. It seals the raw immutable fields
// directly; the per-row HashTxn() value is deliberately excluded as it is just a
// function of fields already sealed here.
func ComputeChainHash(prevHash string, r ChainRow) string {
	canonical := strings.Join([]string{
		prevHash,
		r.TransactionID,
		r.Source,
		r.Destination,
		r.Amount,
		r.PreciseAmount,
		r.Currency,
		r.Status,
		r.Reference,
		r.CreatedAt.UTC().Format(time.RFC3339Nano),
	}, "|")
	sum := sha256.Sum256([]byte(canonical))
	return hex.EncodeToString(sum[:])
}

// ChainState mirrors the blnk.chain_state bookmark row.
type ChainState struct {
	ChainKey    string
	LastSeq     int64
	HeadHash    string
	GenesisHash string
	GenesisAt   time.Time
	UpdatedAt   time.Time
}

// ChainedTransaction is a transaction read back in chain order for
// verification: its canonical fields plus the stored chain linkage.
type ChainedTransaction struct {
	Row           ChainRow
	ChainSeq      int64
	ChainPrevHash string
	ChainHash     string
}
