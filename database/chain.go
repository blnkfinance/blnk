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

package database

import (
	"context"
	"database/sql"
	"time"

	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
	"go.opentelemetry.io/otel"
)

// chainKey is the partition key of the single global chain. The chain_state
// table is keyed so the chain can be partitioned later without a redesign.
const chainKey = "global"

// ChainPendingTransactions seals the next batch of unchained transactions into
// the global hash chain in a single DB transaction. It locks the one chain_state
// row FOR UPDATE (serializing every chainer instance — a second writer blocks,
// then re-reads the advanced head), scans unchained rows older than cutoff in id
// order, links each onto the head via model.ComputeChainHash, writes their
// chain_* columns, and advances chain_state — all atomically, so a crash mid
// batch loses nothing. Returns the number of rows sealed.
func (d Datasource) ChainPendingTransactions(ctx context.Context, cutoff time.Time, batchSize int) (int, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "ChainPendingTransactions")
	defer span.End()

	tx, err := d.Conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelDefault})
	if err != nil {
		return 0, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to begin chain transaction", err)
	}
	defer func() { _ = tx.Rollback() }()

	var lastSeq int64
	var head string
	if err := tx.QueryRowContext(ctx,
		`SELECT last_seq, head_hash FROM blnk.chain_state WHERE chain_key = $1 FOR UPDATE`,
		chainKey,
	).Scan(&lastSeq, &head); err != nil {
		return 0, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to lock chain state", err)
	}

	rows, err := tx.QueryContext(ctx, `
		SELECT id, transaction_id, COALESCE(source, ''), COALESCE(destination, ''),
		       COALESCE(amount::text, ''), COALESCE(precise_amount::text, ''),
		       COALESCE(currency, ''), COALESCE(status, ''), COALESCE(reference, ''),
		       created_at
		FROM blnk.transactions
		WHERE chain_seq IS NULL AND created_at < $1
		ORDER BY id ASC
		LIMIT $2`, cutoff, batchSize)
	if err != nil {
		return 0, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to scan unchained transactions", err)
	}

	type link struct {
		id       int64
		seq      int64
		prevHash string
		hash     string
	}
	var batch []link
	for rows.Next() {
		var id int64
		var r model.ChainRow
		if err := rows.Scan(&id, &r.TransactionID, &r.Source, &r.Destination,
			&r.Amount, &r.PreciseAmount, &r.Currency, &r.Status, &r.Reference,
			&r.CreatedAt); err != nil {
			_ = rows.Close()
			return 0, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to read unchained transaction", err)
		}
		prev := head
		head = model.ComputeChainHash(prev, r)
		lastSeq++
		batch = append(batch, link{id: id, seq: lastSeq, prevHash: prev, hash: head})
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, apierror.NewAPIError(apierror.ErrInternalServer, "Error iterating unchained transactions", err)
	}
	// The rows cursor must be closed before issuing further statements on this tx.
	_ = rows.Close()

	if len(batch) == 0 {
		// Commit to release the chain_state lock without advancing.
		if err := tx.Commit(); err != nil {
			return 0, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to commit empty chain batch", err)
		}
		return 0, nil
	}

	stmt, err := tx.PrepareContext(ctx,
		`UPDATE blnk.transactions SET chain_seq = $1, chain_prev_hash = $2, chain_hash = $3 WHERE id = $4`)
	if err != nil {
		return 0, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to prepare chain update", err)
	}
	defer func() { _ = stmt.Close() }()
	for _, l := range batch {
		if _, err := stmt.ExecContext(ctx, l.seq, l.prevHash, l.hash, l.id); err != nil {
			return 0, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to write chain link", err)
		}
	}

	if _, err := tx.ExecContext(ctx,
		`UPDATE blnk.chain_state SET last_seq = $1, head_hash = $2, updated_at = timezone('UTC', now()) WHERE chain_key = $3`,
		lastSeq, head, chainKey,
	); err != nil {
		return 0, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to advance chain state", err)
	}

	if err := tx.Commit(); err != nil {
		return 0, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to commit chain batch", err)
	}

	span.AddEvent("Chained transaction batch")
	return len(batch), nil
}

// GetChainState returns the global chain bookmark row.
func (d Datasource) GetChainState(ctx context.Context) (*model.ChainState, error) {
	var s model.ChainState
	err := d.Conn.QueryRowContext(ctx,
		`SELECT chain_key, last_seq, head_hash, genesis_hash, genesis_at, updated_at
		 FROM blnk.chain_state WHERE chain_key = $1`, chainKey,
	).Scan(&s.ChainKey, &s.LastSeq, &s.HeadHash, &s.GenesisHash, &s.GenesisAt, &s.UpdatedAt)
	if err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to get chain state", err)
	}
	return &s, nil
}

// GetChainedTransactionsAfter returns chained transactions in chain order with
// chain_seq > afterSeq, for the verifier to page through from genesis.
func (d Datasource) GetChainedTransactionsAfter(ctx context.Context, afterSeq int64, limit int) ([]model.ChainedTransaction, error) {
	rows, err := d.Conn.QueryContext(ctx, `
		SELECT chain_seq, COALESCE(chain_prev_hash, ''), COALESCE(chain_hash, ''),
		       transaction_id, COALESCE(source, ''), COALESCE(destination, ''),
		       COALESCE(amount::text, ''), COALESCE(precise_amount::text, ''),
		       COALESCE(currency, ''), COALESCE(status, ''), COALESCE(reference, ''),
		       created_at
		FROM blnk.transactions
		WHERE chain_seq IS NOT NULL AND chain_seq > $1
		ORDER BY chain_seq ASC
		LIMIT $2`, afterSeq, limit)
	if err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to query chained transactions", err)
	}
	defer func() { _ = rows.Close() }()

	var out []model.ChainedTransaction
	for rows.Next() {
		var ct model.ChainedTransaction
		if err := rows.Scan(&ct.ChainSeq, &ct.ChainPrevHash, &ct.ChainHash,
			&ct.Row.TransactionID, &ct.Row.Source, &ct.Row.Destination,
			&ct.Row.Amount, &ct.Row.PreciseAmount, &ct.Row.Currency, &ct.Row.Status,
			&ct.Row.Reference, &ct.Row.CreatedAt); err != nil {
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to scan chained transaction", err)
		}
		out = append(out, ct)
	}
	if err := rows.Err(); err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Error iterating chained transactions", err)
	}
	return out, nil
}

// CountUnchainedTransactions returns how many transactions older than cutoff are
// still unchained — the chainer's backlog, for the lag metric.
func (d Datasource) CountUnchainedTransactions(ctx context.Context, cutoff time.Time) (int64, error) {
	var n int64
	if err := d.Conn.QueryRowContext(ctx,
		`SELECT count(*) FROM blnk.transactions WHERE chain_seq IS NULL AND created_at < $1`, cutoff,
	).Scan(&n); err != nil {
		return 0, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to count unchained transactions", err)
	}
	return n, nil
}
