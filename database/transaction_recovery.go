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
	"encoding/json"
	"fmt"
	"time"

	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func (d Datasource) GetStuckQueuedTransactions(ctx context.Context, threshold time.Duration, batchSize int) ([]*model.Transaction, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "GetStuckQueuedTransactions")
	defer span.End()

	cutoff := time.Now().UTC().Add(-threshold)

	rows, err := d.Conn.QueryContext(ctx, `
		SELECT transaction_id, parent_transaction, source, reference, amount, precise_amount, precision, currency, destination, description, status, created_at, meta_data, scheduled_for, hash
		FROM blnk.transactions t
		WHERE t.status = 'QUEUED'
		  AND t.created_at < $1
		  AND NOT EXISTS (
		      SELECT 1 FROM blnk.transactions child
		      WHERE child.parent_transaction = t.transaction_id
		  )
		ORDER BY t.created_at ASC
		LIMIT $2
	`, cutoff, batchSize)
	if err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve stuck queued transactions", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			logrus.Errorf("Error closing rows: %v", err)
		}
	}()

	var transactions []*model.Transaction
	for rows.Next() {
		txn := &model.Transaction{}
		var metaDataJSON []byte
		var preciseAmountStr string
		err = rows.Scan(
			&txn.TransactionID, &txn.ParentTransaction, &txn.Source, &txn.Reference,
			&txn.Amount, &preciseAmountStr, &txn.Precision,
			&txn.Currency, &txn.Destination, &txn.Description, &txn.Status,
			&txn.CreatedAt, &metaDataJSON, &txn.ScheduledFor, &txn.Hash,
		)
		if err != nil {
			span.RecordError(err)
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to scan stuck queued transaction", err)
		}

		// A single row with malformed metadata (e.g. a legacy array-shaped
		// meta_data) must not abort the whole recovery sweep; skip it and keep
		// recovering the rest.
		if err = json.Unmarshal(metaDataJSON, &txn.MetaData); err != nil {
			logrus.Warnf("skipping stuck transaction %s: unparseable metadata: %v", txn.TransactionID, err)
			continue
		}

		txn.PreciseAmount, err = parseBigInt(preciseAmountStr)
		if err != nil {
			span.RecordError(err)
			return nil, fmt.Errorf("failed to parse precise_amount: %w", err)
		}

		model.ApplyPrecision(txn)

		transactions = append(transactions, txn)
	}

	if err = rows.Err(); err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Error iterating stuck queued transactions", err)
	}

	span.AddEvent("Stuck queued transactions retrieved", trace.WithAttributes(
		attribute.Int("transaction.count", len(transactions)),
	))
	return transactions, nil
}

// GetQueuedTransactionsForCoalescing retrieves QUEUED transactions for the exact same pair that
// are ready to be coalesced because they do not yet have a child transaction.
