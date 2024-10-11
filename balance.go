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
	"context"
	"fmt"

	"github.com/jerry-enebeli/blnk/internal/notification"
	"github.com/jerry-enebeli/blnk/model"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// balanceTracer is an OpenTelemetry tracer for tracking balance-related transactions.
var (
	balanceTracer = otel.Tracer("blnk.transactions")
)

// NewBalanceTracker creates a new BalanceTracker instance.
// It initializes the Balances and Frequencies maps.
//
// Returns:
// - *model.BalanceTracker: A pointer to the newly created BalanceTracker instance.
func NewBalanceTracker() *model.BalanceTracker {
	return &model.BalanceTracker{
		Balances:    make(map[string]*model.Balance),
		Frequencies: make(map[string]int),
	}
}

// checkBalanceMonitors checks the balance monitors for a given updated balance.
// It starts a tracing span, fetches the monitors, and checks each monitor's condition.
// If a condition is met, it sends a webhook notification.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - updatedBalance *model.Balance: A pointer to the updated Balance model.
func (l *Blnk) checkBalanceMonitors(ctx context.Context, updatedBalance *model.Balance) {
	_, span := balanceTracer.Start(ctx, "CheckBalanceMonitors")
	defer span.End()

	// Fetch monitors for this balance using datasource
	monitors, err := l.datasource.GetBalanceMonitors(updatedBalance.BalanceID)
	if err != nil {
		span.RecordError(err)
		notification.NotifyError(err)
		return
	}

	// Check each monitor's condition
	for _, monitor := range monitors {
		if monitor.CheckCondition(updatedBalance) {
			span.AddEvent(fmt.Sprintf("Condition met for balance: %s", monitor.MonitorID))
			go func(monitor model.BalanceMonitor) {
				err := SendWebhook(NewWebhook{
					Event:   "balance.monitor",
					Payload: monitor,
				})
				if err != nil {
					notification.NotifyError(err)
				}
			}(monitor)
		}
	}
}

// getOrCreateBalanceByIndicator retrieves a balance by its indicator and currency.
// If the balance does not exist, it creates a new one.
// It starts a tracing span, fetches or creates the balance, and records relevant events.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - indicator string: The indicator for the balance.
// - currency string: The currency for the balance.
//
// Returns:
// - *model.Balance: A pointer to the Balance model.
// - error: An error if the balance could not be retrieved or created.
func (l *Blnk) getOrCreateBalanceByIndicator(ctx context.Context, indicator, currency string) (*model.Balance, error) {
	ctx, span := balanceTracer.Start(ctx, "GetOrCreateBalanceByIndicator")
	defer span.End()

	balance, err := l.datasource.GetBalanceByIndicator(indicator, currency)
	if err != nil {
		span.AddEvent("Creating new balance")
		balance = &model.Balance{
			Indicator: indicator,
			LedgerID:  GeneralLedgerID,
			Currency:  currency,
		}
		_, err := l.CreateBalance(ctx, *balance)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		balance, err = l.datasource.GetBalanceByIndicator(indicator, currency)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		span.AddEvent("New balance created", trace.WithAttributes(attribute.String("balance.id", balance.BalanceID)))
		return balance, nil
	}
	span.AddEvent("Balance found", trace.WithAttributes(attribute.String("balance.id", balance.BalanceID)))
	return balance, nil
}

// postBalanceActions performs some actions after a balance has been created.
// It starts a tracing span, sends the balance to the search index queue, and sends a webhook notification.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - balance *model.Balance: A pointer to the newly created Balance model.
func (l *Blnk) postBalanceActions(ctx context.Context, balance *model.Balance) {
	_, span := balanceTracer.Start(ctx, "PostBalanceActions")
	defer span.End()

	go func() {
		err := l.queue.queueIndexData(balance.BalanceID, "balances", balance)
		if err != nil {
			span.RecordError(err)
			notification.NotifyError(err)
		}
		err = SendWebhook(NewWebhook{
			Event:   "balance.created",
			Payload: balance,
		})
		if err != nil {
			span.RecordError(err)
			notification.NotifyError(err)
		}
		span.AddEvent("Post balance actions completed", trace.WithAttributes(attribute.String("balance.id", balance.BalanceID)))
	}()
}

// CreateBalance creates a new balance.
// It starts a tracing span, creates the balance, and performs post-creation actions.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - balance model.Balance: The Balance model to be created.
//
// Returns:
// - model.Balance: The created Balance model.
// - error: An error if the balance could not be created.
func (l *Blnk) CreateBalance(ctx context.Context, balance model.Balance) (model.Balance, error) {
	ctx, span := balanceTracer.Start(ctx, "CreateBalance")
	defer span.End()

	balance, err := l.datasource.CreateBalance(balance)
	if err != nil {
		span.RecordError(err)
		return model.Balance{}, err
	}
	l.postBalanceActions(ctx, &balance)
	span.AddEvent("Balance created", trace.WithAttributes(attribute.String("balance.id", balance.BalanceID)))
	return balance, nil
}

// GetBalanceByID retrieves a balance by its ID.
// It starts a tracing span, fetches the balance, and records relevant events.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - id string: The ID of the balance to retrieve.
// - include []string: A slice of strings specifying additional data to include.
//
// Returns:
// - *model.Balance: A pointer to the Balance model if found.
// - error: An error if the balance could not be retrieved.
func (l *Blnk) GetBalanceByID(ctx context.Context, id string, include []string) (*model.Balance, error) {
	_, span := balanceTracer.Start(ctx, "GetBalanceByID")
	defer span.End()

	balance, err := l.datasource.GetBalanceByID(id, include)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	span.AddEvent("Balance retrieved", trace.WithAttributes(attribute.String("balance.id", id)))
	return balance, nil
}

// GetAllBalances retrieves all balances.
// It starts a tracing span, fetches all balances, and records relevant events.
//
// Parameters:
// - ctx context.Context: The context for the operation.
//
// Returns:
// - []model.Balance: A slice of Balance models.
// - error: An error if the balances could not be retrieved.
func (l *Blnk) GetAllBalances(ctx context.Context, limit, offset int) ([]model.Balance, error) {
	_, span := balanceTracer.Start(ctx, "GetAllBalances")
	defer span.End()

	balances, err := l.datasource.GetAllBalances(limit, offset)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	span.AddEvent("All balances retrieved", trace.WithAttributes(attribute.Int("balance.count", len(balances))))
	return balances, nil
}

// CreateMonitor creates a new balance monitor.
// It starts a tracing span, applies precision to the monitor's condition value, and creates the monitor.
// It records relevant events and errors.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - monitor model.BalanceMonitor: The BalanceMonitor model to be created.
//
// Returns:
// - model.BalanceMonitor: The created BalanceMonitor model.
// - error: An error if the monitor could not be created.
func (l *Blnk) CreateMonitor(ctx context.Context, monitor model.BalanceMonitor) (model.BalanceMonitor, error) {
	_, span := balanceTracer.Start(ctx, "CreateMonitor")
	defer span.End()

	amount := int64(monitor.Condition.Value * monitor.Condition.Precision) // apply precision to value
	amountBigInt := model.Int64ToBigInt(amount)
	monitor.Condition.PreciseValue = amountBigInt
	monitor, err := l.datasource.CreateMonitor(monitor)
	if err != nil {
		span.RecordError(err)
		return model.BalanceMonitor{}, err
	}
	span.AddEvent("Monitor created", trace.WithAttributes(attribute.String("monitor.id", monitor.MonitorID)))
	return monitor, nil
}

// GetMonitorByID retrieves a balance monitor by its ID.
// It starts a tracing span, fetches the monitor, and records relevant events.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - id string: The ID of the monitor to retrieve.
//
// Returns:
// - *model.BalanceMonitor: A pointer to the BalanceMonitor model if found.
// - error: An error if the monitor could not be retrieved.
func (l *Blnk) GetMonitorByID(ctx context.Context, id string) (*model.BalanceMonitor, error) {
	_, span := balanceTracer.Start(ctx, "GetMonitorByID")
	defer span.End()

	monitor, err := l.datasource.GetMonitorByID(id)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	span.AddEvent("Monitor retrieved", trace.WithAttributes(attribute.String("monitor.id", id)))
	return monitor, nil
}

// GetAllMonitors retrieves all balance monitors.
// It starts a tracing span, fetches all monitors, and records relevant events.
//
// Parameters:
// - ctx context.Context: The context for the operation.
//
// Returns:
// - []model.BalanceMonitor: A slice of BalanceMonitor models.
// - error: An error if the monitors could not be retrieved.
func (l *Blnk) GetAllMonitors(ctx context.Context) ([]model.BalanceMonitor, error) {
	_, span := balanceTracer.Start(ctx, "GetAllMonitors")
	defer span.End()

	monitors, err := l.datasource.GetAllMonitors()
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	span.AddEvent("All monitors retrieved", trace.WithAttributes(attribute.Int("monitor.count", len(monitors))))
	return monitors, nil
}

// GetBalanceMonitors retrieves all monitors for a given balance ID.
// It starts a tracing span, fetches the monitors, and records relevant events.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - balanceID string: The ID of the balance for which to retrieve monitors.
//
// Returns:
// - []model.BalanceMonitor: A slice of BalanceMonitor models.
// - error: An error if the monitors could not be retrieved.
func (l *Blnk) GetBalanceMonitors(ctx context.Context, balanceID string) ([]model.BalanceMonitor, error) {
	_, span := balanceTracer.Start(ctx, "GetBalanceMonitors")
	defer span.End()

	monitors, err := l.datasource.GetBalanceMonitors(balanceID)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	span.AddEvent("Monitors retrieved for balance", trace.WithAttributes(attribute.String("balance.id", balanceID), attribute.Int("monitor.count", len(monitors))))
	return monitors, nil
}

// UpdateMonitor updates an existing balance monitor.
// It starts a tracing span, updates the monitor, and records relevant events and errors.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - monitor *model.BalanceMonitor: A pointer to the BalanceMonitor model to be updated.
//
// Returns:
// - error: An error if the monitor could not be updated.
func (l *Blnk) UpdateMonitor(ctx context.Context, monitor *model.BalanceMonitor) error {
	_, span := balanceTracer.Start(ctx, "UpdateMonitor")
	defer span.End()

	err := l.datasource.UpdateMonitor(monitor)
	if err != nil {
		span.RecordError(err)
		return err
	}
	span.AddEvent("Monitor updated", trace.WithAttributes(attribute.String("monitor.id", monitor.MonitorID)))
	return nil
}

// DeleteMonitor deletes a balance monitor by its ID.
// It starts a tracing span, deletes the monitor, and records relevant events and errors.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - id string: The ID of the monitor to delete.
//
// Returns:
// - error: An error if the monitor could not be deleted.
func (l *Blnk) DeleteMonitor(ctx context.Context, id string) error {
	_, span := balanceTracer.Start(ctx, "DeleteMonitor")
	defer span.End()

	err := l.datasource.DeleteMonitor(id)
	if err != nil {
		span.RecordError(err)
		return err
	}
	span.AddEvent("Monitor deleted", trace.WithAttributes(attribute.String("monitor.id", id)))
	return nil
}
