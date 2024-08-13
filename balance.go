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

var (
	balanceTracer = otel.Tracer("blnk.transactions")
)

func NewBalanceTracker() *model.BalanceTracker {
	return &model.BalanceTracker{
		Balances:    make(map[string]*model.Balance),
		Frequencies: make(map[string]int),
	}
}

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

func (l *Blnk) GetAllBalances(ctx context.Context) ([]model.Balance, error) {
	_, span := balanceTracer.Start(ctx, "GetAllBalances")
	defer span.End()

	balances, err := l.datasource.GetAllBalances()
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	span.AddEvent("All balances retrieved", trace.WithAttributes(attribute.Int("balance.count", len(balances))))
	return balances, nil
}

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
