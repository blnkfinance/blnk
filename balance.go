package blnk

import (
	"context"
	"fmt"

	"github.com/jerry-enebeli/blnk/internal/notification"

	"github.com/jerry-enebeli/blnk/model"
)

func NewBalanceTracker() *model.BalanceTracker {
	return &model.BalanceTracker{
		Balances:    make(map[string]*model.Balance),
		Frequencies: make(map[string]int),
	}
}

func (l *Blnk) checkBalanceMonitors(updatedBalance *model.Balance) {
	// Fetch monitors for this balance using datasource
	monitors, _ := l.datasource.GetBalanceMonitors(updatedBalance.BalanceID)
	// Check each monitor's condition
	for _, monitor := range monitors {
		if monitor.CheckCondition(updatedBalance) {
			fmt.Printf("Condition met for balance: %s\n", monitor.MonitorID)
			go func(monitor model.BalanceMonitor) {
				err := SendWebhook(NewWebhook{
					Event:   "balance.monitor",
					Payload: monitor,
				})
				if err != nil {
					notification.NotifyError(err)
					return
				}

			}(monitor)

		}
	}

}

func (l *Blnk) getOrCreateBalanceByIndicator(indicator, currency string) (*model.Balance, error) {
	balance, err := l.datasource.GetBalanceByIndicator(indicator, currency)
	if err != nil {
		balance = &model.Balance{
			Indicator: indicator,
			LedgerID:  GeneralLedgerID,
			Currency:  currency,
		}
		_, err := l.CreateBalance(*balance)
		if err != nil {
			return nil, err
		}
		balance, err = l.datasource.GetBalanceByIndicator(indicator, currency)
		if err != nil {
			return nil, err
		}
		return balance, nil
	}
	return balance, nil
}

func (l *Blnk) postBalanceActions(_ context.Context, balance *model.Balance) {
	go func() {
		err := l.queue.queueIndexData(balance.BalanceID, "balances", balance)
		if err != nil {
			notification.NotifyError(err)
		}
		err = SendWebhook(NewWebhook{
			Event:   "balance.created",
			Payload: balance,
		})
		if err != nil {
			notification.NotifyError(err)
		}
	}()
}

func (l *Blnk) CreateBalance(balance model.Balance) (model.Balance, error) {
	balance, err := l.datasource.CreateBalance(balance)
	if err != nil {
		return model.Balance{}, err
	}
	l.postBalanceActions(context.Background(), &balance)
	return balance, nil
}

func (l *Blnk) GetBalanceByID(id string, include []string) (*model.Balance, error) {
	return l.datasource.GetBalanceByID(id, include)
}

func (l *Blnk) GetAllBalances() ([]model.Balance, error) {
	return l.datasource.GetAllBalances()
}

func (l *Blnk) CreateMonitor(monitor model.BalanceMonitor) (model.BalanceMonitor, error) {
	amount := int64(monitor.Condition.Value * monitor.Condition.Precision) //apply precision to value
	amountBigInt := model.Int64ToBigInt(amount)
	monitor.Condition.PreciseValue = amountBigInt
	return l.datasource.CreateMonitor(monitor)
}

func (l *Blnk) GetMonitorByID(id string) (*model.BalanceMonitor, error) {
	return l.datasource.GetMonitorByID(id)
}

func (l *Blnk) GetAllMonitors() ([]model.BalanceMonitor, error) {
	return l.datasource.GetAllMonitors()
}

func (l *Blnk) GetBalanceMonitors(balanceId string) ([]model.BalanceMonitor, error) {
	return l.datasource.GetBalanceMonitors(balanceId)
}

func (l *Blnk) UpdateMonitor(monitor *model.BalanceMonitor) error {
	return l.datasource.UpdateMonitor(monitor)
}

func (l *Blnk) DeleteMonitor(id string) error {
	return l.datasource.DeleteMonitor(id)
}
