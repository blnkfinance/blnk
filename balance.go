package blnk

import (
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

func (l Blnk) checkBalanceMonitors(updatedBalance *model.Balance) {
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
				notification.NotifyError(err)
			}(monitor)

		}
	}

}

func (l Blnk) getOrCreateBalanceByIndicator(indicator string) (*model.Balance, error) {
	balance, err := l.datasource.GetBalanceByIndicator(indicator)
	if err != nil {
		balance = &model.Balance{
			Indicator: indicator,
			LedgerID:  GeneralLedgerID,
		} //TODO refactor
		// Save the new balance to the datasource
		_, err := l.datasource.CreateBalance(*balance)
		if err != nil {
			return nil, err
		}

		balance, err = l.datasource.GetBalanceByIndicator(indicator)
		if err != nil {
			return nil, err
		}
		return balance, nil
	}
	return balance, nil
}

func (l Blnk) CreateBalance(balance model.Balance) (model.Balance, error) {
	return l.datasource.CreateBalance(balance)
}

func (l Blnk) GetBalanceByID(id string, include []string) (*model.Balance, error) {
	return l.datasource.GetBalanceByID(id, include)
}

func (l Blnk) GetAllBalances() ([]model.Balance, error) {
	return l.datasource.GetAllBalances()
}

func (l Blnk) CreateMonitor(monitor model.BalanceMonitor) (model.BalanceMonitor, error) {
	return l.datasource.CreateMonitor(monitor)
}

func (l Blnk) GetMonitorByID(id string) (*model.BalanceMonitor, error) {
	return l.datasource.GetMonitorByID(id)
}

func (l Blnk) GetAllMonitors() ([]model.BalanceMonitor, error) {
	return l.datasource.GetAllMonitors()
}

func (l Blnk) GetBalanceMonitors(balanceId string) ([]model.BalanceMonitor, error) {
	return l.datasource.GetBalanceMonitors(balanceId)
}

func (l Blnk) UpdateMonitor(monitor *model.BalanceMonitor) error {
	return l.datasource.UpdateMonitor(monitor)
}

func (l Blnk) DeleteMonitor(id string) error {
	return l.datasource.DeleteMonitor(id)
}
