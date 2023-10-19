package pkg

import (
	"fmt"
	"log"

	"github.com/jerry-enebeli/blnk"
)

func (l Blnk) checkBalanceMonitors(updatedBalance *blnk.Balance) {
	// Fetch monitors for this balance using datasource
	monitors, err := l.datasource.GetBalanceMonitors(updatedBalance.BalanceID)
	if err != nil {
		log.Println("Error: Unable to get balance monitors")
	}

	// Check each monitor's condition
	for _, monitor := range monitors {
		if monitor.CheckCondition(updatedBalance) {
			fmt.Printf("Condition met for balance: %s\n", monitor.MonitorID)
			// Here, you can also add logic to send notifications or take other actions.
		}
	}

}

func (l Blnk) CreateBalance(balance blnk.Balance) (blnk.Balance, error) {
	return l.datasource.CreateBalance(balance)
}

func (l Blnk) GetBalanceByID(id string, include []string) (*blnk.Balance, error) {
	return l.datasource.GetBalanceByID(id, include)
}

func (l Blnk) GetAllBalances() ([]blnk.Balance, error) {
	return l.datasource.GetAllBalances()
}

func (l Blnk) CreateMonitor(monitor blnk.BalanceMonitor) (blnk.BalanceMonitor, error) {
	return l.datasource.CreateMonitor(monitor)
}

func (l Blnk) GetMonitorByID(id string) (*blnk.BalanceMonitor, error) {
	return l.datasource.GetMonitorByID(id)
}

func (l Blnk) GetAllMonitors() ([]blnk.BalanceMonitor, error) {
	return l.datasource.GetAllMonitors()
}

func (l Blnk) UpdateMonitor(monitor *blnk.BalanceMonitor) error {
	return l.datasource.UpdateMonitor(monitor)
}

func (l Blnk) DeleteMonitor(id string) error {
	return l.datasource.DeleteMonitor(id)
}
