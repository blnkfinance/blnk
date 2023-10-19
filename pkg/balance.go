package pkg

import (
	"fmt"
	"log"
	"math"
	"sync"

	"github.com/jerry-enebeli/blnk"
)

// Max constants for normalization
const (
	maxChangeFrequency   = 10.0
	maxTransactionAmount = 100000
	maxBalance           = 500000
	maxCreditBalance     = 1000000
	maxDebitBalance      = 700000
)

type BalanceTracker struct {
	balances    map[string]blnk.Balance
	frequencies map[string]int
	mutex       sync.Mutex
}

func NewBalanceTracker() *BalanceTracker {
	return &BalanceTracker{
		balances:    make(map[string]blnk.Balance),
		frequencies: make(map[string]int),
	}
}

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

// ApplyFraudScore updates the balance and computes the fraud score
func (l Blnk) ApplyFraudScore(newBalance blnk.Balance, amount int64) float64 {
	bt := l.bt
	bt.mutex.Lock()
	defer bt.mutex.Unlock()

	oldBalance, exists := bt.balances[newBalance.BalanceID]

	if exists && (oldBalance.CreditBalance != newBalance.CreditBalance || oldBalance.DebitBalance != newBalance.DebitBalance) {
		bt.frequencies[newBalance.BalanceID]++
	}

	bt.balances[newBalance.BalanceID] = newBalance

	changeFrequency := float64(bt.frequencies[newBalance.BalanceID])
	transactionAmount := float64(amount)
	currentBalance := float64(newBalance.Balance)
	creditBalance := float64(newBalance.CreditBalance)
	debitBalance := float64(newBalance.DebitBalance)

	fmt.Println(changeFrequency, transactionAmount, currentBalance, creditBalance, debitBalance)
	return ComputeFraudScore(changeFrequency, transactionAmount, currentBalance, creditBalance, debitBalance)
}

// Normalize function to scale values between 0 and 1
func normalize(value, min, max float64) float64 {
	return (value - min) / (max - min)
}

// ComputeFraudScore computes a fraud score based on various parameters
func ComputeFraudScore(changeFrequency, transactionAmount, currentBalance, creditBalance, debitBalance float64) float64 {
	normalizedChangeFrequency := normalize(changeFrequency, 0, maxChangeFrequency)
	normalizedTransactionAmount := normalize(transactionAmount, 0, maxTransactionAmount)
	normalizedCurrentBalance := normalize(currentBalance, 0, maxBalance)
	normalizedCreditBalance := normalize(creditBalance, 0, maxCreditBalance)
	normalizedDebitBalance := normalize(debitBalance, 0, maxDebitBalance)

	//ensure they sum up to 1
	weightChangeFrequency := 0.3
	weightTransactionAmount := 0.3
	weightCurrentBalance := 0.1
	weightCreditBalance := 0.1
	weightDebitBalance := 0.2

	fraudScore := normalizedChangeFrequency*weightChangeFrequency +
		normalizedTransactionAmount*weightTransactionAmount +
		normalizedCurrentBalance*weightCurrentBalance +
		normalizedCreditBalance*weightCreditBalance +
		normalizedDebitBalance*weightDebitBalance

	return math.Max(0, math.Min(fraudScore, 1))
}
