package test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	blnk "github.com/jerry-enebeli/blnk"
)

func getBalanceMock(credit, debit, balance int64) blnk.Balance {
	return blnk.Balance{CreditBalance: credit, DebitBalance: debit, Balance: balance}
}

func getTransactionMock(amount int64, currency, DRCR string) blnk.Transaction {
	transaction := blnk.Transaction{Amount: amount, Currency: currency, DRCR: DRCR}
	return transaction
}

func TestUpdateBalances(t *testing.T) {

	tests := []struct {
		name        string
		balance     blnk.Balance
		transaction blnk.Transaction
		want        struct {
			Balance       int64
			CreditBalance int64
			DebitBalance  int64
		}
	}{{
		name:        "Credit 1k with starting balance of 0",
		balance:     getBalanceMock(0, 0, 0),
		transaction: getTransactionMock(1000, "NGN", "Credit"),
		want: struct {
			Balance       int64
			CreditBalance int64
			DebitBalance  int64
		}{Balance: 1000, CreditBalance: 1000, DebitBalance: 0},
	},
		{
			name:        "Credit 2k with starting credit balance of 500",
			balance:     getBalanceMock(500, 0, 0),
			transaction: getTransactionMock(2000, "NGN", "Credit"),
			want: struct {
				Balance       int64
				CreditBalance int64
				DebitBalance  int64
			}{Balance: 2500, CreditBalance: 2500, DebitBalance: 0},
		}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			balance := tt.balance
			transaction := tt.transaction

			err := balance.UpdateBalances(&transaction)
			assert.NoError(t, err)
			assert.Equal(t, tt.want.Balance, balance.Balance)
			assert.Equal(t, tt.want.CreditBalance, balance.CreditBalance)
			assert.Equal(t, tt.want.DebitBalance, balance.DebitBalance)
		})
	}

}
