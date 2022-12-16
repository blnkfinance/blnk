package ledger

type Ledger interface {
	CreateLedger()
	CreateBalance()
	RecordTransaction()
	GetLedgers()
	GetBalances()
}

type Tools interface {
}
