package ledger

type Service interface {
	CreateLedger()
	CreateBalance()
	RecordTransaction()
	GetLedgers()
	GetBalances()
}

type Tools interface {
}
