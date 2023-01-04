package datasources

import (
	"context"
	"database/sql"
	"log"

	"github.com/jerry-enebeli/blnk"

	"github.com/uptrace/bun/dialect/mssqldialect"
	"github.com/uptrace/bun/dialect/pgdialect"

	"github.com/uptrace/bun"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

func connectRelational(driver, dns string) *sql.DB {
	db, err := sql.Open(driver, dns)
	if err != nil {
		panic(err)
	}
	err = db.Ping()
	if err != nil {
		log.Printf("%s connection error ❌: %v", driver, err)
		return nil
	}
	return db
}

type relationalDB struct {
	conn *sql.DB
	orm  *bun.DB
}

func createTables(orm *bun.DB) error {
	_, err := orm.NewCreateTable().Model((*blnk.Ledger)(nil)).Exec(context.Background())

	if err != nil {
		log.Println(err)
	}

	orm.NewCreateTable().Model((*blnk.Balance)(nil)).Exec(context.Background())

	orm.NewCreateTable().Model((*blnk.Transaction)(nil)).Exec(context.Background())

	return nil

}

func newRelationalDataSource(db, dns string) DataSource {
	connect := connectRelational(db, dns)

	orm := bun.NewDB(connect, mssqldialect.New())

	if db == "postgres" {
		orm = bun.NewDB(connect, pgdialect.New())
	}

	createTables(orm)

	return &relationalDB{conn: connect, orm: orm}
}

func (p relationalDB) CreateLedger(ledger blnk.Ledger) (blnk.Ledger, error) {
	_, err := p.orm.NewInsert().Model(&ledger).Returning("*").Exec(context.Background())
	if err != nil {
		return blnk.Ledger{}, err
	}
	return ledger, nil
}

func (p relationalDB) CreateBalance(balance blnk.Balance) (blnk.Balance, error) {
	_, err := p.orm.NewInsert().Model(&balance).Exec(context.Background())
	if err != nil {
		return blnk.Balance{}, err
	}
	return balance, nil
}

func (p relationalDB) RecordTransaction(transaction blnk.Transaction) (blnk.Transaction, error) {
	_, err := p.orm.NewInsert().Model(&transaction).Exec(context.Background())
	if err != nil {
		return blnk.Transaction{}, err
	}
	return transaction, nil
}

func (p relationalDB) GetLedger(LedgerID string) (blnk.Ledger, error) {
	var ledger blnk.Ledger
	err := p.orm.NewSelect().Model(&ledger).Where("id = ?", LedgerID).Scan(context.Background())
	if err != nil {
		return blnk.Ledger{}, err
	}

	return ledger, nil
}

func (p relationalDB) GetBalance(BalanceID string) (blnk.Balance, error) {
	var balance blnk.Balance

	err := p.orm.NewSelect().Model(&balance).Where("id = ?", BalanceID).Scan(context.Background())
	if err != nil {
		return blnk.Balance{}, err
	}

	return balance, nil
}

func (p relationalDB) GetTransaction(TransactionID string) (blnk.Transaction, error) {
	var transaction blnk.Transaction

	err := p.orm.NewSelect().Model(&transaction).Where("id = ?", TransactionID).Scan(context.Background())
	if err != nil {
		return blnk.Transaction{}, err
	}

	return transaction, nil
}

func (p relationalDB) GetTransactionByRef(reference string) (blnk.Transaction, error) {
	var transaction blnk.Transaction

	err := p.orm.NewSelect().Model(&transaction).Where("reference = ?", reference).Scan(context.Background())
	if err != nil {
		return blnk.Transaction{}, err
	}

	return transaction, nil
}

func (p relationalDB) UpdateBalance(balanceID string, update blnk.Balance) (blnk.Balance, error) {
	_, err := p.orm.NewUpdate().
		Model(&update).
		Where("id = ?", balanceID).
		Exec(context.Background())

	if err != nil {
		return blnk.Balance{}, err
	}

	return update, nil
}
