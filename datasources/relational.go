package datasources

import (
	"context"
	"database/sql"
	"log"

	"github.com/uptrace/bun/dialect/mssqldialect"
	"github.com/uptrace/bun/dialect/pgdialect"

	"github.com/uptrace/bun"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jerry-enebeli/saifu"
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
	_, err := orm.NewCreateTable().Model((*saifu.Ledger)(nil)).Exec(context.Background())

	if err != nil {
		log.Println(err)
	}

	orm.NewCreateTable().Model((*saifu.Balance)(nil)).Exec(context.Background())

	orm.NewCreateTable().Model((*saifu.Transaction)(nil)).Exec(context.Background())

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

func (p relationalDB) CreateLedger(ledger saifu.Ledger) (saifu.Ledger, error) {
	_, err := p.orm.NewInsert().Model(&ledger).Returning("*").Exec(context.Background())
	if err != nil {
		return saifu.Ledger{}, err
	}
	return ledger, nil
}

func (p relationalDB) CreateBalance(balance saifu.Balance) (saifu.Balance, error) {
	_, err := p.orm.NewInsert().Model(&balance).Exec(context.Background())
	if err != nil {
		return saifu.Balance{}, err
	}
	return balance, nil
}

func (p relationalDB) RecordTransaction(transaction saifu.Transaction) (saifu.Transaction, error) {
	_, err := p.orm.NewInsert().Model(&transaction).Exec(context.Background())
	if err != nil {
		return saifu.Transaction{}, err
	}
	return transaction, nil
}

func (p relationalDB) GetLedger(LedgerID string) (saifu.Ledger, error) {
	var ledger saifu.Ledger
	err := p.orm.NewSelect().Model(&ledger).Where("id = ?", LedgerID).Scan(context.Background())
	if err != nil {
		return saifu.Ledger{}, err
	}

	return ledger, nil
}

func (p relationalDB) GetBalance(BalanceID string) (saifu.Balance, error) {
	var balance saifu.Balance

	err := p.orm.NewSelect().Model(&balance).Where("id = ?", BalanceID).Scan(context.Background())
	if err != nil {
		return saifu.Balance{}, err
	}

	return balance, nil
}

func (p relationalDB) GetTransaction(TransactionID string) (saifu.Transaction, error) {
	var transaction saifu.Transaction

	err := p.orm.NewSelect().Model(&transaction).Where("id = ?", TransactionID).Scan(context.Background())
	if err != nil {
		return saifu.Transaction{}, err
	}

	return transaction, nil
}

func (p relationalDB) GetTransactionByRef(reference string) (saifu.Transaction, error) {
	var transaction saifu.Transaction

	err := p.orm.NewSelect().Model(&transaction).Where("reference = ?", reference).Scan(context.Background())
	if err != nil {
		return saifu.Transaction{}, err
	}

	return transaction, nil
}

func (p relationalDB) UpdateBalance(balanceID string, update saifu.BalanceUpdate) (saifu.BalanceUpdate, error) {
	_, err := p.orm.NewUpdate().
		Model(&update).
		Where("id = ?", balanceID).
		Exec(context.Background())

	if err != nil {
		return saifu.BalanceUpdate{}, err
	}

	return update, nil
}
