package datasources

import (
	"context"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/jerry-enebeli/saifu"

	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

//TODO validate db dns format and ensure it's a mongodb dns

func connectMongo(dns string) *mongo.Client {
	if dns == "" {
		dns = "mongodb://localhost:27017"
	}
	client, _ := mongo.NewClient(options.Client().ApplyURI(dns))

	ctx, _ := context.WithTimeout(context.Background(), 50*time.Second)
	err := client.Connect(ctx)

	if err != nil {
		log.Println("mongodb connection error ❌", err)
		return nil
	}

	err = client.Ping(nil, nil)
	if err != nil {
		log.Println("mongodb connection error ❌", err)
		return nil
	}
	logrus.Println("mongodb  connected ✅")
	return client
}

type mongoDataSource struct {
	conn *mongo.Client
}

func (m mongoDataSource) getCollection(db, collection string) *mongo.Collection {
	return m.conn.Database(db).Collection(collection)
}

func newMongoDataSource(dns string) DataSource {
	return &mongoDataSource{conn: connectMongo(dns)}
}

func (m mongoDataSource) CreateLedger(ledger saifu.Ledger) (saifu.Ledger, error) {
	coll := m.getCollection("saifu", "ledgers")

	_, err := coll.InsertOne(context.Background(), ledger)

	if err != nil {
		return saifu.Ledger{}, err
	}

	return ledger, nil
}

func (m mongoDataSource) CreateBalance(balance saifu.Balance) (saifu.Balance, error) {
	coll := m.getCollection("saifu", "balances")

	_, err := coll.InsertOne(context.Background(), balance)

	if err != nil {
		return saifu.Balance{}, err
	}

	return balance, nil
}

func (m mongoDataSource) RecordTransaction(transaction saifu.Transaction) (saifu.Transaction, error) {
	coll := m.getCollection("saifu", "transactions")

	_, err := coll.InsertOne(context.Background(), transaction)

	if err != nil {
		return saifu.Transaction{}, err
	}

	return transaction, nil
}

func (m mongoDataSource) GetLedger(LedgerID string) (saifu.Ledger, error) {
	coll := m.getCollection("saifu", "ledgers")

	var ledger saifu.Ledger

	err := coll.FindOne(context.Background(), bson.M{"id": LedgerID}).Decode(&ledger)
	if err != nil {
		return saifu.Ledger{}, err
	}

	return ledger, nil
}

func (m mongoDataSource) GetBalance(BalanceID string) (saifu.Balance, error) {
	panic("")
}

func (m mongoDataSource) GetTransaction(TransactionID string) (saifu.Transaction, error) {
	panic("")
}

func (p mongoDataSource) GetTransactionByRef(reference string) (saifu.Transaction, error) {
	panic("")
}

func (p mongoDataSource) UpdateBalance(balanceID string, update saifu.BalanceUpdate) (saifu.BalanceUpdate, error) {
	panic("")
}
