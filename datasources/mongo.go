package datasources

import (
	"context"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"

	blnk "github.com/jerry-enebeli/blnk"
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

	err = client.Ping(context.Background(), nil)
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

func (m mongoDataSource) CreateLedger(ledger blnk.Ledger) (blnk.Ledger, error) {
	coll := m.getCollection("blnk", "ledgers")

	_, err := coll.InsertOne(context.Background(), ledger)

	if err != nil {
		return blnk.Ledger{}, err
	}

	return ledger, nil
}

func (m mongoDataSource) CreateBalance(balance blnk.Balance) (blnk.Balance, error) {
	coll := m.getCollection("blnk", "balances")

	_, err := coll.InsertOne(context.Background(), balance)

	if err != nil {
		return blnk.Balance{}, err
	}

	return balance, nil
}

func (m mongoDataSource) RecordTransaction(transaction blnk.Transaction) (blnk.Transaction, error) {
	coll := m.getCollection("blnk", "transactions")

	_, err := coll.InsertOne(context.Background(), transaction)

	if err != nil {
		return blnk.Transaction{}, err
	}

	return transaction, nil
}

func (m mongoDataSource) GetLedger(LedgerID string) (blnk.Ledger, error) {
	coll := m.getCollection("blnk", "ledgers")

	var ledger blnk.Ledger

	err := coll.FindOne(context.Background(), bson.M{"id": LedgerID}).Decode(&ledger)
	if err != nil {
		return blnk.Ledger{}, err
	}

	return ledger, nil
}

func (m mongoDataSource) GetBalance(BalanceID string) (blnk.Balance, error) {
	coll := m.getCollection("blnk", "balances")

	var balance blnk.Balance

	err := coll.FindOne(context.Background(), bson.M{"id": BalanceID}).Decode(&balance)
	if err != nil {
		return blnk.Balance{}, err
	}

	return balance, nil
}

func (m mongoDataSource) GetTransaction(TransactionID string) (blnk.Transaction, error) {
	coll := m.getCollection("blnk", "transactions")

	var transaction blnk.Transaction

	err := coll.FindOne(context.Background(), bson.M{"id": TransactionID}).Decode(&transaction)
	if err != nil {
		return blnk.Transaction{}, err
	}

	return transaction, nil
}

func (m mongoDataSource) GetTransactionByRef(reference string) (blnk.Transaction, error) {
	coll := m.getCollection("blnk", "transactions")

	var transaction blnk.Transaction

	err := coll.FindOne(context.Background(), bson.M{"reference": reference}).Decode(&transaction)
	if err != nil {
		return blnk.Transaction{}, err
	}

	return transaction, nil
}

func (m mongoDataSource) UpdateBalance(balanceID string, update blnk.Balance) (blnk.Balance, error) {
	coll := m.getCollection("blnk", "balances")

	err := coll.FindOneAndUpdate(context.Background(), bson.M{"id": balanceID}, bson.M{"$set": update}).Decode(&update)

	if err != nil {
		return blnk.Balance{}, err
	}

	return update, nil
}
