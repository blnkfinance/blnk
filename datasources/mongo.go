package datasources

import (
	"context"
	"log"
	"time"

	"github.com/joho/godotenv"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

//TODO validate db dns format and ensure it's a mongodb dns

func connect(dns string) *mongo.Client {
	godotenv.Load()
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

func newMongoDataSource(dns string) DataSource {
	return &mongoDataSource{conn: connect(dns)}
}
