package blnk

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/typesense/typesense-go/typesense/api"

	"github.com/typesense/typesense-go/typesense"

	"github.com/jerry-enebeli/blnk/model"
)

type TypesenseClient struct {
	Client *typesense.Client
}

type TypesenseError struct {
	Message string `json:"message"`
}

type NotificationPayload struct {
	Table string                 `json:"table"`
	Data  map[string]interface{} `json:"data"` // This structure may need to be adjusted based on your actual data
}

func NewTypesenseClient(apiKey string, hosts []string) *TypesenseClient {
	client := typesense.NewClient(
		typesense.WithServer(hosts[0]),
		typesense.WithAPIKey(apiKey),
		typesense.WithConnectionTimeout(5*time.Second),
		typesense.WithCircuitBreakerMaxRequests(50),
		typesense.WithCircuitBreakerInterval(2*time.Minute),
		typesense.WithCircuitBreakerTimeout(1*time.Minute),
	)
	return &TypesenseClient{Client: client}
}

func EnsureCollectionsExist(client *TypesenseClient, ctx context.Context) error {
	ledger := model.Ledger{}
	balance := model.Balance{}
	transaction := model.Transaction{}
	_, err := client.CreateCollection(ctx, ledger.ToSchema())
	if err != nil {
		logrus.Error(err)
	}
	_, err = client.CreateCollection(ctx, balance.ToSchema())
	if err != nil {
		logrus.Error(err)
	}
	_, err = client.CreateCollection(ctx, transaction.ToSchema())
	if err != nil {
		logrus.Error(err)
	}
	return nil
}

func (t *TypesenseClient) CreateCollection(ctx context.Context, schema *api.CollectionSchema) (*api.CollectionResponse, error) {
	return t.Client.Collections().Create(ctx, schema)
}

func (t *TypesenseClient) Search(ctx context.Context, collection string, searchParams *api.SearchCollectionParams) (*api.SearchResult, error) {
	return t.Client.Collection(collection).Documents().Search(ctx, searchParams)
}

func (t *TypesenseClient) HandleNotification(table string, data map[string]interface{}) error {
	if err := EnsureCollectionsExist(t, context.Background()); err != nil {
		fmt.Println("Failed to ensure collections exist:", err)
		logrus.Error(err)
	}
	metaData, ok := data["meta_data"]
	if ok {
		jsonString, err := json.Marshal(metaData)
		if err != nil {
			return err
		}
		data["meta_data"] = string(jsonString)
	}

	_, err := t.Client.Collection(table).Documents().Upsert(context.Background(), data)
	if err != nil {
		log.Printf("Error indexing document in Typesense: %v", err)
		return err
	}

	fmt.Printf("Successfully indexed document in collection %s\n", table)
	return nil
}
