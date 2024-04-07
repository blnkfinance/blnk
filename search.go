package blnk

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"time"

	"github.com/go-resty/resty/v2"
)

type TypesenseClient struct {
	ApiKey     string
	Hosts      []string
	HttpClient *resty.Client
}

type TypesenseError struct {
	Message string `json:"message"`
}

func NewTypesenseClient(apiKey string, hosts []string) *TypesenseClient {
	client := &TypesenseClient{
		ApiKey: apiKey,
		Hosts:  hosts,
		HttpClient: resty.New().
			SetRetryCount(3).
			SetRetryWaitTime(5 * time.Second).
			SetRetryMaxWaitTime(20 * time.Second).
			AddRetryCondition(
				func(r *resty.Response, err error) bool {
					return r.StatusCode() == http.StatusTooManyRequests || r.StatusCode() == http.StatusServiceUnavailable || err != nil
				},
			),
	}
	return client
}

func (client *TypesenseClient) doRequest(ctx context.Context, method, url string, body interface{}) (*resty.Response, error) {
	req := client.HttpClient.R().
		SetHeader("X-TYPESENSE-API-KEY", client.ApiKey).
		SetBody(body).
		SetContext(ctx)

	var resp *resty.Response
	var err error
	switch method {
	case http.MethodPost:
		resp, err = req.Post(url)
	case http.MethodGet:
		resp, err = req.Get(url)
		// Add other methods as needed
	default:
		return nil, fmt.Errorf("unsupported method: %s", method)
	}

	if err != nil {
		return nil, err
	}

	if resp.IsError() {
		var tsErr TypesenseError
		if err := json.Unmarshal(resp.Body(), &tsErr); err == nil {
			return nil, fmt.Errorf("typesense API error: %s", tsErr.Message)
		}
		return nil, fmt.Errorf("unknown error, status code: %d", resp.StatusCode())
	}

	return resp, nil
}

func (client *TypesenseClient) CreateCollection(ctx context.Context, schema interface{}) (*resty.Response, error) {
	url := fmt.Sprintf("%s/collections", client.Hosts[0])
	return client.doRequest(ctx, http.MethodPost, url, schema)
}

func (client *TypesenseClient) IndexDocument(ctx context.Context, collection string, document interface{}) (*resty.Response, error) {
	url := fmt.Sprintf("%s/collections/%s/documents", client.Hosts[0], collection)
	return client.doRequest(ctx, http.MethodPost, url, document)
}

func (client *TypesenseClient) Search(ctx context.Context, collection string, searchParams map[string]interface{}) (*resty.Response, error) {
	url := fmt.Sprintf("%s/collections/%s/documents/search", client.Hosts[0], collection)
	return client.doRequest(ctx, http.MethodGet, url, searchParams)
}

func structToSchema(name string, obj interface{}, defaultSortingField string) (map[string]interface{}, error) {
	t := reflect.TypeOf(obj)
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("provided object is not a struct")
	}

	fields := make([]map[string]interface{}, 0, t.NumField())

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		jsonTag := field.Tag.Get("json")
		if jsonTag == "" || jsonTag == "-" {
			continue
		}

		// Split json tag on comma to ignore json options like omitempty
		tagParts := strings.Split(jsonTag, ",")
		fieldName := tagParts[0]

		fieldType := field.Type.Kind()
		typesenseType := ""
		switch fieldType {
		case reflect.String:
			typesenseType = "string"
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			typesenseType = "int32" // Typesense uses int32, adjust based on your needs
		case reflect.Float32, reflect.Float64:
			typesenseType = "float" // Simplification, Typesense has float and optional
		default:
			return nil, fmt.Errorf("unsupported field type: %v", fieldType)
		}

		fieldSchema := map[string]interface{}{
			"name": fieldName,
			"type": typesenseType,
		}
		fields = append(fields, fieldSchema)
	}

	schema := map[string]interface{}{
		"name":                  name,
		"fields":                fields,
		"default_sorting_field": defaultSortingField,
	}

	return schema, nil
}
