package blnk

import (
	"fmt"
	"strings"

	"github.com/jerry-enebeli/blnk/model"

	"github.com/mitchellh/mapstructure"
)

func processMapping(event map[string]interface{}, mapping map[string]string) map[string]interface{} {
	result := make(map[string]interface{})
	for key, value := range mapping {
		if strings.HasPrefix(value, "{") && strings.HasSuffix(value, "}") {
			path := strings.Trim(value, "{}")
			extractedValue, err := extractValueFromPath(event, path)
			if err == nil {
				result[key] = extractedValue
			}
		} else {
			result[key] = value
		}
	}

	return result
}

func extractValueFromPath(data map[string]interface{}, path string) (interface{}, error) {
	keys := strings.Split(path, ".")
	currentData := data
	for _, key := range keys {
		value, exists := currentData[key]
		if !exists {
			return nil, fmt.Errorf("key %s not found", key)
		}

		switch v := value.(type) {
		case map[string]interface{}:
			currentData = v
		default:
			if key == keys[len(keys)-1] {
				return v, nil
			} else {
				return nil, fmt.Errorf("unexpected data type for key %s", key)
			}
		}
	}
	return nil, fmt.Errorf("path not found")
}

func (l Blnk) CreatEvent(event model.Event) (model.Transaction, error) {
	eventMapper, err := l.GetEventMapperByID(event.MapperID)
	if err != nil {
		return model.Transaction{}, err
	}
	var transaction model.Transaction

	transactionRecord := processMapping(event.Data, eventMapper.MappingInstruction)
	err = mapstructure.Decode(transactionRecord, &transaction)
	if err != nil {
		return model.Transaction{}, err
	}

	transaction.DRCR = event.Drcr
	transaction.BalanceID = event.BalanceID

	transaction, err = l.QueueTransaction(transaction)
	if err != nil {
		return model.Transaction{}, err
	}

	return transaction, nil
}

func (l Blnk) CreateEventMapper(mapper model.EventMapper) (model.EventMapper, error) {
	return l.datasource.CreateEventMapper(mapper)
}

func (l Blnk) GetAllEventMappers() ([]model.EventMapper, error) {
	return l.datasource.GetAllEventMappers()
}

func (l Blnk) GetEventMapperByID(id string) (*model.EventMapper, error) {
	return l.datasource.GetEventMapperByID(id)
}

func (l Blnk) UpdateEventMapper(mapper model.EventMapper) error {
	return l.datasource.UpdateEventMapper(mapper)
}

func (l Blnk) DeleteEventMapperByID(id string) error {
	return l.datasource.DeleteEventMapper(id)
}
