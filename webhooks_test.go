/*
Copyright 2024 Blnk Finance Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package blnk

import (
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/jerry-enebeli/blnk/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockConfigFetcher is a mock for the config fetching
type MockConfigFetcher struct {
	mock.Mock
}

func (m *MockConfigFetcher) Fetch() (*config.Configuration, error) {
	args := m.Called()
	return args.Get(0).(*config.Configuration), args.Error(1)
}

func TestSendWebhook(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("an error '%s' occurred when starting miniredis", err)
	}
	defer mr.Close()

	mockConfig := &config.Configuration{
		Redis: config.RedisConfig{
			Dns: mr.Addr(),
		},
		Notification: config.Notification{Webhook: struct {
			Url     string            `json:"url"`
			Headers map[string]string `json:"headers"`
		}(struct {
			Url     string
			Headers map[string]string
		}{Url: "https:localhost:5001/webhook", Headers: nil})},
	}

	config.ConfigStore.Store(mockConfig)
	testData := NewWebhook{
		Event:   "transaction.queued",
		Payload: getTransactionMock(10000, false),
	}

	err = SendWebhook(testData)
	assert.NoError(t, err)

	// Verify that the task was enqueued
	assert.NoError(t, err)
	tasks := mr.Keys()
	t.Log(tasks)
	assert.NoError(t, err)
	assert.NotEmpty(t, tasks)

}
