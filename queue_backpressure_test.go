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
	"context"
	"testing"

	"github.com/blnkfinance/blnk/config"
	"github.com/go-redis/redismock/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseRedisMemoryInfo(t *testing.T) {
	raw := `# Memory
used_memory:9000000
used_memory_human:8.58M
maxmemory:10000000
maxmemory_human:9.54M
`
	snap := parseRedisMemoryInfo(raw)
	assert.Equal(t, int64(9000000), snap.usedBytes)
	assert.Equal(t, int64(10000000), snap.maxBytes)

	ratio, ok := snap.usageRatio()
	require.True(t, ok)
	assert.InDelta(t, 0.9, ratio, 0.001)
}

func TestEvaluateBackpressureRejectsHighMemory(t *testing.T) {
	rdb, mock := redismock.NewClientMock()
	mock.ExpectInfo("memory").SetVal("used_memory:9000000\r\nmaxmemory:10000000\r\n")

	q := &Queue{
		redis: rdb,
		config: &config.Configuration{
			Queue: config.QueueConfig{
				EnableBackpressure:        true,
				BackpressureMemoryPercent: 85,
			},
		},
	}

	reject, err := q.evaluateBackpressure(context.Background())
	require.NoError(t, err)
	assert.True(t, reject)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestEvaluateBackpressureAllowsHealthyMemory(t *testing.T) {
	rdb, mock := redismock.NewClientMock()
	mock.ExpectInfo("memory").SetVal("used_memory:5000000\r\nmaxmemory:10000000\r\n")

	q := &Queue{
		redis: rdb,
		config: &config.Configuration{
			Queue: config.QueueConfig{
				EnableBackpressure:        true,
				BackpressureMemoryPercent: 85,
			},
		},
	}

	reject, err := q.evaluateBackpressure(context.Background())
	require.NoError(t, err)
	assert.False(t, reject)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestAssertEnqueueAllowedDisabled(t *testing.T) {
	q := &Queue{
		config: &config.Configuration{
			Queue: config.QueueConfig{EnableBackpressure: false},
		},
	}
	assert.NoError(t, q.assertEnqueueAllowed(context.Background()))
}
